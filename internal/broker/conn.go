package broker

import (
	"errors"
	"io"
	"log"
	"net"
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/topic"
)

// clientConn 单客户端连接处理，全部状态挂在连接对象上，连接间互不污染。
type clientConn struct {
	broker  *Broker
	conn    net.Conn
	sess    *session.Session
	version byte

	sendCh   chan *packet.Packet
	done     chan struct{}
	once     sync.Once
	graceful bool // 收到 DISCONNECT，正常关闭（不发遗嘱）
}

// serve 处理连接生命周期：CONNECT 握手 -> 报文循环 -> 清理。
func (c *clientConn) serve() {
	defer c.cleanup()

	// 首个报文必须是 CONNECT
	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	ptype, flags, body, err := packet.ReadRaw(c.conn)
	if err != nil {
		return
	}
	if ptype != packet.TypeCONNECT {
		// 状态错误：未连接先发送其他报文
		return
	}
	first := &packet.Packet{Header: packet.FixedHeader{Type: ptype, Flags: flags}}
	if err := first.Decode(body, packet.Version311); err != nil {
		// 协议错误：尽量回 CONNACK 拒绝码（无法确定版本时按 3.1.1）
		c.rejectConnect(packet.ConnRefusedProtocol)
		return
	}
	c.version = first.ProtocolLevel

	if first.ClientID == "" {
		c.rejectConnect(packet.ConnRefusedClientID)
		return
	}

	sess, resumed := c.broker.sessions.Get(first.ClientID, first.CleanStart)
	c.sess = sess
	c.broker.register(first.ClientID, c)

	// CONNACK
	ack := &packet.Packet{
		Header:         packet.FixedHeader{Type: packet.TypeCONNACK},
		SessionPresent: resumed && !first.CleanStart,
		ReasonCode:     packet.ConnAccepted,
	}
	if err := c.send(ack); err != nil {
		return
	}

	// 恢复会话：
	// 1) 重发普通订阅的出方向在途（置 DUP）；共享组在途已在掉线时转交他人，
	//    不再由本人重发，避免组内重复消费。
	// 2) 重新激活本会话的共享订阅（可能领到组兜底堆积）。
	// 3) 按发布顺序补发离线队列（过期消息已在 Drain 内清理）。
	for _, in := range sess.InflightOut() {
		if in.GroupKey != "" {
			continue
		}
		rp := *in.Packet
		rp.Dup = true
		c.enqueue(&rp)
	}
	for f := range sess.Subs() {
		if sf, ok := topic.ParseShared(f); ok {
			k := groupKey(sf.Group, sf.Filter)
			c.broker.groups.join(k, sess.ClientID)
			c.broker.groups.activate(k, sess.ClientID, c)
		}
	}
	for _, p := range sess.Drain() {
		c.enqueuePublish(p, "", 0)
	}

	// 保存遗嘱，异常断开时发布
	var will *packet.Packet
	if first.HasWill {
		will = &packet.Packet{
			Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
			Topic:   first.WillTopic,
			Payload: first.WillPayload,
			QoS:     first.WillQoS,
			Retain:  first.WillRetain,
		}
	}

	go c.writeLoop()
	c.readLoop(will)
}

// readLoop 报文处理循环。
func (c *clientConn) readLoop(will *packet.Packet) {
	keepAlive := time.Duration(0)
	if c.sess != nil {
		// keepAlive 在 CONNECT 里；这里简化：用 1.5 倍 keepalive 做读超时
	}
	_ = keepAlive

	for {
		c.conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
		ptype, flags, body, err := packet.ReadRaw(c.conn)
		if err != nil {
			if !c.isClosed() {
				c.publishWill(will)
			}
			return
		}
		p := &packet.Packet{Header: packet.FixedHeader{Type: ptype, Flags: flags}}
		if err := p.Decode(body, c.version); err != nil {
			// 协议错误：拒绝并断开
			log.Printf("broker: client %s protocol error: %v", c.sess.ClientID, err)
			c.publishWill(will)
			return
		}
		if err := c.handle(p); err != nil {
			if errors.Is(err, packet.ErrProtocol) || errors.Is(err, packet.ErrState) {
				log.Printf("broker: client %s: %v", c.sess.ClientID, err)
				c.publishWill(will)
			}
			return
		}
	}
}

// handle 分发单个报文。
func (c *clientConn) handle(p *packet.Packet) error {
	switch p.Header.Type {
	case packet.TypePUBLISH:
		return c.handlePublish(p)
	case packet.TypePUBACK:
		c.ackInflight(p.PacketID)
		return nil
	case packet.TypePUBREC:
		if !c.sess.AdvanceOut(p.PacketID) {
			return &packet.ProtocolError{Reason: "PUBREC for unknown packet id"}
		}
		return c.send(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBREL}, PacketID: p.PacketID})
	case packet.TypePUBREL:
		// 入方向 QoS2 完成；路由已在收到 PUBLISH 时做过（TrackIn 去重）
		c.sess.CompleteIn(p.PacketID)
		return c.send(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBCOMP}, PacketID: p.PacketID})
	case packet.TypePUBCOMP:
		c.ackInflight(p.PacketID)
		return nil
	case packet.TypeSUBSCRIBE:
		return c.handleSubscribe(p)
	case packet.TypeUNSUBSCRIBE:
		return c.handleUnsubscribe(p)
	case packet.TypePINGREQ:
		return c.send(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePINGRESP}})
	case packet.TypeDISCONNECT:
		c.graceful = true
		return io.EOF
	case packet.TypeCONNECT:
		return &packet.ProtocolError{Reason: "second CONNECT on same connection"}
	default:
		return &packet.ProtocolError{Reason: "unexpected packet type"}
	}
}

// handlePublish 处理入方向 PUBLISH，按 QoS 等级走对应握手。
// QoS1/2 的确认报文在 v5 下携带投递结果原因码（3.1.1 无原因码字段）。
func (c *clientConn) handlePublish(p *packet.Packet) error {
	if err := topic.ValidatePublish(p.Topic); err != nil {
		return &packet.ProtocolError{Reason: "invalid publish topic: " + err.Error()}
	}
	switch p.QoS {
	case 0:
		c.forwardReport(p)
		return nil
	case 1:
		report := c.forwardReport(p)
		return c.send(&packet.Packet{
			Header:     packet.FixedHeader{Type: packet.TypePUBACK},
			PacketID:   p.PacketID,
			ReasonCode: c.deliveryReason(report),
		})
	case 2:
		// 去重：同一 PacketID 重复投递只路由一次，但仍回 PUBREC。
		report := (*Report)(nil)
		if dup := c.sess.TrackIn(p.PacketID); !dup {
			report = c.forwardReport(p)
		}
		code := packet.ReasonSuccess
		if report != nil {
			code = c.deliveryReason(report)
		}
		return c.send(&packet.Packet{
			Header:     packet.FixedHeader{Type: packet.TypePUBREC},
			PacketID:   p.PacketID,
			ReasonCode: code,
		})
	default:
		return &packet.ProtocolError{Reason: "invalid QoS"}
	}
}

// deliveryReason 按连接版本选择确认原因码（仅 v5 承载）。
func (c *clientConn) deliveryReason(r *Report) byte {
	if c.version >= packet.Version5 {
		return r.v5Reason()
	}
	return 0
}

// forwardReport 处理保留消息并路由，返回投递明细。
func (c *clientConn) forwardReport(p *packet.Packet) *Report {
	if p.Retain {
		c.broker.setRetained(p)
	}
	return c.broker.route(p)
}

// ackInflight 完成一条出方向握手；若是共享组投递，同时摘除组内在途记录。
func (c *clientConn) ackInflight(packetID uint16) {
	in, ok := c.sess.AckOut(packetID)
	if ok && in != nil && in.GroupKey != "" {
		c.broker.groups.ackToken(in.GroupKey, in.Token)
	}
}

// handleSubscribe 校验并登记订阅，回 SUBACK，随后下发命中的保留消息。
// 共享订阅（$share/group/filter）登记到组管理器，不触发保留消息。
func (c *clientConn) handleSubscribe(p *packet.Packet) error {
	if len(p.Subscriptions) == 0 {
		return &packet.ProtocolError{Reason: "SUBSCRIBE with no filters"}
	}
	granted := make([]packet.Subscription, 0, len(p.Subscriptions))
	type retainedSub struct {
		filter string
		qos    byte
	}
	var retained []retainedSub
	for _, sub := range p.Subscriptions {
		if sf, shared := topic.ParseShared(sub.Topic); shared {
			if err := topic.ValidateShared(sf); err != nil || sub.QoS > 2 {
				granted = append(granted, packet.Subscription{Topic: sub.Topic, QoS: 0x80})
				continue
			}
			qos := sub.QoS
			c.sess.AddSub(sub.Topic, qos)
			k := groupKey(sf.Group, sf.Filter)
			c.broker.groups.join(k, c.sess.ClientID)
			c.broker.groups.activate(k, c.sess.ClientID, c)
			granted = append(granted, packet.Subscription{Topic: sub.Topic, QoS: qos})
			continue
		}
		if err := topic.ValidateFilter(sub.Topic); err != nil {
			// 可区分的拒绝：该过滤器失败，其余照常
			granted = append(granted, packet.Subscription{Topic: sub.Topic, QoS: 0x80})
			continue
		}
		qos := sub.QoS
		if qos > 2 {
			granted = append(granted, packet.Subscription{Topic: sub.Topic, QoS: 0x80})
			continue
		}
		c.sess.AddSub(sub.Topic, qos)
		granted = append(granted, packet.Subscription{Topic: sub.Topic, QoS: qos})
		retained = append(retained, retainedSub{filter: sub.Topic, qos: qos})
	}
	ack := &packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeSUBACK},
		PacketID:      p.PacketID,
		Subscriptions: granted,
	}
	if err := c.send(ack); err != nil {
		return err
	}
	// 保留消息仅对普通订阅下发（共享订阅按协议不接收保留消息）
	for _, sub := range retained {
		for _, rp := range c.broker.retainedFor(sub.filter) {
			out := &packet.Packet{
				Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
				Topic:   rp.Topic,
				Payload: rp.Payload,
				Retain:  true,
			}
			out.QoS = rp.QoS
			if sub.qos < out.QoS {
				out.QoS = sub.qos
			}
			c.enqueuePublish(out, "", 0)
		}
	}
	return nil
}

func (c *clientConn) handleUnsubscribe(p *packet.Packet) error {
	for _, sub := range p.Subscriptions {
		c.sess.RemoveSub(sub.Topic)
		if sf, ok := topic.ParseShared(sub.Topic); ok {
			c.broker.groups.leave(groupKey(sf.Group, sf.Filter), c.sess.ClientID)
		}
	}
	return c.send(&packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeUNSUBACK},
		PacketID:      p.PacketID,
		Subscriptions: p.Subscriptions,
	})
}

// enqueuePublish 为出方向 PUBLISH 分配 PacketID 并登记在途，再入发送队列。
// groupKey/token 非空时表示这是一次共享组投递，在途记录携带组信息供掉线接管。
func (c *clientConn) enqueuePublish(p *packet.Packet, groupKey string, token uint64) {
	if p.QoS > 0 {
		id := c.sess.NextPacketID()
		if id == 0 {
			log.Printf("broker: client %s packet ids exhausted; dropping message", c.sess.ClientID)
			return
		}
		p.PacketID = id
		c.sess.TrackOut(&session.Inflight{
			PacketID: id,
			Packet:   p,
			Stage:    map[byte]int{1: 0, 2: 1}[p.QoS],
			GroupKey: groupKey,
			Token:    token,
		})
	}
	c.enqueue(p)
}

// tryEnqueue 非阻塞入发送队列；队列满或连接关闭时返回 false（共享组选择成员用）。
func (c *clientConn) tryEnqueue(p *packet.Packet) bool {
	select {
	case c.sendCh <- p:
		return true
	case <-c.done:
		return false
	default:
		return false
	}
}

func (c *clientConn) enqueue(p *packet.Packet) {
	select {
	case c.sendCh <- p:
	case <-c.done:
	}
}

// send 同步写一个报文（握手响应等）。
func (c *clientConn) send(p *packet.Packet) error {
	raw, err := p.Encode(c.version)
	if err != nil {
		return err
	}
	c.conn.SetWriteDeadline(time.Now().Add(10 * time.Second))
	_, err = c.conn.Write(raw)
	return err
}

// writeLoop 串行写出队列中的报文。
func (c *clientConn) writeLoop() {
	for {
		select {
		case p := <-c.sendCh:
			if err := c.send(p); err != nil {
				c.close()
				return
			}
		case <-c.done:
			return
		}
	}
}

func (c *clientConn) publishWill(will *packet.Packet) {
	if will != nil && !c.graceful {
		if will.Retain {
			c.broker.setRetained(will)
		}
		c.broker.route(will)
	}
}

// cleanup 连接退出：摘除在线表，处理共享组在途接管，按 CleanStart 语义清理会话。
func (c *clientConn) cleanup() {
	c.close()
	if c.sess == nil {
		return
	}
	id := c.sess.ClientID
	c.broker.unregister(id, c)

	if c.sess.CleanStart {
		// 临时会话：彻底摘除组成员身份，其未确认组消息转交他人或回兜底队列。
		c.broker.groups.removeClient(id)
		c.broker.sessions.Remove(id)
		return
	}

	// 持久会话：仅当该 ClientID 已无任何在线连接（非会话接管）时，
	// 才把未确认的共享组消息转交其他在线成员；同 ID 新连接接管时保留，
	// 由新连接继续持有，避免组内重复消费。
	if !c.broker.isOnline(id) {
		for f := range c.sess.Subs() {
			sf, ok := topic.ParseShared(f)
			if !ok {
				continue
			}
			k := groupKey(sf.Group, sf.Filter)
			c.broker.groups.deactivate(k, id, c)
		}
		// 幂等摘出（按组令牌）：防止读写协程退出与主动关闭竞态导致的重复接管。
		for _, in := range c.sess.GroupInflightTokens() {
			got := c.sess.ReclaimGroupInflightToken(in.GroupKey, in.Token)
			if got == nil {
				continue // 已被另一条清理路径摘出
			}
			c.broker.groups.reclaim(got.GroupKey, id, got.Token, got.Packet)
		}
	}
}

func (c *clientConn) close() {
	c.once.Do(func() {
		close(c.done)
		c.conn.Close()
	})
}

func (c *clientConn) isClosed() bool {
	select {
	case <-c.done:
		return true
	default:
		return false
	}
}

// rejectConnect 回 CONNACK 拒绝码后关闭。
func (c *clientConn) rejectConnect(code byte) {
	ack := &packet.Packet{
		Header:     packet.FixedHeader{Type: packet.TypeCONNACK},
		ReasonCode: code,
	}
	raw, err := ack.Encode(packet.Version311)
	if err == nil {
		c.conn.Write(raw)
	}
}
