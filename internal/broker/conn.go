package broker

import (
	"errors"
	"io"
	"log"
	"net"
	"strings"
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/group"
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
	sess.MarkConnected(c.broker.now())
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

	// 恢复会话：重发出方向在途（置 DUP），随后按发布顺序补发离线队列。
	// 共享组在途消息只在“同 ID 会话接管”（旧连接被踢、本连接复用同一会话）
	// 时仍存在于此，照常用 DUP 重发；正常重连时它们已被旧连接的清理逻辑
	// 回收给组，不会出现在这里，因此不会重复消费。
	for _, in := range sess.InflightOut() {
		rp := *in.Packet
		rp.Dup = true
		c.enqueue(&rp)
	}
	drained := sess.DrainAt(c.broker.now())
	// 离开离线队列：可补发的与已过期的都从全局堆积计数中回收
	c.broker.queuedTotal.Add(-int64(len(drained.Messages) + drained.Expired))
	for _, p := range drained.Messages {
		c.enqueuePublish(p, "")
	}
	// 持久会话恢复后，驱动它所属的各共享组补发“无在线成员期间”的兜底消息。
	c.pumpSharedGroupsOnConnect()

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
		c.sess.AckOut(p.PacketID)
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
		c.sess.AckOut(p.PacketID)
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
func (c *clientConn) handlePublish(p *packet.Packet) error {
	if err := topic.ValidatePublish(p.Topic); err != nil {
		return &packet.ProtocolError{Reason: "invalid publish topic: " + err.Error()}
	}
	switch p.QoS {
	case 0:
		c.forward(p)
	case 1:
		c.forward(p)
		return c.send(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: p.PacketID})
	case 2:
		// 去重：同一 PacketID 重复投递只路由一次，但仍回 PUBREC
		if dup := c.sess.TrackIn(p.PacketID); !dup {
			c.forward(p)
		}
		return c.send(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBREC}, PacketID: p.PacketID})
	default:
		return &packet.ProtocolError{Reason: "invalid QoS"}
	}
	return nil
}

// forward 路由并处理保留消息。
func (c *clientConn) forward(p *packet.Packet) {
	if p.Retain {
		c.broker.setRetained(p)
	}
	c.broker.route(p)
}

// grantedSub 记录一条被接受的订阅及其共享组信息。
type grantedSub struct {
	filter string
	qos    byte
	shared string // 非空表示共享订阅，值为组名
}

// handleSubscribe 校验并登记订阅，回 SUBACK，随后下发命中的保留消息
// （共享订阅按 MQTT 规范不下发保留消息，改为驱动组兜底队列补发）。
func (c *clientConn) handleSubscribe(p *packet.Packet) error {
	if len(p.Subscriptions) == 0 {
		return &packet.ProtocolError{Reason: "SUBSCRIBE with no filters"}
	}
	granted := make([]packet.Subscription, 0, len(p.Subscriptions))
	accepted := make([]grantedSub, 0, len(p.Subscriptions))
	for _, sub := range p.Subscriptions {
		groupName := ""
		validateTarget := sub.Topic
		if gname, suffix, ok := parseSharedFilter(sub.Topic); ok {
			groupName = gname
			validateTarget = suffix
		} else if strings.HasPrefix(sub.Topic, sharePrefix) {
			// $share/ 前缀但组名或过滤器缺失：非法
			granted = append(granted, packet.Subscription{Topic: sub.Topic, QoS: 0x80})
			continue
		}
		if err := topic.ValidateFilter(validateTarget); err != nil {
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
		if groupName != "" {
			c.broker.groups.AddMember(groupName, c.sess.ClientID)
		}
		granted = append(granted, packet.Subscription{Topic: sub.Topic, QoS: qos})
		accepted = append(accepted, grantedSub{filter: sub.Topic, qos: qos, shared: groupName})
	}
	ack := &packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeSUBACK},
		PacketID:      p.PacketID,
		Subscriptions: granted,
	}
	if err := c.send(ack); err != nil {
		return err
	}
	// 订阅成功后的后续动作
	for _, g := range accepted {
		if g.shared != "" {
			// 共享订阅：新成员上线，驱动该组兜底队列（组内只选在线成员投递）
			c.broker.pumpGroup(g.shared)
			continue
		}
		// 普通订阅：保留消息在订阅成功后下发
		for _, rp := range c.broker.retainedFor(g.filter) {
			out := &packet.Packet{
				Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
				Topic:   rp.Topic,
				Payload: rp.Payload,
				Retain:  true,
			}
			out.QoS = rp.QoS
			if g.qos < out.QoS {
				out.QoS = g.qos
			}
			c.enqueuePublish(out, "")
		}
	}
	return nil
}

func (c *clientConn) handleUnsubscribe(p *packet.Packet) error {
	for _, sub := range p.Subscriptions {
		c.sess.RemoveSub(sub.Topic)
		if gname, _, ok := parseSharedFilter(sub.Topic); ok {
			c.broker.groups.RemoveMember(gname, c.sess.ClientID)
		}
	}
	return c.send(&packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeUNSUBACK},
		PacketID:      p.PacketID,
		Subscriptions: p.Subscriptions,
	})
}

// enqueuePublish 为出方向 PUBLISH 分配 PacketID 并登记在途，再入发送队列。
// sharedGroup 非空时把该在途消息标记为属于共享组，供掉线回收。
// 返回 false 表示连接已关闭、消息未进入发送队列（调用方需另行兜底）。
func (c *clientConn) enqueuePublish(p *packet.Packet, sharedGroup string) bool {
	if p.QoS > 0 {
		id := c.sess.NextPacketID()
		if id == 0 {
			log.Printf("broker: client %s inflight window exhausted, message dropped topic=%s", c.sess.ClientID, p.Topic)
			return false // 在途耗尽，丢弃并记日志
		}
		p.PacketID = id
		c.sess.TrackOut(&session.Inflight{
			PacketID:    id,
			Packet:      p,
			Stage:       map[byte]int{1: 0, 2: 1}[p.QoS],
			SharedGroup: sharedGroup,
		})
	}
	return c.enqueue(p)
}

// enqueue 入发送队列；连接关闭时返回 false，绝不阻塞在已关闭连接上。
func (c *clientConn) enqueue(p *packet.Packet) bool {
	select {
	case c.sendCh <- p:
		return true
	case <-c.done:
		return false
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

// cleanup 连接退出：摘除在线表，按 CleanStart/会话接管语义清理。
func (c *clientConn) cleanup() {
	c.close()
	if c.sess == nil {
		return
	}
	id := c.sess.ClientID
	// 仅当摘除的确实是本连接时才做离线处理；同 ID 会话接管时新连接仍在线，
	// 不能把它标记成离线，也不能回收它持有的共享在途消息。
	c.broker.mu.Lock()
	current := c.broker.online[id]
	takenOver := current != nil && current != c
	if current == c {
		delete(c.broker.online, id)
	}
	c.broker.mu.Unlock()

	if takenOver {
		return
	}

	c.sess.SetOnline(false, c.broker.now())

	// 共享组：把本连接未确认完的消息回收给组，并立即驱动组分发给在线成员。
	// 这些消息已从本会话移除，原成员重连后不会再收到，避免重复消费。
	reclaimed := c.sess.ReclaimSharedInflight()
	if len(reclaimed) > 0 {
		byGroup := map[string][]*group.Queued{}
		now := c.broker.now()
		for _, in := range reclaimed {
			byGroup[in.SharedGroup] = append(byGroup[in.SharedGroup], &group.Queued{
				Topic:      in.Packet.Topic,
				Payload:    in.Packet.Payload,
				QoS:        in.Packet.QoS,
				Retain:     in.Packet.Retain,
				EnqueuedAt: now,
			})
		}
		for gname, qs := range byGroup {
			if g := c.broker.groups.Get(gname); g != nil {
				g.Splice(qs, now)
				c.broker.pumpGroup(gname)
			}
		}
	}

	// CleanStart 会话：断开即销毁，并从所有共享组移除。
	// 持久会话保留（离线堆积/过期由会话生命周期负责）。
	if c.sess.CleanStart {
		c.broker.groups.RemoveClient(id)
		c.broker.sessions.Remove(id)
	}
}

// pumpSharedGroupsOnConnect 持久会话重连后，对其订阅涉及的每个共享组
// 触发一次兜底队列补发（有在线成员才会真正投递）。
func (c *clientConn) pumpSharedGroupsOnConnect() {
	seen := map[string]struct{}{}
	for f := range c.sess.Subs() {
		if gname, _, ok := parseSharedFilter(f); ok {
			if _, dup := seen[gname]; dup {
				continue
			}
			seen[gname] = struct{}{}
			c.broker.pumpGroup(gname)
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
