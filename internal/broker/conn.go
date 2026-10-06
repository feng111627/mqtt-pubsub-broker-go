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

	// 恢复会话：重发出方向在途（置 DUP），随后补发离线队列
	for _, in := range sess.InflightOut() {
		rp := *in.Packet
		rp.Dup = true
		c.enqueue(&rp)
	}
	for _, p := range sess.Drain() {
		c.enqueuePublish(p)
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

// handleSubscribe 校验并登记订阅，回 SUBACK，随后下发命中的保留消息。
func (c *clientConn) handleSubscribe(p *packet.Packet) error {
	if len(p.Subscriptions) == 0 {
		return &packet.ProtocolError{Reason: "SUBSCRIBE with no filters"}
	}
	granted := make([]packet.Subscription, 0, len(p.Subscriptions))
	for _, sub := range p.Subscriptions {
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
	}
	ack := &packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeSUBACK},
		PacketID:      p.PacketID,
		Subscriptions: granted,
	}
	if err := c.send(ack); err != nil {
		return err
	}
	// 保留消息在订阅成功后下发
	for _, sub := range granted {
		if sub.QoS == 0x80 {
			continue
		}
		for _, rp := range c.broker.retainedFor(sub.Topic) {
			out := &packet.Packet{
				Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
				Topic:   rp.Topic,
				Payload: rp.Payload,
				Retain:  true,
			}
			out.QoS = rp.QoS
			if sub.QoS < out.QoS {
				out.QoS = sub.QoS
			}
			c.enqueuePublish(out)
		}
	}
	return nil
}

func (c *clientConn) handleUnsubscribe(p *packet.Packet) error {
	for _, sub := range p.Subscriptions {
		c.sess.RemoveSub(sub.Topic)
	}
	return c.send(&packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeUNSUBACK},
		PacketID:      p.PacketID,
		Subscriptions: p.Subscriptions,
	})
}

// enqueuePublish 为出方向 PUBLISH 分配 PacketID 并登记在途，再入发送队列。
func (c *clientConn) enqueuePublish(p *packet.Packet) {
	if p.QoS > 0 {
		id := c.sess.NextPacketID()
		if id == 0 {
			return // 在途耗尽，丢弃并记日志
		}
		p.PacketID = id
		c.sess.TrackOut(&session.Inflight{
			PacketID: id,
			Packet:   p,
			Stage:    map[byte]int{1: 0, 2: 1}[p.QoS],
		})
	}
	c.enqueue(p)
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

// cleanup 连接退出：摘除在线表，按 CleanStart 语义清理会话。
func (c *clientConn) cleanup() {
	c.close()
	if c.sess != nil {
		c.broker.unregister(c.sess.ClientID, c)
		if c.sess.CleanStart {
			c.broker.sessions.Remove(c.sess.ClientID)
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
