// Package client 提供简单的 MQTT 客户端，供设备侧/后台侧本地验证使用。
package client

import (
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// DeliveryError 表示发布已到达代理，但代理在投递阶段返回了非成功原因码
// （仅 MQTT 5.0 的 PUBACK/PUBREC/PUBCOMP 可携带；3.1.1 无此字段）。
// 调用方可据 ReasonCode 区分"无订阅者/队列满/会话过期"等结果。
type DeliveryError struct {
	ReasonCode byte
}

func (e *DeliveryError) Error() string {
	return fmt.Sprintf("mqtt: broker delivery failed, reason code 0x%02x", e.ReasonCode)
}

// ackError 把 v5 原因码映射为可区分错误；0x00 或 3.1.1（无原因码）为成功。
func ackError(code byte) error {
	if code == 0 || code == packet.ReasonSuccess {
		return nil
	}
	return &DeliveryError{ReasonCode: code}
}

// Message 收到的一条订阅消息。
type Message struct {
	Topic   string
	Payload []byte
	QoS     byte
	Retain  bool
	Dup     bool
}

// Options 连接选项。
type Options struct {
	ClientID   string
	CleanStart bool
	KeepAlive  uint16
	Version    byte // packet.Version311 或 Version5
}

// Client MQTT 客户端。
type Client struct {
	opts Options
	conn net.Conn

	mu      sync.Mutex
	nextID  uint16
	pending map[uint16]*pendingReq // 等待 ACK 的请求

	incoming chan Message
	done     chan struct{}
	once     sync.Once

	lastGranted byte // 最近一次 SUBACK 授予的 QoS
	writeMu     sync.Mutex
}

// Connect 建立连接并完成 CONNECT/CONNACK 握手。
func Connect(addr string, opts Options) (*Client, error) {
	if opts.Version == 0 {
		opts.Version = packet.Version311
	}
	conn, err := net.DialTimeout("tcp", addr, 5*time.Second)
	if err != nil {
		return nil, fmt.Errorf("dial: %w", err)
	}
	c := &Client{
		opts:     opts,
		conn:     conn,
		nextID:   1,
		pending:  make(map[uint16]*pendingReq),
		incoming: make(chan Message, 64),
		done:     make(chan struct{}),
	}

	connect := &packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeCONNECT},
		ProtocolName:  "MQTT",
		ProtocolLevel: opts.Version,
		CleanStart:    opts.CleanStart,
		KeepAlive:     opts.KeepAlive,
		ClientID:      opts.ClientID,
	}
	if err := c.write(encodeConnect(connect)); err != nil {
		conn.Close()
		return nil, err
	}
	conn.SetReadDeadline(time.Now().Add(5 * time.Second))
	ack, err := packet.ReadPacket(conn)
	if err != nil {
		conn.Close()
		return nil, fmt.Errorf("read connack: %w", err)
	}
	conn.SetReadDeadline(time.Time{})
	if ack.Header.Type != packet.TypeCONNACK {
		conn.Close()
		return nil, fmt.Errorf("expected CONNACK, got type %d", ack.Header.Type)
	}
	if ack.ReasonCode != packet.ConnAccepted {
		conn.Close()
		return nil, fmt.Errorf("connection refused, reason code %d", ack.ReasonCode)
	}

	go c.readLoop()
	return c, nil
}

// encodeConnect 手工编码 CONNECT（含 v5 空属性区）。
func encodeConnect(p *packet.Packet) []byte {
	body := appendStr(nil, "MQTT")
	body = append(body, p.ProtocolLevel)
	flags := byte(0)
	if p.CleanStart {
		flags |= 0x02
	}
	body = append(body, flags, byte(p.KeepAlive>>8), byte(p.KeepAlive))
	if p.ProtocolLevel == packet.Version5 {
		body = append(body, 0x00) // properties
	}
	body = appendStr(body, p.ClientID)
	out := []byte{packet.TypeCONNECT << 4}
	out = appendVarint(out, len(body))
	return append(out, body...)
}

func appendStr(dst []byte, s string) []byte {
	dst = append(dst, byte(len(s)>>8), byte(len(s)))
	return append(dst, s...)
}

func appendVarint(dst []byte, n int) []byte {
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			dst = append(dst, b|0x80)
		} else {
			return append(dst, b)
		}
	}
}

// Subscribe 订阅一个主题过滤器，返回服务端授予的 QoS。
func (c *Client) Subscribe(filter string, qos byte) (byte, error) {
	id := c.allocID()
	ch := c.register(id)
	defer c.unregister(id)

	p := &packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeSUBSCRIBE, Flags: 0x02},
		PacketID:      id,
		Subscriptions: []packet.Subscription{{Topic: filter, QoS: qos}},
	}
	if err := c.writePacket(p); err != nil {
		return 0, err
	}
	if err := c.wait(ch); err != nil {
		return 0x80, err
	}
	c.mu.Lock()
	g := c.lastGranted
	c.mu.Unlock()
	return g, nil
}

// Unsubscribe 取消订阅。
func (c *Client) Unsubscribe(filter string) error {
	id := c.allocID()
	ch := c.register(id)
	defer c.unregister(id)
	p := &packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeUNSUBSCRIBE, Flags: 0x02},
		PacketID:      id,
		Subscriptions: []packet.Subscription{{Topic: filter}},
	}
	if err := c.writePacket(p); err != nil {
		return err
	}
	return c.wait(ch)
}

// Publish 发布消息，按 QoS 完成对应握手后返回。
func (c *Client) Publish(topic string, qos byte, retain bool, payload []byte) error {
	p := &packet.Packet{
		Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
		Topic:   topic,
		QoS:     qos,
		Retain:  retain,
		Payload: payload,
	}
	if qos == 0 {
		return c.writePacket(p)
	}
	id := c.allocID()
	ch := c.register(id)
	defer c.unregister(id)
	p.PacketID = id
	if err := c.writePacket(p); err != nil {
		return err
	}
	return c.wait(ch)
}

// Conn 返回底层连接（测试与诊断用）。
func (c *Client) Conn() net.Conn { return c.conn }

// Messages 返回接收消息的通道。
func (c *Client) Messages() <-chan Message { return c.incoming }

// Close 断开连接。
func (c *Client) Close() error {
	c.once.Do(func() {
		close(c.done)
		c.conn.Close()
	})
	return nil
}

// ---------- 内部 ----------

func (c *Client) allocID() uint16 {
	c.mu.Lock()
	defer c.mu.Unlock()
	id := c.nextID
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	return id
}

// pendingReq 一个在途请求。
type pendingReq struct {
	ch chan error
}

func (c *Client) register(id uint16) chan error {
	ch := make(chan error, 1)
	c.mu.Lock()
	c.pending[id] = &pendingReq{ch: ch}
	c.mu.Unlock()
	return ch
}

func (c *Client) unregister(id uint16) {
	c.mu.Lock()
	delete(c.pending, id)
	c.mu.Unlock()
}

func (c *Client) resolve(id uint16, err error) {
	c.mu.Lock()
	req, ok := c.pending[id]
	c.mu.Unlock()
	if ok {
		req.ch <- err
	}
}

func (c *Client) wait(ch chan error) error {
	select {
	case err := <-ch:
		return err
	case <-c.done:
		return fmt.Errorf("client closed")
	case <-time.After(10 * time.Second):
		return fmt.Errorf("ack timeout")
	}
}

func (c *Client) write(raw []byte) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.conn.SetWriteDeadline(time.Now().Add(5 * time.Second))
	_, err := c.conn.Write(raw)
	return err
}

func (c *Client) writePacket(p *packet.Packet) error {
	raw, err := p.Encode(c.opts.Version)
	if err != nil {
		return err
	}
	return c.write(raw)
}

func (c *Client) readLoop() {
	for {
		ptype, flags, body, err := packet.ReadRaw(c.conn)
		if err != nil {
			c.Close()
			return
		}
		p := &packet.Packet{Header: packet.FixedHeader{Type: ptype, Flags: flags}}
		if err := p.Decode(body, c.opts.Version); err != nil {
			continue // 畸形报文：忽略，保持连接
		}
		c.handle(p)
	}
}

func (c *Client) handle(p *packet.Packet) {
	switch p.Header.Type {
	case packet.TypePUBLISH:
		msg := Message{Topic: p.Topic, Payload: p.Payload, QoS: p.QoS, Retain: p.Retain, Dup: p.Dup}
		select {
		case c.incoming <- msg:
		default:
		}
		switch p.QoS {
		case 1:
			c.writePacket(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: p.PacketID})
		case 2:
			c.writePacket(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBREC}, PacketID: p.PacketID})
		}
	case packet.TypePUBACK, packet.TypePUBCOMP:
		c.resolve(p.PacketID, ackError(p.ReasonCode))
	case packet.TypePUBREC:
		if err := ackError(p.ReasonCode); err != nil {
			// v5：代理在 PUBREC 即拒绝（无订阅者/队列满/会话过期），停止 QoS2 握手。
			c.resolve(p.PacketID, err)
			return
		}
		c.writePacket(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBREL, Flags: 0x02}, PacketID: p.PacketID})
	case packet.TypePUBREL:
		c.writePacket(&packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBCOMP}, PacketID: p.PacketID})
	case packet.TypeSUBACK:
		if len(p.Subscriptions) > 0 && p.Subscriptions[0].QoS == 0x80 {
			c.resolve(p.PacketID, fmt.Errorf("subscribe rejected"))
		} else {
			c.mu.Lock()
			if len(p.Subscriptions) > 0 {
				c.lastGranted = p.Subscriptions[0].QoS
			}
			c.mu.Unlock()
			c.resolve(p.PacketID, nil)
		}
	case packet.TypeUNSUBACK:
		c.resolve(p.PacketID, nil)
	case packet.TypePINGRESP:
	}
}
