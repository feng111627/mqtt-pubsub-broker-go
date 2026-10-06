// Package client is a self-written MQTT client library used by the local CLI
// and by end-to-end tests. It supports protocol levels 3.1, 3.1.1 and 5.0,
// automatic reconnect with session-aware resubscription, and the QoS 0/1/2
// sender/receiver state machines.
package client

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// Message is a delivered application message.
type Message struct {
	Topic    string
	Payload  []byte
	QoS      byte
	Retain   bool
	Dup      bool
	PacketID uint16
}

// Handler receives matched messages.
type Handler func(m *Message)

// Config configures a Client.
type Config struct {
	Addr                string
	ProtocolLevel       byte
	ClientID            string
	Clean               bool
	KeepAlive           uint16
	Username            string
	Password            string
	WillTopic           string
	WillPayload         []byte
	WillQoS             byte
	WillRetain          bool
	AutoReconnect       bool
	MaxReconnectDelayMS int
	// DialTimeout bounds each TCP dial.
	DialTimeout time.Duration
	// MessageHandler receives every matched application message. It must be
	// safe to set before Connect.
	MessageHandler Handler
}

// subscription is a client-side filter registration.
type subscription struct {
	filter  string
	qos     byte
	handler Handler
}

// inflightPub is a QoS1/QoS2 publish awaiting broker acknowledgement.
type inflightPub struct {
	packet *packet.Packet
	stage  byte // 0 awaiting PUBACK/PUBREC, 1 QoS2 PUBREL sent awaiting PUBCOMP
	done   chan error
}

// inboundQoS2 tracks a QoS2 PUBLISH id being processed:
//
//	stage 0: PUBLISH received, PUBREC sent, awaiting self->server PUBREL completion
//	(we send PUBREC then immediately PUBREL; the map retains ids until PUBCOMP
//	so repeated PUBLISH with same id is a no-op).
type inboundState struct {
	completed bool // PUBREL sent, PUBCOMP seen?
}

// Client is the MQTT client.
type Client struct {
	cfg Config

	mu      sync.Mutex
	conn    net.Conn
	reader  *packet.Reader
	writeMu sync.Mutex
	closed  bool

	nextID   uint16
	inflight map[uint16]*inflightPub
	// pending QoS2 inbound ids for exactly-once receive semantics
	recvQoS2 map[uint16]bool

	subs []*subscription

	// ack waiters for SUBSCRIBE/UNSUBSCRIBE, keyed by packet id.
	ackMu     sync.Mutex
	ackWaiter map[uint16]chan *packet.Packet

	// connectedCh is closed each time a live session is established.
	connectedCh chan struct{}
	// disconnectCh is closed by Disconnect; the loop exits without reconnect.
	disconnectCh chan struct{}
	// connWait is closed when the background loop terminates after Disconnect.
	connWait       chan struct{}
	sessionPresent bool
}

// New constructs a client (no network activity until Connect).
func New(cfg Config) *Client {
	if cfg.ProtocolLevel == 0 {
		cfg.ProtocolLevel = packet.V311
	}
	if cfg.MaxReconnectDelayMS == 0 {
		cfg.MaxReconnectDelayMS = 10000
	}
	if cfg.DialTimeout == 0 {
		cfg.DialTimeout = 5 * time.Second
	}
	if cfg.ClientID == "" {
		cfg.ClientID = fmt.Sprintf("go-cli-%d", time.Now().UnixNano())
	}
	return &Client{
		cfg:          cfg,
		nextID:       1,
		inflight:     make(map[uint16]*inflightPub),
		recvQoS2:     make(map[uint16]bool),
		ackWaiter:    make(map[uint16]chan *packet.Packet),
		disconnectCh: make(chan struct{}),
	}
}

func (c *Client) allocID() uint16 {
	id := c.nextID
	c.nextID++
	if c.nextID == 0 {
		c.nextID = 1
	}
	return id
}

// Connect performs the initial dial+handshake and starts the background loop.
// With Clean=false the broker may resume an existing session; on reconnects
// the client checks SessionPresent and only re-subscribes when the broker
// reported no session (old session expired or was clean).
func (c *Client) Connect(ctx context.Context) error {
	if err := c.dialAndHandshake(ctx); err != nil {
		return err
	}
	c.connectedCh = make(chan struct{})
	close(c.connectedCh)
	c.connWait = make(chan struct{})
	go c.loop()
	return nil
}

// dialAndHandshake opens one TCP connection and completes CONNECT/CONNACK.
func (c *Client) dialAndHandshake(ctx context.Context) error {
	dialer := net.Dialer{Timeout: c.cfg.DialTimeout}
	conn, err := dialer.DialContext(ctx, "tcp", c.cfg.Addr)
	if err != nil {
		return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "dial %s: %v", c.cfg.Addr, err)
	}
	c.mu.Lock()
	c.conn = conn
	c.reader = packet.NewReader(conn, 1<<20)
	c.reader.SetLevel(c.cfg.ProtocolLevel)
	c.mu.Unlock()

	cp := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNECT},
		CleanStart: c.cfg.Clean,
		KeepAlive:  c.cfg.KeepAlive,
		ClientID:   c.cfg.ClientID,
		HasUser:    c.cfg.Username != "",
		Username:   c.cfg.Username,
	}
	if c.cfg.Password != "" {
		cp.HasPass = true
		cp.Password = []byte(c.cfg.Password)
	}
	if c.cfg.WillTopic != "" {
		cp.WillFlag = true
		cp.WillTopic = c.cfg.WillTopic
		cp.WillMsg = append([]byte(nil), c.cfg.WillPayload...)
		cp.WillQoS = c.cfg.WillQoS
		cp.WillRetain = c.cfg.WillRetain
	}
	if c.cfg.ProtocolLevel == packet.V50 {
		cp.Props = packet.NewProperties()
	}
	if err := c.writePacket(cp); err != nil {
		conn.Close()
		return err
	}
	ack, err := c.reader.ReadOne()
	if err != nil {
		conn.Close()
		return classifyRead(err)
	}
	if ack.Fixed.Type != packet.TypeCONNACK {
		conn.Close()
		return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "expected CONNACK, got %s", ack.Fixed.Type)
	}
	if ack.ReasonCode != 0 {
		conn.Close()
		return c.connackError(ack.ReasonCode)
	}
	c.mu.Lock()
	c.sessionPresent = ack.SessionPresent
	c.mu.Unlock()
	return nil
}

func (c *Client) connackError(code byte) error {
	class := mqerr.ClassProtocol
	switch code {
	case 0x03, byte(mqerr.CodeServerBusy):
		class = mqerr.ClassState
	}
	return mqerr.New(class, mqerr.Code(code), "CONNACK rejected: code 0x%02x", code)
}

func classifyRead(err error) error {
	var me *mqerr.Error
	if errors.As(err, &me) {
		return me
	}
	return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "%v", err)
}

// writePacket serialises under the write lock.
func (c *Client) writePacket(p *packet.Packet) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	c.mu.Lock()
	conn := c.conn
	c.mu.Unlock()
	if conn == nil {
		return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "not connected")
	}
	if err := p.WriteTo(conn, c.cfg.ProtocolLevel); err != nil {
		return err
	}
	return nil
}

// Disconnect performs a clean MQTT DISCONNECT and closes the socket. With
// Clean=false the broker-side session is kept.
func (c *Client) Disconnect() error {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil
	}
	c.closed = true
	ch := c.connWait
	c.mu.Unlock()
	close(c.disconnectCh)
	disc := &packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypeDISCONNECT}}
	_ = c.writePacket(disc)
	c.mu.Lock()
	if c.conn != nil {
		c.conn.Close()
	}
	c.mu.Unlock()
	if ch != nil {
		<-ch
	}
	return nil
}

// Wait blocks until the background loop has stopped.
func (c *Client) Wait() {
	c.mu.Lock()
	ch := c.connWait
	c.mu.Unlock()
	if ch != nil {
		<-ch
	}
}

// IsConnected reports current liveness.
func (c *Client) IsConnected() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.conn != nil && !c.closed
}
