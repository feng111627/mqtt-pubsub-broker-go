package broker

import (
	"net"
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// clientConn runs the per-connection state machine for one TCP peer.
type clientConn struct {
	broker  *Broker
	conn    net.Conn
	reader  *packet.Reader
	uid     uint64
	session *Session
	level   byte

	writeMu  sync.Mutex
	stopCh   chan struct{}
	stopOnce sync.Once

	// keepalive bookkeeping
	keepAlive time.Duration // effective timeout (0 = disabled)
	deadline  time.Time
	deadMu    sync.Mutex

	// pump signaling: kickCh receives an empty token whenever new queue work
	// or acks may allow more sends.
	kickCh chan struct{}

	// takeover is closed when a newer connection for the same client id wins.
	takeover chan struct{}
	takeOnce sync.Once
	taken    bool

	handshakeDone bool
}

// handle performs the CONNECT handshake and then the packet loop.
func (c *clientConn) handle() {
	defer func() {
		// Triggered on any loop exit. If the handshake never finished there is
		// no session to clean up.
	}()
	c.reader = packet.NewReader(c.conn, c.broker.cfg.MaxPacketSize)
	if err := c.handshake(); err != nil {
		return // handshake already sent the rejection/closed the socket
	}
	c.runPumps()
}

// writePacket serialises and sends one packet under the write lock.
func (c *clientConn) writePacket(p *packet.Packet) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return p.WriteTo(c.conn, c.level)
}

// closeWith optionally sends a v5 DISCONNECT reason and closes the socket.
func (c *clientConn) closeWith(reason byte) {
	if c.level == packet.V50 && c.handshakeDone && reason != 0 {
		disc := &packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypeDISCONNECT}, Reason: reason}
		_ = c.writePacket(disc) // best effort
	}
	c.stopOnce.Do(func() { c.conn.Close() })
}

// kick wakes the outbound pump.
func (c *clientConn) kick() {
	select {
	case c.kickCh <- struct{}{}:
	default:
	}
}

func (c *clientConn) markTakenOver() {
	c.taken = true
	c.takeOnce.Do(func() { close(c.takeover) })
	c.stopOnce.Do(func() { c.conn.Close() })
}

// keepaliveExpired is checked by a dedicated timer goroutine.
func (c *clientConn) keepaliveExpired() bool {
	c.deadMu.Lock()
	defer c.deadMu.Unlock()
	return c.keepAlive > 0 && time.Now().After(c.deadline)
}

func (c *clientConn) touch() {
	c.deadMu.Lock()
	if c.keepAlive > 0 {
		c.deadline = time.Now().Add(c.keepAlive)
	}
	c.deadMu.Unlock()
}

var _ = mqerr.Of
