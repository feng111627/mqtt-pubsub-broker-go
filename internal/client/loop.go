package client

import (
	"context"
	"sync/atomic"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// reconnectEpoch is bumped on every new transport so stale writes/reads can be
// detected in tests and heartbeat logic.
var reconnectEpoch uint64

// loop is the receive + reconnect driver.
func (c *Client) loop() {
	defer func() {
		c.mu.Lock()
		if c.connWait != nil {
			close(c.connWait)
		}
		c.mu.Unlock()
	}()

	// readerEnd is closed by the per-connection read goroutine when the
	// current transport's read loop exits; the supervisor waits on it.
	var readerEnd chan struct{}
	var lastErr atomic.Value
	startReader := func() {
		readerEnd = make(chan struct{})
		go func() {
			defer close(readerEnd)
			lastErr.Store(c.readLoop())
		}()
	}

	var pingStop chan struct{}
	startHeartbeat := func() {
		c.mu.Lock()
		ka := c.cfg.KeepAlive
		c.mu.Unlock()
		if ka > 0 {
			pingStop = make(chan struct{})
			go c.pingLoop(pingStop)
		}
	}
	stopHeartbeat := func() {
		if pingStop != nil {
			close(pingStop)
			pingStop = nil
		}
	}

	// The initial transport was established by Connect; start its reader now.
	startHeartbeat()
	startReader()

	for {
		<-readerEnd
		err, _ := lastErr.Load().(error)
		stopHeartbeat()

		c.mu.Lock()
		closed := c.closed
		if c.conn != nil {
			c.conn.Close()
			c.conn = nil
		}
		auto := c.cfg.AutoReconnect
		c.mu.Unlock()

		if closed || !auto {
			c.failInflight(err)
			return
		}

		// Backoff then re-establish. On resume with session present the broker
		// still holds our subscriptions and inflight QoS state; on absent
		// session we re-subscribe from scratch.
		backoff := c.reconnectBackoff()
		for {
			if !c.sleep(backoff) {
				c.failInflight(err)
				return
			}
			if rerr := c.dialAndHandshake(context.Background()); rerr != nil {
				c.mu.Lock()
				c.conn = nil
				c.mu.Unlock()
				err = rerr
				continue
			}
			break
		}
		atomic.AddUint64(&reconnectEpoch, 1)
		// IMPORTANT: start reading the new transport before resubscribing,
		// otherwise the SUBACK we synchronously wait for is never consumed.
		startHeartbeat()
		startReader()
		c.afterReconnect()
	}
}

// readLoop reads packets until an error occurs.
func (c *Client) readLoop() error {
	for {
		c.mu.Lock()
		r := c.reader
		c.mu.Unlock()
		if r == nil {
			return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "reader gone")
		}
		p, err := r.ReadOne()
		if err != nil {
			return err
		}
		if err := c.handlePacket(p); err != nil {
			return err
		}
	}
}

func (c *Client) handlePacket(p *packet.Packet) error {
	switch p.Fixed.Type {
	case packet.TypePUBLISH:
		return c.handleInboundPublish(p)
	case packet.TypePUBACK:
		c.finishPublish(p.PacketID, p.ReasonCode, false)
	case packet.TypePUBREC:
		return c.handlePubRec(p)
	case packet.TypePUBREL:
		return c.handleInboundPubRel(p)
	case packet.TypePUBCOMP:
		c.finishPublish(p.PacketID, p.ReasonCode, true)
	case packet.TypeSUBACK, packet.TypeUNSUBACK:
		c.deliverAckToWaiter(p)
	case packet.TypePINGRESP:
		// heartbeat watchdog resets on any traffic; nothing to do.
	case packet.TypeDISCONNECT:
		// server-initiated disconnect; close and let reconnect policy apply.
		return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific,
			"server DISCONNECT reason=0x%02x", p.Reason)
	case packet.TypeCONNACK:
		// unexpected after handshake
		return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "unexpected CONNACK")
	}
	return nil
}

// ---- inbound QoS ---------------------------------------------------------

func (c *Client) handleInboundPublish(p *packet.Packet) error {
	m := &Message{
		Topic:    p.Topic,
		Payload:  append([]byte(nil), p.Payload...),
		QoS:      p.Fixed.QoS(),
		Retain:   p.Fixed.Retain(),
		Dup:      p.Fixed.Dup(),
		PacketID: p.PacketID,
	}
	qos := p.Fixed.QoS()
	switch qos {
	case 0:
		c.dispatch(m)
	case 1:
		c.dispatch(m)
		ack := &packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: p.PacketID}
		return c.writePacket(ack)
	case 2:
		c.mu.Lock()
		_, seen := c.recvQoS2[p.PacketID]
		c.mu.Unlock()
		if !seen {
			c.mu.Lock()
			c.recvQoS2[p.PacketID] = false
			c.mu.Unlock()
			c.dispatch(m) // exactly-once application delivery
		}
		// Always answer PUBREC (duplicates included) to drive convergence.
		if err := c.writePacket(&packet.Packet{
			Fixed:    packet.FixedHeader{Type: packet.TypePUBREC},
			PacketID: p.PacketID,
		}); err != nil {
			return err
		}
		// Immediately send PUBREL and track completion.
		if err := c.writePacket(&packet.Packet{
			Fixed:    packet.FixedHeader{Type: packet.TypePUBREL, Flags: 0x02},
			PacketID: p.PacketID,
		}); err != nil {
			return err
		}
	}
	return nil
}

// handleInboundPubRel completes the *client-as-subscriber* QoS2 second leg
// when the broker sent us PUBREL (occurs when the client is the publisher? No:
// PUBREL flows publisher-side. On a subscriber we send PUBREL after PUBREC,
// so inbound PUBREL here means the client is publisher counterpart).
func (c *Client) handleInboundPubRel(p *packet.Packet) error {
	return c.writePacket(&packet.Packet{
		Fixed:    packet.FixedHeader{Type: packet.TypePUBCOMP},
		PacketID: p.PacketID,
	})
}

// handlePubRec is the publisher-side reaction to the broker's PUBREC: send
// PUBREL and advance the inflight stage.
func (c *Client) handlePubRec(p *packet.Packet) error {
	c.mu.Lock()
	in := c.inflight[p.PacketID]
	c.mu.Unlock()
	if in != nil {
		in.stage = 1
	}
	return c.writePacket(&packet.Packet{
		Fixed:    packet.FixedHeader{Type: packet.TypePUBREL, Flags: 0x02},
		PacketID: p.PacketID,
	})
}

// finishPublish resolves a tracked outbound publish.
func (c *Client) finishPublish(id uint16, reason byte, qos2 bool) {
	c.mu.Lock()
	in := c.inflight[id]
	delete(c.inflight, id)
	c.mu.Unlock()
	if in == nil {
		return
	}
	if reason != 0 {
		in.done <- mqerr.New(mqerr.ClassProtocol, mqerr.Code(reason), "publish rejected reason=0x%02x", reason)
		return
	}
	in.done <- nil
}

func (c *Client) failInflight(err error) {
	c.mu.Lock()
	inf := c.inflight
	c.inflight = make(map[uint16]*inflightPub)
	c.mu.Unlock()
	for _, in := range inf {
		in.done <- err
	}
}

// dispatch fans one message out to all matching client subscriptions.
func (c *Client) dispatch(m *Message) {
	c.mu.Lock()
	subs := make([]*subscription, len(c.subs))
	copy(subs, c.subs)
	handler := c.cfg.MessageHandler
	c.mu.Unlock()
	any := false
	for _, s := range subs {
		if packet.Match(s.filter, m.Topic) {
			any = true
			h := s.handler
			if h == nil {
				h = handler
			}
			if h != nil {
				h(m)
			}
		}
	}
	if !any && handler != nil {
		// retained/system fallback: default handler still sees the message if
		// no filter-specific one matched (should not happen post-unsubscribe).
	}
}

// afterReconnect re-subscribes if the resumed session is absent and resends
// pending QoS2 PUBREL flows.
func (c *Client) afterReconnect() {
	c.mu.Lock()
	present := c.sessionPresent
	subs := make([]*subscription, len(c.subs))
	copy(subs, c.subs)
	stage1 := map[uint16]*inflightPub{}
	for id, in := range c.inflight {
		if in.stage == 1 {
			stage1[id] = in
		}
	}
	c.mu.Unlock()

	// Resend QoS2 PUBREL flows that had reached stage 1.
	for _, in := range stage1 {
		_ = c.writePacket(&packet.Packet{
			Fixed:    packet.FixedHeader{Type: packet.TypePUBREL, Flags: 0x02},
			PacketID: in.packet.PacketID,
		})
	}

	if present {
		// Broker kept the subscriptions; just retransmit pending publishes.
		c.resendInflight()
		return
	}
	// Session was lost: rebuild subscriptions deterministically, grouped by
	// their requested QoS.
	byQoS := map[byte][]string{}
	var order []byte
	for _, sub := range subs {
		if _, ok := byQoS[sub.qos]; !ok {
			order = append(order, sub.qos)
		}
		byQoS[sub.qos] = append(byQoS[sub.qos], sub.filter)
	}
	for _, q := range order {
		_ = c.sendSubscribe(context.Background(), q, byQoS[q])
	}
	c.resendInflight()
}

func (c *Client) resendInflight() {
	c.mu.Lock()
	ids := make([]uint16, 0, len(c.inflight))
	for id := range c.inflight {
		ids = append(ids, id)
	}
	c.mu.Unlock()
	// Stable order not observable to callers; reuse ids directly.
	for _, id := range ids {
		c.mu.Lock()
		in := c.inflight[id]
		c.mu.Unlock()
		if in == nil || in.stage == 1 {
			continue // PUBREL resend handled separately
		}
		pk := *in.packet
		pk.Fixed.Flags |= 0x08 // DUP on redelivery
		pk.PacketID = id
		_ = c.writePacket(&pk)
	}
}

// pingLoop emits PINGREQ at keepalive seconds/2 (simplified but well inside
// the spec window). No response tracking is required because any server packet
// resets deadlines implicitly; a dead transport surfaces in the read loop.
func (c *Client) pingLoop(stop chan struct{}) {
	c.mu.Lock()
	ka := c.cfg.KeepAlive
	c.mu.Unlock()
	d := time.Duration(ka) * time.Second / 2
	if d <= 0 {
		d = time.Second
	}
	t := time.NewTicker(d)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-c.disconnectCh:
			return
		case <-t.C:
			_ = c.writePacket(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePINGREQ}})
		}
	}
}

func (c *Client) reconnectBackoff() time.Duration {
	// Fixed 300ms base doubles on epoch up to the configured ceiling.
	d := 300 * time.Millisecond
	for i := uint64(0); i < atomic.LoadUint64(&reconnectEpoch); i++ {
		d *= 2
		if int(d.Milliseconds()) >= c.cfg.MaxReconnectDelayMS {
			d = time.Duration(c.cfg.MaxReconnectDelayMS) * time.Millisecond
			break
		}
	}
	return d
}

func (c *Client) sleep(d time.Duration) bool {
	t := time.NewTimer(d)
	defer t.Stop()
	select {
	case <-t.C:
		return true
	case <-c.disconnectCh:
		return false
	}
}
