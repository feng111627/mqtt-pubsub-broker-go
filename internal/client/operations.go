package client

import (
	"context"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// Publish sends a message at the given QoS and retain flag.
//
//	QoS0: returns once the bytes are written;
//	QoS1: blocks until PUBACK (or context cancellation/connection loss);
//	QoS2: blocks until PUBCOMP.
//
// Failures carry the structured *mqerr.Error so callers can distinguish
// protocol rejections (e.g. 0x90 topic name invalid), state errors and
// connection errors.
func (c *Client) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	if !packet.ValidTopicName(topic) {
		return mqerr.New(mqerr.ClassProtocol, mqerr.CodeTopicNameInvalid, "invalid topic name %q", topic)
	}
	if qos > 2 {
		return mqerr.New(mqerr.ClassState, mqerr.CodeProtocolError, "qos must be 0..2, got %d", qos)
	}
	var flags byte = qos << 1
	if retain {
		flags |= 1
	}
	pk := &packet.Packet{
		Fixed:   packet.FixedHeader{Type: packet.TypePUBLISH, Flags: flags},
		Topic:   topic,
		Payload: append([]byte(nil), payload...),
	}
	if c.cfg.ProtocolLevel == packet.V50 {
		pk.Props = packet.NewProperties()
	}
	var done chan error
	if qos > 0 {
		c.mu.Lock()
		id := c.allocID()
		pk.PacketID = id
		done = make(chan error, 1)
		c.inflight[id] = &inflightPub{packet: pk, done: done}
		c.mu.Unlock()
	}
	if err := c.writePacket(pk); err != nil {
		if qos > 0 {
			c.mu.Lock()
			delete(c.inflight, pk.PacketID)
			c.mu.Unlock()
		}
		return err
	}
	if qos == 0 {
		return nil
	}
	select {
	case err := <-done:
		return err
	case <-ctx.Done():
		c.mu.Lock()
		delete(c.inflight, pk.PacketID)
		c.mu.Unlock()
		return ctx.Err()
	case <-c.disconnectCh:
		return mqerr.New(mqerr.ClassState, mqerr.CodeImplSpecific, "client disconnected")
	}
}

// Subscribe registers filters. All filters share the given QoS; messages are
// delivered to the per-client MessageHandler. It blocks until SUBACK and
// returns an error if any filter was rejected (v5 reason code exposed).
func (c *Client) Subscribe(ctx context.Context, qos byte, handler Handler, filters ...string) error {
	for _, f := range filters {
		if !packet.ValidFilter(f) {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeTopicFilterInvalid, "invalid topic filter %q", f)
		}
	}
	if err := c.sendSubscribe(ctx, qos, filters); err != nil {
		return err
	}
	c.mu.Lock()
	for _, f := range filters {
		c.subs = append(c.subs, &subscription{filter: f, qos: qos, handler: handler})
	}
	c.mu.Unlock()
	return nil
}

// sendSubscribe builds and sends SUBSCRIBE and waits for SUBACK.
func (c *Client) sendSubscribe(ctx context.Context, qos byte, filters []string) error {
	c.mu.Lock()
	id := c.allocID()
	c.mu.Unlock()
	pk := &packet.Packet{
		Fixed:    packet.FixedHeader{Type: packet.TypeSUBSCRIBE},
		PacketID: id,
	}
	if c.cfg.ProtocolLevel == packet.V50 {
		pk.Props = packet.NewProperties()
	}
	for _, f := range filters {
		pk.Filters = append(pk.Filters, packet.SubFilter{Filter: f, QoS: qos})
	}
	w := make(chan *packet.Packet, 1)
	c.ackMu.Lock()
	c.ackWaiter[id] = w
	c.ackMu.Unlock()
	defer func() {
		c.ackMu.Lock()
		delete(c.ackWaiter, id)
		c.ackMu.Unlock()
	}()

	if err := c.writePacket(pk); err != nil {
		return err
	}
	rerr := c.awaitSubAck(ctx, w, filters)
	return rerr
}

func (c *Client) awaitSubAck(ctx context.Context, w chan *packet.Packet, filters []string) error {
	select {
	case ack := <-w:
		for i, rc := range ack.ReturnCodes {
			if rc == 0x80 || rc >= 0x80 {
				return mqerr.New(mqerr.ClassProtocol, mqerr.Code(rc),
					"subscription rejected for %q with reason 0x%02x", filters[i], rc)
			}
		}
		return nil
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return mqerr.New(mqerr.ClassState, mqerr.CodeImplSpecific, "timeout waiting SUBACK")
	case <-c.disconnectCh:
		return mqerr.New(mqerr.ClassState, mqerr.CodeImplSpecific, "client disconnected while subscribing")
	}
}

// Unsubscribe removes filters and blocks until UNSUBACK.
func (c *Client) Unsubscribe(ctx context.Context, filters ...string) error {
	for _, f := range filters {
		if !packet.ValidFilter(f) {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeTopicFilterInvalid, "invalid topic filter %q", f)
		}
	}
	c.mu.Lock()
	id := c.allocID()
	c.mu.Unlock()
	pk := &packet.Packet{
		Fixed:    packet.FixedHeader{Type: packet.TypeUNSUBSCRIBE},
		PacketID: id,
	}
	if c.cfg.ProtocolLevel == packet.V50 {
		pk.Props = packet.NewProperties()
	}
	for _, f := range filters {
		pk.Filters = append(pk.Filters, packet.SubFilter{Filter: f})
	}
	w := make(chan *packet.Packet, 1)
	c.ackMu.Lock()
	c.ackWaiter[id] = w
	c.ackMu.Unlock()
	defer func() {
		c.ackMu.Lock()
		delete(c.ackWaiter, id)
		c.ackMu.Unlock()
	}()
	if err := c.writePacket(pk); err != nil {
		return err
	}
	select {
	case <-w:
	case <-ctx.Done():
		return ctx.Err()
	case <-time.After(5 * time.Second):
		return mqerr.New(mqerr.ClassState, mqerr.CodeImplSpecific, "timeout waiting UNSUBACK")
	case <-c.disconnectCh:
		return mqerr.New(mqerr.ClassState, mqerr.CodeImplSpecific, "client disconnected while unsubscribing")
	}
	c.mu.Lock()
	for _, f := range filters {
		for i := len(c.subs) - 1; i >= 0; i-- {
			if c.subs[i].filter == f {
				c.subs = append(c.subs[:i], c.subs[i+1:]...)
			}
		}
	}
	c.mu.Unlock()
	return nil
}

// deliverAckToWaiter resolves SUBACK/UNSUBACK waiters.
func (c *Client) deliverAckToWaiter(p *packet.Packet) {
	c.ackMu.Lock()
	w := c.ackWaiter[p.PacketID]
	c.ackMu.Unlock()
	if w != nil {
		w <- p
	}
}
