package broker

import (
	"sort"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// ---- SUBSCRIBE / UNSUBSCRIBE --------------------------------------------

func (c *clientConn) handleSubscribe(p *packet.Packet) error {
	s := c.session
	codes := make([]byte, len(p.Filters))
	// v5: shared subscriptions ($share/...) are not supported.
	if c.level == packet.V50 && len(p.Props.AuthMethod) > 0 {
		// not a real condition; placeholder removed below
	}
	for i, f := range p.Filters {
		if len(f.Filter) >= len("$share/") && f.Filter[:len("$share/")] == "$share/" {
			if c.level == packet.V50 {
				codes[i] = byte(mqerr.CodeSharedSubUnsupported)
			} else {
				codes[i] = byte(mqerr.CodeTopicFilterInvalid)
			}
			continue
		}
		if !packet.ValidFilter(f.Filter) {
			if c.level == packet.V50 {
				codes[i] = byte(mqerr.CodeTopicFilterInvalid)
			} else {
				codes[i] = 0x80
			}
			continue
		}
		// Grant no more than the requested QoS (and never above 2).
		granted := f.QoS
		if granted > 2 {
			granted = 2
		}
		sub := &subscription{
			Filter:         f.Filter,
			QoS:            granted,
			NoLocal:        f.NoLocal,
			RetainAsPub:    f.RetainAsPub,
			RetainHandling: f.RetainHandling,
		}
		// Retain handling decides whether retained messages are sent:
		//   0 = always (default for 3.1.1),
		//   1 = only when the subscription is new,
		//   2 = never.
		rh := byte(0)
		if c.level == packet.V50 {
			rh = f.RetainHandling
		}
		s.mu.Lock()
		_, existed := s.subs[f.Filter]
		s.subs[f.Filter] = sub
		s.mu.Unlock()

		c.broker.routeMu.Lock()
		c.broker.trie.add(s.UID, sub)
		c.broker.routeMu.Unlock()
		codes[i] = granted

		if rh == 2 || (rh == 1 && existed) {
			continue
		}
		c.deliverRetained(f.Filter, sub, s)
	}
	rsp := &packet.Packet{
		Fixed:       packet.FixedHeader{Type: packet.TypeSUBACK},
		PacketID:    p.PacketID,
		ReturnCodes: codes,
	}
	if c.level == packet.V50 {
		rsp.Props = packet.NewProperties()
	}
	return c.writePacket(rsp)
}

func (c *clientConn) handleUnsubscribe(p *packet.Packet) error {
	s := c.session
	codes := []byte{}
	for _, f := range p.Filters {
		s.mu.Lock()
		delete(s.subs, f.Filter)
		s.mu.Unlock()
		c.broker.routeMu.Lock()
		c.broker.trie.remove(s.UID, f.Filter)
		c.broker.routeMu.Unlock()
		if c.level == packet.V50 {
			codes = append(codes, 0)
		}
	}
	rsp := &packet.Packet{
		Fixed:    packet.FixedHeader{Type: packet.TypeUNSUBACK},
		PacketID: p.PacketID,
	}
	if c.level == packet.V50 {
		rsp.Props = packet.NewProperties()
		rsp.ReturnCodes = codes
	}
	return c.writePacket(rsp)
}

// deliverRetained enqueues matching retained messages as if freshly published,
// honoring Retain As Published (3.1.1 sends them with retain flag set; v5 only
// sets it when the subscription requested RAP=1).
func (c *clientConn) deliverRetained(filter string, sub *subscription, s *Session) {
	c.broker.routeMu.RLock()
	var msgs []*retainedMessage
	for _, rm := range c.broker.retained {
		if packet.Match(filter, rm.topic) {
			msgs = append(msgs, rm)
		}
	}
	c.broker.routeMu.RUnlock()
	sort.Slice(msgs, func(i, j int) bool { return msgs[i].topic < msgs[j].topic })
	for _, rm := range msgs {
		qos := sub.QoS
		if rm.qos < qos {
			qos = rm.qos
		}
		retainFlag := true
		if c.level == packet.V50 && !sub.RetainAsPub {
			retainFlag = false
		}
		c.enqueueTo(s, &queuedMessage{
			QoS:     qos,
			Retain:  retainFlag,
			Topic:   rm.topic,
			Payload: append([]byte(nil), rm.payload...),
		})
	}
}

// ---- routing -------------------------------------------------------------

// route distributes one application message.
//
//   - retain=true with empty payload clears the retained slot; otherwise the
//     last retained value is replaced (QoS0 retained clears when payload empty
//     at any QoS; spec ties storage to PUBLISH retain bit).
//   - the live delivery fan-out uses the routing trie; sessions that are
//     offline still receive durable QoS1/2 copies via their session queue.
//   - publisher exclusion (NoLocal) and QoS downgrade are applied here.
func (b *Broker) route(topic string, qos byte, payload []byte, retain bool, publisherUID uint64, props packet.Properties, subID int) {
	if !packet.ValidTopicName(topic) {
		return // callers validate; defensive
	}
	if retain {
		b.routeMu.Lock()
		if len(payload) == 0 {
			delete(b.retained, topic)
		} else {
			b.retained[topic] = &retainedMessage{
				qos:     qos,
				payload: append([]byte(nil), payload...),
				topic:   topic,
			}
		}
		b.routeMu.Unlock()
	}
	b.routeMu.RLock()
	hits := b.trie.match(topic)
	b.routeMu.RUnlock()
	for _, h := range hits {
		if h.NoLocal && h.SessionID == publisherUID {
			continue
		}
		deliverQoS := qos
		if h.QoS < deliverQoS {
			deliverQoS = h.QoS
		}
		b.mu.Lock()
		sess := b.sessionByUID[h.SessionID]
		b.mu.Unlock()
		if sess == nil {
			continue
		}
		// Live fan-out must not carry the publisher's retain flag: retained
		// values are only delivered as retained on subscription.
		b.enqueueTo(sess, &queuedMessage{
			QoS:     deliverQoS,
			Retain:  false,
			Topic:   topic,
			Payload: append([]byte(nil), payload...),
		})
	}
}

// enqueueTo adds a message to a session and, when online, wakes the pump.
func (b *Broker) enqueueTo(s *Session, m *queuedMessage) {
	s.mu.Lock()
	online := s.connected != nil
	if !s.enqueue(m) {
		s.mu.Unlock()
		return
	}
	conn := s.connected
	s.mu.Unlock()
	if online && conn != nil {
		conn.kick()
	}
}

func (c *clientConn) enqueueTo(s *Session, m *queuedMessage) {
	c.broker.enqueueTo(s, m)
}

// ---- outbound pump -------------------------------------------------------

// drainQueue sends every currently eligible queued message, respecting the
// receive maximum. It is safe to call repeatedly from the owning loop.
func (c *clientConn) drainQueue(reason string) {
	s := c.session
	for {
		s.mu.Lock()
		if s.connected != c {
			s.mu.Unlock()
			return
		}
		m := s.takeQueued(int(c.broker.cfg.ReceiveMaximum))
		if m == nil {
			s.mu.Unlock()
			return
		}
		s.markInflight(m)
		s.mu.Unlock()

		p := &packet.Packet{
			Topic:    m.Topic,
			Payload:  append([]byte(nil), m.Payload...),
			PacketID: m.ID,
		}
		var flags byte
		flags |= m.QoS << 1
		if m.Retain {
			flags |= 0x01
		}
		if m.Dup {
			flags |= 0x08
		}
		p.Fixed = packet.FixedHeader{Type: packet.TypePUBLISH, Flags: flags}
		if c.level == packet.V50 {
			p.Props = packet.NewProperties()
		}
		if err := c.writePacket(p); err != nil {
			// Undo inflight so a later reconnect redelivers.
			s.mu.Lock()
			delete(s.inflight, m.ID)
			m.Inflight = false
			s.mu.Unlock()
			return
		}
		if m.QoS == 0 {
			s.mu.Lock()
			s.dropQueueEntry(m)
			s.mu.Unlock()
		}
	}
}

// resendPendingQoS2 re-emits PUBREL packets for QoS2 flows that had passed
// PUBREC before the (re)attached connection existed.
func (c *clientConn) resendPendingQoS2() {
	s := c.session
	s.mu.Lock()
	ids := make([]uint16, 0, len(s.pendingQoS2))
	for id := range s.pendingQoS2 {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	sort.Slice(ids, func(i, j int) bool { return ids[i] < ids[j] })
	for _, id := range ids {
		if err := c.sendPubRel(id); err != nil {
			return
		}
	}
}

// pumpLoop is the single writer-side driver for one connection. It waits for
// queue work signals and a periodic tick (to recover from missed wakes).
func (c *clientConn) pumpLoop(done chan struct{}) {
	defer close(done)
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-c.stopCh:
			return
		case <-c.takeover:
			return
		case <-c.kickCh:
		case <-ticker.C:
		}
		c.drainQueue("pump wake")
	}
}
