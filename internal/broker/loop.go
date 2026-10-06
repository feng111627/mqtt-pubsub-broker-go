package broker

import (
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// runPumps starts the keepalive timer and drives the read loop. The outbound
// pump for this connection is driven from the same goroutine via explicit
// drain calls after every state change, which keeps session state
// single-threaded per connection (no lock races inside a session's queue).
func (c *clientConn) runPumps() {
	s := c.session
	pumpDone := make(chan struct{})
	go c.pumpLoop(pumpDone)
	// On session resume, redeliver stale inflight QoS2 PUBRELs first, then
	// release queued messages.
	c.resendPendingQoS2()
	c.kick()

	// Keep-alive watchdog.
	kaStop := make(chan struct{})
	if c.keepAlive > 0 {
		go c.keepaliveWatchdog(kaStop)
	}
	defer close(kaStop)

	// Publish retained messages for a freshly established clean subscription
	// or a resumed session respecting Retain Handling — done by SUBSCRIBE.

	cleanClose := false
	for {
		c.conn.SetReadDeadline(time.Time{})
		p, err := c.reader.ReadOne()
		if err != nil {
			if me, ok := err.(*mqerr.Error); ok && me.Class == mqerr.ClassConnection {
				// transport gone (including takeover-induced close): no will
				// unless the takeover path already handled semantics.
			} else if err != nil {
				c.closeWith(0x81)
			}
			break
		}
		c.touch()

		clo, fatal, perr := c.dispatch(p)
		if perr != nil {
			// protocol error: reply/close path already taken by the handler
			_ = perr
		}
		c.kick()
		if fatal {
			cleanClose = clo
			break
		}
		if clo {
			cleanClose = true
			break
		}
	}
	// Session teardown / offline transition. A graceful DISCONNECT or a
	// takeover suppresses the will; an abrupt transport loss publishes it.
	c.broker.onDisconnect(s, c, cleanClose || c.taken)
}

func (c *clientConn) keepaliveWatchdog(stop chan struct{}) {
	interval := c.keepAlive / 4
	if interval <= 0 {
		interval = time.Second
	}
	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-stop:
			return
		case <-c.stopCh:
			return
		case <-c.takeover:
			return
		case <-t.C:
			if c.keepaliveExpired() {
				// 5.0: send DISCONNECT 0x8D first when possible.
				c.closeWith(byte(mqerr.CodeKeepAliveTimeout))
				return
			}
		}
	}
}

// dispatch processes one inbound packet. Returns:
//
//	requestClose  — the loop should end;
//	clean         — the end is a graceful DISCONNECT (suppress will);
//	err           — a protocol/state error that caused termination.
func (c *clientConn) dispatch(p *packet.Packet) (clean bool, fatal bool, err error) {
	switch p.Fixed.Type {
	case packet.TypePUBLISH:
		return false, false, c.handlePublish(p)
	case packet.TypePUBACK:
		return false, false, c.handlePubAck(p)
	case packet.TypePUBREC:
		return false, false, c.handlePubRec(p)
	case packet.TypePUBREL:
		if p.Fixed.Flags != 0x02 {
			c.closeWith(0x82)
			return false, true, mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "PUBREL flags must be 0010")
		}
		return false, false, c.handlePubRel(p)
	case packet.TypePUBCOMP:
		return false, false, c.handlePubComp(p)
	case packet.TypeSUBSCRIBE:
		if p.Fixed.Flags != 0x02 {
			c.closeWith(0x82)
			return false, true, mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "SUBSCRIBE flags invalid")
		}
		return false, false, c.handleSubscribe(p)
	case packet.TypeUNSUBSCRIBE:
		if p.Fixed.Flags != 0x02 {
			c.closeWith(0x82)
			return false, true, mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "UNSUBSCRIBE flags invalid")
		}
		return false, false, c.handleUnsubscribe(p)
	case packet.TypePINGREQ:
		rsp := &packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePINGRESP}}
		if werr := c.writePacket(rsp); werr != nil {
			return false, true, werr
		}
		return false, false, nil
	case packet.TypeDISCONNECT:
		if c.level == packet.V50 && p.Reason != 0 {
			// Only 0x00 is a normal client disconnect. 0x04 etc. still mean the
			// client went away cleanly (will suppressed); other codes (e.g.
			// 0x8D keep alive) are abnormal but originate client-side.
			switch p.Reason {
			case 0x04, 0x80, 0x8D, 0x8E, 0x98, 0x99, 0x9A, 0x9B:
				return false, true, nil
			}
		}
		return true, false, nil
	case packet.TypeAUTH:
		// enhanced auth unsupported: already rejected at CONNECT.
		c.closeWith(byte(mqerr.CodeBadAuthMethod))
		return false, true, mqerr.New(mqerr.ClassProtocol, mqerr.CodeBadAuthMethod, "AUTH not supported")
	default:
		c.closeWith(byte(mqerr.CodeProtocolError))
		return false, true, mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "unexpected packet %s", p.Fixed.Type)
	}
}

// ---- inbound PUBLISH -----------------------------------------------------

func (c *clientConn) handlePublish(p *packet.Packet) error {
	s := c.session
	qos := p.Fixed.QoS()
	topic := p.Topic
	if !packet.ValidTopicName(topic) {
		return c.rejectPublish(p, mqerr.CodeTopicNameInvalid, "invalid publish topic %q", topic)
	}
	// Retained handling: retained messages are only stored for QoS0/1.
	retain := p.Fixed.Retain()

	switch qos {
	case packet.QoS0:
		c.broker.route(topic, 0, p.Payload, retain, s.UID, p.Props, -1)
		return nil
	case packet.QoS1:
		id := p.PacketID
		s.mu.Lock()
		if _, dup := s.inboundQoS1[id]; dup {
			s.mu.Unlock()
			// Redelivery of a still-unacknowledged QoS1 id: per spec respond
			// 0x91 Packet Identifier in use on MQTT5; 3.1.1 has no negative ack
			// shape so retransmit PUBACK (idempotent consumer view).
			if c.level == packet.V50 {
				return c.sendAck(packet.TypePUBACK, id, byte(mqerr.CodePacketIDInUse))
			}
			return c.sendAck(packet.TypePUBACK, id, 0)
		}
		s.inboundQoS1[id] = struct{}{}
		s.mu.Unlock()
		c.broker.route(topic, 1, p.Payload, retain, s.UID, p.Props, -1)
		return c.sendAck(packet.TypePUBACK, id, 0)
	case packet.QoS2:
		id := p.PacketID
		s.mu.Lock()
		stage, seen := s.inboundQoS2[id]
		s.mu.Unlock()
		if seen && !stage {
			// Duplicate initial PUBLISH: retransmit PUBREC, do not redeliver.
			return c.sendAck(packet.TypePUBREC, id, 0)
		}
		if seen {
			// Already at PUBREL stage; duplicate PUBLISH is a protocol misuse.
			if c.level == packet.V50 {
				return c.sendAck(packet.TypePUBREC, id, byte(mqerr.CodeProtocolError))
			}
			return c.sendAck(packet.TypePUBREC, id, 0)
		}
		s.mu.Lock()
		s.inboundQoS2[id] = false // awaiting PUBREL
		s.mu.Unlock()
		c.broker.route(topic, 2, p.Payload, false, s.UID, p.Props, -1)
		return c.sendAck(packet.TypePUBREC, id, 0)
	}
	return nil
}

func (c *clientConn) handlePubRel(p *packet.Packet) error {
	s := c.session
	id := p.PacketID
	s.mu.Lock()
	stage, seen := s.inboundQoS2[id]
	if !seen {
		s.mu.Unlock()
		if c.level == packet.V50 {
			return c.sendAck(packet.TypePUBCOMP, id, byte(mqerr.CodePacketIDNotFound))
		}
		// 3.1.1: still answer PUBCOMP to allow client state to converge.
		return c.sendAck(packet.TypePUBCOMP, id, 0)
	}
	_ = stage
	delete(s.inboundQoS2, id) // exactly-once barrier released after PUBREL
	s.mu.Unlock()
	return c.sendAck(packet.TypePUBCOMP, id, 0)
}

// rejectPublish answers an invalid PUBLISH without tearing down the connection
// when the packet is individually rejectable (v5); pre-v5 has no per-message
// negative ack so the connection is closed per spec.
func (c *clientConn) rejectPublish(p *packet.Packet, code mqerr.Code, format string, args ...any) error {
	e := mqerr.New(mqerr.ClassProtocol, code, format, args...)
	if c.level == packet.V50 {
		switch p.Fixed.QoS() {
		case 1:
			_ = c.sendAck(packet.TypePUBACK, p.PacketID, byte(code))
		case 2:
			_ = c.sendAck(packet.TypePUBREC, p.PacketID, byte(code))
		}
		return e
	}
	c.closeWith(0x82)
	return e
}

func (c *clientConn) sendAck(typ packet.Type, id uint16, reason byte) error {
	p := &packet.Packet{Fixed: packet.FixedHeader{Type: typ}, PacketID: id, ReasonCode: reason}
	return c.writePacket(p)
}

// ---- server-side outbound ack handling (QoS publisher was *us*) ----------

func (c *clientConn) handlePubAck(p *packet.Packet) error {
	return c.completeOutbound(p.PacketID, 1, false, p.ReasonCode)
}

func (c *clientConn) handlePubRec(p *packet.Packet) error {
	s := c.session
	s.mu.Lock()
	m := s.removeInflight(p.PacketID)
	if m != nil && m.QoS == 2 {
		s.pendingQoS2[p.PacketID] = m
		m.QoS2RelSent = false
	}
	s.mu.Unlock()
	if m == nil {
		if c.level == packet.V50 {
			// Unexpected PUBREC: answer PUBREL with 0x92 not-found? There is no
			// negative PUBREL path for unknown ids other than PUBCOMP 0x92.
			return c.sendAck(packet.TypePUBCOMP, p.PacketID, byte(mqerr.CodePacketIDNotFound))
		}
		return nil
	}
	return c.sendPubRel(p.PacketID)
}

func (c *clientConn) handlePubComp(p *packet.Packet) error {
	return c.completeOutbound(p.PacketID, 2, true, p.ReasonCode)
}

// sendPubRel emits the PUBREL half of QoS2.
func (c *clientConn) sendPubRel(id uint16) error {
	p := &packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBREL, Flags: 0x02}, PacketID: id}
	return c.writePacket(p)
}

// completeOutbound finishes server-published QoS1 (PUBACK) and QoS2 (PUBCOMP).
func (c *clientConn) completeOutbound(id uint16, qos byte, qos2Comp bool, reason byte) error {
	s := c.session
	s.mu.Lock()
	var m *queuedMessage
	if qos2Comp {
		m = s.pendingQoS2[id]
		delete(s.pendingQoS2, id)
	} else {
		m = s.removeInflight(id)
	}
	if m != nil {
		s.dropQueueEntry(m)
	}
	s.mu.Unlock()
	if m == nil && reason == 0 {
		// Duplicate/late ack: benign for idempotence; for v5 unknown ids the
		// spec suggests no action on PUBACK; simply ignore.
		return nil
	}
	return nil
}
