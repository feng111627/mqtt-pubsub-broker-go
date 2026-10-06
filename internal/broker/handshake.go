package broker

import (
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// handshake performs CONNECT read, validation, session (re)binding and the
// CONNACK reply. Any rejection is answered with the version-appropriate code.
func (c *clientConn) handshake() error {
	// Give peers a bounded window to send CONNECT.
	c.conn.SetReadDeadline(time.Now().Add(10 * time.Second))
	p, err := c.reader.ReadOne()
	c.conn.SetReadDeadline(time.Time{})
	if err != nil {
		// A malformed-but-readable CONNECT with an unsupported version still
		// deserves a CONNACK; an unreadable transport simply drops.
		c.rejectUnreadable(err)
		return err
	}
	if p.Fixed.Type != packet.TypeCONNECT {
		// First packet must be CONNECT. v5 peers get 0x84 protocol error.
		c.closeWith(0x84)
		return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "first packet was %s, expected CONNECT", p.Fixed.Type)
	}

	c.level = p.ProtoLevel
	c.reader.SetLevel(c.level)

	// Identifier rules: 3.1 allows empty id with CleanSession=1 and assigns
	// one; 3.1.1/5.0 empty id is legal only with CleanStart=1 (server assigns).
	clientID := p.ClientID
	clean := p.CleanStart
	if clientID == "" {
		if !clean {
			c.sendConnackReject(p.ProtoLevel, emptyIDCode(p.ProtoLevel))
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeIdentifierRejected, "empty client id with clean=false")
		}
		clientID = c.broker.generateClientID()
	}
	if len(clientID) > 23 && p.ProtoLevel == packet.V31 {
		c.sendConnackReject(packet.V31, 0x02)
		return mqerr.New(mqerr.ClassProtocol, mqerr.CodeIdentifierRejected, "MQTT 3.1 client id exceeds 23 characters")
	}

	if c.broker.cfg.Auth != nil {
		user := ""
		if p.HasUser {
			user = p.Username
		}
		if err := c.broker.cfg.Auth(user, p.Password); err != nil {
			c.rejectAuth(p.ProtoLevel, err)
			return err
		}
	}

	var will *willMessage
	if p.WillFlag {
		will = &willMessage{
			topic:   p.WillTopic,
			payload: append([]byte(nil), p.WillMsg...),
			qos:     p.WillQoS,
			retain:  p.WillRetain,
			props:   p.WillProps,
		}
	}

	sess, present, oldConn := c.broker.lookupOrCreateSession(c.level, clientID, clean, will)
	c.session = sess
	c.uid = sess.UID

	// Take over an existing live connection (MQTT: old session must be
	// disconnected exactly as if it had sent DISCONNECT, but its will is not
	// republished).
	if oldConn != nil {
		oldConn.markTakenOver()
	}

	// Bind this socket to the session before replying so an immediate client
	// action after CONNACK finds an attached session.
	c.kickCh = make(chan struct{}, 1)
	c.takeover = make(chan struct{})
	sess.attach(c)

	// Keep-alive: factor 1.5 on the client value; v5 server may override.
	if p.KeepAlive > 0 {
		c.keepAlive = time.Duration(float64(p.KeepAlive)*c.broker.cfg.KeepAliveFactor) * time.Second
	}
	if level := c.level; level == packet.V50 {
		// Nothing negotiated back for now (no keepalive override); defaults apply.
	}
	c.touch()

	ack := &packet.Packet{
		Fixed:          packet.FixedHeader{Type: packet.TypeCONNACK},
		SessionPresent: present && !clean, // must be false for clean start per spec
		ReasonCode:     0x00,
	}
	if c.level == packet.V50 {
		props := packet.NewProperties()
		rm := c.broker.cfg.ReceiveMaximum
		props.ReceiveMaximum = &rm
		maxPS := uint32(c.broker.cfg.MaxPacketSize)
		props.MaxPacketSize = &maxPS
		topicAliasMax := uint16(0)
		props.TopicAliasMax = &topicAliasMax
		ack.Props = props
	}
	if err := c.writePacket(ack); err != nil {
		return err
	}
	c.handshakeDone = true
	return nil
}

// rejectUnreadable handles errors raised while reading CONNECT. When the cause
// is an unsupported version we did at least decode the name/level pair, which
// the decoder reports as ClassProtocol CodeUnsupportedProtoV before bailing:
// answer with a version-rejection CONNACK so the client can downgrade.
func (c *clientConn) rejectUnreadable(err error) {
	me, ok := err.(*mqerr.Error)
	if !ok {
		c.stopOnce.Do(func() { c.conn.Close() })
		return
	}
	switch me.Code {
	case mqerr.CodeUnsupportedProtoV:
		// Level unknown; reply in 3.1.1 shape (code 0x01).
		c.sendConnackReject(packet.V311, 0x01)
	default:
		// Malformed CONNECT: for v3 there is no valid CONNACK shape without a
		// parsed level; close the connection (the distinguishable cause is
		// surfaced server-side in logs/tests).
		c.stopOnce.Do(func() { c.conn.Close() })
	}
}

func (c *clientConn) rejectAuth(level byte, err error) {
	code := byte(0x05)
	if mqerr.Of(err, mqerr.CodeBadUsernameOrPwd) || mqerr.Of(err, mqerr.CodeBadCred311) {
		if level == packet.V50 {
			code = byte(mqerr.CodeBadUsernameOrPwd)
		} else {
			code = 0x04
		}
	}
	c.sendConnackReject(level, code)
}

func (c *clientConn) sendConnackReject(level byte, code byte) {
	p := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNACK},
		ReasonCode: code,
	}
	if level == packet.V50 {
		p.Props = packet.NewProperties()
		p.ReasonCode = v5RejectCode(code)
	}
	_ = c.writePacket(p)
	c.stopOnce.Do(func() { c.conn.Close() })
}

// v5RejectCode maps 3.1.1 CONNACK codes onto MQTT 5 reason codes.
func v5RejectCode(code byte) byte {
	switch code {
	case 0x01:
		return byte(mqerr.CodeUnsupportedProtoV)
	case 0x02:
		return byte(mqerr.CodeImplSpecific)
	case 0x03:
		return byte(mqerr.CodeServerBusy)
	case 0x04:
		return byte(mqerr.CodeBadUsernameOrPwd)
	case 0x05:
		return byte(mqerr.CodeNotAuthorized)
	default:
		return code
	}
}

func emptyIDCode(level byte) byte {
	if level == packet.V50 {
		return byte(mqerr.CodeImplSpecific)
	}
	return 0x02 // identifier rejected
}

// generateClientID assigns an opaque id for anonymous clean sessions.
func (b *Broker) generateClientID() string {
	b.uidSeq++
	return "auto-" + itoa(b.uidSeq) + "-" + randomSuffix()
}
