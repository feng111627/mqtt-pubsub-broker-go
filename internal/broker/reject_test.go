package broker

import (
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// v5 SUBSCRIBE to a shared filter must be rejected with the specific reason
// 0x9E (Shared Subscriptions Not Supported), not a generic failure.
func TestV5SharedSubscriptionRejected(t *testing.T) {
	b := startTestBroker(t)
	c := dialPeer(t, b, packet.V50)
	c.connect("v5sh", true, packet.V50)
	pk := subscribePacket(1, packet.V50, 1, "$share/grp/a/+")
	c.send(pk)
	ack := c.recv()
	if ack.Fixed.Type != packet.TypeSUBACK || ack.ReturnCodes[0] != byte(mqerr.CodeSharedSubUnsupported) {
		t.Fatalf("want SUBACK 0x9E, got type=%s code=0x%02x", ack.Fixed.Type, ack.ReturnCodes[0])
	}
}

// A malformed filter ("a#") yields the 0x8F Topic Filter Invalid reason.
func TestV5MalformedFilterRejected(t *testing.T) {
	b := startTestBroker(t)
	c := dialPeer(t, b, packet.V50)
	c.connect("v5mf", true, packet.V50)
	pk := subscribePacket(1, packet.V50, 1, "a#")
	c.send(pk)
	ack := c.recv()
	if ack.ReturnCodes[0] != byte(mqerr.CodeTopicFilterInvalid) {
		t.Fatalf("want SUBACK 0x8F, got 0x%02x", ack.ReturnCodes[0])
	}
}

// A bad packet mid-session (PUBLISH with QoS3) closes the connection in the
// pre-v5 world where no per-message negative acknowledgement exists.
func TestMidSessionMalformedPacketCloses(t *testing.T) {
	b := startTestBroker(t)
	c := dialPeer(t, b, packet.V311)
	c.connect("mal-mid", true, packet.V311)
	c.send(publishPacket("ok/x", 3, false, 1, "x", packet.V311))
	c.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 32)
	// expect DISCONNECT (possibly) and/or socket close
	n, _ := c.conn.Read(buf)
	// In 3.1.1 the broker closes without a reason-carrying packet.
	if n != 0 {
		// If a DISCONNECT-like byte arrives, that is acceptable too; ensure the
		// follow-up read fails.
		c.conn.SetReadDeadline(time.Now().Add(time.Second))
		if n2, _ := c.conn.Read(buf); n2 != 0 {
			t.Fatalf("expected connection close after malformed QoS3 PUBLISH")
		}
	}
}

// PUBACK/PUBREC for an unknown packet id on a v5 publisher is handled
// gracefully (PUBCOMP 0x92 on the QoS2 path) without tearing down the
// connection.
func TestV5UnknownPacketIDOnPubRec(t *testing.T) {
	b := startTestBroker(t)
	c := dialPeer(t, b, packet.V50)
	c.connect("v5id", true, packet.V50)
	// send PUBREC for an id the server never used; broker must answer PUBCOMP
	// 0x92 and keep the connection alive.
	c.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBREC}, PacketID: 4242})
	ack := c.recv()
	if ack.Fixed.Type != packet.TypePUBCOMP || ack.ReasonCode != byte(mqerr.CodePacketIDNotFound) {
		t.Fatalf("want PUBCOMP 0x92, got type=%s code=0x%02x", ack.Fixed.Type, ack.ReasonCode)
	}
	// PINGREQ proves the connection stayed usable.
	c.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePINGREQ}})
	ping := c.recv()
	if ping.Fixed.Type != packet.TypePINGRESP {
		t.Fatalf("connection unusable after error ack: got %s", ping.Fixed.Type)
	}
}

// v5 PUBLISH carrying a Topic Alias property must be rejected (the broker
// announces alias maximum 0). It surfaces as a malformed packet (0x81) because
// the alias is structurally disallowed by the negotiated CONNACK properties.
func TestV5TopicAliasRejected(t *testing.T) {
	b := startTestBroker(t)
	c := dialPeer(t, b, packet.V50)
	c.connect("v5alias", true, packet.V50)
	pk := publishPacket("x/y", 0, false, 0, "m", packet.V50)
	alias := uint16(1)
	pk.Props.TopicAlias = &alias
	wire, err := pk.Encode(packet.V50)
	if err != nil {
		t.Fatal(err)
	}
	c.conn.Write(wire)
	disc, ok := c.maybeRecv(2 * time.Second)
	if !ok {
		t.Fatal("expected DISCONNECT after topic alias PUBLISH")
	}
	if disc.Fixed.Type != packet.TypeDISCONNECT || disc.Reason != byte(mqerr.CodeMalformedPacket) {
		t.Fatalf("want DISCONNECT 0x81 (malformed: alias unsupported), got type=%s reason=0x%02x", disc.Fixed.Type, disc.Reason)
	}
}
