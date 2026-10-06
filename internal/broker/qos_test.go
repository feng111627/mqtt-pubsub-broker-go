package broker

import (
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// QoS1 message published while the persistent subscriber is offline must be
// queued and delivered (DUP=0 first time) on reconnect.
func TestQoS1QueuedAndDeliveredOnReconnect(t *testing.T) {
	b := startTestBroker(t)

	sub := dialPeer(t, b, packet.V311)
	sub.connect("qos1-sub", false, packet.V311)
	sub.send(subscribePacket(1, packet.V311, 1, "q/1"))
	sub.recv()
	sub.close()
	waitUntil(t, time.Second, func() bool { return b.Stats().Sessions == 1 })

	pub := dialPeer(t, b, packet.V311)
	pub.connect("qos1-pub", true, packet.V311)
	pub.send(publishPacket("q/1", 1, false, 20, "late", packet.V311))
	pub.close()
	time.Sleep(100 * time.Millisecond)

	sub2 := dialPeer(t, b, packet.V311)
	ack := sub2.connect("qos1-sub", false, packet.V311)
	if !ack.SessionPresent {
		t.Fatal("resumed session should be present")
	}
	got := sub2.recv()
	if got.Topic != "q/1" || string(got.Payload) != "late" {
		t.Fatalf("queued message mismatch: %+v", got)
	}
	if got.Fixed.Dup() {
		t.Fatal("first delivery after reconnect must have DUP=0")
	}
	if got.Fixed.QoS() != 1 || got.PacketID == 0 {
		t.Fatalf("qos/id mismatch: qos=%d id=%d", got.Fixed.QoS(), got.PacketID)
	}
}

// When a QoS1 PUBLISH is delivered but the connection drops before PUBACK,
// reconnect must redeliver the same message with DUP=1 and no duplicate
// application consumption happens (one stored copy).
func TestQoS1RedeliveryMarksDup(t *testing.T) {
	b := startTestBroker(t)

	sub := dialPeer(t, b, packet.V311)
	sub.connect("qos1r-sub", false, packet.V311)
	sub.send(subscribePacket(1, packet.V311, 1, "q/r"))
	sub.recv()

	pub := dialPeer(t, b, packet.V311)
	pub.connect("qos1r-pub", true, packet.V311)
	pub.send(publishPacket("q/r", 1, false, 30, "once", packet.V311))

	first := sub.recv()
	if first.Fixed.Dup() {
		t.Fatal("first delivery must be DUP=0")
	}
	firstID := first.PacketID
	// subscriber crashes BEFORE PUBACK
	sub.close()
	time.Sleep(100 * time.Millisecond)

	sub2 := dialPeer(t, b, packet.V311)
	sub2.connect("qos1r-sub", false, packet.V311)
	second := sub2.recv()
	if string(second.Payload) != "once" {
		t.Fatalf("redelivery payload mismatch: %s", second.Payload)
	}
	if !second.Fixed.Dup() {
		t.Fatal("redelivery must set DUP=1")
	}
	if second.PacketID != firstID {
		t.Fatalf("redelivery must reuse packet identifier: %d vs %d", second.PacketID, firstID)
	}
	// exactly one stored copy: after ACK there must be no further delivery.
	sub2.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: second.PacketID})
	if pk, ok := sub2.maybeRecv(300 * time.Millisecond); ok {
		t.Fatalf("no further delivery expected after PUBACK, got %+v", pk)
	}
}

// Full QoS2 handshake from subscriber perspective (server as sender):
// PUBLISH(server) -> PUBREC(client) -> PUBREL(server) -> PUBCOMP(client).
func TestQoS2HandshakeServerPublishes(t *testing.T) {
	b := startTestBroker(t)
	sub := dialPeer(t, b, packet.V311)
	sub.connect("qos2-sub", false, packet.V311)
	sub.send(subscribePacket(1, packet.V311, 2, "q/2"))
	sub.recv()

	pub := dialPeer(t, b, packet.V311)
	pub.connect("qos2-pub", true, packet.V311)
	pub.send(publishPacket("q/2", 2, false, 40, "exact", packet.V311))

	d1 := sub.recv()
	if d1.Fixed.QoS() != 2 || string(d1.Payload) != "exact" {
		t.Fatalf("qos2 publish mismatch: %+v", d1)
	}
	sub.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBREC}, PacketID: d1.PacketID})
	rel := sub.recv()
	if rel.Fixed.Type != packet.TypePUBREL || rel.PacketID != d1.PacketID || rel.Fixed.Flags != 0x02 {
		t.Fatalf("expected PUBREL, got %+v", rel)
	}
	sub.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBCOMP}, PacketID: rel.PacketID})
	time.Sleep(100 * time.Millisecond)
}

// QoS2 duplicate initial PUBLISH (client->server) must not deliver twice:
// the server retransmits PUBREC and routes the message only once. We observe
// exactly one downstream copy via a second subscriber.
func TestQoS2DuplicatePublishFromClientDeliveredOnce(t *testing.T) {
	b := startTestBroker(t)
	sink := dialPeer(t, b, packet.V311)
	sink.connect("qos2-sink", true, packet.V311)
	sink.send(subscribePacket(1, packet.V311, 2, "q/dup"))
	sink.recv()

	sender := dialPeer(t, b, packet.V311)
	sender.connect("qos2-sender", true, packet.V311)
	sender.send(publishPacket("q/dup", 2, false, 50, "dup-msg", packet.V311))

	// sink receives the one routed PUBLISH and sends PUBREC
	d := sink.recv()
	if string(d.Payload) != "dup-msg" {
		t.Fatalf("payload: %s", d.Payload)
	}
	sink.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBREC}, PacketID: d.PacketID})
	rel := sink.recv()
	sink.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBCOMP}, PacketID: rel.PacketID})

	// the *sender* (a different connection acting as publisher client)
	// retransmits the same QoS2 PUBLISH with the same id and DUP=1
	retry := publishPacket("q/dup", 2, false, 50, "dup-msg", packet.V311)
	retry.Fixed.Flags |= 0x08 // DUP
	sender.send(retry)
	rec1 := sender.recv() // PUBREC
	if rec1.Fixed.Type != packet.TypePUBREC {
		t.Fatalf("expected PUBREC for duplicate, got %s", rec1.Fixed.Type)
	}
	// sink must not see a second application copy
	if pk, ok := sink.maybeRecv(400 * time.Millisecond); ok {
		t.Fatalf("message delivered twice on duplicate QoS2 PUBLISH: %+v", pk)
	}
}

// A second SUBSCRIBE on a live connection replaces the existing subscription
// options without duplicating routing state.
func TestResubscribeDoesNotDuplicate(t *testing.T) {
	b := startTestBroker(t)
	sub := dialPeer(t, b, packet.V50)
	sub.connect("resub", true, packet.V50)
	sub.send(subscribePacket(1, packet.V50, 1, "a/b"))
	if r := sub.recv(); r.Fixed.Type != packet.TypeSUBACK {
		t.Fatal("want SUBACK")
	}
	sub.send(subscribePacket(2, packet.V50, 0, "a/b"))
	sub.recv()

	pub := dialPeer(t, b, packet.V50)
	pub.connect("resub-pub", true, packet.V50)
	pub.reader.SetLevel(packet.V50)
	pub.send(publishPacket("a/b", 1, false, 9, "x", packet.V50))
	// Exactly one delivery (at downgraded QoS0 since second subscribe asked 0).
	got := sub.recv()
	if got.Fixed.QoS() != 0 {
		t.Fatalf("effective qos = %d, want 0 after resubscribe", got.Fixed.QoS())
	}
	if pk, ok := sub.maybeRecv(300 * time.Millisecond); ok {
		t.Fatalf("duplicate delivery after resubscribe: %+v", pk)
	}
}
