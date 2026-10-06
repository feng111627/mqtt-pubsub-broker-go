package broker

import (
	"net"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// testPeer is a minimal scriptable MQTT-over-TCP peer used to drive the
// broker without the client package (keeps broker tests independent of client
// correctness).
type testPeer struct {
	t      *testing.T
	conn   net.Conn
	reader *packet.Reader
	level  byte
}

func startTestBroker(t *testing.T) *Broker {
	t.Helper()
	b := New(Config{ListenAddr: "127.0.0.1:0", QueueMax: 100, ReceiveMaximum: 16, KeepAliveFactor: 1.5})
	if err := b.Start(); err != nil {
		t.Fatalf("start broker: %v", err)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func dialPeer(t *testing.T, b *Broker, level byte) *testPeer {
	t.Helper()
	conn, err := net.DialTimeout("tcp", b.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatalf("dial: %v", err)
	}
	p := &testPeer{t: t, conn: conn, reader: packet.NewReader(conn, 1<<20), level: level}
	return p
}

func (p *testPeer) send(pk *packet.Packet) {
	p.t.Helper()
	wire, err := pk.Encode(p.level)
	if err != nil {
		p.t.Fatalf("encode: %v", err)
	}
	if _, err := p.conn.Write(wire); err != nil {
		p.t.Fatalf("write: %v", err)
	}
}

func (p *testPeer) recv() *packet.Packet {
	p.t.Helper()
	p.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	pk, err := p.reader.ReadOne()
	if err != nil {
		p.t.Fatalf("recv: %v", err)
	}
	return pk
}

func (p *testPeer) maybeRecv(d time.Duration) (*packet.Packet, bool) {
	p.conn.SetReadDeadline(time.Now().Add(d))
	pk, err := p.reader.ReadOne()
	if err != nil {
		return nil, false
	}
	return pk, true
}

func (p *testPeer) close() { p.conn.Close() }

// connect sends CONNECT and expects CONNACK success; returns the CONNACK.
func (p *testPeer) connect(id string, clean bool, level byte) *packet.Packet {
	p.level = level
	pk := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNECT},
		CleanStart: clean,
		KeepAlive:  0,
		ClientID:   id,
	}
	if level == packet.V50 {
		pk.Props = packet.NewProperties()
	}
	p.reader.SetLevel(level)
	p.send(pk)
	ack := p.recv()
	if ack.Fixed.Type != packet.TypeCONNACK || ack.ReasonCode != 0 {
		p.t.Fatalf("expected successful CONNACK, got type=%s code=%d", ack.Fixed.Type, ack.ReasonCode)
	}
	p.reader.SetLevel(level)
	return ack
}

func subscribePacket(id uint16, level byte, qos byte, filters ...string) *packet.Packet {
	pk := &packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypeSUBSCRIBE}, PacketID: id}
	if level == packet.V50 {
		pk.Props = packet.NewProperties()
	}
	for _, f := range filters {
		pk.Filters = append(pk.Filters, packet.SubFilter{Filter: f, QoS: qos})
	}
	return pk
}

func publishPacket(topic string, qos byte, retain bool, id uint16, payload string, level byte) *packet.Packet {
	var flags byte = qos << 1
	if retain {
		flags |= 1
	}
	pk := &packet.Packet{
		Fixed:    packet.FixedHeader{Type: packet.TypePUBLISH, Flags: flags},
		Topic:    topic,
		PacketID: id,
		Payload:  []byte(payload),
	}
	if level == packet.V50 {
		pk.Props = packet.NewProperties()
	}
	return pk
}

func TestConnectSessionPresentSemantics(t *testing.T) {
	b := startTestBroker(t)

	// clean=false, first connect: session present must be 0.
	s1 := dialPeer(t, b, packet.V311)
	ack := s1.connect("dev-A", false, packet.V311)
	if ack.SessionPresent {
		t.Fatalf("first persistent connect: SessionPresent=true, want false")
	}
	// add a subscription then drop the TCP connection abruptly.
	s1.send(subscribePacket(1, packet.V311, 1, "a/b"))
	suback := s1.recv()
	if suback.Fixed.Type != packet.TypeSUBACK || suback.ReturnCodes[0] != 1 {
		t.Fatalf("bad SUBACK: %+v", suback)
	}
	s1.close()
	waitUntil(t, time.Second, func() bool {
		st := b.Stats()
		return st.Sessions == 1 && st.Subscriptions == 1
	})

	// reconnect clean=false: session present must be 1 and subscription kept.
	s2 := dialPeer(t, b, packet.V311)
	ack = s2.connect("dev-A", false, packet.V311)
	if !ack.SessionPresent {
		t.Fatalf("resume connect: SessionPresent=false, want true")
	}
	if got := b.Stats().Subscriptions; got != 1 {
		t.Fatalf("subscription count after resume = %d, want 1", got)
	}
	s2.close()

	// clean=true reconnect wipes the old session: present must be 0.
	s3 := dialPeer(t, b, packet.V311)
	ack = s3.connect("dev-A", true, packet.V311)
	if ack.SessionPresent {
		t.Fatalf("clean reconnect: SessionPresent=true, want false")
	}
	waitUntil(t, time.Second, func() bool { return b.Stats().Subscriptions == 0 })
}

func TestSessionIsolationBetweenClients(t *testing.T) {
	b := startTestBroker(t)
	a := dialPeer(t, b, packet.V311)
	a.connect("iso-A", false, packet.V311)
	a.send(subscribePacket(1, packet.V311, 1, "x/+"))
	a.recv()
	a.close()
	c := dialPeer(t, b, packet.V311)
	ack := c.connect("iso-B", false, packet.V311)
	if ack.SessionPresent {
		t.Fatal("new client must not inherit another client's session")
	}
	waitUntil(t, time.Second, func() bool { return b.Stats().Sessions == 2 })
	if got := b.Stats().Subscriptions; got != 1 {
		t.Fatalf("subscriptions = %d, want only iso-A's 1", got)
	}
}

func TestUnsupportedVersionRejected(t *testing.T) {
	b := startTestBroker(t)
	conn, err := net.DialTimeout("tcp", b.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	pk := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNECT},
		CleanStart: true,
		ClientID:   "x",
	}
	// forge level 9 with name MQTT by encoding then patching
	wire, err := pk.Encode(packet.V311)
	if err != nil {
		t.Fatal(err)
	}
	// layout: 10 <rlen> 00 04 "MQTT" <level> ...  MQTT at index 4..7, level 8
	if string(wire[4:8]) != "MQTT" {
		t.Fatalf("unexpected CONNECT layout: % x", wire)
	}
	wire[8] = 9
	if _, err := conn.Write(wire); err != nil {
		t.Fatal(err)
	}
	r := packet.NewReader(conn, 1<<20)
	ack, err := r.ReadOne()
	if err != nil {
		t.Fatalf("expected CONNACK rejection, got error: %v", err)
	}
	if ack.Fixed.Type != packet.TypeCONNACK || ack.ReasonCode != 0x01 {
		t.Fatalf("want 3.1.1 unacceptable version code 0x01, got type=%s code=%d", ack.Fixed.Type, ack.ReasonCode)
	}
}

func TestMalformedFirstPacketCloses(t *testing.T) {
	b := startTestBroker(t)
	conn, err := net.DialTimeout("tcp", b.Addr().String(), 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	// QoS3 PUBLISH as the very first packet -> connection closed, no session.
	pk := publishPacket("a/b", 3, false, 1, "x", packet.V311)
	wire, _ := pk.Encode(packet.V311)
	conn.Write(wire)
	buf := make([]byte, 16)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	n, _ := conn.Read(buf)
	if n != 0 {
		t.Fatalf("expected immediate close, got %d bytes: % x", n, buf[:n])
	}
}

func TestWildcardRoutingEndToEnd(t *testing.T) {
	b := startTestBroker(t)
	sub := dialPeer(t, b, packet.V311)
	sub.connect("sub-wild", true, packet.V311)
	sub.send(subscribePacket(1, packet.V311, 1, "sport/+"))
	sub.recv()
	pub := dialPeer(t, b, packet.V311)
	pub.connect("pub-wild", true, packet.V311)

	// hit
	pub.send(publishPacket("sport/tennis", 1, false, 10, "hit1", packet.V311))
	got := sub.recv()
	if got.Topic != "sport/tennis" || string(got.Payload) != "hit1" || got.Fixed.QoS() != 1 {
		t.Fatalf("wildcard hit mismatch: %+v", got)
	}
	// subscriber PUBACK
	sub.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: got.PacketID})

	// miss: deeper topic must not match "sport/+"
	pub.send(publishPacket("sport/tennis/player1", 0, false, 0, "miss", packet.V311))
	if pk, ok := sub.maybeRecv(300 * time.Millisecond); ok {
		t.Fatalf("unexpected delivery for deeper topic: %+v", pk)
	}

	// exact filter miss
	pub.send(publishPacket("other/x", 0, false, 0, "miss2", packet.V311))
	if pk, ok := sub.maybeRecv(300 * time.Millisecond); ok {
		t.Fatalf("unexpected delivery for other tree: %+v", pk)
	}
}

func TestRetainedMessageDelivery(t *testing.T) {
	b := startTestBroker(t)
	pub := dialPeer(t, b, packet.V311)
	pub.connect("ret-pub", true, packet.V311)
	pub.send(publishPacket("status/door", 1, true, 1, "open", packet.V311))
	// QoS1 publish receives no server ack (server->subscriber is the delivery);
	// publisher side PUBLISH is simply accepted.
	pub.close()

	sub := dialPeer(t, b, packet.V311)
	sub.connect("ret-sub", true, packet.V311)
	sub.send(subscribePacket(1, packet.V311, 1, "status/+"))
	// first receive SUBACK, then the retained PUBLISH
	sub.recv()
	got := sub.recv()
	if got.Topic != "status/door" || string(got.Payload) != "open" || !got.Fixed.Retain() {
		t.Fatalf("retained delivery mismatch: topic=%s payload=%s retain=%v", got.Topic, got.Payload, got.Fixed.Retain())
	}

	// clearing: publish empty retained payload then a fresh subscriber gets none.
	pub2 := dialPeer(t, b, packet.V311)
	pub2.connect("ret-pub2", true, packet.V311)
	pub2.send(publishPacket("status/door", 1, true, 2, "", packet.V311))
	pub2.close()
	sub2 := dialPeer(t, b, packet.V311)
	sub2.connect("ret-sub2", true, packet.V311)
	sub2.send(subscribePacket(1, packet.V311, 0, "#"))
	sub2.recv()
	if pk, ok := sub2.maybeRecv(300 * time.Millisecond); ok {
		t.Fatalf("retained slot should be cleared, got: %+v", pk)
	}
}

func TestTakeoverPublishesNoWillAndKillsOld(t *testing.T) {
	b := startTestBroker(t)
	// A with a will
	a := dialPeer(t, b, packet.V311)
	will := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNECT},
		CleanStart: false,
		ClientID:   "take",
		WillFlag:   true,
		WillQoS:    1,
		WillTopic:  "lw/take",
		WillMsg:    []byte("bye"),
	}
	a.send(will)
	if ack := a.recv(); ack.ReasonCode != 0 {
		t.Fatalf("connect A: code %d", ack.ReasonCode)
	}
	// monitor subscribes to the will topic
	mon := dialPeer(t, b, packet.V311)
	mon.connect("monitor", true, packet.V311)
	mon.send(subscribePacket(1, packet.V311, 1, "lw/#"))
	mon.recv()

	// B takes over the same client id
	bp := dialPeer(t, b, packet.V311)
	bp.connect("take", false, packet.V311)
	// the old socket must be closed
	a.conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	buf := make([]byte, 8)
	if n, _ := a.conn.Read(buf); n != 0 {
		t.Fatalf("old connection should be closed, read %d bytes", n)
	}
	// the will must NOT be published
	if pk, ok := mon.maybeRecv(400 * time.Millisecond); ok {
		t.Fatalf("will must not be published on takeover, got %+v", pk)
	}
	// B gracefully disconnects -> still no will (will belonged to session and
	// is replaced/cleared semantics: on takeover we keep the NEW connection's
	// will, which is nil; either way the monitor sees nothing).
	bp.send(&packet.Packet{Fixed: packet.FixedHeader{Type: packet.TypeDISCONNECT}})
	bp.close()
	if pk, ok := mon.maybeRecv(400 * time.Millisecond); ok {
		t.Fatalf("no will expected after clean disconnect, got %+v", pk)
	}
}

func TestWillPublishedOnAbruptDrop(t *testing.T) {
	b := startTestBroker(t)
	mon := dialPeer(t, b, packet.V311)
	mon.connect("will-mon", true, packet.V311)
	mon.send(subscribePacket(1, packet.V311, 1, "lw/#"))
	mon.recv()

	w := dialPeer(t, b, packet.V311)
	wp := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNECT},
		CleanStart: true,
		ClientID:   "willdev",
		WillFlag:   true,
		WillQoS:    1,
		WillRetain: false,
		WillTopic:  "lw/dev",
		WillMsg:    []byte("offline"),
	}
	w.send(wp)
	w.recv()
	w.close() // abrupt: no DISCONNECT

	got := waitPacket(t, mon, time.Second)
	if got.Topic != "lw/dev" || string(got.Payload) != "offline" {
		t.Fatalf("will message mismatch: topic=%s payload=%s", got.Topic, got.Payload)
	}
}

func TestInvalidTopicPublishRejected(t *testing.T) {
	b := startTestBroker(t)
	// v5: PUBLISH with wildcard-ish topic is individually rejected.
	c := dialPeer(t, b, packet.V50)
	cp := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNECT},
		CleanStart: true,
		ClientID:   "v5bad",
		Props:      packet.NewProperties(),
	}
	c.reader.SetLevel(packet.V50)
	c.send(cp)
	c.recv()
	c.send(publishPacket("bad/+", 1, false, 5, "x", packet.V50))
	got := c.recv()
	if got.Fixed.Type != packet.TypePUBACK || got.ReasonCode != byte(mqerr.CodeTopicNameInvalid) {
		t.Fatalf("want PUBACK 0x90, got type=%s code=0x%02x", got.Fixed.Type, got.ReasonCode)
	}
}

func TestTrieDeterministicDedup(t *testing.T) {
	tr := newTopicTrie()
	tr.add(1, &subscription{Filter: "a/+", QoS: 2})
	tr.add(1, &subscription{Filter: "a/#", QoS: 1})
	tr.add(2, &subscription{Filter: "#", QoS: 0})
	hits := tr.match("a/b")
	if len(hits) != 2 {
		t.Fatalf("hits = %d, want 2 sessions: %+v", len(hits), hits)
	}
	if hits[0].SessionID != 1 || hits[0].QoS != 1 {
		t.Fatalf("session 1 effective QoS = %d, want min(2,1)=1", hits[0].QoS)
	}
	if hits[1].SessionID != 2 || hits[1].QoS != 0 {
		t.Fatalf("session 2 mismatch: %+v", hits[1])
	}
}

// ---- helpers ----

func waitUntil(t *testing.T, timeout time.Duration, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if cond() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("condition not met within %s", timeout)
}

func waitPacket(t *testing.T, p *testPeer, timeout time.Duration) *packet.Packet {
	t.Helper()
	pk, ok := p.maybeRecv(timeout)
	if !ok {
		t.Fatalf("no packet within %s", timeout)
	}
	return pk
}
