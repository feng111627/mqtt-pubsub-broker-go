package broker_test

import (
	"net"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// rawConnect 用原始连接完成 CONNECT 握手，返回连接（可精细控制 ACK 行为）。
func rawConnect(t *testing.T, addr, id string, clean bool) net.Conn {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x04}
	flags := byte(0)
	if clean {
		flags |= 0x02
	}
	body = append(body, flags, 0x00, 0x3C)
	body = append(body, byte(len(id)>>8), byte(len(id)))
	body = append(body, id...)
	raw := append([]byte{0x10, byte(len(body))}, body...)
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ack, err := packet.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read connack: %v", err)
	}
	if ack.ReasonCode != packet.ConnAccepted {
		t.Fatalf("connack refused: %d", ack.ReasonCode)
	}
	return conn
}

func rawSubscribe(t *testing.T, conn net.Conn, id uint16, filter string, qos byte) {
	t.Helper()
	p := &packet.Packet{
		Header:        packet.FixedHeader{Type: packet.TypeSUBSCRIBE, Flags: 0x02},
		PacketID:      id,
		Subscriptions: []packet.Subscription{{Topic: filter, QoS: qos}},
	}
	raw, _ := p.Encode(packet.Version311)
	conn.Write(raw)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ack, err := packet.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read suback: %v", err)
	}
	if ack.Header.Type != packet.TypeSUBACK {
		t.Fatalf("expected SUBACK, got %d", ack.Header.Type)
	}
}

// TestQoS1RetransmitOnReconnect 验证：订阅方收到 QoS1 消息但不回 PUBACK，
// 重连后代理应以 DUP=1 重发同一条消息，且确认后不再投递。
func TestQoS1RetransmitOnReconnect(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	// 持久会话订阅
	conn := rawConnect(t, addr, "flaky", false)
	rawSubscribe(t, conn, 1, "flaky/topic", 1)

	// 发布一条 QoS1 消息
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	if err := pub.Publish("flaky/topic", 1, false, []byte("important")); err != nil {
		t.Fatal(err)
	}

	// 收到但不 ACK，直接断开（模拟连接抖动）
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := packet.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read publish: %v", err)
	}
	if first.Header.Type != packet.TypePUBLISH || string(first.Payload) != "important" {
		t.Fatalf("unexpected packet: %+v", first)
	}
	conn.Close()
	time.Sleep(100 * time.Millisecond)

	// 重连：应收到 DUP=1 的重发，PacketID 相同
	conn2 := rawConnect(t, addr, "flaky", false)
	defer conn2.Close()
	conn2.SetReadDeadline(time.Now().Add(2 * time.Second))
	second, err := packet.ReadPacket(conn2)
	if err != nil {
		t.Fatalf("read retransmit: %v", err)
	}
	if second.Header.Type != packet.TypePUBLISH {
		t.Fatalf("expected PUBLISH, got %d", second.Header.Type)
	}
	if !second.Dup {
		t.Fatal("retransmitted PUBLISH should have DUP=1")
	}
	if second.PacketID != first.PacketID {
		t.Fatalf("retransmit packet id %d != original %d", second.PacketID, first.PacketID)
	}
	if string(second.Payload) != "important" {
		t.Fatalf("retransmit payload %q", second.Payload)
	}

	// 这次正常 ACK，之后不应再有重复投递
	ack := &packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: second.PacketID}
	raw, _ := ack.Encode(packet.Version311)
	conn2.Write(raw)
	conn2.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if _, err := packet.ReadPacket(conn2); err == nil {
		t.Fatal("unexpected extra packet after ack")
	}
}

// TestQoS2DuplicatePublishDedup 验证：发布方 QoS2 重复投递同一 PacketID，
// 订阅方只收到一次。
func TestQoS2DuplicatePublishDedup(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	sub := mustConnect(t, addr, "sub", true)
	defer sub.Close()
	sub.Subscribe("qos2/dedup", 2)

	// 原始连接模拟发布方：发两次相同 PacketID 的 QoS2 PUBLISH（DUP 重传）
	conn := rawConnect(t, addr, "qos2pub", true)
	defer conn.Close()
	send := func(dup bool) {
		flags := byte(0x04) // QoS2
		if dup {
			flags |= 0x08
		}
		p := &packet.Packet{
			Header:   packet.FixedHeader{Type: packet.TypePUBLISH, Flags: flags},
			Topic:    "qos2/dedup",
			QoS:      2,
			Dup:      dup,
			PacketID: 77,
			Payload:  []byte("once-only"),
		}
		raw, _ := p.Encode(packet.Version311)
		conn.Write(raw)
		conn.SetReadDeadline(time.Now().Add(2 * time.Second))
		resp, err := packet.ReadPacket(conn)
		if err != nil {
			t.Fatalf("read pubrec: %v", err)
		}
		if resp.Header.Type != packet.TypePUBREC || resp.PacketID != 77 {
			t.Fatalf("expected PUBREC(77), got %+v", resp)
		}
	}
	send(false)
	send(true) // 重传

	// 订阅方只收到一次
	m := recvOne(t, sub, 2*time.Second)
	if string(m.Payload) != "once-only" {
		t.Fatalf("bad payload %q", m.Payload)
	}
	expectNone(t, sub, 400*time.Millisecond)

	// 完成 QoS2 握手
	rel := &packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBREL, Flags: 0x02}, PacketID: 77}
	raw, _ := rel.Encode(packet.Version311)
	conn.Write(raw)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	resp, err := packet.ReadPacket(conn)
	if err != nil || resp.Header.Type != packet.TypePUBCOMP {
		t.Fatalf("expected PUBCOMP, got %v %v", resp, err)
	}
}

// TestDuplicateDeliveryToSubscriber 验证：订阅方 QoS2 场景下，
// 代理重发（DUP）不会导致应用层重复消费 —— 由 PacketID 可识别。
func TestUnsubscribeStopsDelivery(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	sub := mustConnect(t, addr, "sub", true)
	defer sub.Close()
	sub.Subscribe("unsub/topic", 1)
	if err := sub.Unsubscribe("unsub/topic"); err != nil {
		t.Fatal(err)
	}
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	pub.Publish("unsub/topic", 1, false, []byte("gone"))
	expectNone(t, sub, 400*time.Millisecond)
}
