package broker_test

import (
	"net"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

func mkPublish(topicName string, qos byte, id uint16, payload []byte, dup bool) *packet.Packet {
	flags := (qos & 3) << 1
	if dup {
		flags |= 0x08
	}
	return &packet.Packet{
		Header:   packet.FixedHeader{Type: packet.TypePUBLISH, Flags: flags},
		Topic:    topicName,
		QoS:      qos,
		PacketID: id,
		Payload:  payload,
		Dup:      dup,
	}
}

func mkAck(ptype byte, id uint16, flags byte) *packet.Packet {
	return &packet.Packet{Header: packet.FixedHeader{Type: ptype, Flags: flags}, PacketID: id}
}

func mustRead(t *testing.T, conn net.Conn) *packet.Packet {
	t.Helper()
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	p, err := packet.ReadPacket(conn)
	if err != nil {
		t.Fatalf("read packet: %v", err)
	}
	return p
}

func readAny(conn net.Conn) (*packet.Packet, error) {
	return packet.ReadPacket(conn)
}

// 业务：共享组 QoS2 恰好一次 —— 消息单选投递，PUBCOMP 后组在途令牌被摘除，
// 订阅者重连不会再收到该消息。
func TestSharedGroupQoS2(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	pub := rawConnect(t, addr, "q2pub", true)
	defer pub.Close()
	sub := rawConnect(t, addr, "q2sub", false)
	defer sub.Close()
	rawSubscribe(t, sub, 1, "$share/q2/t", 2)

	// 发布方 <-> broker 完成 QoS2
	p := mkPublish("t", 2, 100, []byte("exactly"), false)
	raw, _ := p.Encode(packet.Version311)
	pub.Write(raw)
	rec := mustRead(t, pub)
	if rec.Header.Type != packet.TypePUBREC {
		t.Fatalf("expected PUBREC got %d", rec.Header.Type)
	}
	r2, _ := mkAck(packet.TypePUBREL, rec.PacketID, 0x02).Encode(packet.Version311)
	pub.Write(r2)
	if c := mustRead(t, pub); c.Header.Type != packet.TypePUBCOMP {
		t.Fatalf("expected PUBCOMP got %d", c.Header.Type)
	}

	// broker -> 订阅者 的 QoS2
	got := mustRead(t, sub)
	if got.Header.Type != packet.TypePUBLISH || string(got.Payload) != "exactly" {
		t.Fatalf("bad delivered: %+v", got)
	}
	sr, _ := mkAck(packet.TypePUBREC, got.PacketID, 0).Encode(packet.Version311)
	sub.Write(sr)
	subRel := mustRead(t, sub)
	if subRel.Header.Type != packet.TypePUBREL {
		t.Fatalf("expected PUBREL got %d", subRel.Header.Type)
	}
	sc, _ := mkAck(packet.TypePUBCOMP, subRel.PacketID, 0).Encode(packet.Version311)
	sub.Write(sc)

	time.Sleep(200 * time.Millisecond)
	// 订阅者断线重连：QoS2 已完成（组在途已摘除），不应重发
	sub.Close()
	time.Sleep(150 * time.Millisecond)
	sub2 := rawConnect(t, addr, "q2sub", false)
	defer sub2.Close()
	sub2.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, err := readAny(sub2); err == nil {
		t.Fatal("completed QoS2 group message must not be redelivered after reconnect")
	}
	t.Logf("业务=共享组QoS2恰好一次 用例=单成员完整四次握手后重连 结果: 无重发，组在途令牌已摘除")
}
