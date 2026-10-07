package broker_test

import (
	"net"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// sendAck 发送 QoS1 PUBACK。
func sendAck(t *testing.T, conn net.Conn, id uint16) {
	t.Helper()
	ack := &packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: id}
	raw, _ := ack.Encode(packet.Version311)
	if _, err := conn.Write(raw); err != nil {
		t.Fatalf("write ack: %v", err)
	}
}

// 业务：掉线在途接管 —— 成员收到 QoS1 但不确认就崩溃，
// 消息应转交给后上线的另一个组内成员（DUP=1），既不丢失也不重复消费。
func TestSharedGroupFailoverUnacked(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	// 阶段1：组里只有 c1，发一条 QoS1，c1 收到但不 ACK。
	c1 := rawConnect(t, addr, "f1", false)
	rawSubscribe(t, c1, 1, "$share/fail/job", 1)
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	if err := pub.Publish("job", 1, false, []byte("task-1")); err != nil {
		t.Fatal(err)
	}
	c1.SetReadDeadline(time.Now().Add(2 * time.Second))
	victim, err := packet.ReadPacket(c1)
	if err != nil || victim.Header.Type != packet.TypePUBLISH || string(victim.Payload) != "task-1" {
		t.Fatalf("c1 should receive the only message, err=%v pkt=%+v", err, victim)
	}

	// 阶段2：c2 加入同一组（在线），随后 c1 不 ACK 崩溃。
	c2 := rawConnect(t, addr, "f2", false)
	defer c2.Close()
	rawSubscribe(t, c2, 2, "$share/fail/job", 1)
	time.Sleep(150 * time.Millisecond)
	c1.Close()
	time.Sleep(200 * time.Millisecond)

	// c2 应收到 DUP=1 的转交消息，且只有一条
	c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	relay, err := packet.ReadPacket(c2)
	if err != nil || relay.Header.Type != packet.TypePUBLISH {
		t.Fatalf("failover message not handed over: err=%v pkt=%+v", err, relay)
	}
	if string(relay.Payload) != "task-1" || !relay.Dup {
		t.Fatalf("bad handover: payload=%q dup=%v", relay.Payload, relay.Dup)
	}
	sendAck(t, c2, relay.PacketID)

	// 再发新消息，应正常由在线成员 c2 收到
	if err := pub.Publish("job", 1, false, []byte("task-2")); err != nil {
		t.Fatal(err)
	}
	c2.SetReadDeadline(time.Now().Add(2 * time.Second))
	next, err := packet.ReadPacket(c2)
	if err != nil || string(next.Payload) != "task-2" {
		t.Fatalf("post-failover publish should reach c2: err=%v", err)
	}
	sendAck(t, c2, next.PacketID)

	// 不应再有多余消息
	c2.SetReadDeadline(time.Now().Add(400 * time.Millisecond))
	if _, err := packet.ReadPacket(c2); err == nil {
		t.Fatal("unexpected duplicate after failover")
	}
	t.Logf("业务=掉线在途接管 用例=c1持未ACK消息崩溃,c2在线 结果: task-1 DUP=%v转c2且仅一次, 后续task-2正常", relay.Dup)
}

// 业务：成员陆续退出触发重平衡 —— 成员离线后新消息只投给仍在线成员。
func TestSharedGroupRebalanceOnLeave(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	m1 := mustConnect(t, addr, "r1", false)
	defer m1.Close()
	m2 := mustConnect(t, addr, "r2", false)
	m3 := mustConnect(t, addr, "r3", false)
	defer m3.Close()
	for _, cc := range []*client.Client{m1, m2, m3} {
		if _, err := cc.Subscribe("$share/rb/job", 1); err != nil {
			t.Fatal(err)
		}
	}
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()

	m2.Close()
	time.Sleep(150 * time.Millisecond)
	for i := 0; i < 6; i++ {
		if err := pub.Publish("job", 1, false, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(400 * time.Millisecond)
	n1, n3 := drain(m1), drain(m3)
	if n1+n3 != 6 {
		t.Fatalf("after one member leaves, 6 messages should reach the 2 online members, got %d+%d", n1, n3)
	}
	t.Logf("业务=成员退出重平衡 用例=3成员退1个后发6条 结果: 在线 m1=%d m3=%d 合计=%d", n1, n3, n1+n3)
}
