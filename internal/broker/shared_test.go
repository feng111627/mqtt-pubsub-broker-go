package broker_test

import (
	"net"
	"reflect"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// recvN 在总超时内从客户端收集恰好 n 条消息，不足则失败。
func recvN(t *testing.T, c *client.Client, n int, total time.Duration) []client.Message {
	t.Helper()
	out := make([]client.Message, 0, n)
	deadline := time.After(total)
	for len(out) < n {
		select {
		case m := <-c.Messages():
			out = append(out, m)
		case <-deadline:
			t.Fatalf("expected %d messages, got %d", n, len(out))
		}
	}
	return out
}

// drainFor 在 d 时间内继续读取消息，返回多读到的消息（应为空）。
func drainFor(t *testing.T, c *client.Client, d time.Duration) []client.Message {
	t.Helper()
	var extra []client.Message
	for {
		select {
		case m := <-c.Messages():
			extra = append(extra, m)
		case <-time.After(d):
			return extra
		}
	}
}

// collectFrom 从多个客户端中收集恰好 n 条消息（谁先到算谁），总超时收敛。
func collectFrom(t *testing.T, cs []*client.Client, n int, total time.Duration) []client.Message {
	t.Helper()
	out := make([]client.Message, 0, n)
	deadline := time.After(total)
	for len(out) < n {
		cases := make([]reflect.SelectCase, 0, len(cs)+1)
		for _, c := range cs {
			cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(c.Messages())})
		}
		cases = append(cases, reflect.SelectCase{Dir: reflect.SelectRecv, Chan: reflect.ValueOf(deadline)})
		idx, v, _ := reflect.Select(cases)
		if idx == len(cs) {
			t.Fatalf("collectFrom: only got %d of %d messages", len(out), n)
		}
		out = append(out, v.Interface().(client.Message))
	}
	return out
}

// waitGroupSize 轮询等待共享组成员数达到 want（成员退出清理是异步的）。
func waitGroupSize(t *testing.T, b *broker.Broker, name string, want int, total time.Duration) {
	t.Helper()
	deadline := time.Now().Add(total)
	for time.Now().Before(deadline) {
		if b.GroupMemberCountForTest(name) == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("group %q member count did not reach %d (now %d)", name, want, b.GroupMemberCountForTest(name))
}

// TestSharedSubscriptionOneDeliveryPerGroup 组内多成员订阅同一主题时，
// 每条消息只投递给其中一个成员，且整体负载均衡、无重复。
func TestSharedSubscriptionOneDeliveryPerGroup(t *testing.T) {
	t.Log("业务[共享订阅-组内单投] 用例: 同组 a/c 两个在线成员订阅 jobs，发布 10 条，" +
		"期望每条只投一个成员、合起来无重复且两边都有分担")
	b := startBroker(t)
	addr := b.Addr()

	a := mustConnect(t, addr, "grp-a", true)
	defer a.Close()
	c := mustConnect(t, addr, "grp-c", true)
	defer c.Close()
	if _, err := a.Subscribe("$share/workers/jobs", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe("$share/workers/jobs", 1); err != nil {
		t.Fatal(err)
	}
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()

	const n = 10
	for i := 0; i < n; i++ {
		if err := pub.Publish("jobs", 1, false, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	// 轮询分发，两个成员各收 5 条
	gotA := recvN(t, a, n/2, 2*time.Second)
	gotC := recvN(t, c, n/2, 2*time.Second)
	if len(gotA)+len(gotC) != n {
		t.Fatalf("expected exactly %d deliveries, got a=%d c=%d", n, len(gotA), len(gotC))
	}
	seen := map[byte]bool{}
	for _, m := range append(gotA, gotC...) {
		if len(m.Payload) != 1 {
			t.Fatalf("bad payload %v", m.Payload)
		}
		if seen[m.Payload[0]] {
			t.Fatalf("message %d delivered twice within group", m.Payload[0])
		}
		seen[m.Payload[0]] = true
	}
	if len(seen) != n {
		t.Fatalf("coverage=%d, want %d", len(seen), n)
	}
	if extra := drainFor(t, a, 300*time.Millisecond); len(extra) != 0 {
		t.Fatalf("a received extra group messages: %d", len(extra))
	}
	t.Logf("业务[共享订阅-组内单投] 结果: PASS，a=%d c=%d，组内无重复无广播", len(gotA), len(gotC))
}

// TestSharedRebalanceOnJoinLeave 成员退出/加入后分发自动重新平衡。
func TestSharedRebalanceOnJoinLeave(t *testing.T) {
	t.Log("业务[共享订阅-重平衡] 用例: 初始 a/b 两成员各收一部分；b 退出后新消息只给 a；" +
		"新成员 d 加入后分担后续消息")
	b := startBroker(t)
	addr := b.Addr()

	a := mustConnect(t, addr, "s-a", true)
	defer a.Close()
	mb := mustConnect(t, addr, "s-b", true)
	a.Subscribe("$share/g/t", 1)
	mb.Subscribe("$share/g/t", 1)
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()

	// drainOne 从 a/b 中谁先到取一条，并清空双方当前缓冲。
	pub.Publish("t", 1, false, []byte("1"))
	pub.Publish("t", 1, false, []byte("2"))
	first := collectFrom(t, []*client.Client{a, mb}, 2, 2*time.Second)
	if len(first) != 2 {
		t.Fatalf("first round should deliver 2, got %d", len(first))
	}
	// mb 断开前把它通道里残余读完（collectFrom 已凑齐 2 即停，可能某边通道还有残余）。
	// 由于恰好发布 2 条、组内单投，总量就是 2，无残余。

	// b 退出：等待旧连接清理完成（摘除在线表 + 组回收），避免抖动误判。
	mb.Close()
	waitGroupSize(t, b, "g", 1, 2*time.Second)

	// 排空 a 在 mb 断开前可能已缓冲的消息（断开竞态窗口内），确保后续断言干净。
	// 此阶段只发布了 1、2 两条，a 通道此刻即便有残余也只能是 1/2，排空即可。
	drainNonBlocking(t, a)

	// b 退出后 2 条都应由 a 处理，且顺序为 3->4
	pub.Publish("t", 1, false, []byte("3"))
	pub.Publish("t", 1, false, []byte("4"))
	got := recvN(t, a, 2, 2*time.Second)
	if got[0].Payload[0] != '3' || got[1].Payload[0] != '4' {
		t.Fatalf("after leave, a should get 3 then 4: %q %q", got[0].Payload, got[1].Payload)
	}

	// d 加入：后续 4 条消息应在 a/d 间分担
	d := mustConnect(t, addr, "s-d", true)
	defer d.Close()
	d.Subscribe("$share/g/t", 1)
	for i := 0; i < 4; i++ {
		pub.Publish("t", 1, false, []byte{byte('5' + i)})
	}
	gotA, gotD := 0, 0
	deadline := time.After(2 * time.Second)
	for gotA+gotD < 4 {
		select {
		case <-a.Messages():
			gotA++
		case <-d.Messages():
			gotD++
		case <-deadline:
			t.Fatalf("rebalance after join incomplete: a=%d d=%d", gotA, gotD)
		}
	}
	if gotA == 0 || gotD == 0 {
		t.Fatalf("new member should take share: a=%d d=%d", gotA, gotD)
	}
	t.Logf("业务[共享订阅-重平衡] 结果: PASS，退出后单投 a(3->4)，新成员 d 加入后 a=%d d=%d", gotA, gotD)
}

// drainNonBlocking 非阻塞地清空客户端当前缓冲（隔离前后阶段消息）。
func drainNonBlocking(t *testing.T, c *client.Client) {
	t.Helper()
	for {
		select {
		case <-c.Messages():
		default:
			return
		}
	}
}

// TestSharedInflightReclaimOnDisconnect 成员收到 QoS1 消息但未确认就掉线，
// 消息应转移给组内仍在线成员，且原成员重连不会重复收到。
func TestSharedInflightReclaimOnDisconnect(t *testing.T) {
	t.Log("业务[共享订阅-掉线转移] 用例: 组内 worker-1 收到 QoS1 但不 ACK 直接断线，" +
		"未确认消息转交仍在线的 worker-2，且 worker-1 重连后不重复消费")
	b := startBroker(t)
	addr := b.Addr()

	w1 := rawConnect(t, addr, "worker-1", false) // 持久会话，便于验证重连不重复
	rawSubscribe(t, w1, 1, "$share/j/order", 1)
	w2 := rawConnect(t, addr, "worker-2", true)
	defer w2.Close()
	rawSubscribe(t, w2, 2, "$share/j/order", 1)

	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	pub.Publish("order", 1, false, []byte("pay"))

	// 消息被轮询给某一个 worker；读出它，不 ACK 直接断线
	w1.SetReadDeadline(time.Now().Add(2 * time.Second))
	first, err := packetRead(w1)
	if err != nil {
		t.Fatalf("worker-1 read: %v", err)
	}
	if string(first.Payload) != "pay" || first.Header.Type != packet.TypePUBLISH {
		t.Fatalf("worker-1 got unexpected packet: %+v", first)
	}
	w1.Close()
	time.Sleep(150 * time.Millisecond)

	// worker-2 应收到转移过来的消息（新 PacketID）
	w2.SetReadDeadline(time.Now().Add(2 * time.Second))
	second, err := packetRead(w2)
	if err != nil {
		t.Fatalf("worker-2 should receive reclaimed message: %v", err)
	}
	if string(second.Payload) != "pay" {
		t.Fatalf("reclaimed payload=%q", second.Payload)
	}
	// worker-2 正常 ACK
	ack := mustEncodeAck(t, second.PacketID)
	w2.Write(ack)

	// worker-1 重连：不应再收到这条消息（未确认消息已被组回收，不在其会话）
	w1b := rawConnect(t, addr, "worker-1", false)
	defer w1b.Close()
	w1b.SetReadDeadline(time.Now().Add(500 * time.Millisecond))
	if p, err := packetRead(w1b); err == nil {
		t.Fatalf("worker-1 reconnected but got duplicate: %+v", p)
	}
	t.Log("业务[共享订阅-掉线转移] 结果: PASS，未确认消息转交 worker-2，worker-1 重连无重复")
}

// TestSharedNoMemberFallbackQueue 组内无在线成员时消息进兜底队列，
// 成员重新上线后按发布顺序补发；只投递给一个成员。
func TestSharedNoMemberFallbackQueue(t *testing.T) {
	t.Log("业务[共享订阅-无成员兜底] 用例: 唯一成员离线期间发布 3 条进入组兜底，" +
		"重连后按顺序补发，随后新消息恢复在线分发")
	b := startBroker(t)
	addr := b.Addr()

	w := mustConnect(t, addr, "only-w", false)
	w.Subscribe("$share/backup/data", 1)
	w.Close()
	time.Sleep(100 * time.Millisecond)

	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	pub.Publish("data", 1, false, []byte("m1"))
	pub.Publish("data", 1, false, []byte("m2"))
	pub.Publish("data", 1, false, []byte("m3"))
	time.Sleep(150 * time.Millisecond)

	w2 := mustConnect(t, addr, "only-w", false)
	defer w2.Close()
	got := recvN(t, w2, 3, 2*time.Second)
	for i, want := range []string{"m1", "m2", "m3"} {
		if string(got[i].Payload) != want {
			t.Fatalf("fallback replay order wrong at %d: got %q want %q", i, got[i].Payload, want)
		}
	}
	if extra := drainFor(t, w2, 300*time.Millisecond); len(extra) != 0 {
		t.Fatalf("fallback replay duplicated %d messages", len(extra))
	}
	t.Log("业务[共享订阅-无成员兜底] 结果: PASS，兜底消息 m1->m2->m3 按序补发且无重复")
}

// packetRead 从原始连接读取一个完整报文。
func packetRead(conn net.Conn) (*packet.Packet, error) {
	return packet.ReadPacket(conn)
}

// mustEncodeAck 编码 QoS1 的 PUBACK。
func mustEncodeAck(t *testing.T, id uint16) []byte {
	t.Helper()
	ack := &packet.Packet{Header: packet.FixedHeader{Type: packet.TypePUBACK}, PacketID: id}
	raw, err := ack.Encode(packet.Version311)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
