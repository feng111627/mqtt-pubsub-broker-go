package broker_test

import (
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
)

// drain 非阻塞地取出当前通道里的消息数量。
func drain(c *client.Client) int {
	n := 0
	for {
		select {
		case <-c.Messages():
			n++
		default:
			return n
		}
	}
}

// 业务：共享订阅组内单选投递 —— 同组两个成员，每条消息只到其中一个，不组内广播。
func TestSharedGroupSingleDelivery(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	a := mustConnect(t, addr, "ga", false)
	defer a.Close()
	c := mustConnect(t, addr, "gc", false)
	defer c.Close()
	if _, err := a.Subscribe("$share/g/jobs/+", 1); err != nil {
		t.Fatal(err)
	}
	if _, err := c.Subscribe("$share/g/jobs/+", 1); err != nil {
		t.Fatal(err)
	}
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()

	for i := 0; i < 4; i++ {
		if err := pub.Publish("jobs/1", 1, false, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(400 * time.Millisecond)

	na, nc := drain(a), drain(c)
	if na+nc != 4 {
		t.Fatalf("expected exactly 4 group deliveries, got a=%d c=%d", na, nc)
	}
	t.Logf("业务=共享组单选投递 用例=两成员各订阅$share/g/jobs/+ 结果: A=%d C=%d 组内合计=%d", na, nc, na+nc)
}

// 业务：组内负载均衡 —— 多消息在多个在线成员间轮询分摊，而非全落同一成员。
func TestSharedGroupBalanced(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()
	ids := []string{"m1", "m2", "m3"}
	members := map[string]*client.Client{}
	for _, id := range ids {
		cc := mustConnect(t, addr, id, false)
		defer cc.Close()
		if _, err := cc.Subscribe("$share/bal/bal/topic", 1); err != nil {
			t.Fatal(err)
		}
		members[id] = cc
	}
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()

	const n = 12
	for i := 0; i < n; i++ {
		if err := pub.Publish("bal/topic", 1, false, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	time.Sleep(500 * time.Millisecond)

	total := 0
	got := map[string]int{}
	for _, id := range ids {
		k := drain(members[id])
		got[id] = k
		total += k
	}
	if total != n {
		t.Fatalf("total=%d want %d (%v)", total, n, got)
	}
	for _, id := range ids {
		if got[id] == 0 {
			t.Fatalf("member %s got no messages; rebalance did not spread load: %v", id, got)
		}
	}
	t.Logf("业务=组内轮询负载均衡 用例=3成员12消息 结果: 分摊=%v 合计=%d", got, total)
}

// 业务：组内暂无在线成员时兜底 —— 消息进入组队列，成员上线后补发，不凭空消失。
func TestSharedGroupBufferedWhenNoMember(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	a := mustConnect(t, addr, "buf-a", false)
	if _, err := a.Subscribe("$share/buf/buf/task", 1); err != nil {
		t.Fatal(err)
	}
	a.Close()
	time.Sleep(150 * time.Millisecond)

	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	if err := pub.Publish("buf/task", 1, false, []byte("while-empty")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)

	a2 := mustConnect(t, addr, "buf-a", false)
	defer a2.Close()
	m := recvOne(t, a2, 2*time.Second)
	if string(m.Payload) != "while-empty" {
		t.Fatalf("buffered group message lost: %q", m.Payload)
	}
	t.Logf("业务=组无成员兜底堆积 用例=唯一成员离线期间发布 结果: 重连后补发=%q", m.Payload)
}
