package broker_test

import (
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
)

// 业务：离线堆积按发布顺序补发（含 QoS0 消息也会为持久会话堆积）。
func TestOfflineAccumulationOrdered(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	sub := mustConnect(t, addr, "persist", false)
	sub.Subscribe("off/+", 1)
	sub.Close()
	time.Sleep(150 * time.Millisecond)

	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	// QoS1 与 QoS0 混合，顺序都应保留
	if err := pub.Publish("off/1", 1, false, []byte("m1")); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish("off/2", 0, false, []byte("m2")); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish("off/3", 1, false, []byte("m3")); err != nil {
		t.Fatal(err)
	}
	time.Sleep(150 * time.Millisecond)

	sub2 := mustConnect(t, addr, "persist", false)
	defer sub2.Close()
	want := []string{"m1", "m2", "m3"}
	for i, w := range want {
		m := recvOne(t, sub2, 2*time.Second)
		if string(m.Payload) != w {
			t.Fatalf("msg %d out of order: got %q want %q", i, m.Payload, w)
		}
	}
	expectNone(t, sub2, 300*time.Millisecond)
	t.Logf("业务=离线堆积按序补发 用例=QoS1/QoS0混合3条 结果: 重连后按发布顺序收到 %v", want)
}

// 业务：容量上限 —— 队列满后新消息被拒，报告 StatusQueueFull，且旧消息不被挤掉。
func TestOfflineQueueFullRejected(t *testing.T) {
	cfg := broker.Config{
		Session: session.Limits{MaxQueue: 2, MessageTTL: time.Hour, SessionTTL: time.Hour},
	}
	b := startBrokerWith(t, cfg)

	sub := mustConnect(t, b.Addr(), "cap", false)
	sub.Subscribe("q/+", 1)
	sub.Close()
	time.Sleep(150 * time.Millisecond)

	rep1, err := b.Publish("q/1", 1, false, []byte("a"))
	if err != nil {
		t.Fatal(err)
	}
	rep2, _ := b.Publish("q/2", 1, false, []byte("b"))
	rep3, _ := b.Publish("q/3", 1, false, []byte("c"))

	if rep1.Targets[0].Status != broker.StatusQueuedOffline {
		t.Fatalf("first status=%d want QueuedOffline", rep1.Targets[0].Status)
	}
	if rep2.Targets[0].Status != broker.StatusQueuedOffline {
		t.Fatalf("second status=%d want QueuedOffline", rep2.Targets[0].Status)
	}
	if rep3.Targets[0].Status != broker.StatusQueueFull {
		t.Fatalf("third status=%d want QueueFull", rep3.Targets[0].Status)
	}
	t.Logf("业务=离线队列容量上限 用例=容量2发3条 结果: 前2=%v 第3=%v（拒新保旧）",
		rep2.Targets[0].Status, rep3.Targets[0].Status)

	// 重连只能收到 a、b，顺序不变
	sub2 := mustConnect(t, b.Addr(), "cap", false)
	defer sub2.Close()
	if string(recvOne(t, sub2, time.Second).Payload) != "a" ||
		string(recvOne(t, sub2, time.Second).Payload) != "b" {
		t.Fatal("older messages must survive queue-full rejection")
	}
	expectNone(t, sub2, 300*time.Millisecond)
}

// 业务：消息过期 —— 超过 MessageTTL 的离线消息在补发前被清理，未过期的仍送达。
func TestOfflineMessageExpiry(t *testing.T) {
	cfg := broker.Config{
		Session: session.Limits{MaxQueue: 100, MessageTTL: 400 * time.Millisecond, SessionTTL: time.Hour},
		Group:   broker.GroupConfig{MaxPending: 100, MessageTTL: 400 * time.Millisecond},
	}
	b := startBrokerWith(t, cfg)
	sub := mustConnect(t, b.Addr(), "exp", false)
	sub.Subscribe("e/+", 1)
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	b.Publish("e/1", 1, false, []byte("stale"))
	time.Sleep(500 * time.Millisecond) // 超过 MessageTTL
	b.Publish("e/2", 1, false, []byte("fresh"))

	sub2 := mustConnect(t, b.Addr(), "exp", false)
	defer sub2.Close()
	m := recvOne(t, sub2, 2*time.Second)
	if string(m.Payload) != "fresh" {
		t.Fatalf("expired message should be dropped, got %q", m.Payload)
	}
	expectNone(t, sub2, 300*time.Millisecond)
	t.Logf("业务=离线消息过期 用例=TTL=400ms 结果: stale 已过期丢弃，fresh 正常补发")
}

// 业务：会话过期 —— 超过 SessionTTL 后订阅清除，消息结果标记 SessionExpired；
// 墓碑期满后则彻底等同于无订阅者（NoSubscriber）。
func TestSessionExpiry(t *testing.T) {
	st := startBrokerStoreHooks(t) // 见下方构造：极短 TTL，手动驱动 sweep
	addr := st.addr

	sub := mustConnect(t, addr, "gone", false)
	sub.Subscribe("g/+", 1)
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	// 会话 TTL=100ms、sweep=50ms。轮询等待进入"已过期且墓碑保留"状态，
	// 不依赖固定睡眠时间，避免临界窗口抖动。
	deadline := time.Now().Add(2 * time.Second)
	var rep *broker.Report
	for time.Now().Before(deadline) {
		rep, _ = st.b.Publish("g/1", 1, false, []byte("x"))
		if len(rep.Targets) > 0 && rep.Targets[0].Status == broker.StatusSessionExpired {
			break
		}
		time.Sleep(20 * time.Millisecond)
	}
	if rep == nil || rep.NoSubscriber() {
		t.Fatal("tombstone window should report an expired session target")
	}
	if rep.Targets[0].Status != broker.StatusSessionExpired {
		t.Fatalf("status=%d want SessionExpired", rep.Targets[0].Status)
	}
	t.Logf("业务=会话过期 用例=TTL到期后发布 结果: 状态=%d（SessionExpired，可区分）", rep.Targets[0].Status)

	// 墓碑保留满一个 TTL 后彻底清除，之后发布等同无订阅者
	time.Sleep(250 * time.Millisecond)
	rep2, _ := st.b.Publish("g/1", 1, false, []byte("y"))
	if !rep2.NoSubscriber() {
		t.Fatalf("after tombstone purge expected no subscriber, got %+v", rep2.Targets)
	}
	t.Logf("业务=墓碑期满清除 结果: 之后发布 NoSubscriber=%v", rep2.NoSubscriber())
}

// 业务：无订阅者命中 —— 发布到无人订阅的主题返回 NoSubscriber。
func TestNoSubscriberResult(t *testing.T) {
	b := startBroker(t)
	rep, err := b.Publish("nobody/here", 1, false, []byte("x"))
	if err != nil {
		t.Fatal(err)
	}
	if !rep.NoSubscriber() || rep.Accepted() {
		t.Fatalf("expected no subscriber, got %+v", rep.Targets)
	}
	t.Logf("业务=无订阅者 用例=发布到未订阅主题 结果: NoSubscriber=%v", rep.NoSubscriber())
}

// 业务：MQTT 5.0 远程发布者可从 PUBACK 原因码区分投递结果。
func TestV5PubAckReasonCodes(t *testing.T) {
	b := startBroker(t)

	// 无订阅者：v5 PUBACK 应为 0x10
	code := publishV5Reason(t, b.Addr(), "none/topic", 1)
	if code != packet.ReasonNoMatchingSubscribers {
		t.Fatalf("no-subscriber reason=0x%02x want 0x%02x", code, packet.ReasonNoMatchingSubscribers)
	}
	t.Logf("业务=v5投递结果原因码 用例=无订阅者QoS1 结果: PUBACK=0x%02x", code)

	// 有在线订阅者：v5 PUBACK 应为成功 0x00
	sub := mustConnect(t, b.Addr(), "ok-sub", false)
	defer sub.Close()
	sub.Subscribe("ok/+", 1)
	code2 := publishV5Reason(t, b.Addr(), "ok/1", 1)
	if code2 != packet.ReasonSuccess {
		t.Fatalf("delivered reason=0x%02x want 0x00", code2)
	}
	t.Logf("业务=v5投递结果原因码 用例=有在线订阅者QoS1 结果: PUBACK=0x%02x", code2)
	if m := recvOne(t, sub, time.Second); string(m.Payload) != "x" {
		t.Fatalf("subscriber should receive message, got %q", m.Payload)
	}
}

// 业务：组兜底队列容量上限 —— 组无在线成员且堆积满后，新消息被拒，状态 GroupNoMember。
func TestGroupPendingFullRejected(t *testing.T) {
	cfg := broker.Config{
		Session: session.Limits{MaxQueue: 100, MessageTTL: time.Hour, SessionTTL: time.Hour},
		Group:   broker.GroupConfig{MaxPending: 2, MessageTTL: time.Hour},
	}
	b := startBrokerWith(t, cfg)
	addr := b.Addr()

	a := mustConnect(t, addr, "gm", false)
	a.Subscribe("$share/gp/j", 1)
	a.Close()
	time.Sleep(150 * time.Millisecond)

	r1, _ := b.Publish("j", 1, false, []byte("1"))
	r2, _ := b.Publish("j", 1, false, []byte("2"))
	r3, _ := b.Publish("j", 1, false, []byte("3"))
	if r1.Targets[0].Status != broker.StatusGroupBuffered {
		t.Fatalf("first=%d want Buffered", r1.Targets[0].Status)
	}
	if r2.Targets[0].Status != broker.StatusGroupBuffered {
		t.Fatalf("second=%d want Buffered", r2.Targets[0].Status)
	}
	if r3.Targets[0].Status != broker.StatusGroupNoMember {
		t.Fatalf("third=%d want GroupNoMember(full)", r3.Targets[0].Status)
	}
	t.Logf("业务=组兜底容量上限 用例=空组容量2发3条 结果: 前2=Buffered 第3=%d（GroupNoMember）", r3.Targets[0].Status)
}
