package broker_test

import (
	"sync"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/delivery"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/group"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
)

// startBrokerWith 用自定义配置与可控时钟启动代理（不监听 TCP 时也可直接 Route）。
func startBrokerWith(t *testing.T, cfg broker.Config, now func() time.Time) *broker.Broker {
	t.Helper()
	b := broker.NewWithConfigForTest(cfg, now)
	t.Cleanup(func() { b.Close() })
	return b
}

func pubPacket(topic string, qos byte) *packet.Packet {
	return &packet.Packet{
		Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
		Topic:   topic,
		QoS:     qos,
		Payload: []byte("x"),
	}
}

// TestResultNoSubscribers 没有任何订阅命中时结果可区分为 NoSubscribers。
func TestResultNoSubscribers(t *testing.T) {
	t.Log("业务[结果分类-无订阅者] 用例: 向无人订阅的主题发布，结果应为 no-subscribers")
	b := startBrokerWith(t, broker.DefaultConfig(), time.Now)
	res := b.Route(pubPacket("nobody/here", 1))
	if !res.HasStatus(delivery.NoSubscribers) {
		t.Fatalf("expected no-subscribers, got %+v", res.Summary())
	}
	t.Log("业务[结果分类-无订阅者] 结果: PASS")
}

// TestResultQueuedOffline 订阅者离线（持久会话）时结果为 queued-offline。
func TestResultQueuedOffline(t *testing.T) {
	t.Log("业务[结果分类-离线入队] 用例: 持久订阅者断线后发布，结果应为 queued-offline")
	cfg := broker.DefaultConfig()
	now := time.Unix(1_000_000, 0)
	clock := func() time.Time { return now }
	b := startBrokerWith(t, cfg, clock)

	sess := broker.MustSessionForTest(t, b, "persist-x", false)
	sess.AddSub("off/x", 1)
	sess.SetOnline(false, now)

	res := b.Route(pubPacket("off/x", 1))
	if !res.HasStatus(delivery.QueuedOffline) {
		t.Fatalf("expected queued-offline, got %+v", res.Summary())
	}
	if res.AnyDropped() {
		t.Fatal("queued-offline must not count as dropped")
	}
	t.Log("业务[结果分类-离线入队] 结果: PASS")
}

// TestResultSessionExpired 会话过期后再发布，结果为 session-expired。
func TestResultSessionExpired(t *testing.T) {
	t.Log("业务[结果分类-会话过期] 用例: 会话过期后命中订阅，结果应为 session-expired")
	cfg := broker.DefaultConfig()
	cfg.Session.SessionExpiry = time.Minute
	now := time.Unix(2_000_000, 0)
	clock := func() time.Time { return now }
	b := startBrokerWith(t, cfg, clock)

	sess := broker.MustSessionForTest(t, b, "exp-c", false)
	sess.AddSub("exp/x", 1)
	sess.SetOnline(false, now)
	// 推进时间并触发一轮巡检，使会话过期并被摘除
	now = now.Add(2 * time.Minute)
	b.SweepForTest(now)

	res := b.Route(pubPacket("exp/x", 1))
	if !res.HasStatus(delivery.SessionExpired) {
		t.Fatalf("expected session-expired, got %+v", res.Summary())
	}
	t.Log("业务[结果分类-会话过期] 结果: PASS")
}

// TestResultGroupUnavailable 组兜底关闭且无在线成员时，结果为 group-unavailable。
func TestResultGroupUnavailable(t *testing.T) {
	t.Log("业务[结果分类-组无可用成员] 用例: 兜底队列关闭、组内无在线成员，" +
		"结果应为 group-unavailable（不是无订阅者，也不是静默吞掉）")
	cfg := broker.DefaultConfig()
	cfg.Group = group.Config{GroupQueueCap: 0} // 关闭兜底
	now := time.Unix(3_000_000, 0)
	b := startBrokerWith(t, cfg, func() time.Time { return now })

	sess := broker.MustSessionForTest(t, b, "gm", false)
	sess.AddSub("$share/g/task", 1)
	sess.SetOnline(false, now)
	b.AddGroupMemberForTest("g", "gm")

	res := b.Route(pubPacket("task", 1))
	if !res.HasStatus(delivery.GroupUnavailable) {
		t.Fatalf("expected group-unavailable, got %+v", res.Summary())
	}
	t.Log("业务[结果分类-组无可用成员] 结果: PASS")
}

// TestResultQueueFullSession 单会话队列打满后，新消息结果为 queue-full，
// 且其他会话不受影响。
func TestResultQueueFullSession(t *testing.T) {
	t.Log("业务[结果分类-队列满] 用例: 会话队列容量=2，离线发 3 条，第 3 条 queue-full，" +
		"另一会话仍可正常入队")
	cfg := broker.DefaultConfig()
	cfg.Session = session.Config{MaxQueue: 2, MessageTTL: 0, SessionExpiry: 0}
	now := time.Unix(4_000_000, 0)
	b := startBrokerWith(t, cfg, func() time.Time { return now })

	s1 := broker.MustSessionForTest(t, b, "full-c", false)
	s1.AddSub("q/full", 1)
	s1.SetOnline(false, now)
	s2 := broker.MustSessionForTest(t, b, "other-c", false)
	s2.AddSub("q/other", 1)
	s2.SetOnline(false, now)

	r1 := b.Route(pubPacket("q/full", 1))
	r2 := b.Route(pubPacket("q/full", 1))
	r3 := b.Route(pubPacket("q/full", 1))
	if !r1.HasStatus(delivery.QueuedOffline) || !r2.HasStatus(delivery.QueuedOffline) {
		t.Fatalf("first two should queue: %+v %+v", r1.Summary(), r2.Summary())
	}
	if !r3.HasStatus(delivery.QueueFull) || !r3.AnyDropped() {
		t.Fatalf("third should be queue-full, got %+v", r3.Summary())
	}
	ro := b.Route(pubPacket("q/other", 1))
	if !ro.HasStatus(delivery.QueuedOffline) {
		t.Fatalf("other session must be unaffected, got %+v", ro.Summary())
	}
	t.Log("业务[结果分类-队列满] 结果: PASS，满会话被拒，其他会话正常")
}

var _ = client.Connect // 保持 client 包引用（供后续真实 TCP 用例）

// TestConcurrentRouteAndMembership 进程内并发：大量 Route 与共享组成员增删
// 并发执行，验证路由可区分结果始终自洽（不 panic、不卡死）且全局堆积有界。
func TestConcurrentRouteAndMembership(t *testing.T) {
	t.Log("业务[并发路由-成员增删] 用例: 100 个离线持久会话订阅共享组，" +
		"并发 Route 与组增删/巡检，验证不 panic、不卡死、堆积计数自洽")
	cfg := broker.DefaultConfig()
	cfg.Group = group.Config{GroupQueueCap: 50, MessageTTL: time.Hour}
	cfg.Session = session.Config{MaxQueue: 5, MessageTTL: time.Hour}
	now := time.Unix(5_000_000, 0)
	var mu sync.Mutex
	cur := now
	clock := func() time.Time {
		mu.Lock()
		defer mu.Unlock()
		return cur
	}
	b := startBrokerWith(t, cfg, clock)

	// 一组离线成员（组兜底队列容量 50）
	for i := 0; i < 10; i++ {
		id := "mem-" + itoa(i)
		s := broker.MustSessionForTest(t, b, id, false)
		s.AddSub("$share/grp/work", 1)
		s.SetOnline(false, now)
		b.AddGroupMemberForTest("grp", id)
	}

	var wg sync.WaitGroup
	stop := make(chan struct{})

	// 发布者：持续向共享主题路由
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 500; i++ {
			b.Route(pubPacket("work", 1))
		}
	}()

	// 成员抖动：增/删组成员
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 200; i++ {
			id := "mem-" + itoa(i%10)
			if i%2 == 0 {
				b.AddGroupMemberForTest("grp", id)
			} else {
				b.RemoveGroupMemberForTest("grp", id)
			}
		}
	}()

	// 巡检：推进时间触发 TTL/过期
	wg.Add(1)
	go func() {
		defer wg.Done()
		for i := 0; i < 20; i++ {
			mu.Lock()
			cur = cur.Add(time.Minute)
			snap := cur
			mu.Unlock()
			b.SweepForTest(snap)
		}
		close(stop)
	}()

	done := make(chan struct{})
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("concurrent routing did not finish (deadlock?)")
	}

	// 全局堆积计数不应超过各组队列容量之和与会话队列之和的合理上界。
	// 组兜底上限 50；离线成员都不在线，但消息在组队列与个别会话队列间，
	// 计数必须非负且不发散。
	total := b.QueuedTotalForTest()
	if total < 0 {
		t.Fatalf("queued total negative: %d", total)
	}
	if total > int64(cfg.Group.GroupQueueCap)+int64(cfg.Session.MaxQueue)*10+int64(cfg.Group.GroupQueueCap) {
		t.Fatalf("queued total unbounded: %d", total)
	}
	t.Logf("业务[并发路由-成员增删] 结果: PASS，无 panic/卡死，全局堆积=%d（有界）", total)
}
