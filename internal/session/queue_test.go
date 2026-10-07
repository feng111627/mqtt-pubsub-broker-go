package session

import (
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// 本组用例覆盖：离线堆积按发布顺序、容量上限拒绝新消息（不淘汰旧消息）、
// 消息 TTL 过期清理（补发时与入队惰性清理两种触发）、会话过期语义，
// 以及会话之间互不挤占/串号。

func TestOfflineQueueFIFOOrder(t *testing.T) {
	t.Log("业务[离线堆积-顺序] 用例: 离线期间入队 3 条，补发必须严格按发布顺序")
	s := New("c", false)
	base := time.Unix(1000, 0)
	for _, topic := range []string{"a", "b", "c"} {
		if r := s.EnqueueAt(&packet.Packet{Topic: topic}, base); !r.Accepted {
			t.Fatalf("enqueue %s rejected", topic)
		}
	}
	d := s.DrainAt(base)
	if len(d.Messages) != 3 {
		t.Fatalf("drained=%d", len(d.Messages))
	}
	for i, want := range []string{"a", "b", "c"} {
		if d.Messages[i].Topic != want {
			t.Fatalf("position %d got %s want %s", i, d.Messages[i].Topic, want)
		}
	}
	t.Log("业务[离线堆积-顺序] 结果: PASS，补发顺序 a->b->c")
}

func TestOfflineQueueCapRejectsNewKeepsOld(t *testing.T) {
	t.Log("业务[离线堆积-容量上限] 用例: 容量=2，第 3 条应被拒(Full)，" +
		"旧消息顺序不动；补发只得到前 2 条")
	cfg := Config{MaxQueue: 2, MessageTTL: 0}
	s := NewWithConfig("c", false, cfg)
	base := time.Unix(1000, 0)
	if r := s.EnqueueAt(&packet.Packet{Topic: "1"}, base); !r.Accepted || r.Full {
		t.Fatal("enqueue 1 should accept")
	}
	if r := s.EnqueueAt(&packet.Packet{Topic: "2"}, base); !r.Accepted {
		t.Fatal("enqueue 2 should accept")
	}
	r := s.EnqueueAt(&packet.Packet{Topic: "3"}, base)
	if r.Accepted || !r.Full {
		t.Fatalf("enqueue 3 must be rejected as Full, got %+v", r)
	}
	if s.DroppedCount() != 1 {
		t.Fatalf("dropped count=%d", s.DroppedCount())
	}
	d := s.DrainAt(base)
	if len(d.Messages) != 2 || d.Messages[0].Topic != "1" || d.Messages[1].Topic != "2" {
		t.Fatalf("old messages must survive full: %+v", d.Messages)
	}
	t.Log("业务[离线堆积-容量上限] 结果: PASS，满了拒绝新消息，旧消息 1->2 保留")
}

func TestOfflineMessageTTLExpiry(t *testing.T) {
	t.Log("业务[离线堆积-消息TTL] 用例: TTL=1m，3 条同在 t0 入队；" +
		"补发时刻 t0+2m 全部过期不补发，t0+30s 则全部按序补发")
	cfg := Config{MaxQueue: 10, MessageTTL: time.Minute}
	t0 := time.Unix(1000, 0)

	s1 := NewWithConfig("c1", false, cfg)
	for _, tp := range []string{"m1", "m2", "m3"} {
		if r := s1.EnqueueAt(&packet.Packet{Topic: tp}, t0); !r.Accepted {
			t.Fatal("enqueue", tp)
		}
	}
	d1 := s1.DrainAt(t0.Add(2 * time.Minute))
	if d1.Expired != 3 || len(d1.Messages) != 0 {
		t.Fatalf("all should expire: expired=%d kept=%d", d1.Expired, len(d1.Messages))
	}

	s2 := NewWithConfig("c2", false, cfg)
	for _, tp := range []string{"m1", "m2", "m3"} {
		s2.EnqueueAt(&packet.Packet{Topic: tp}, t0)
	}
	d2 := s2.DrainAt(t0.Add(30 * time.Second))
	if d2.Expired != 0 || len(d2.Messages) != 3 {
		t.Fatalf("all should survive: expired=%d kept=%d", d2.Expired, len(d2.Messages))
	}
	for i, want := range []string{"m1", "m2", "m3"} {
		if d2.Messages[i].Topic != want {
			t.Fatalf("order broken at %d: %s", i, d2.Messages[i].Topic)
		}
	}
	t.Log("业务[离线堆积-消息TTL] 结果: PASS，过 TTL 全部过期，TTL 内全部按序补发")
}

func TestOfflineQueueLazyPruneOnEnqueue(t *testing.T) {
	t.Log("业务[离线堆积-入队惰性清理] 用例: 容量=2，两条旧消息过期后再入队新消息，" +
		"过期者被清、容量释放，最终只保留新消息")
	cfg := Config{MaxQueue: 2, MessageTTL: time.Minute}
	s := NewWithConfig("c", false, cfg)
	t0 := time.Unix(1000, 0)
	s.EnqueueAt(&packet.Packet{Topic: "old1"}, t0)
	s.EnqueueAt(&packet.Packet{Topic: "old2"}, t0)
	r := s.EnqueueAt(&packet.Packet{Topic: "new"}, t0.Add(2*time.Minute))
	if !r.Accepted || r.Expired != 2 || r.Full {
		t.Fatalf("expected prune+accept, got %+v", r)
	}
	d := s.DrainAt(t0.Add(2 * time.Minute))
	if len(d.Messages) != 1 || d.Messages[0].Topic != "new" {
		t.Fatalf("only new should survive: %+v", d.Messages)
	}
	t.Log("业务[离线堆积-入队惰性清理] 结果: PASS，旧消息过期释放容量，新消息入队成功")
}

func TestSessionsDoNotCrowdEachOther(t *testing.T) {
	t.Log("业务[离线堆积-会话隔离] 用例: 会话 A 堆满不应挤占会话 B，且 A/B 消息不串号")
	cfg := Config{MaxQueue: 2, MessageTTL: 0}
	a := NewWithConfig("a", false, cfg)
	b := NewWithConfig("b", false, cfg)
	base := time.Unix(1000, 0)
	a.EnqueueAt(&packet.Packet{Topic: "a1"}, base)
	a.EnqueueAt(&packet.Packet{Topic: "a2"}, base)
	if r := a.EnqueueAt(&packet.Packet{Topic: "a3"}, base); !r.Full {
		t.Fatal("A should be full")
	}
	if r := b.EnqueueAt(&packet.Packet{Topic: "b1"}, base); !r.Accepted {
		t.Fatal("B must accept even when A is full")
	}
	db := b.DrainAt(base)
	if len(db.Messages) != 1 || db.Messages[0].Topic != "b1" {
		t.Fatalf("B queue contaminated: %+v", db.Messages)
	}
	t.Log("业务[离线堆积-会话隔离] 结果: PASS，A 满不影响 B，消息不串号")
}

func TestSessionExpiry(t *testing.T) {
	t.Log("业务[会话过期] 用例: 会话离线超过 SessionExpiry 后应过期，" +
		"过期会话不再入队；重连同 ID 得到新会话")
	store := NewStoreWithConfig(Config{MaxQueue: 10, SessionExpiry: time.Minute})
	sess, _ := store.Get("c", false)
	sess.AddSub("x", 1)
	t0 := time.Unix(1000, 0)
	sess.SetOnline(false, t0)

	if sess.IsExpiredAt(t0.Add(59 * time.Second)) {
		t.Fatal("should not expire before deadline")
	}
	if !sess.ExpireAt(t0.Add(time.Minute)) {
		t.Fatal("should expire at deadline")
	}
	if !sess.Expired() {
		t.Fatal("Expired() should be true")
	}
	if r := sess.EnqueueAt(&packet.Packet{Topic: "x"}, t0.Add(time.Minute)); r.Accepted {
		t.Fatal("expired session must not accept new queue messages")
	}
	if s2, resumed := store.Get("c", false); resumed || s2 == sess {
		t.Fatal("expired session must not be resumed")
	}
	t.Log("业务[会话过期] 结果: PASS，到期过期、不再入队、重连同 ID 得新会话")
}
