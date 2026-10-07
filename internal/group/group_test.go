package group

import (
	"testing"
	"time"
)

func mkQ(topic string, at time.Time) *Queued {
	return &Queued{Topic: topic, EnqueuedAt: at}
}

func TestRoundRobinBalancesAndRebalances(t *testing.T) {
	g := newGroup("g", DefaultConfig())
	g.AddMember("a")
	g.AddMember("b")
	g.AddMember("c")
	online := func(id string) bool { return true }
	got := map[string]int{}
	for i := 0; i < 6; i++ {
		id := g.Pick(online)
		if id == "" {
			t.Fatal("expected a member")
		}
		got[id]++
	}
	if got["a"] != 2 || got["b"] != 2 || got["c"] != 2 {
		t.Fatalf("not balanced: %v", got)
	}
	// 成员退出后自动重新平衡：只剩 a/c 时不会选中已退出的 b
	g.RemoveMember("b")
	got = map[string]int{}
	for i := 0; i < 4; i++ {
		got[g.Pick(online)]++
	}
	if got["b"] != 0 || got["a"]+got["c"] != 4 {
		t.Fatalf("rebalance failed: %v", got)
	}
}

func TestPickSkipsOfflineMembers(t *testing.T) {
	g := newGroup("g", DefaultConfig())
	g.AddMember("a")
	g.AddMember("b")
	online := func(id string) bool { return id == "b" }
	for i := 0; i < 3; i++ {
		if g.Pick(online) != "b" {
			t.Fatal("only b is online")
		}
	}
	onlineNone := func(string) bool { return false }
	if g.Pick(onlineNone) != "" {
		t.Fatal("no online member should pick empty")
	}
}

func TestFallbackQueueCapAndTTL(t *testing.T) {
	base := time.Unix(1000, 0)
	cfg := Config{GroupQueueCap: 2, MessageTTL: 90 * time.Second}
	g := newGroup("g", cfg)
	if !g.Enqueue(mkQ("1", base), base) {
		t.Fatal("enqueue 1")
	}
	if !g.Enqueue(mkQ("2", base.Add(40*time.Second)), base.Add(40*time.Second)) {
		t.Fatal("enqueue 2")
	}
	if g.Enqueue(mkQ("3", base.Add(40*time.Second)), base.Add(40*time.Second)) {
		t.Fatal("queue full must reject")
	}
	if g.DroppedCount() != 1 {
		t.Fatalf("dropped=%d", g.DroppedCount())
	}
	// 到 base+2m：第 1 条（age 2m）已过期腾出容量；第 2 条（age 80s）保留
	now := base.Add(2 * time.Minute)
	if !g.Enqueue(mkQ("3", now), now) {
		t.Fatal("after ttl expiry a slot frees")
	}
	kept, _ := g.Drain(now)
	if len(kept) != 2 || kept[0].Topic != "2" || kept[1].Topic != "3" {
		t.Fatalf("kept order wrong: %+v", kept)
	}
	if g.ExpiredCount() != 1 {
		t.Fatalf("expired count=%d", g.ExpiredCount())
	}
}

func TestSpliceReclaimedBeforeNew(t *testing.T) {
	base := time.Unix(1000, 0)
	g := newGroup("g", Config{GroupQueueCap: 1})
	g.Enqueue(mkQ("new1", base), base)
	// 回收消息豁免容量上限：即便 cap=1 且已有 1 条新消息，也不丢回收消息
	g.Splice([]*Queued{mkQ("reclaimed1", base), mkQ("reclaimed2", base)}, base)
	kept, _ := g.Drain(base)
	var order []string
	for _, q := range kept {
		order = append(order, q.Topic)
	}
	if len(order) != 3 || order[0] != "reclaimed1" || order[1] != "reclaimed2" || order[2] != "new1" {
		t.Fatalf("reclaimed must precede queued, got %v", order)
	}
}

func TestRegistryAddRemoveReap(t *testing.T) {
	r := NewRegistry(DefaultConfig())
	r.AddMember("g1", "a")
	r.AddMember("g1", "b")
	if got := r.Get("g1").Members(); len(got) != 2 {
		t.Fatalf("members=%v", got)
	}
	r.RemoveMember("g1", "a")
	r.RemoveMember("g1", "b")
	if r.Get("g1") != nil {
		t.Fatal("idle group should be reaped")
	}
	r.AddMember("g2", "c")
	r.RemoveClient("c")
	if r.Get("g2") != nil {
		t.Fatal("RemoveClient should reap empty group")
	}
}
