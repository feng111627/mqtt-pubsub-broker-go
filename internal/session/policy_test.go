package session

import (
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

func newTestSession() *Session {
	return NewWithLimits("c", false, Limits{
		MaxQueue:   3,
		MessageTTL: time.Hour,
		SessionTTL: 24 * time.Hour,
	})
}

// TestOfflineQueueCapRejectsNew 验证容量上限：满了拒新，不淘汰旧消息，不串会话。
func TestOfflineQueueCapRejectsNew(t *testing.T) {
	s := newTestSession()
	base := time.Now()
	for i := 0; i < 3; i++ {
		if st := s.EnqueueAt(&packet.Packet{Topic: "t", Payload: []byte{byte(i)}}, base); st != EnqueueAccepted {
			t.Fatalf("enqueue %d should be accepted, got %v", i, st)
		}
	}
	if st := s.EnqueueAt(&packet.Packet{Topic: "t", Payload: []byte{9}}, base); st != EnqueueQueueFull {
		t.Fatalf("4th enqueue should be QueueFull, got %v", st)
	}
	// 旧消息仍在且顺序不变
	got := s.DrainAt(base)
	if len(got) != 3 || got[0].Payload[0] != 0 || got[2].Payload[0] != 2 {
		t.Fatalf("old messages must be kept in order, got %v", got)
	}
	if st := s.Stats(); st.FullRejects != 1 {
		t.Fatalf("FullRejects=%d want 1", st.FullRejects)
	}
}

// TestOfflineQueueTTLExpiry 验证过期规则：补发前惰性清理，未过期保留且顺序不变。
func TestOfflineQueueTTLExpiry(t *testing.T) {
	s := NewWithLimits("c", false, Limits{MaxQueue: 100, MessageTTL: time.Hour, SessionTTL: 0})
	t0 := time.Now()
	s.EnqueueAt(&packet.Packet{Payload: []byte("old1")}, t0)
	s.EnqueueAt(&packet.Packet{Payload: []byte("old2")}, t0.Add(30*time.Minute))
	s.EnqueueAt(&packet.Packet{Payload: []byte("fresh")}, t0.Add(70*time.Minute))

	// 在 t0+2h：old1/old2 已过期（>1h），fresh 距入队 50m 未过期
	got := s.DrainAt(t0.Add(2 * time.Hour))
	if len(got) != 1 || string(got[0].Payload) != "fresh" {
		t.Fatalf("only fresh message should survive, got %v", got)
	}
	if s.Stats().ExpiredDrops != 2 {
		t.Fatalf("ExpiredDrops=%d want 2", s.Stats().ExpiredDrops)
	}
}

// TestSessionTTLAndTombstone 验证持久会话过期转墓碑、可区分、墓碑期满清除。
func TestSessionTTLAndTombstone(t *testing.T) {
	st := NewStoreWithLimits(Limits{MaxQueue: 10, MessageTTL: 0, SessionTTL: time.Hour})
	sess, _ := st.Get("persist", false)
	sess.AddSub("a/#", 1)
	sess.Enqueue(&packet.Packet{Topic: "a"})

	now := time.Now()
	if sess.Expired(now.Add(30*time.Minute), time.Hour) {
		t.Fatal("session should not be expired within TTL")
	}
	// 超过 TTL，sweep 转墓碑
	expired := st.Sweep(now.Add(2 * time.Hour))
	if len(expired) != 1 || expired[0] != "persist" {
		t.Fatalf("unexpected expired list: %v", expired)
	}
	if st.Peek("persist") != nil {
		t.Fatal("expired session should be removed from active store")
	}
	if !st.IsExpired("persist") {
		t.Fatal("expired client should be in tombstone window")
	}
	subs := st.ExpiredSubs("persist")
	if len(subs) != 1 {
		t.Fatalf("tombstone should retain subscription snapshot, got %v", subs)
	}
	// 客户端在墓碑期内回来：撤销墓碑，会话新建
	if _, resumed := st.Get("persist", false); resumed {
		t.Fatal("expired session reconnect should not resume old session")
	}
	if st.IsExpired("persist") {
		t.Fatal("reconnect should clear tombstone")
	}

	// 再次过期：墓碑保留满一个 TTL 后彻底删除
	st.Get("persist", false)
	t2 := time.Now()
	st.Sweep(t2.Add(2 * time.Hour)) // 转墓碑
	if !st.IsExpired("persist") {
		t.Fatal("should be in tombstone window again")
	}
	st.Sweep(t2.Add(4 * time.Hour)) // 墓碑满一个 TTL
	if st.IsExpired("persist") {
		t.Fatal("tombstone should be purged after a full TTL")
	}
}

// TestSessionsDoNotStarveEachOther 验证一个会话积压不影响另一个会话。
func TestSessionsDoNotStarveEachOther(t *testing.T) {
	s1 := NewWithLimits("c1", false, Limits{MaxQueue: 2, MessageTTL: 0, SessionTTL: 0})
	s2 := NewWithLimits("c2", false, Limits{MaxQueue: 2, MessageTTL: 0, SessionTTL: 0})
	s1.Enqueue(&packet.Packet{Payload: []byte("1")})
	s1.Enqueue(&packet.Packet{Payload: []byte("2")})
	if s1.Enqueue(&packet.Packet{Payload: []byte("3")}) != EnqueueQueueFull {
		t.Fatal("s1 should be full")
	}
	// s2 容量独立，不受 s1 积压影响
	if s2.Enqueue(&packet.Packet{Payload: []byte("a")}) != EnqueueAccepted {
		t.Fatal("s2 should still accept messages")
	}
	if got := s2.Drain(); len(got) != 1 || string(got[0].Payload) != "a" {
		t.Fatalf("s2 queue corrupted: %v", got)
	}
}

// TestReclaimGroupInflight 验证掉线时只摘出共享组在途，普通在途保留。
func TestReclaimGroupInflight(t *testing.T) {
	s := New("c", false)
	id1 := s.NextPacketID()
	s.TrackOut(&Inflight{PacketID: id1, GroupKey: "g\x00f", Token: 7})
	id2 := s.NextPacketID()
	s.TrackOut(&Inflight{PacketID: id2}) // 普通订阅在途

	reclaimed := s.ReclaimGroupInflight()
	if len(reclaimed) != 1 || reclaimed[0].Token != 7 || reclaimed[0].GroupKey != "g\x00f" {
		t.Fatalf("unexpected reclaimed: %+v", reclaimed)
	}
	if len(s.InflightOut()) != 1 {
		t.Fatal("normal inflight must be retained for reconnect retransmit")
	}
	if len(s.ReclaimGroupInflight()) != 0 {
		t.Fatal("group inflight already removed")
	}
}
