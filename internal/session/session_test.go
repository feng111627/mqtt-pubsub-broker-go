package session

import (
	"testing"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

func TestStoreCleanStartSemantics(t *testing.T) {
	st := NewStore()
	s1, resumed := st.Get("c1", false)
	if resumed {
		t.Fatal("first get should not resume")
	}
	s1.AddSub("a/#", 1)

	// 持久会话重连：订阅保留
	s2, resumed := st.Get("c1", false)
	if !resumed || s2 != s1 {
		t.Fatal("expected resume of persistent session")
	}
	if len(s2.Subs()) != 1 {
		t.Fatal("subscriptions lost on resume")
	}

	// CleanStart=true：旧会话被丢弃，订阅清空
	s3, resumed := st.Get("c1", true)
	if resumed || s3 == s1 {
		t.Fatal("clean start should discard old session")
	}
	if len(s3.Subs()) != 0 {
		t.Fatal("clean start should have no subscriptions")
	}
}

func TestQoS2InboundDedup(t *testing.T) {
	s := New("c", false)
	if s.TrackIn(10) {
		t.Fatal("first TrackIn should not be dup")
	}
	if !s.TrackIn(10) {
		t.Fatal("second TrackIn should be dup")
	}
	if !s.CompleteIn(10) {
		t.Fatal("CompleteIn should succeed")
	}
	if s.TrackIn(10) {
		t.Fatal("after complete, same id is a new message")
	}
}

func TestInflightOutLifecycle(t *testing.T) {
	s := New("c", false)
	id := s.NextPacketID()
	if id == 0 {
		t.Fatal("packet id must not be 0")
	}
	s.TrackOut(&Inflight{PacketID: id, Stage: 1})
	if !s.AdvanceOut(id) {
		t.Fatal("AdvanceOut should succeed at stage 1")
	}
	if s.AdvanceOut(id) {
		t.Fatal("AdvanceOut should fail at stage 2")
	}
	if _, ok := s.AckOut(id); !ok {
		t.Fatal("AckOut should find inflight")
	}
	if _, ok := s.AckOut(id); ok {
		t.Fatal("AckOut twice should fail")
	}
}

func TestOfflineQueue(t *testing.T) {
	s := New("c", false)
	s.Enqueue(&packet.Packet{Topic: "a"})
	s.Enqueue(&packet.Packet{Topic: "b"})
	got := s.Drain()
	if len(got) != 2 || got[0].Topic != "a" || got[1].Topic != "b" {
		t.Fatalf("drain = %v", got)
	}
	if len(s.Drain()) != 0 {
		t.Fatal("drain should empty the queue")
	}
}

func TestPacketIDSkipsInflight(t *testing.T) {
	s := New("c", false)
	id1 := s.NextPacketID()
	s.TrackOut(&Inflight{PacketID: id1})
	id2 := s.NextPacketID()
	if id2 == id1 {
		t.Fatal("packet id collision with inflight")
	}
}
