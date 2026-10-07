package broker

import (
	"sync"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// fakeMember 模拟一个在线成员连接，记录收到的消息。
type fakeMember struct {
	id       string
	closed   bool
	mu       sync.Mutex
	got      []*packet.Packet
	failNext bool
}

// 用 groupManager 时需要 *clientConn，但内部只用到 isClosed/tryEnqueue/sess。
// 因此组管理器的核心行为通过集成测试（真实连接）覆盖；这里只做纯数据结构层面的
// 轮询/兜底/过期断言，避免依赖网络。

func TestGroupPendingTTLAndCap(t *testing.T) {
	now := time.Unix(1000, 0)
	m := newGroupManager(GroupConfig{MaxPending: 2, MessageTTL: time.Hour}, nil)
	m.now = func() time.Time { return now }

	m.mu.Lock()
	g := m.groupLocked("g\x00f")
	if m.bufferLocked(g, &packet.Packet{Payload: []byte("m1")}) != groupBuffered {
		t.Fatal("m1 should buffer")
	}
	if m.bufferLocked(g, &packet.Packet{Payload: []byte("m2")}) != groupBuffered {
		t.Fatal("m2 should buffer")
	}
	if m.bufferLocked(g, &packet.Packet{Payload: []byte("m3")}) != groupFull {
		t.Fatal("m3 should be rejected: pending full")
	}
	if len(g.pending) != 2 {
		t.Fatalf("pending len=%d want 2", len(g.pending))
	}

	// 推进时间超过 TTL，过期消息应被清掉，之后又能入队
	m.now = func() time.Time { return now.Add(2 * time.Hour) }
	m.prunePendingLocked(g)
	if len(g.pending) != 0 {
		t.Fatalf("expired pending should be pruned, got %d", len(g.pending))
	}
	if m.bufferLocked(g, &packet.Packet{Payload: []byte("m4")}) != groupBuffered {
		t.Fatal("m4 should buffer after prune")
	}
	m.mu.Unlock()
}

// TestPublishWithoutGroupBuffers 验证组尚未建立时的发布结果为缓冲语义（不会 panic）。
func TestPublishUnknownGroup(t *testing.T) {
	m := newGroupManager(DefaultGroupConfig, nil)
	res := m.publish("nope", &packet.Packet{QoS: 0})
	if res.status != groupBuffered {
		t.Fatalf("unknown group publish status=%d want buffered", res.status)
	}
}
