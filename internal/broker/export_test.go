package broker

import (
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
)

// NewWithConfigForTest 用配置与可控时钟创建代理（测试用）。
func NewWithConfigForTest(cfg Config, now func() time.Time) *Broker {
	if now == nil {
		now = time.Now
	}
	return newWithClock(cfg, now)
}

// MustSessionForTest 取出或创建会话（测试用）。
func MustSessionForTest(t *testing.T, b *Broker, clientID string, cleanStart bool) *session.Session {
	t.Helper()
	sess, _ := b.sessions.Get(clientID, cleanStart)
	return sess
}

// SweepForTest 立即执行一轮巡检（测试用）。
func (b *Broker) SweepForTest(now time.Time) { b.sweep(now) }

// AddGroupMemberForTest 直接把成员加入共享组（测试用）。
func (b *Broker) AddGroupMemberForTest(groupName, clientID string) {
	b.groups.AddMember(groupName, clientID)
}

// QueuedTotalForTest 返回当前全局堆积计数（测试用）。
func (b *Broker) QueuedTotalForTest() int64 { return b.queuedTotal.Load() }

// RemoveGroupMemberForTest 从共享组移除成员（测试用）。
func (b *Broker) RemoveGroupMemberForTest(groupName, clientID string) {
	b.groups.RemoveMember(groupName, clientID)
}

// GroupMemberCountForTest 返回共享组当前成员数（组不存在返回 0）。
func (b *Broker) GroupMemberCountForTest(groupName string) int {
	g := b.groups.Get(groupName)
	if g == nil {
		return 0
	}
	return len(g.Members())
}
