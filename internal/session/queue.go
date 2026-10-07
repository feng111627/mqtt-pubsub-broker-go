package session

import (
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// 默认容量与过期策略（均可通过 Config 覆盖）。
const (
	// DefaultMaxQueueMessages 单个持久会话离线队列默认上限（条数）。
	DefaultMaxQueueMessages = 1000
	// DefaultMessageTTL 离线消息默认最长存活时间。
	DefaultMessageTTL = time.Hour
)

// Config 会话离线堆积与过期配置。
type Config struct {
	// MaxQueue 每个会话离线队列容量上限（条数）。<=0 时取默认值。
	// 队列满时采用“拒绝新消息”策略：不淘汰旧消息，更不会挤占其他会话。
	MaxQueue int
	// MessageTTL 离线消息存活时间；入队早于 now-TTL 的消息在入队/补发/
	// 巡检时被惰性清除。0 表示永不过期。
	MessageTTL time.Duration
	// SessionExpiry 会话在离线多久后过期；0 表示永不过期（持久会话默认）。
	// 过期会话保留一个空壳用于结果分类（SessionExpired），不再接收消息，
	// 同 ClientID 重连时按新会话处理。
	SessionExpiry time.Duration
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		MaxQueue:      DefaultMaxQueueMessages,
		MessageTTL:    DefaultMessageTTL,
		SessionExpiry: 0,
	}
}

func (c Config) resolved() Config {
	if c.MaxQueue <= 0 {
		c.MaxQueue = DefaultMaxQueueMessages
	}
	return c
}

// QueuedMsg 离线队列中的一条消息及其入队时间（用于 TTL 判定）。
type QueuedMsg struct {
	Packet     *packet.Packet
	EnqueuedAt time.Time
}

// EnqueueResult 入队尝试的可区分结果。
type EnqueueResult struct {
	// Accepted 消息是否已进入离线队列。
	Accepted bool
	// Full 是否因队列容量上限被拒（消息被丢，由调用方记 QueueFull）。
	Full bool
	// Expired 本次调用顺带清除的已过期消息数（调用方据此回收全局配额）。
	Expired int
}

// DrainResult 补发结果：未过期可补发的消息与顺带清除的过期消息数。
type DrainResult struct {
	Messages []*packet.Packet
	Expired  int
}

// EnqueueAt 尝试把消息入离线队列，入队时间由调用方给定。
// 先惰性清除队首已过期消息；会话已过期返回 Accepted=false；
// 队列达到容量上限时拒绝新消息（Full=true），旧消息保持原顺序不动。
func (s *Session) EnqueueAt(p *packet.Packet, now time.Time) EnqueueResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	res := EnqueueResult{}
	if s.expired {
		return res
	}
	var n int
	s.queued, n = pruneExpired(s.queued, s.cfg.MessageTTL, now)
	res.Expired = n
	s.expiredDroppedCount += int64(n)
	if len(s.queued) >= s.cfg.MaxQueue {
		s.droppedCount++
		res.Full = true
		return res
	}
	s.queued = append(s.queued, &QueuedMsg{Packet: p, EnqueuedAt: now})
	res.Accepted = true
	return res
}

// Enqueue 使用会话时钟入队（兼容简单调用场景）。
func (s *Session) Enqueue(p *packet.Packet) EnqueueResult {
	return s.EnqueueAt(p, s.now())
}

// DrainAt 取出并清空离线队列，补发时间由调用方给定；过期消息不补发。
func (s *Session) DrainAt(now time.Time) DrainResult {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept, expired := pruneExpired(s.queued, s.cfg.MessageTTL, now)
	s.expiredDroppedCount += int64(expired)
	s.queued = nil
	out := DrainResult{Expired: expired}
	for _, q := range kept {
		out.Messages = append(out.Messages, q.Packet)
	}
	return out
}

// Drain 取出并清空离线队列（返回未过期消息，兼容旧调用）。
func (s *Session) Drain() []*packet.Packet {
	return s.DrainAt(s.now()).Messages
}

// PendingLen 当前离线队列长度（先做惰性过期清理）。
func (s *Session) PendingLen() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept, n := pruneExpired(s.queued, s.cfg.MessageTTL, s.now())
	s.queued = kept
	s.expiredDroppedCount += int64(n)
	return len(kept)
}

// SweepExpired 主动清理离线队列中的过期消息，返回被清掉的条数（顺序不变）。
func (s *Session) SweepExpired(now time.Time) int {
	s.mu.Lock()
	defer s.mu.Unlock()
	kept, n := pruneExpired(s.queued, s.cfg.MessageTTL, now)
	s.queued = kept
	s.expiredDroppedCount += int64(n)
	return n
}

// IsExpiredAt 会话在指定时刻是否已过期（惰性判定，不修改状态）。
func (s *Session) IsExpiredAt(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expired {
		return true
	}
	if s.cfg.SessionExpiry <= 0 || s.online || s.lastDisconnect.IsZero() {
		return false
	}
	return now.Sub(s.lastDisconnect) >= s.cfg.SessionExpiry
}

// ExpireAt 若到达过期时刻则让会话过期：保留订阅快照用于结果分类，
// 释放队列与在途消息占用的内存。返回是否发生了过期。
func (s *Session) ExpireAt(now time.Time) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.expired {
		return false
	}
	if s.cfg.SessionExpiry <= 0 || s.online || s.lastDisconnect.IsZero() {
		return false
	}
	if now.Sub(s.lastDisconnect) < s.cfg.SessionExpiry {
		return false
	}
	s.expireLocked()
	return true
}

// expireLocked 标记过期并释放消息内存（调用方持锁）。
func (s *Session) expireLocked() {
	s.expired = true
	s.expiredDroppedCount += int64(len(s.queued))
	s.queued = nil
	s.inflightOut = make(map[uint16]*Inflight)
	s.inflightIn = make(map[uint16]struct{})
}

// SetOnline 更新在线状态与离线计时起点（连接接入/退出时调用）。
func (s *Session) SetOnline(online bool, now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.online = online
	if !online {
		s.lastDisconnect = now
	}
}

// MarkConnected 重连接入：清除离线计时（过期判定只统计离线时长）。
func (s *Session) MarkConnected(now time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.online = true
	s.lastDisconnect = time.Time{}
}

// SetClock 注入时间源（测试用，控制 TTL/会话过期判定）。
func (s *Session) SetClock(f func() time.Time) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if f != nil {
		s.now = f
	}
}

// Expired 返回会话是否已被标记过期。
func (s *Session) Expired() bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expired
}

// DroppedCount 因队列满被拒的累计消息数。
func (s *Session) DroppedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.droppedCount
}

// ExpiredDroppedCount 因会话过期/消息 TTL 被清掉的累计消息数。
func (s *Session) ExpiredDroppedCount() int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.expiredDroppedCount
}

// pruneExpired 从队首开始移除已过期消息，返回保留队列与移除条数。
// 队列按发布顺序 FIFO；循环在遇到第一条“未过期”消息时立即停止，
// 绝不删除其后的任何消息（它们更新、只会更晚过期）。
func pruneExpired(q []*QueuedMsg, ttl time.Duration, now time.Time) ([]*QueuedMsg, int) {
	if ttl <= 0 {
		return q, 0
	}
	drop := 0
	for drop < len(q) {
		if now.Sub(q[drop].EnqueuedAt) < ttl {
			break // 队首及之后都未过期
		}
		drop++
	}
	if drop == 0 {
		return q, 0
	}
	return append(q[:0], q[drop:]...), drop
}
