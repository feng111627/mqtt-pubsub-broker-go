// Package session 管理客户端会话：订阅关系、QoS 在途消息与离线队列。
// 会话按 ClientID 索引，CleanStart 语义决定复用或重建，保证不串号。
package session

import (
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// EnqueueStatus 离线入队结果，调用方可据此区分"已排队"与"队列满被拒"。
type EnqueueStatus int

const (
	// EnqueueAccepted 已按发布顺序进入离线队列。
	EnqueueAccepted EnqueueStatus = iota
	// EnqueueQueueFull 队列达到容量上限，消息被拒（不淘汰旧消息）。
	EnqueueQueueFull
)

// Queued 离线队列中的一条消息，记录入队时间用于过期清理。
type Queued struct {
	Packet     *packet.Packet
	EnqueuedAt time.Time
}

// Inflight 表示一条尚未完成 QoS 握手的在途消息。
type Inflight struct {
	PacketID uint16
	Packet   *packet.Packet
	// Stage: 0=等待PUBACK(QoS1) 1=等待PUBREC(QoS2) 2=等待PUBCOMP(QoS2)
	Stage int
	// GroupKey 非空表示该消息来自共享组投递；成员掉线时由组管理器接管转交。
	GroupKey string
	// Token 组内投递令牌（仅 GroupKey 非空时有效），ACK 后用于摘除组在途记录。
	Token uint64
}

// Session 一个客户端会话。所有方法并发安全。
type Session struct {
	ClientID   string
	CleanStart bool

	mu          sync.Mutex
	subs        map[string]byte      // filter -> qos
	inflightOut map[uint16]*Inflight // broker -> client 方向
	inflightIn  map[uint16]struct{}  // client -> broker 方向（QoS2 去重）
	pending     []Queued             // 离线期间按发布顺序缓存的消息
	nextID      uint16

	// 容量与过期策略（由 Store 统一配置；0 值使用 DefaultLimits）。
	maxQueue    int
	messageTTL  time.Duration
	lastActive  time.Time // 最近一次连接/活动时间，用于会话过期
	expiredDrop int       // 因过期被丢弃的离线消息数（诊断用）
	fullRejects int       // 因队列满被拒的消息数（诊断用）
}

// Limits 会话离线堆积与过期策略。
type Limits struct {
	// MaxQueue 单个会话离线队列容量上限（条数）。<=0 使用默认值。
	MaxQueue int
	// MessageTTL 离线消息存活时间；超过后在补发前被惰性清理。<=0 表示不过期。
	MessageTTL time.Duration
	// SessionTTL 持久会话离线保留时间；超过后会话过期、订阅与队列清除。<=0 表示不过期。
	SessionTTL time.Duration
}

// DefaultLimits 容量与过期策略的默认值。
var DefaultLimits = Limits{
	MaxQueue:   1000,
	MessageTTL: 24 * time.Hour,
	SessionTTL: 7 * 24 * time.Hour,
}

func (l Limits) withDefaults() Limits {
	d := DefaultLimits
	if l.MaxQueue != 0 {
		d.MaxQueue = l.MaxQueue
	}
	d.MessageTTL = l.MessageTTL
	d.SessionTTL = l.SessionTTL
	return d
}

// New 创建会话（使用 DefaultLimits）。
func New(clientID string, cleanStart bool) *Session {
	return NewWithLimits(clientID, cleanStart, DefaultLimits)
}

// NewWithLimits 按指定策略创建会话。
func NewWithLimits(clientID string, cleanStart bool, l Limits) *Session {
	l = l.withDefaults()
	return &Session{
		ClientID:    clientID,
		CleanStart:  cleanStart,
		subs:        make(map[string]byte),
		inflightOut: make(map[uint16]*Inflight),
		inflightIn:  make(map[uint16]struct{}),
		nextID:      1,
		maxQueue:    l.MaxQueue,
		messageTTL:  l.MessageTTL,
		lastActive:  time.Now(),
	}
}

// Touch 更新会话最近活动时间。
func (s *Session) Touch() {
	s.mu.Lock()
	s.lastActive = time.Now()
	s.mu.Unlock()
}

// LastActive 返回最近活动时间。
func (s *Session) LastActive() time.Time {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.lastActive
}

// Expired 判断持久会话离线时长是否已超过 TTL。CleanStart 会话永不过期。
func (s *Session) Expired(now time.Time, ttl time.Duration) bool {
	if ttl <= 0 {
		return false
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	return !s.CleanStart && now.Sub(s.lastActive) > ttl
}

// Stats 诊断计数。
type Stats struct {
	ExpiredDrops int
	FullRejects  int
	QueueLen     int
}

// Stats 返回队列长度与丢弃/拒绝计数快照。
func (s *Session) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{ExpiredDrops: s.expiredDrop, FullRejects: s.fullRejects, QueueLen: len(s.pending)}
}

// Tombstone 已过期会话的墓碑：会话本体（队列/在途）已释放，
// 仅保留订阅快照，使路由在墓碑保留期内可区分"会话已过期"与"从未订阅"。
type Tombstone struct {
	At   time.Time
	Subs map[string]byte
}

// Store 会话仓库，按 ClientID 索引。
type Store struct {
	mu         sync.Mutex
	sessions   map[string]*Session
	tombstones map[string]*Tombstone
	limits     Limits
}

// NewStore 创建会话仓库（使用 DefaultLimits）。
func NewStore() *Store {
	return NewStoreWithLimits(DefaultLimits)
}

// NewStoreWithLimits 按指定策略创建会话仓库。
func NewStoreWithLimits(l Limits) *Store {
	return &Store{
		sessions:   make(map[string]*Session),
		tombstones: make(map[string]*Tombstone),
		limits:     l.withDefaults(),
	}
}

// Get 按 cleanStart 语义获取或创建会话：
// cleanStart=true 时丢弃旧会话重建；否则复用已有会话（resumed=true）。
func (s *Store) Get(clientID string, cleanStart bool) (sess *Session, resumed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.tombstones, clientID) // 客户端回来了，撤销过期标记
	if cleanStart {
		sess = NewWithLimits(clientID, true, s.limits)
		s.sessions[clientID] = sess
		return sess, false
	}
	if old, ok := s.sessions[clientID]; ok {
		old.Touch()
		return old, true
	}
	sess = NewWithLimits(clientID, false, s.limits)
	s.sessions[clientID] = sess
	return sess, false
}

// Range 遍历全部会话，fn 返回 false 时停止。
func (s *Store) Range(fn func(sess *Session) bool) {
	s.mu.Lock()
	sessions := make([]*Session, 0, len(s.sessions))
	for _, sess := range s.sessions {
		sessions = append(sessions, sess)
	}
	s.mu.Unlock()
	for _, sess := range sessions {
		if !fn(sess) {
			return
		}
	}
}

// Peek 返回会话快照指针，不存在返回 nil（不改变会话生命周期）。
func (s *Store) Peek(clientID string) *Session {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.sessions[clientID]
}

// Remove 删除会话（CleanStart 会话断开时调用）。
func (s *Store) Remove(clientID string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sessions, clientID)
}

// Expire 将过期会话转为墓碑：清除订阅与队列（立即释放堆积内存），
// 但在墓碑保留期内仍可区分"会话曾经存在但已过期"与"从未订阅"。
func (s *Store) Expire(clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	sess, ok := s.sessions[clientID]
	if !ok {
		return false
	}
	delete(s.sessions, clientID)
	s.tombstones[clientID] = &Tombstone{At: s.now(), Subs: sess.Subs()}
	return true
}

// IsExpired 返回该 clientID 是否处于墓碑保留期（会话已过期）。
func (s *Store) IsExpired(clientID string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, ok := s.tombstones[clientID]
	return ok
}

// ExpiredSubs 返回过期会话保留的订阅快照；不在墓碑期返回 nil。
func (s *Store) ExpiredSubs(clientID string) map[string]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	if t, ok := s.tombstones[clientID]; ok {
		out := make(map[string]byte, len(t.Subs))
		for f, q := range t.Subs {
			out[f] = q
		}
		return out
	}
	return nil
}

// RangeTombstones 遍历所有墓碑（clientID, 订阅快照）。
func (s *Store) RangeTombstones(fn func(clientID string, subs map[string]byte) bool) {
	s.mu.Lock()
	ids := make([]string, 0, len(s.tombstones))
	for id := range s.tombstones {
		ids = append(ids, id)
	}
	s.mu.Unlock()
	for _, id := range ids {
		subs := s.ExpiredSubs(id)
		if subs == nil {
			continue
		}
		if !fn(id, subs) {
			return
		}
	}
}

// Sweep 执行一轮过期清理：
//   - 离线超过 SessionTTL 的持久会话转为墓碑（订阅快照保留，队列立即释放）；
//   - 墓碑保留满一个 SessionTTL 后彻底删除（防止无限占内存）。
//
// 返回本轮被转为墓碑的 clientID 列表（供组管理器摘除其成员身份）。
func (s *Store) Sweep(now time.Time) []string {
	s.mu.Lock()
	ttl := s.limits.SessionTTL
	var expired []string
	for id, sess := range s.sessions {
		if sess.Expired(now, ttl) {
			expired = append(expired, id)
			delete(s.sessions, id)
			s.tombstones[id] = &Tombstone{At: now, Subs: sess.Subs()}
		}
	}
	for id, t := range s.tombstones {
		if ttl > 0 && now.Sub(t.At) > ttl {
			delete(s.tombstones, id)
		}
	}
	s.mu.Unlock()
	return expired
}

// now 可被测试替换的时间源（默认 time.Now）。
func (s *Store) now() time.Time { return time.Now() }

// Limits 返回仓库使用的策略。
func (s *Store) Limits() Limits { return s.limits }

// AddSub 添加订阅，返回是否为重复订阅（覆盖已有过滤器）。
func (s *Session) AddSub(filter string, qos byte) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	_, dup := s.subs[filter]
	s.subs[filter] = qos
	return dup
}

// RemoveSub 移除订阅，返回是否存在。
func (s *Session) RemoveSub(filter string) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.subs[filter]; !ok {
		return false
	}
	delete(s.subs, filter)
	return true
}

// Subs 返回当前订阅快照。
func (s *Session) Subs() map[string]byte {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]byte, len(s.subs))
	for f, q := range s.subs {
		out[f] = q
	}
	return out
}

// ClearSubs 清空订阅。
func (s *Session) ClearSubs() {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.subs = make(map[string]byte)
}

// NextPacketID 分配下一个报文标识符（跳过在途中的 ID，永不返回 0）。
func (s *Session) NextPacketID() uint16 {
	s.mu.Lock()
	defer s.mu.Unlock()
	for i := 0; i < 0xFFFF; i++ {
		id := s.nextID
		s.nextID++
		if s.nextID == 0 {
			s.nextID = 1
		}
		if _, busy := s.inflightOut[id]; !busy {
			return id
		}
	}
	return 0 // 全部耗尽，调用方应视为错误
}

// TrackOut 登记一条出方向在途消息。
func (s *Session) TrackOut(in *Inflight) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.inflightOut[in.PacketID] = in
}

// AckOut 出方向握手完成（QoS1 PUBACK / QoS2 PUBCOMP），移除在途记录。
func (s *Session) AckOut(packetID uint16) (*Inflight, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inflightOut[packetID]
	if ok {
		delete(s.inflightOut, packetID)
	}
	return in, ok
}

// AdvanceOut 推进出方向 QoS2 状态机（收到 PUBREC，转为等待 PUBCOMP）。
func (s *Session) AdvanceOut(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	in, ok := s.inflightOut[packetID]
	if !ok || in.Stage != 1 {
		return false
	}
	in.Stage = 2
	return true
}

// TrackIn 入方向 QoS2 去重登记；已存在表示重复投递（应回 PUBREC 但不重复路由）。
func (s *Session) TrackIn(packetID uint16) (dup bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflightIn[packetID]; ok {
		return true
	}
	s.inflightIn[packetID] = struct{}{}
	return false
}

// CompleteIn 入方向 QoS2 完成（收到 PUBREL），移除去重记录。
func (s *Session) CompleteIn(packetID uint16) bool {
	s.mu.Lock()
	defer s.mu.Unlock()
	if _, ok := s.inflightIn[packetID]; !ok {
		return false
	}
	delete(s.inflightIn, packetID)
	return true
}

// Enqueue 离线缓存一条消息（按发布顺序追加）。
// 返回 EnqueueQueueFull 表示达到容量上限、消息被拒（采用"满了拒新"策略，
// 不淘汰队首旧消息，以保证先发布的消息不被后发布的挤掉）。
// 入队前惰性清理已过 TTL 的队首消息；各会话队列相互独立，互不挤占。
func (s *Session) Enqueue(p *packet.Packet) EnqueueStatus {
	return s.EnqueueAt(p, time.Now())
}

// EnqueueAt 使用指定时间入队（测试可注入时间）。
func (s *Session) EnqueueAt(p *packet.Packet, now time.Time) EnqueueStatus {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(now)
	if s.maxQueue > 0 && len(s.pending) >= s.maxQueue {
		s.fullRejects++
		return EnqueueQueueFull
	}
	s.pending = append(s.pending, Queued{Packet: p, EnqueuedAt: now})
	return EnqueueAccepted
}

// pruneExpiredLocked 清理队首起所有已过期消息。
// 离线队列按发布顺序排列，过期消息只可能集中在队首。
func (s *Session) pruneExpiredLocked(now time.Time) {
	if s.messageTTL <= 0 {
		return
	}
	cut := 0
	for cut < len(s.pending) && now.Sub(s.pending[cut].EnqueuedAt) > s.messageTTL {
		cut++
	}
	if cut > 0 {
		s.pending = append(s.pending[:0:0], s.pending[cut:]...)
		s.expiredDrop += cut
	}
}

// Drain 取出并清空离线缓存；补发前先清理过期消息，保证重连后不会收到已过期内容。
func (s *Session) Drain() []*packet.Packet {
	return s.DrainAt(time.Now())
}

// DrainAt 使用指定时间取出（测试可注入时间）。
func (s *Session) DrainAt(now time.Time) []*packet.Packet {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pruneExpiredLocked(now)
	out := make([]*packet.Packet, len(s.pending))
	for i, q := range s.pending {
		out[i] = q.Packet
	}
	s.pending = nil
	return out
}

// ReclaimGroupInflight 取出（并移除）本会话中所有来自共享组的未确认在途消息，
// 用于成员掉线时把消息转交给组内其他在线成员；普通订阅的在途消息保留，
// 仍按"重连 DUP 重发"语义处理。
func (s *Session) ReclaimGroupInflight() []*Inflight {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []*Inflight
	for id, in := range s.inflightOut {
		if in.GroupKey != "" {
			out = append(out, in)
			delete(s.inflightOut, id)
		}
	}
	return out
}

// ReclaimGroupInflightToken 幂等地按组令牌摘出单条组在途消息：
// 已被摘除（同令牌重复调用）时返回 nil。用于消除"连接读写协程与主动关闭"
// 竞态导致的重复接管。
func (s *Session) ReclaimGroupInflightToken(groupKey string, token uint64) *Inflight {
	s.mu.Lock()
	defer s.mu.Unlock()
	for id, in := range s.inflightOut {
		if in.GroupKey == groupKey && in.Token == token {
			delete(s.inflightOut, id)
			return in
		}
	}
	return nil
}

// GroupInflightTokenRef 组在途的轻量引用（组键+令牌）。
type GroupInflightTokenRef struct {
	GroupKey string
	Token    uint64
}

// GroupInflightTokens 返回全部组在途的组键/令牌快照（不移除）。
func (s *Session) GroupInflightTokens() []GroupInflightTokenRef {
	s.mu.Lock()
	defer s.mu.Unlock()
	var out []GroupInflightTokenRef
	for _, in := range s.inflightOut {
		if in.GroupKey != "" {
			out = append(out, GroupInflightTokenRef{GroupKey: in.GroupKey, Token: in.Token})
		}
	}
	return out
}

// InflightOut 返回全部出方向在途消息（重连重发用）。
func (s *Session) InflightOut() []*Inflight {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make([]*Inflight, 0, len(s.inflightOut))
	for _, in := range s.inflightOut {
		out = append(out, in)
	}
	return out
}
