// Package group 实现共享订阅组（shared subscription）。
//
// 订阅过滤器形如 "$share/<组名>/<过滤器>"：同组多个成员订阅同一主题时，
// 一条消息在组内只投递给其中一个成员（轮询负载均衡）。成员掉线时，其手上
// 未确认完的消息被组回收，重新分发给仍在线的成员；组内暂时没有在线成员时，
// 消息进入该组的兜底队列（有容量上限与过期规则），等成员重新上线后补发。
package group

import (
	"sort"
	"sync"
	"time"
)

// 默认共享组兜底策略。
const (
	// DefaultGroupQueueCap 每个组兜底队列默认上限（条数）。
	DefaultGroupQueueCap = 1000
	// DefaultMessageTTL 兜底队列消息默认最长存活时间。
	DefaultMessageTTL = time.Hour
)

// Config 共享组相关容量与过期配置。
type Config struct {
	// GroupQueueCap 每个组“无在线成员兜底队列”的最大条数；0 表示关闭兜底
	// （无在线成员时新消息直接返回 GroupUnavailable，不缓存）。
	GroupQueueCap int
	// MessageTTL 兜底队列中消息的最长存活时间；0 表示永不过期。
	MessageTTL time.Duration
}

// DefaultConfig 返回默认配置：兜底队列 1000 条，消息 TTL 1 小时。
func DefaultConfig() Config {
	return Config{GroupQueueCap: DefaultGroupQueueCap, MessageTTL: DefaultMessageTTL}
}

// Queued 组兜底队列中的一条待投消息（裸载荷，重投时重新分配 PacketID）。
type Queued struct {
	Topic      string
	Payload    []byte
	QoS        byte
	Retain     bool
	EnqueuedAt time.Time
	// Counted 表示该消息是否计入代理的全局堆积配额：新发布入队为 true；
	// 从掉线成员手上回收的未确认消息豁免配额，为 false。
	Counted bool
}

// Group 单个共享订阅组的状态。
type Group struct {
	name string
	cfg  Config

	// onExpired 消息因 TTL 被清理时回调，参数为其中计入全局配额的条数。
	onExpired func(counted int64)

	mu      sync.Mutex
	members map[string]struct{} // 当前“已加入且尚未离开”的成员 clientID
	cursor  int                 // 轮询游标
	rr      []string            // 稳定的成员顺序（用于确定性轮询）
	queue   []*Queued           // 无在线成员时的兜底 FIFO

	droppedCount int64 // 兜底队列满被拒条数
	expiredCount int64 // TTL 过期条数
}

func newGroup(name string, cfg Config) *Group {
	return &Group{name: name, cfg: cfg, members: make(map[string]struct{})}
}

// onExpiredCounted 触发过期回调（若已设置）。
func (g *Group) onExpiredCounted(dropped []*Queued) {
	if g.onExpired != nil && len(dropped) > 0 {
		g.onExpired(counted(dropped))
	}
}

// Name 返回组名。
func (g *Group) Name() string { return g.name }

// AddMember 成员加入组（幂等）。
func (g *Group) AddMember(clientID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.members[clientID]; ok {
		return
	}
	g.members[clientID] = struct{}{}
	g.insertOrdered(clientID)
}

// insertOrdered 按字典序插入成员，保证轮询顺序确定、与接入先后无关。
func (g *Group) insertOrdered(clientID string) {
	i := sort.SearchStrings(g.rr, clientID)
	g.rr = append(g.rr, "")
	copy(g.rr[i+1:], g.rr[i:])
	g.rr[i] = clientID
}

// RemoveMember 成员离开组（取消订阅 / 会话清理时调用，幂等）。
func (g *Group) RemoveMember(clientID string) {
	g.mu.Lock()
	defer g.mu.Unlock()
	if _, ok := g.members[clientID]; !ok {
		return
	}
	delete(g.members, clientID)
	if i := sort.SearchStrings(g.rr, clientID); i < len(g.rr) && g.rr[i] == clientID {
		g.rr = append(g.rr[:i], g.rr[i+1:]...)
		// 游标保持在“下一个待投位置”，删除后夹到合法范围，避免跳过成员
		if g.cursor > 0 {
			g.cursor--
		}
	}
}

// HasMember 成员是否在组内。
func (g *Group) HasMember(clientID string) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	_, ok := g.members[clientID]
	return ok
}

// Members 返回组成员快照（字典序）。
func (g *Group) Members() []string {
	g.mu.Lock()
	defer g.mu.Unlock()
	out := make([]string, len(g.rr))
	copy(out, g.rr)
	return out
}

// Pick 从当前在线的组成员中轮询选择一个；online 判定成员此刻是否在线。
// 返回空串表示没有可用的在线成员。每次成功选择都会推进轮询游标，
// 因而消息在在线成员间均匀分发，成员增删后自动重新平衡。
func (g *Group) Pick(online func(clientID string) bool) string {
	g.mu.Lock()
	defer g.mu.Unlock()
	n := len(g.rr)
	if n == 0 {
		return ""
	}
	for i := 0; i < n; i++ {
		idx := (g.cursor + i) % n
		id := g.rr[idx]
		if online(id) {
			g.cursor = (idx + 1) % n
			return id
		}
	}
	return ""
}

// Enqueue 在没有在线成员时把消息放入兜底队列尾部（按发布顺序）。
// 先惰性清掉队首过期消息；返回 true 表示已入队，false 表示兜底关闭或已满。
// 满时拒绝新消息（不淘汰旧消息），由调用方记 QueueFull。
func (g *Group) Enqueue(q *Queued, now time.Time) bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	var dropped []*Queued
	g.queue, dropped = prune(g.queue, g.cfg.MessageTTL, now)
	g.expiredCount += int64(len(dropped))
	g.onExpiredCounted(dropped)
	if g.cfg.GroupQueueCap == 0 {
		return false // 兜底关闭
	}
	if len(g.queue) >= g.cfg.GroupQueueCap {
		g.droppedCount++
		return false
	}
	g.queue = append(g.queue, q)
	return true
}

// Splice 把掉线成员手上未确认的消息优先拼到兜底队列最前面（保持相对顺序），
// 使它们先于“无成员期间新发布”的消息被补发。回收消息来自成员已投递但未
// 确认完的消息，豁免容量上限（上限只拒绝“新发布”），因此本函数不丢弃任何
// 回收消息，保证掉线转移既不丢也不重复。
func (g *Group) Splice(qs []*Queued, now time.Time) {
	g.mu.Lock()
	defer g.mu.Unlock()
	var dropped []*Queued
	g.queue, dropped = prune(g.queue, g.cfg.MessageTTL, now)
	g.expiredCount += int64(len(dropped))
	g.onExpiredCounted(dropped)
	if len(qs) > 0 {
		g.queue = append(qs, g.queue...)
	}
}

// Drain 取出并清空兜底队列，返回未过期消息与被清掉的过期消息。
func (g *Group) Drain(now time.Time) (kept, expired []*Queued) {
	g.mu.Lock()
	defer g.mu.Unlock()
	kept, dropped := prune(g.queue, g.cfg.MessageTTL, now)
	g.expiredCount += int64(len(dropped))
	g.onExpiredCounted(dropped)
	g.queue = nil
	return kept, dropped
}

// TakeExpired 仅清理并返回过期消息（不清空有效队列）。
func (g *Group) SweepExpired(now time.Time) int {
	g.mu.Lock()
	defer g.mu.Unlock()
	var dropped []*Queued
	g.queue, dropped = prune(g.queue, g.cfg.MessageTTL, now)
	g.expiredCount += int64(len(dropped))
	g.onExpiredCounted(dropped)
	return len(dropped)
}

// QueueLen 返回当前兜底队列长度（不含过期清理，仅快照用途）。
func (g *Group) QueueLen() int {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.queue)
}

// DroppedCount 兜底队列满被拒条数。
func (g *Group) DroppedCount() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.droppedCount
}

// ExpiredCount TTL 过期清掉的条数。
func (g *Group) ExpiredCount() int64 {
	g.mu.Lock()
	defer g.mu.Unlock()
	return g.expiredCount
}

// Idle 组是否已无成员且兜底队列为空（可被注册表回收）。
func (g *Group) Idle() bool {
	g.mu.Lock()
	defer g.mu.Unlock()
	return len(g.rr) == 0 && len(g.queue) == 0
}

// prune 移除队首已过期消息，返回保留队列与被移除条目（用于全局计数回收）。
func prune(q []*Queued, ttl time.Duration, now time.Time) ([]*Queued, []*Queued) {
	if ttl <= 0 {
		return q, nil
	}
	drop := 0
	for drop < len(q) && now.Sub(q[drop].EnqueuedAt) >= ttl {
		drop++
	}
	if drop == 0 {
		return q, nil
	}
	dropped := q[:drop]
	return append(q[:0], q[drop:]...), dropped
}

// counted 返回条目中计入全局配额的条数。
func counted(qs []*Queued) int64 {
	var n int64
	for _, q := range qs {
		if q.Counted {
			n++
		}
	}
	return n
}

// Registry 全部共享组的注册表，按组名索引，并发安全。
type Registry struct {
	cfg       Config
	onExpired func(counted int64) // 组内消息 TTL 过期时回调（全局配额回收）

	mu     sync.Mutex
	groups map[string]*Group
}

// NewRegistry 创建注册表。
func NewRegistry(cfg Config) *Registry {
	return NewRegistryWithHook(cfg, nil)
}

// NewRegistryWithHook 创建注册表并设置消息过期回调。
func NewRegistryWithHook(cfg Config, onExpired func(counted int64)) *Registry {
	return &Registry{cfg: cfg, onExpired: onExpired, groups: make(map[string]*Group)}
}

// AddMember 将成员加入指定组（组不存在则自动创建）。
func (r *Registry) AddMember(name, clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[name]
	if !ok {
		g = newGroup(name, r.cfg)
		g.onExpired = r.onExpired
		r.groups[name] = g
	}
	g.AddMember(clientID)
}

// RemoveMember 将成员移出指定组；组变空闲时回收。
func (r *Registry) RemoveMember(name, clientID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	g, ok := r.groups[name]
	if !ok {
		return
	}
	g.RemoveMember(clientID)
	if g.Idle() {
		delete(r.groups, name)
	}
}

// RemoveClient 把某客户端从它加入的所有组中移除（会话过期/清理时用）。
// 返回受影响的组名快照。
func (r *Registry) RemoveClient(clientID string) []string {
	r.mu.Lock()
	groups := make([]*Group, 0, len(r.groups))
	names := make([]string, 0, len(r.groups))
	for name, g := range r.groups {
		groups = append(groups, g)
		names = append(names, name)
	}
	r.mu.Unlock()
	var touched []string
	for i, g := range groups {
		if g.HasMember(clientID) {
			g.RemoveMember(clientID)
			touched = append(touched, names[i])
		}
	}
	// 清理变空闲的组
	r.mu.Lock()
	for _, name := range touched {
		if g, ok := r.groups[name]; ok && g.Idle() {
			delete(r.groups, name)
		}
	}
	r.mu.Unlock()
	return touched
}

// Get 返回组（不存在返回 nil）。
func (r *Registry) Get(name string) *Group {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.groups[name]
}

// Names 返回当前全部组名快照。
func (r *Registry) Names() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]string, 0, len(r.groups))
	for name := range r.groups {
		out = append(out, name)
	}
	sort.Strings(out)
	return out
}

// Sweep 清理各兜底队列中的过期消息，并回收空闲组；
// 返回回收的组数与过期消息总数。
func (r *Registry) Sweep(now time.Time) (reapedGroups, expired int) {
	r.mu.Lock()
	defer r.mu.Unlock()
	for name, g := range r.groups {
		expired += g.SweepExpired(now)
		if g.Idle() {
			delete(r.groups, name)
			reapedGroups++
		}
	}
	return reapedGroups, expired
}
