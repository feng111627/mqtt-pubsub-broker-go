// 共享订阅组（Shared Subscription）管理：组成员维护、组内单选投递、
// 轮询负载均衡、掉线在途接管与组级离线兜底堆积。
package broker

import (
	"sort"
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// groupStatus 组投递结果。
type groupStatus int

const (
	groupDelivered groupStatus = iota // 已投递给组内某个在线成员
	groupBuffered                     // 组内无可用成员，已进入组级兜底队列
	groupFull                         // 兜底队列已满，消息被拒
)

// dispatchFn 把一条消息实际投递给某个在线连接：分配 PacketID、登记在途、入发送队列。
// 返回的 packetID 为该成员会话上分配的报文标识符；false 表示该连接此刻不可投递。
// 实现方不得回调 groupManager（避免持锁重入）。
type dispatchFn func(c *clientConn, p *packet.Packet, groupKey string, token uint64) (packetID uint16, ok bool)

// groupManager 管理所有共享订阅组。
type groupManager struct {
	mu     sync.Mutex
	groups map[string]*shareGroup
	cfg    GroupConfig
	now    func() time.Time
	send   dispatchFn
}

// shareGroup 单个共享组。
type shareGroup struct {
	key      string
	members  map[string]bool        // 订阅了该组的 clientID（含离线持久成员）
	active   map[string]*clientConn // 当前在线成员连接（指针用于会话接管比对）
	next     int                    // 轮询游标
	pending  []groupQueued          // 无在线成员时的兜底堆积（按发布顺序）
	inflight map[uint64]*groupDelivery
	token    uint64
}

// groupQueued 组级离线堆积条目。
type groupQueued struct {
	packet *packet.Packet
	at     time.Time
}

// groupDelivery 一次组内在途投递记录（令牌先于发送登记，杜绝 ACK 竞态漏删）。
type groupDelivery struct {
	token    uint64
	clientID string
	packetID uint16
	packet   *packet.Packet
}

// GroupConfig 共享组兜底堆积策略。
type GroupConfig struct {
	// MaxPending 单个组无在线成员时兜底堆积的条数上限。<=0 使用默认值。
	MaxPending int
	// MessageTTL 兜底消息存活时间，补发前惰性清理。<=0 表示不过期。
	MessageTTL time.Duration
}

// DefaultGroupConfig 组兜底堆积默认策略。
var DefaultGroupConfig = GroupConfig{
	MaxPending: 1000,
	MessageTTL: 24 * time.Hour,
}

func (c GroupConfig) withDefaults() GroupConfig {
	d := DefaultGroupConfig
	if c.MaxPending != 0 {
		d.MaxPending = c.MaxPending
	}
	d.MessageTTL = c.MessageTTL
	return d
}

func newGroupManager(cfg GroupConfig, send dispatchFn) *groupManager {
	return &groupManager{
		groups: make(map[string]*shareGroup),
		cfg:    cfg.withDefaults(),
		now:    time.Now,
		send:   send,
	}
}

// groupKey 组的唯一键：组名 + 内层过滤器。同组名订阅不同过滤器是不同组。
func groupKey(group, filter string) string { return group + "\x00" + filter }

// group 取（或建）组，调用方持锁。
func (m *groupManager) groupLocked(k string) *shareGroup {
	g := m.groups[k]
	if g == nil {
		g = &shareGroup{
			key:      k,
			members:  make(map[string]bool),
			active:   make(map[string]*clientConn),
			inflight: make(map[uint64]*groupDelivery),
		}
		m.groups[k] = g
	}
	return g
}

// join 增加组成员（订阅时调用）。
func (m *groupManager) join(key, clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.groupLocked(key).members[clientID] = true
}

// leave 移除组成员及其未确认在途（退订/会话过期时调用）。
// 被移除成员尚未确认的组消息：若还有其他在线成员则转交，否则回兜底队列。
func (m *groupManager) leave(key, clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g := m.groups[key]
	if g == nil {
		return
	}
	delete(g.members, clientID)
	delete(g.active, clientID)
	m.redispatchMemberInflightLocked(g, clientID)
	if len(g.members) == 0 && len(g.pending) == 0 && len(g.inflight) == 0 {
		delete(m.groups, key)
	}
}

// removeClient 从所有组中摘除一个客户端（CleanStart 断开/会话过期时调用）。
func (m *groupManager) removeClient(clientID string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	for key, g := range m.groups {
		if !g.members[clientID] {
			continue
		}
		delete(g.members, clientID)
		delete(g.active, clientID)
		m.redispatchMemberInflightLocked(g, clientID)
		if len(g.members) == 0 && len(g.pending) == 0 && len(g.inflight) == 0 {
			delete(m.groups, key)
		}
	}
}

// activate 标记成员上线并登记连接；随后把兜底队列按发布顺序补发给它。
// 连接指针用于会话接管：只有指针仍匹配时该连接才算该成员。
func (m *groupManager) activate(key, clientID string, c *clientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key]
	if !ok {
		return // 未订阅该组（理论上不会发生）
	}
	g.active[clientID] = c
	// 补发兜底堆积（先清过期）。整组无在线成员期间攒下的消息交给该成员。
	m.prunePendingLocked(g)
	pending := g.pending
	g.pending = nil
	for _, q := range pending {
		m.dispatchLocked(g, q.packet)
	}
}

// deactivate 成员离线：摘除在线连接，并把它手上未确认的组消息转交其他在线成员
// （或回兜底队列）。conn 指针不匹配表示这是被接管的旧连接，不应影响新连接。
func (m *groupManager) deactivate(key, clientID string, c *clientConn) {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key]
	if !ok {
		return
	}
	if cur := g.active[clientID]; cur != nil && cur != c {
		return // 会话已被同 ClientID 新连接接管
	}
	delete(g.active, clientID)
	m.redispatchMemberInflightLocked(g, clientID)
}

// reclaim 成员掉线时，把会话里已摘出的组在途消息转交给组内其他在线成员，
// 没有在线成员则放回兜底队列队首（它发布得更早，补发顺序保持先旧后新）。
// 按令牌幂等：该令牌的在途记录已不存在（已被 ACK 或已接管）时直接返回 false，
// 杜绝连接读写协程退出与主动关闭竞态下的重复转交。
func (m *groupManager) reclaim(key, clientID string, token uint64, p *packet.Packet) bool {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key]
	if !ok {
		return false
	}
	d := g.inflight[token]
	if d == nil || d.clientID != clientID {
		return false // 已被确认或已被另一条路径接管
	}
	delete(g.inflight, token)
	rp := *p
	rp.Dup = true
	rp.PacketID = 0
	m.dispatchOrBufferLocked(g, &rp, true)
	return true
}

// ackToken 成员确认了一条组消息（PUBACK/PUBCOMP 后），移除在途记录。
func (m *groupManager) ackToken(key string, token uint64) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if g, ok := m.groups[key]; ok {
		delete(g.inflight, token)
	}
}

// publishResult 一次组投递的结果与被选中的成员。
type publishResult struct {
	status groupStatus
	member string
}

// publish 向组投递一条消息：在线成员轮询单选；无可用成员则兜底堆积。
func (m *groupManager) publish(key string, p *packet.Packet) publishResult {
	m.mu.Lock()
	defer m.mu.Unlock()
	g, ok := m.groups[key]
	if !ok {
		return publishResult{status: groupBuffered}
	}
	m.prunePendingLocked(g)
	if len(g.active) > 0 {
		if member, ok := m.dispatchLocked(g, p); ok {
			return publishResult{status: groupDelivered, member: member}
		}
	}
	return publishResult{status: m.bufferLocked(g, p)}
}

// dispatchLocked 选择一个在线成员投递，失败依次尝试其他成员。
// 返回被选中的成员 clientID。调用方持组锁。
func (m *groupManager) dispatchLocked(g *shareGroup, p *packet.Packet) (string, bool) {
	if len(g.active) == 0 {
		return "", false
	}
	ids := make([]string, 0, len(g.active))
	for id, c := range g.active {
		if c != nil {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)
	n := len(ids)
	for try := 0; try < n; try++ {
		id := ids[g.next%n]
		g.next++
		c := g.active[id]
		if c == nil || c.isClosed() {
			continue
		}
		// QoS>0 需要令牌登记（先登记后发送，ACK 任何时刻到达都能命中）；
		// QoS0 至多一次，无确认、无需在途记录。
		var token uint64
		if p.QoS > 0 {
			g.token++
			token = g.token
			g.inflight[token] = &groupDelivery{token: token, clientID: id, packet: p}
		}
		pid, ok := m.send(c, p, g.key, token)
		if !ok {
			if token != 0 {
				delete(g.inflight, token)
			}
			continue
		}
		if token != 0 {
			g.inflight[token].packetID = pid
		}
		return id, true
	}
	return "", false
}

// dispatchOrBufferLocked 先尝试在线投递，不行则入兜底队列。
// front=true 时放回队首（掉线接管的旧消息）。调用方持锁。
func (m *groupManager) dispatchOrBufferLocked(g *shareGroup, p *packet.Packet, front bool) {
	if _, ok := m.dispatchLocked(g, p); ok {
		return
	}
	st := m.bufferLocked(g, p)
	if front && st == groupBuffered {
		// bufferLocked 追加到队尾；掉线接管的消息更早，移到队首。
		last := g.pending[len(g.pending)-1]
		copy(g.pending[1:], g.pending[:len(g.pending)-1])
		g.pending[0] = last
	}
}

// bufferLocked 入兜底队列；满了采用"拒新"策略。调用方持锁。
func (m *groupManager) bufferLocked(g *shareGroup, p *packet.Packet) groupStatus {
	if m.cfg.MaxPending > 0 && len(g.pending) >= m.cfg.MaxPending {
		return groupFull
	}
	g.pending = append(g.pending, groupQueued{packet: p, at: m.now()})
	return groupBuffered
}

// prunePendingLocked 清理队首起所有过 TTL 的兜底消息。调用方持锁。
func (m *groupManager) prunePendingLocked(g *shareGroup) {
	if m.cfg.MessageTTL <= 0 {
		return
	}
	now := m.now()
	cut := 0
	for cut < len(g.pending) && now.Sub(g.pending[cut].at) > m.cfg.MessageTTL {
		cut++
	}
	if cut > 0 {
		g.pending = append(g.pending[:0:0], g.pending[cut:]...)
	}
}

// redispatchMemberInflightLocked 成员直接离开（退订/过期）时，把它未确认的组消息
// 转交给其余在线成员，没有在线成员则回兜底队列；消息不丢弃。调用方持锁。
func (m *groupManager) redispatchMemberInflightLocked(g *shareGroup, clientID string) {
	for token, d := range g.inflight {
		if d.clientID != clientID {
			continue
		}
		delete(g.inflight, token)
		rp := *d.packet
		rp.Dup = true
		rp.PacketID = 0
		m.dispatchOrBufferLocked(g, &rp, false)
	}
}
