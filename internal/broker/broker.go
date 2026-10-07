// Package broker 实现 MQTT 代理核心：连接接入、主题路由、
// 共享订阅组、保留消息、离线堆积与会话生命周期管理。路由与会话逻辑均为自实现。
package broker

import (
	"errors"
	"net"
	"sync"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
	topicpkg "github.com/feng111627/mqtt-pubsub-broker-go/internal/topic"
)

// Config 代理容量与过期策略。零值字段使用默认值。
type Config struct {
	// Session 持久会话离线堆积/过期策略。
	Session session.Limits
	// Group 共享组无在线成员时的兜底堆积策略。
	Group GroupConfig
	// SweepInterval 后台过期扫描间隔。<=0 时为 30s。
	SweepInterval time.Duration
}

// Broker 消息代理。
type Broker struct {
	sessions *session.Store
	groups   *groupManager
	cfg      Config

	mu       sync.RWMutex
	retained map[string]*packet.Packet // topic -> retained publish
	online   map[string]*clientConn    // clientID -> 当前在线连接

	ln     net.Listener
	closed chan struct{}
	once   sync.Once
}

// New 使用默认策略创建代理。
func New() *Broker {
	return NewWithConfig(Config{})
}

// NewWithConfig 按指定策略创建代理。
func NewWithConfig(cfg Config) *Broker {
	b := &Broker{
		sessions: session.NewStoreWithLimits(cfg.Session),
		cfg:      cfg,
		retained: make(map[string]*packet.Packet),
		online:   make(map[string]*clientConn),
		closed:   make(chan struct{}),
	}
	b.groups = newGroupManager(cfg.Group, b.dispatchToConn)
	return b
}

// SessionLimits 返回生效的会话策略（含默认值）。
func (b *Broker) SessionLimits() session.Limits { return b.sessions.Limits() }

// ListenAndServe 在指定地址监听并服务，直到 Close。
func (b *Broker) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.ln = ln
	b.mu.Unlock()

	go b.sweepLoop()

	for {
		conn, err := ln.Accept()
		if err != nil {
			select {
			case <-b.closed:
				return nil
			default:
				return err
			}
		}
		cc := &clientConn{
			broker: b,
			conn:   conn,
			sendCh: make(chan *packet.Packet, 64),
			done:   make(chan struct{}),
		}
		go cc.serve()
	}
}

// sweepLoop 周期性清理过期会话并摘除其共享组成员身份。
func (b *Broker) sweepLoop() {
	interval := b.cfg.SweepInterval
	if interval <= 0 {
		interval = 30 * time.Second
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-b.closed:
			return
		case <-ticker.C:
			for _, id := range b.sessions.Sweep(time.Now()) {
				b.groups.removeClient(id)
			}
		}
	}
}

// Addr 返回实际监听地址。
func (b *Broker) Addr() string {
	b.mu.RLock()
	defer b.mu.RUnlock()
	if b.ln == nil {
		return ""
	}
	return b.ln.Addr().String()
}

// Close 关闭代理。
func (b *Broker) Close() error {
	b.once.Do(func() {
		close(b.closed)
		b.mu.Lock()
		if b.ln != nil {
			b.ln.Close()
		}
		conns := make([]*clientConn, 0, len(b.online))
		for _, c := range b.online {
			conns = append(conns, c)
		}
		b.mu.Unlock()
		for _, c := range conns {
			c.close()
		}
	})
	return nil
}

// register 将连接登记为某 clientID 的在线连接；踢掉同 ID 旧连接（会话接管）。
func (b *Broker) register(clientID string, c *clientConn) {
	b.mu.Lock()
	old := b.online[clientID]
	b.online[clientID] = c
	b.mu.Unlock()
	if old != nil && old != c {
		old.close() // 同一 ClientID 新连接接管，旧连接断开
	}
}

// unregister 仅当当前在线连接仍是 c 时才摘除，避免误删新连接。
func (b *Broker) unregister(clientID string, c *clientConn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.online[clientID] == c {
		delete(b.online, clientID)
	}
}

// TargetKind 命中目标的种类。
type TargetKind int

const (
	// TargetNormal 普通（非共享）订阅。
	TargetNormal TargetKind = iota
	// TargetShared 共享订阅组。
	TargetShared
)

// Status 单个命中目标的投递结果，调用方可据此区分处理。
type Status int

const (
	// StatusDelivered 已投递给在线订阅者（或在线组成员）。
	StatusDelivered Status = iota
	// StatusQueuedOffline 订阅者离线，消息已进入其持久会话离线队列。
	StatusQueuedOffline
	// StatusQueueFull 订阅者离线且其离线队列已满，消息被拒。
	StatusQueueFull
	// StatusSessionExpired 命中的持久会话已过期（墓碑保留期内）。
	StatusSessionExpired
	// StatusGroupNoMember 共享组暂无可用成员且兜底队列满，消息被拒。
	StatusGroupNoMember
	// StatusGroupBuffered 共享组暂无在线成员，消息已进入组兜底队列等待成员上线。
	StatusGroupBuffered
)

// Target 一个命中目标及其投递结果。
type Target struct {
	Kind     TargetKind
	ClientID string // 普通订阅：客户端 ID；共享组：实际被选中的成员（缓冲时为空）
	Group    string // 共享组名（普通订阅为空）
	Filter   string // 命中的过滤器（共享组为内层过滤器）
	Status   Status
}

// Report 一次发布的投递报告。
type Report struct {
	Topic   string
	Targets []Target
}

// NoSubscriber 是否没有任何命中（已过期会话不再算作成功命中）。
func (r *Report) NoSubscriber() bool { return len(r.Targets) == 0 }

// Rejected 是否存在被拒目标（队列满/会话过期/组无可用成员）。
func (r *Report) Rejected() bool {
	for _, t := range r.Targets {
		switch t.Status {
		case StatusQueueFull, StatusSessionExpired, StatusGroupNoMember:
			return true
		}
	}
	return false
}

// Accepted 是否至少有一个目标成功（在线投递/离线排队/组缓冲）。
func (r *Report) Accepted() bool {
	for _, t := range r.Targets {
		switch t.Status {
		case StatusDelivered, StatusQueuedOffline, StatusGroupBuffered:
			return true
		}
	}
	return false
}

// v5Reason MQTT 5.0 PUBACK/PUBREC 原因码（完整区分见 Report）。
func (r *Report) v5Reason() byte {
	switch {
	case r.NoSubscriber():
		return packet.ReasonNoMatchingSubscribers
	case !r.Accepted():
		for _, t := range r.Targets {
			if t.Status == StatusSessionExpired {
				return packet.ReasonSessionTakenOver
			}
		}
		return packet.ReasonQuotaExceeded
	case r.Rejected():
		return packet.ReasonQuotaExceeded
	default:
		return packet.ReasonSuccess
	}
}

// Publish 进程内发布入口（CLI/嵌入方可拿到区分明细的 Report）。
// retain 语义与 PUBLISH 报文一致；网络发布与进程内发布共用同一套路由。
func (b *Broker) Publish(topicName string, qos byte, retain bool, payload []byte) (*Report, error) {
	if err := topicpkg.ValidatePublish(topicName); err != nil {
		return nil, err
	}
	p := &packet.Packet{
		Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
		Topic:   topicName,
		Payload: payload,
		QoS:     qos,
		Retain:  retain,
	}
	if retain {
		b.setRetained(p)
	}
	return b.route(p), nil
}

type subscriber struct {
	clientID string
	qos      byte
}

type groupHit struct {
	key    string
	group  string
	filter string
	qos    byte
}

// match 遍历全部会话与墓碑，返回普通订阅命中者、共享组命中者与过期会话。
func (b *Broker) match(topicName string) ([]subscriber, []groupHit, []string) {
	var normals []subscriber
	var groups []groupHit
	var expiredHits []string
	seenGroup := map[string]bool{}

	b.sessions.Range(func(sess *session.Session) bool {
		var best byte
		matched := false
		for filter, q := range sess.Subs() {
			if sf, ok := topicpkg.ParseShared(filter); ok {
				k := groupKey(sf.Group, sf.Filter)
				if seenGroup[k] {
					continue
				}
				if topicpkg.Match(sf.Filter, topicName) {
					seenGroup[k] = true
					groups = append(groups, groupHit{key: k, group: sf.Group, filter: sf.Filter, qos: q})
				}
				continue
			}
			if topicpkg.Match(filter, topicName) {
				if !matched || q > best {
					best = q
				}
				matched = true
			}
		}
		if matched {
			normals = append(normals, subscriber{clientID: sess.ClientID, qos: best})
		}
		return true
	})

	// 过期会话：其普通订阅若命中，单独标记；共享组成员身份已在过期时摘除。
	b.sessions.RangeTombstones(func(clientID string, subs map[string]byte) bool {
		for filter := range subs {
			if _, ok := topicpkg.ParseShared(filter); ok {
				continue
			}
			if topicpkg.Match(filter, topicName) {
				expiredHits = append(expiredHits, clientID)
				break
			}
		}
		return true
	})
	return normals, groups, expiredHits
}

// route 将一条 PUBLISH 路由给所有命中目标并返回明细报告。
func (b *Broker) route(p *packet.Packet) *Report {
	report := &Report{Topic: p.Topic}
	normals, groups, expired := b.match(p.Topic)

	for _, m := range normals {
		qos := p.QoS
		if m.qos < qos {
			qos = m.qos
		}
		out := copyPublish(p, qos)
		st := b.deliver(m.clientID, out)
		report.Targets = append(report.Targets, Target{
			Kind: TargetNormal, ClientID: m.clientID, Status: st,
		})
	}

	for _, gh := range groups {
		qos := p.QoS
		if gh.qos < qos {
			qos = gh.qos
		}
		out := copyPublish(p, qos)
		res := b.deliverGroup(gh, out)
		report.Targets = append(report.Targets, Target{
			Kind: TargetShared, ClientID: res.member, Group: gh.group, Filter: gh.filter, Status: res.status,
		})
	}

	for _, id := range expired {
		report.Targets = append(report.Targets, Target{
			Kind: TargetNormal, ClientID: id, Status: StatusSessionExpired,
		})
	}
	return report
}

func copyPublish(p *packet.Packet, qos byte) *packet.Packet {
	payload := append([]byte(nil), p.Payload...)
	return &packet.Packet{
		Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
		Topic:   p.Topic,
		Payload: payload,
		QoS:     qos,
		Retain:  p.Retain,
	}
}

// onlineConn 返回某 clientID 的当前在线连接（可能为 nil）。
func (b *Broker) onlineConn(clientID string) *clientConn {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.online[clientID]
}

// isOnline 返回 clientID 当前是否有在线连接。
func (b *Broker) isOnline(clientID string) bool {
	return b.onlineConn(clientID) != nil
}

// deliver 投递给普通订阅客户端：在线直发，离线持久会话入有界队列。
// CleanStart 会话断开即删除，不会走到这里（未命中由 NoSubscriber 体现）。
func (b *Broker) deliver(clientID string, p *packet.Packet) Status {
	if c := b.onlineConn(clientID); c != nil {
		c.enqueuePublish(p, "", 0)
		return StatusDelivered
	}
	if sess := b.sessions.Peek(clientID); sess != nil && !sess.CleanStart {
		if sess.Enqueue(p) == session.EnqueueQueueFull {
			return StatusQueueFull
		}
		return StatusQueuedOffline
	}
	return StatusGroupNoMember
}

// groupResult 组投递结果（状态 + 实际成员）。
type groupResult struct {
	status Status
	member string
}

// deliverGroup 投递给共享组：在线成员单选，无成员时组级兜底。
func (b *Broker) deliverGroup(gh groupHit, p *packet.Packet) groupResult {
	res := b.groups.publish(gh.key, p)
	switch res.status {
	case groupDelivered:
		return groupResult{status: StatusDelivered, member: res.member}
	case groupBuffered:
		return groupResult{status: StatusGroupBuffered}
	default:
		return groupResult{status: StatusGroupNoMember}
	}
}

// dispatchToConn 是 groupManager 的发送回调（在组锁内调用）：
// 在成员会话上分配 PacketID、登记在途（携带组键与令牌），非阻塞入发送队列。
// 不得回调 groupManager，以免持锁重入。
func (b *Broker) dispatchToConn(c *clientConn, p *packet.Packet, groupKey string, token uint64) (uint16, bool) {
	if c.isClosed() {
		return 0, false
	}
	if p.QoS > 0 {
		id := c.sess.NextPacketID()
		if id == 0 {
			return 0, false
		}
		p.PacketID = id
		c.sess.TrackOut(&session.Inflight{
			PacketID: id,
			Packet:   p,
			Stage:    map[byte]int{1: 0, 2: 1}[p.QoS],
			GroupKey: groupKey,
			Token:    token,
		})
	}
	if !c.tryEnqueue(p) {
		if p.QoS > 0 {
			c.sess.AckOut(p.PacketID) // 回滚在途，组将选择下一个成员
		}
		return 0, false
	}
	return p.PacketID, true
}

// setRetained 更新保留消息；payload 为空表示清除。
func (b *Broker) setRetained(p *packet.Packet) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if len(p.Payload) == 0 {
		delete(b.retained, p.Topic)
		return
	}
	cp := *p
	b.retained[p.Topic] = &cp
}

// retainedFor 返回命中过滤器的全部保留消息。
func (b *Broker) retainedFor(filter string) []*packet.Packet {
	b.mu.RLock()
	defer b.mu.RUnlock()
	var out []*packet.Packet
	for t, p := range b.retained {
		if topicpkg.Match(filter, t) {
			out = append(out, p)
		}
	}
	return out
}

// errConnClosed 供连接循环区分正常关闭。
var errConnClosed = errors.New("broker: connection closed")
