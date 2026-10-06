// Package session 管理客户端会话：订阅关系、QoS 在途消息与离线队列。
// 会话按 ClientID 索引，CleanStart 语义决定复用或重建，保证不串号。
package session

import (
	"sync"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// Inflight 表示一条尚未完成 QoS 握手的在途消息。
type Inflight struct {
	PacketID uint16
	Packet   *packet.Packet
	// Stage: 0=等待PUBACK(QoS1) 1=等待PUBREC(QoS2) 2=等待PUBCOMP(QoS2)
	Stage int
}

// Session 一个客户端会话。所有方法并发安全。
type Session struct {
	ClientID   string
	CleanStart bool

	mu          sync.Mutex
	subs        map[string]byte      // filter -> qos
	inflightOut map[uint16]*Inflight // broker -> client 方向
	inflightIn  map[uint16]struct{}  // client -> broker 方向（QoS2 去重）
	pending     []*packet.Packet     // 离线期间缓存的 QoS>0 消息
	nextID      uint16
}

// New 创建会话。
func New(clientID string, cleanStart bool) *Session {
	return &Session{
		ClientID:    clientID,
		CleanStart:  cleanStart,
		subs:        make(map[string]byte),
		inflightOut: make(map[uint16]*Inflight),
		inflightIn:  make(map[uint16]struct{}),
		nextID:      1,
	}
}

// Store 会话仓库，按 ClientID 索引。
type Store struct {
	mu       sync.Mutex
	sessions map[string]*Session
}

// NewStore 创建会话仓库。
func NewStore() *Store {
	return &Store{sessions: make(map[string]*Session)}
}

// Get 按 cleanStart 语义获取或创建会话：
// cleanStart=true 时丢弃旧会话重建；否则复用已有会话（resumed=true）。
func (s *Store) Get(clientID string, cleanStart bool) (sess *Session, resumed bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if cleanStart {
		sess = New(clientID, true)
		s.sessions[clientID] = sess
		return sess, false
	}
	if old, ok := s.sessions[clientID]; ok {
		return old, true
	}
	sess = New(clientID, false)
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

// Enqueue 离线缓存一条消息。
func (s *Session) Enqueue(p *packet.Packet) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.pending = append(s.pending, p)
}

// Drain 取出并清空离线缓存。
func (s *Session) Drain() []*packet.Packet {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := s.pending
	s.pending = nil
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
