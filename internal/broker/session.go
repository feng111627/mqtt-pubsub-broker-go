package broker

import (
	"sync"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// queuedMessage is one application message waiting delivery (or redelivery)
// for a specific session.
type queuedMessage struct {
	ID     uint16 // packet identifier in use for this session, 0 if unassigned
	QoS    byte
	Retain bool
	Topic  string
	// Payload is copied at enqueue time so publisher-side reuse is safe.
	Payload []byte
	// Inflight: already sent, waiting PUBACK (QoS1) or PUBREC (QoS2 first half).
	Inflight bool
	Dup      bool
	// QoS2RelSent: PUBREC was received, PUBREL sent; now waiting PUBCOMP.
	QoS2RelSent bool
}

// subscription is one filter owned by a session.
type subscription struct {
	Filter         string
	QoS            byte
	NoLocal        bool
	RetainAsPub    bool
	RetainHandling byte
}

// willMessage is the client's testament.
type willMessage struct {
	topic   string
	payload []byte
	qos     byte
	retain  bool
	props   packet.Properties
}

// Session is a client's durable or transient broker-side state.
//
// The Session outlives a single TCP connection when CleanSession/CleanStart is
// false: subscriptions and queued QoS1/QoS2 messages survive disconnect and
// are resumed on reconnect with the same client id. A clean session is
// destroyed both at disconnect and when replaced by a clean reconnect.
type Session struct {
	ClientID      string
	UID           uint64
	ProtoLevel    byte
	CleanStart    bool
	ExpireSeconds uint32 // v5 session expiry; this implementation keeps only 0 semantics
	subs          map[string]*subscription
	queue         []*queuedMessage
	inflight      map[uint128]*queuedMessage // sent, awaiting ACK/PUBREC
	pendingQoS2   map[uint16]*queuedMessage  // PUBREC'd, awaiting PUBCOMP
	nextPacketID  uint16
	will          *willMessage
	queueMax      int
	// connected is the current live connection; nil when offline.
	connected *clientConn

	mu sync.Mutex

	// inboundQoS1 records packet ids of QoS1 PUBLISHes the client has not yet
	// seen acknowledged (reuse detection -> 0x91).
	inboundQoS1 map[uint16]struct{}
	// inboundQoS2 tracks QoS2 ids: true after PUBLISH (awaiting PUBREL/PUBCOMP),
	// which is the only stage we need for duplicate suppression.
	inboundQoS2 map[uint16]bool
}

// uint128 key placeholder replaced with plain uint16 below; kept out of the
// public surface by aliasing.
type uint128 = uint16

func newSession(id string, uid uint64, level byte, clean bool, queueMax int) *Session {
	return &Session{
		ClientID:     id,
		UID:          uid,
		ProtoLevel:   level,
		CleanStart:   clean,
		subs:         make(map[string]*subscription),
		inflight:     make(map[uint16]*queuedMessage),
		pendingQoS2:  make(map[uint16]*queuedMessage),
		inboundQoS1:  make(map[uint16]struct{}),
		inboundQoS2:  make(map[uint16]bool),
		nextPacketID: 1,
		queueMax:     queueMax,
	}
}

// nextID returns the next free packet identifier (1..65535). The boolean is
// false when the whole space is exhausted (receive maximum worth of inflight).
func (s *Session) nextID() (uint16, bool) {
	for range 65535 {
		id := s.nextPacketID
		s.nextPacketID++
		if s.nextPacketID == 0 {
			s.nextPacketID = 1
		}
		if _, busy := s.inflight[id]; !busy {
			if _, busy2 := s.pendingQoS2[id]; !busy2 {
				return id, true
			}
		}
	}
	return 0, false
}

// enqueue appends an outbound message. QoS0 messages are dropped when the
// session is offline; QoS1/2 are stored subject to queueMax. Returns false
// when the message had to be discarded due to queue pressure.
func (s *Session) enqueue(m *queuedMessage) bool {
	if m.QoS == packet.QoS0 && s.connected == nil {
		return false
	}
	if m.QoS > 0 && s.connected == nil && s.queueMax > 0 && len(s.queue) >= s.queueMax {
		return false
	}
	s.queue = append(s.queue, m)
	return true
}

// empty reports whether the session carries no durable state worth keeping.
func (s *Session) empty() bool {
	return len(s.subs) == 0 && len(s.queue) == 0 &&
		len(s.inflight) == 0 && len(s.pendingQoS2) == 0
}

// inflightCount reports how many outbound messages await acknowledgement.
func (s *Session) inflightCount() int {
	return len(s.inflight)
}

// takeQueued pops the first not-inflight queue entry that can be sent now,
// assigning a packet identifier for QoS>0. Returns nil when no send credit or
// no eligible message exists.
func (s *Session) takeQueued(receiveMax int) *queuedMessage {
	for _, m := range s.queue {
		if m.Inflight || m.QoS2RelSent {
			continue
		}
		if m.QoS > 0 {
			if receiveMax > 0 && len(s.inflight) >= receiveMax {
				return nil
			}
			if m.ID == 0 {
				id, ok := s.nextID()
				if !ok {
					return nil
				}
				m.ID = id
			}
		}
		return m
	}
	return nil
}

// markInflight records a queue entry as sent and moves it to the inflight set.
func (s *Session) markInflight(m *queuedMessage) {
	m.Inflight = true
	if m.QoS > 0 {
		s.inflight[m.ID] = m
	}
}

// removeInflight completes QoS1 delivery (PUBACK) or the first half of QoS2
// (PUBREC): the entry leaves the inflight set; the queue entry is dropped for
// QoS1 or moved to pendingQoS2 by the caller for QoS2.
func (s *Session) removeInflight(id uint16) *queuedMessage {
	m := s.inflight[id]
	delete(s.inflight, id)
	return m
}

// dropQueueEntry removes the finished message from the persistent queue.
func (s *Session) dropQueueEntry(m *queuedMessage) {
	for i, q := range s.queue {
		if q == m {
			s.queue = append(s.queue[:i], s.queue[i+1:]...)
			return
		}
	}
}

// goOfflineLocked transitions a session offline. Caller must hold s.mu.
// Inflight messages stay in the queue and are marked DUP for redelivery;
// QoS2 messages past PUBREC remain in pendingQoS2 and resume by resending
// PUBREL.
func (s *Session) goOfflineLocked(c *clientConn) {
	if s.connected == c {
		s.connected = nil
	}
	for _, m := range s.inflight {
		m.Inflight = false
		m.Dup = true
	}
	s.inflight = make(map[uint16]*queuedMessage)
}

// attach binds a (possibly re-established) connection. Caller must not rely on
// the returned list; pending QoS2 flows are queried separately.
func (s *Session) attach(c *clientConn) {
	s.mu.Lock()
	s.connected = c
	s.mu.Unlock()
}

// subCount is a test/observability helper.
func (s *Session) subCount() int { return len(s.subs) }

// queuedCount is a test/observability helper.
func (s *Session) queuedCount() int { return len(s.queue) }
