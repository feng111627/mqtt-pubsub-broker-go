package broker

import (
	"fmt"
	"net"
	"sync"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
)

// Config holds broker tunables.
type Config struct {
	// ListenAddr is the TCP address, e.g. "127.0.0.1:1883".
	ListenAddr string
	// MaxPacketSize caps accepted remaining length. Default 1 MiB.
	MaxPacketSize int
	// ReceiveMaximum caps server-side inflight QoS messages per client
	// (mirrored to v5 clients). Default 64.
	ReceiveMaximum uint16
	// QueueMax bounds stored QoS1/2 messages for an offline persistent
	// session. 0 means unbounded. Default 1000.
	QueueMax int
	// MaxClients bounds simultaneous connections. 0 means unlimited.
	MaxClients int
	// KeepAliveFactor is applied to the client keepalive; default 1.5 per spec.
	KeepAliveFactor float64
	// Auth, if non-nil, validates CONNECT credentials. The returned error
	// should be *mqerr.Error so the right CONNACK code is sent.
	Auth func(username string, password []byte) error
}

// DefaultConfig returns loopback-oriented defaults.
func DefaultConfig() Config {
	return Config{
		ListenAddr:      "127.0.0.1:1883",
		MaxPacketSize:   1 << 20,
		ReceiveMaximum:  64,
		QueueMax:        1000,
		KeepAliveFactor: 1.5,
	}
}

// Broker is the MQTT server.
type Broker struct {
	cfg      Config
	listener net.Listener

	mu           sync.Mutex
	sessions     map[string]*Session
	sessionByUID map[uint64]*Session
	uidSeq       uint64
	conns        map[*clientConn]struct{}
	trie         *topicTrie
	retained     map[string]*retainedMessage
	// routeMu guards the routing trie and retained store against concurrent
	// subscription and publish operations.
	routeMu sync.RWMutex
	closed  bool
	closeCh chan struct{}
	wg      sync.WaitGroup
}

type retainedMessage struct {
	qos     byte
	payload []byte
	topic   string
}

// New creates a Broker with the given configuration.
func New(cfg Config) *Broker {
	if cfg.ListenAddr == "" {
		d := DefaultConfig()
		cfg.ListenAddr = d.ListenAddr
	}
	if cfg.MaxPacketSize <= 0 {
		cfg.MaxPacketSize = 1 << 20
	}
	if cfg.ReceiveMaximum == 0 {
		cfg.ReceiveMaximum = 64
	}
	if cfg.QueueMax == 0 {
		cfg.QueueMax = 1000
	}
	if cfg.KeepAliveFactor <= 0 {
		cfg.KeepAliveFactor = 1.5
	}
	return &Broker{
		cfg:          cfg,
		sessions:     make(map[string]*Session),
		sessionByUID: make(map[uint64]*Session),
		conns:        make(map[*clientConn]struct{}),
		trie:         newTopicTrie(),
		retained:     make(map[string]*retainedMessage),
		closeCh:      make(chan struct{}),
	}
}

// Start listens on cfg.ListenAddr and serves in background goroutines.
func (b *Broker) Start() error {
	ln, err := net.Listen("tcp", b.cfg.ListenAddr)
	if err != nil {
		return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "listen %s: %v", b.cfg.ListenAddr, err)
	}
	b.listener = ln
	b.wg.Add(1)
	go func() {
		defer b.wg.Done()
		b.acceptLoop()
	}()
	return nil
}

// Serve listens and blocks serving until Close.
func (b *Broker) Serve() error {
	if b.listener == nil {
		if err := b.Start(); err != nil {
			return err
		}
	}
	b.wg.Wait()
	return nil
}

func (b *Broker) acceptLoop() {
	for {
		conn, err := b.listener.Accept()
		if err != nil {
			select {
			case <-b.closeCh:
				return
			default:
			}
			if ne, ok := err.(net.Error); ok && ne.Timeout() {
				continue
			}
			return
		}
		b.mu.Lock()
		if b.cfg.MaxClients > 0 && len(b.conns) >= b.cfg.MaxClients {
			b.mu.Unlock()
			conn.Close()
			continue
		}
		c := &clientConn{
			broker: b,
			conn:   conn,
			stopCh: make(chan struct{}),
		}
		b.conns[c] = struct{}{}
		b.wg.Add(1)
		b.mu.Unlock()
		go func() {
			defer b.wg.Done()
			defer b.connDone(c)
			c.handle()
		}()
	}
}

func (b *Broker) connDone(c *clientConn) {
	c.conn.Close()
	close(c.stopCh)
	b.mu.Lock()
	delete(b.conns, c)
	b.mu.Unlock()
}

// Addr returns the actual listen address (useful with port 0).
func (b *Broker) Addr() net.Addr {
	if b.listener == nil {
		return nil
	}
	return b.listener.Addr()
}

// Close stops accepting and closes every live connection.
func (b *Broker) Close() error {
	b.mu.Lock()
	if b.closed {
		b.mu.Unlock()
		return nil
	}
	b.closed = true
	close(b.closeCh)
	ln := b.listener
	conns := make([]*clientConn, 0, len(b.conns))
	for c := range b.conns {
		conns = append(conns, c)
	}
	b.mu.Unlock()
	if ln != nil {
		ln.Close()
	}
	for _, c := range conns {
		c.conn.Close()
	}
	b.wg.Wait()
	return nil
}

// allocUID assigns a monotonically increasing session-identity token.
func (b *Broker) allocUID() uint64 {
	b.uidSeq++
	return b.uidSeq
}

// lookupOrCreateSession implements the CONNECT session semantics and takeover.
//
//   - clean=true:  any existing session (and connection) is removed; a fresh
//     empty session starts and is destroyed at disconnect.
//   - clean=false: an existing session is resumed (CONNACK session present=1)
//     and its old connection is taken over; otherwise a new
//     persistent session is created and reported as absent.
//
// The returned sessionPresent flag goes straight into CONNACK.
func (b *Broker) lookupOrCreateSession(level byte, clientID string, clean bool, will *willMessage) (*Session, bool, *clientConn) {
	b.mu.Lock()
	defer b.mu.Unlock()
	existing := b.sessions[clientID]
	var oldConn *clientConn
	if existing != nil {
		oldConn = existing.connected
	}
	if clean {
		if existing != nil {
			b.trie.remove(existing.UID, "")
			delete(b.sessions, clientID)
		}
		s := newSession(clientID, b.allocUID(), level, true, b.cfg.QueueMax)
		s.will = will
		b.sessions[clientID] = s
		b.sessionByUID[s.UID] = s
		return s, false, oldConn
	}
	if existing != nil {
		existing.ProtoLevel = level
		existing.CleanStart = false
		existing.will = will
		return existing, true, oldConn
	}
	s := newSession(clientID, b.allocUID(), level, false, b.cfg.QueueMax)
	s.will = will
	b.sessions[clientID] = s
	b.sessionByUID[s.UID] = s
	return s, false, nil
}

// destroySession removes a clean session on normal disconnect and its trie
// state. Persistent sessions are kept.
func (b *Broker) destroySession(s *Session) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.trie.remove(s.UID, "")
	delete(b.sessionByUID, s.UID)
	if cur := b.sessions[s.ClientID]; cur == s {
		delete(b.sessions, s.ClientID)
	}
}

// onDisconnect is called by the connection loop. The will is published only
// when (a) the client did not disconnect gracefully, (b) this socket is still
// the session's current connection (no takeover), and (c) a will exists.
func (b *Broker) onDisconnect(s *Session, c *clientConn, suppressWill bool) {
	s.mu.Lock()
	stillOurs := s.connected == c
	will := s.will
	if stillOurs {
		s.goOfflineLocked(c)
		s.will = nil
	}
	s.mu.Unlock()
	if s.CleanStart && stillOurs {
		b.destroySession(s)
	}
	if !suppressWill && stillOurs && will != nil {
		b.publishWill(will)
	}
}

func (b *Broker) publishWill(w *willMessage) {
	b.route(w.topic, w.qos, w.payload, true, 0, w.props, 0)
}

// Stats returns a snapshot used by tests and diagnostics (see exported type).
// Stats is an observable snapshot of broker state, useful for diagnostics.
type Stats struct {
	Clients       int
	Sessions      int
	Subscriptions int
	Retained      int
}

// Stats returns a point-in-time snapshot.
func (b *Broker) Stats() Stats {
	b.mu.Lock()
	defer b.mu.Unlock()
	total := 0
	for _, s := range b.sessions {
		total += len(s.subs)
	}
	return Stats{Clients: len(b.conns), Sessions: len(b.sessions), Subscriptions: total, Retained: len(b.retained)}
}

// String renders the broker for log lines.
func (b *Broker) String() string {
	return fmt.Sprintf("mqtt-broker@%s", b.cfg.ListenAddr)
}

var _ = mqerr.ClassState
