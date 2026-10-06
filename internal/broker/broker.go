// Package broker 实现 MQTT 代理核心：连接接入、主题路由、
// 保留消息与会话生命周期管理。路由与会话逻辑均为自实现。
package broker

import (
	"errors"
	"net"
	"sync"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/topic"
)

// Broker 消息代理。
type Broker struct {
	sessions *session.Store

	mu       sync.RWMutex
	retained map[string]*packet.Packet // topic -> retained publish
	online   map[string]*clientConn    // clientID -> 当前在线连接

	ln     net.Listener
	closed chan struct{}
	once   sync.Once
}

// New 创建代理。
func New() *Broker {
	return &Broker{
		sessions: session.NewStore(),
		retained: make(map[string]*packet.Packet),
		online:   make(map[string]*clientConn),
		closed:   make(chan struct{}),
	}
}

// ListenAndServe 在指定地址监听并服务，直到 Close。
func (b *Broker) ListenAndServe(addr string) error {
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return err
	}
	b.mu.Lock()
	b.ln = ln
	b.mu.Unlock()

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

// route 将一条 PUBLISH 路由给所有命中的订阅者。
// 在线则直接下发（QoS>0 时登记在途），离线且为持久会话则入队。
func (b *Broker) route(p *packet.Packet) {
	matched := b.matchSubscribers(p.Topic)
	for _, m := range matched {
		qos := p.QoS
		if m.qos < qos {
			qos = m.qos // 投递 QoS = min(发布QoS, 订阅QoS)
		}
		out := &packet.Packet{
			Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
			Topic:   p.Topic,
			Payload: p.Payload,
			QoS:     qos,
			Retain:  p.Retain,
		}
		b.deliver(m.clientID, out)
	}
}

type subscriber struct {
	clientID string
	qos      byte
}

// matchSubscribers 遍历全部会话的订阅，返回命中者。
// 判定逻辑集中在 topic.Match，结果确定且可解释。
func (b *Broker) matchSubscribers(topicName string) []subscriber {
	var out []subscriber
	b.sessions.Range(func(sess *session.Session) bool {
		// 同一客户端多条过滤器命中时取最高 QoS，只投递一次
		var best byte
		matched := false
		for filter, q := range sess.Subs() {
			if topic.Match(filter, topicName) {
				if !matched || q > best {
					best = q
				}
				matched = true
			}
		}
		if matched {
			out = append(out, subscriber{clientID: sess.ClientID, qos: best})
		}
		return true
	})
	return out
}

// deliver 投递给指定客户端：在线直发，离线持久会话入队。
func (b *Broker) deliver(clientID string, p *packet.Packet) {
	b.mu.RLock()
	c := b.online[clientID]
	b.mu.RUnlock()
	if c != nil {
		c.enqueuePublish(p)
		return
	}
	// 离线：仅持久会话缓存 QoS>0 消息
	if sess := b.sessions.Peek(clientID); sess != nil && !sess.CleanStart && p.QoS > 0 {
		sess.Enqueue(p)
	}
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
		if topic.Match(filter, t) {
			out = append(out, p)
		}
	}
	return out
}

// errConnClosed 供连接循环区分正常关闭。
var errConnClosed = errors.New("broker: connection closed")
