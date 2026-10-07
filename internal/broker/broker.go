// Package broker 实现 MQTT 代理核心：连接接入、主题路由、
// 保留消息与会话生命周期管理。路由与会话逻辑均为自实现。
package broker

import (
	"errors"
	"fmt"
	"log"
	"net"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/delivery"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/group"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/topic"
)

// Broker 消息代理。
type Broker struct {
	cfg      Config
	sessions *session.Store
	groups   *group.Registry

	// queuedTotal 统计当前全部会话离线队列 + 共享组兜底队列占用条数，
	// 作为全局内存兜底；只增删计数，绝不因此淘汰其他目标的消息。
	queuedTotal atomic.Int64

	mu       sync.RWMutex
	retained map[string]*packet.Packet // topic -> retained publish
	online   map[string]*clientConn    // clientID -> 当前在线连接

	ln     net.Listener
	closed chan struct{}
	once   sync.Once

	nowFunc func() time.Time // 时间源（测试可替换以驱动 TTL/过期）
}

func (b *Broker) now() time.Time { return b.nowFunc() }

// New 创建代理（默认容量与过期配置）。
func New() *Broker {
	return NewWithConfig(DefaultConfig())
}

// NewWithConfig 使用指定配置创建代理。
func NewWithConfig(cfg Config) *Broker {
	if cfg.SweepInterval <= 0 {
		cfg.SweepInterval = defaultSweepInterval
	}
	b := &Broker{
		cfg:      cfg,
		sessions: session.NewStoreWithConfig(cfg.Session),
		retained: make(map[string]*packet.Packet),
		online:   make(map[string]*clientConn),
		closed:   make(chan struct{}),
	}
	// 共享组消息 TTL 过期时回收全局堆积配额。
	b.groups = group.NewRegistryWithHook(cfg.Group, func(n int64) { b.queuedTotal.Add(-n) })
	b.nowFunc = time.Now
	return b
}

// newWithClock 使用可控时钟创建代理（测试用）：代理、会话仓库与组队列
// 都以该时钟作为 TTL/过期判定的时间来源。
func newWithClock(cfg Config, f func() time.Time) *Broker {
	b := NewWithConfig(cfg)
	b.nowFunc = f
	b.sessions.SetClock(f)
	return b
}

// Config 返回代理配置（只读用途）。
func (b *Broker) Config() Config { return b.cfg }

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

// route 将一条 PUBLISH 路由给所有命中的订阅者（普通 + 共享），
// 并记录每个目标可区分的投递结果。
func (b *Broker) route(p *packet.Packet) *delivery.Result {
	res := b.routeWithResult(p)
	logRouteResult(p, res)
	return res
}

// Route 导出的路由入口，供库方式嵌入的调用方直接发布并拿到可区分结果
// （不走网络接入时使用）。
func (b *Broker) Route(p *packet.Packet) *delivery.Result {
	return b.route(p)
}

// logRouteResult 用一行汇总日志说明本条消息各目标的投递状态。
func logRouteResult(p *packet.Packet, res *delivery.Result) {
	summary := res.Summary()
	parts := make([]string, 0, len(summary))
	for status, n := range summary {
		parts = append(parts, fmt.Sprintf("%s=%d", status, n))
	}
	sort.Strings(parts)
	if res.AnyDropped() {
		log.Printf("broker: PUBLISH topic=%s qos=%d DROPPED: %s", p.Topic, p.QoS, strings.Join(parts, ","))
	} else {
		log.Printf("broker: PUBLISH topic=%s qos=%d result: %s", p.Topic, p.QoS, strings.Join(parts, ","))
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
