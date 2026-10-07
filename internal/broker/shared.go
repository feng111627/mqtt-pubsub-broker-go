package broker

import (
	"log"
	"strings"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/delivery"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/group"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/topic"
)

// sharePrefix 是共享订阅过滤器前缀（MQTT 5.0 规定的 $share 约定，
// 本代理在 3.1.1 与 5.0 接入上都接受）。
const sharePrefix = "$share/"

// 默认后台巡检周期。
const defaultSweepInterval = time.Minute

// Config 代理容量与过期配置。
type Config struct {
	// Session 持久会话离线堆积与过期配置。
	Session session.Config
	// Group 共享组兜底队列配置。
	Group group.Config
	// TotalQueuedMessages 全部会话离线队列 + 全部共享组兜底队列允许占用的
	// 总条数上限（全局内存兜底）。单个会话先达到自身上限时拒绝自身；
	// 全局上限达到时拒绝新入队者，但绝不淘汰、挤占其他会话/组的消息。
	// 0 表示不设全局上限。
	TotalQueuedMessages int64
	// SweepInterval 后台巡检（会话过期、消息 TTL、空闲组回收）周期；0 取默认。
	SweepInterval time.Duration
}

// DefaultConfig 返回默认配置。
func DefaultConfig() Config {
	return Config{
		Session:             session.DefaultConfig(),
		Group:               group.DefaultConfig(),
		TotalQueuedMessages: 100_000,
		SweepInterval:       defaultSweepInterval,
	}
}

// parseSharedFilter 解析 "$share/<组名>/<过滤器>"。
// 非共享过滤器返回 ok=false；组名或剩余过滤器为空也视为非法（ok=false，
// 由调用方按协议错误拒绝订阅）。
func parseSharedFilter(filter string) (groupName, suffix string, ok bool) {
	if !strings.HasPrefix(filter, sharePrefix) {
		return "", "", false
	}
	rest := filter[len(sharePrefix):]
	idx := strings.IndexByte(rest, '/')
	if idx <= 0 || idx == len(rest)-1 {
		return "", "", false // 缺组名或缺真实过滤器
	}
	groupName = rest[:idx]
	suffix = rest[idx+1:]
	if strings.ContainsAny(groupName, "/#") {
		return "", "", false
	}
	return groupName, suffix, true
}

// routeWithResult 路由一条 PUBLISH 并返回每个命中目标的可区分结果。
//
// 命中目标分两类（彼此独立、互不去重）：
//   - 普通（独占）订阅：每个会话至多一个 Outcome；在线直发，离线持久会话入队。
//   - 共享订阅：每个“组”至多一个 Outcome；组内只选一个在线成员轮询投递，
//     无在线成员则进组兜底队列，兜底关闭/已满分别给出可区分结果。
//
// 一条消息可同时命中多个普通会话和多个组；一个客户端既是普通订阅者又是
// 某组唯一在线成员时会各收一份（两种订阅语义独立，符合 MQTT 共享订阅规范）。
func (b *Broker) routeWithResult(p *packet.Packet) *delivery.Result {
	now := b.now()
	res := &delivery.Result{Topic: p.Topic}
	matched := b.matchTargets(p.Topic)

	for _, t := range matched.normal {
		qos := p.QoS
		if t.qos < qos {
			qos = t.qos
		}
		out := copyForDelivery(p, qos)
		b.routeNormal(t.sess, out, res, now)
	}
	for _, gt := range matched.shared {
		qos := p.QoS
		if gt.qos < qos {
			qos = gt.qos
		}
		out := copyForDelivery(p, qos)
		b.routeShared(gt, out, res, now)
	}
	if len(res.Outcomes) == 0 {
		res.Outcomes = append(res.Outcomes, delivery.Outcome{
			Kind:   delivery.KindNormal,
			Status: delivery.NoSubscribers,
		})
	}
	return res
}

// routeNormal 处理一个普通订阅目标。
func (b *Broker) routeNormal(sess *session.Session, out *packet.Packet, res *delivery.Result, now time.Time) {
	oc := delivery.Outcome{Kind: delivery.KindNormal, Target: sess.ClientID}
	if sess.Expired() {
		oc.Status = delivery.SessionExpired
		res.Outcomes = append(res.Outcomes, oc)
		return
	}
	if c := b.onlineConn(sess.ClientID); c != nil {
		if c.enqueuePublish(out, "") {
			oc.Status = delivery.DeliveredOnline
			res.Outcomes = append(res.Outcomes, oc)
			return
		}
		// 在线表仍指向它但发送队列已关闭（极端竞态）：落入下方离线分支
	}
	// 离线：仅持久会话缓存（CleanStart 会话断开即删，走到这里一定是持久会话）
	er := sess.EnqueueAt(out, now)
	// EnqueueAt 内部裁剪掉的过期消息回收全局配额
	if er.Expired > 0 {
		b.queuedTotal.Add(-int64(er.Expired))
	}
	if er.Accepted {
		b.queuedTotal.Add(1)
		oc.Status = delivery.QueuedOffline
		res.Outcomes = append(res.Outcomes, oc)
		return
	}
	// 未入队：会话过期 / 本会话队列满 / 全局上限
	switch {
	case sess.Expired():
		oc.Status = delivery.SessionExpired
	case b.globalQueueFull() || er.Full:
		oc.Status = delivery.QueueFull
		oc.Dropped = true
		log.Printf("broker: message to %s rejected: session queue full (topic=%s)", sess.ClientID, out.Topic)
	default:
		oc.Status = delivery.SessionExpired
	}
	res.Outcomes = append(res.Outcomes, oc)
}

// routeShared 处理一个共享组目标：组内只投递给一个在线成员。
func (b *Broker) routeShared(gt sharedTarget, out *packet.Packet, res *delivery.Result, now time.Time) {
	oc := delivery.Outcome{Kind: delivery.KindShared, Target: sharePrefix + gt.name}
	// 组内轮询选择在线成员；若选中后入队时发现连接刚好关闭，则依次尝试
	// 其余在线成员（至多成员数次），保证与掉线并发时不丢消息。
	delivered := false
	for i, n := 0, len(gt.g.Members()); i < n; i++ {
		id := gt.g.Pick(b.isOnline)
		if id == "" {
			break
		}
		c := b.onlineConn(id)
		if c != nil && c.enqueuePublish(out, gt.name) {
			delivered = true
			break
		}
	}
	if delivered {
		oc.Status = delivery.SharedDelivered
		res.Outcomes = append(res.Outcomes, oc)
		return
	}
	b.enqueueGroupFallback(gt, out, now, res, oc)
}

// enqueueGroupFallback 将消息放入共享组兜底队列，并产出可区分结果。
func (b *Broker) enqueueGroupFallback(gt sharedTarget, out *packet.Packet, now time.Time, res *delivery.Result, oc delivery.Outcome) {
	q := &group.Queued{
		Topic:      out.Topic,
		Payload:    out.Payload,
		QoS:        out.QoS,
		Retain:     out.Retain,
		EnqueuedAt: now,
		Counted:    true, // 新发布消息占用全局堆积配额
	}
	if b.globalQueueFull() {
		oc.Status = delivery.QueueFull
		oc.Dropped = true
		log.Printf("broker: message to group %s rejected: global queue cap (topic=%s)", gt.name, out.Topic)
		res.Outcomes = append(res.Outcomes, oc)
		return
	}
	if gt.g.Enqueue(q, now) {
		b.queuedTotal.Add(1)
		oc.Status = delivery.SharedQueued
		res.Outcomes = append(res.Outcomes, oc)
		return
	}
	// Enqueue 返回 false：兜底关闭（GroupUnavailable）或组队列满（QueueFull）
	if b.cfg.Group.GroupQueueCap == 0 {
		oc.Status = delivery.GroupUnavailable
	} else {
		oc.Status = delivery.QueueFull
		oc.Dropped = true
		log.Printf("broker: message to group %s rejected: group queue full (topic=%s)", gt.name, out.Topic)
	}
	res.Outcomes = append(res.Outcomes, oc)
}

// globalQueueFull 全局堆积上限是否已达到（0 表示不限制）。
func (b *Broker) globalQueueFull() bool {
	return b.cfg.TotalQueuedMessages > 0 && b.queuedTotal.Load() >= b.cfg.TotalQueuedMessages
}

// onlineConn 返回某客户端当前在线连接（无锁读快照）。
func (b *Broker) onlineConn(clientID string) *clientConn {
	b.mu.RLock()
	defer b.mu.RUnlock()
	return b.online[clientID]
}

// isOnline 供组轮询判定成员在线状态。
func (b *Broker) isOnline(clientID string) bool {
	return b.onlineConn(clientID) != nil
}

// copyForDelivery 复制一份出站报文，避免不同目标共享同一 Packet（PacketID 独立）。
func copyForDelivery(p *packet.Packet, qos byte) *packet.Packet {
	return &packet.Packet{
		Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
		Topic:   p.Topic,
		Payload: append([]byte(nil), p.Payload...),
		QoS:     qos,
		Retain:  p.Retain,
	}
}

// normalTarget 一个命中的普通订阅。
type normalTarget struct {
	sess *session.Session
	qos  byte
}

// sharedTarget 一个命中的共享订阅组。
type sharedTarget struct {
	name string
	g    *group.Group
	qos  byte
}

type matchedTargets struct {
	normal []normalTarget
	shared []sharedTarget
}

// matchTargets 遍历全部会话，按普通订阅与共享订阅分别聚合命中结果。
// 普通订阅：同会话多过滤器命中取最高 QoS，只投一次。
// 共享订阅：同组多成员/多过滤器命中取最高 QoS，组内只投一份。
func (b *Broker) matchTargets(topicName string) matchedTargets {
	var out matchedTargets
	groupQoS := map[string]byte{}
	b.sessions.Range(func(sess *session.Session) bool {
		if sess.Expired() {
			// 过期会话仍检查订阅，用于产出 SessionExpired 结果
			if sessMatches(sess, topicName) {
				out.normal = append(out.normal, normalTarget{sess: sess})
			}
			return true
		}
		var best byte
		matched := false
		for filter, q := range sess.Subs() {
			if gname, suffix, ok := parseSharedFilter(filter); ok {
				if topic.Match(suffix, topicName) {
					if cur := groupQoS[gname]; q > cur {
						groupQoS[gname] = q
					}
				}
				continue
			}
			if topic.Match(filter, topicName) {
				if !matched || q > best {
					best = q
				}
				matched = true
			}
		}
		if matched {
			out.normal = append(out.normal, normalTarget{sess: sess, qos: best})
		}
		return true
	})
	// 组成员可能在遍历时退出：注册表查不到的组跳过
	for name, q := range groupQoS {
		if g := b.groups.Get(name); g != nil {
			out.shared = append(out.shared, sharedTarget{name: name, g: g, qos: q})
		}
	}
	return out
}

// sessMatches 仅判断会话是否有普通过滤器命中（过期会话结果分类用）。
func sessMatches(sess *session.Session, topicName string) bool {
	for filter := range sess.Subs() {
		if _, _, shared := parseSharedFilter(filter); shared {
			continue
		}
		if topic.Match(filter, topicName) {
			return true
		}
	}
	return false
}

// pumpGroup 尝试把某共享组兜底队列中的消息分发给当前在线成员。
// 组内仍然没有在线成员时消息原样留在队列里，等待下一次上线/回收触发。
// 每次分发都经组的轮询选择，因此多个成员同时在线时自动负载均衡；
// 分发过程中选中的成员恰好掉线，则把这条消息放回组队列，稍后重试，不丢失。
func (b *Broker) pumpGroup(name string) {
	g := b.groups.Get(name)
	if g == nil {
		return
	}
	now := b.now()
	// Drain 只做过期裁剪并取空队列；若无在线成员，取出后立即原样放回。
	qs, expiredQs := g.Drain(now)
	if n := queuedCounted(expiredQs); n > 0 {
		b.queuedTotal.Add(-n)
	}
	var unsent []*group.Queued
	for _, q := range qs {
		id := g.Pick(b.isOnline)
		c := b.onlineConn(id)
		out := &packet.Packet{
			Header:  packet.FixedHeader{Type: packet.TypePUBLISH},
			Topic:   q.Topic,
			Payload: q.Payload,
			QoS:     q.QoS,
			Retain:  q.Retain,
		}
		if id != "" && c != nil && c.enqueuePublish(out, name) {
			if q.Counted {
				b.queuedTotal.Add(-1) // 已投递，离开全局堆积
			}
			continue
		}
		unsent = append(unsent, q)
	}
	if len(unsent) > 0 {
		// 放回队首，保持相对顺序，等待下次上线/回收再投（计数保持不变）
		g.Splice(unsent, now)
	}
}

// queuedCounted 统计一批组队列消息中计入全局配额的条数。
func queuedCounted(qs []*group.Queued) int64 {
	var n int64
	for _, q := range qs {
		if q.Counted {
			n++
		}
	}
	return n
}

// removeExpiredSession 会话过期后从共享组摘除（消息内存在 ExpireAt 时已释放）。
// 会话壳仍保留在仓库中：路由据此返回 SessionExpired（区别于“从无订阅者”），
// 同 ClientID 重连时由 Store.Get 用新会话替换，不构成内存泄漏。
func (b *Broker) removeExpiredSession(clientID string) {
	b.groups.RemoveClient(clientID)
}

// sweepLoop 周期性处理会话过期、消息 TTL 与空闲共享组回收。
func (b *Broker) sweepLoop() {
	t := time.NewTicker(b.cfg.SweepInterval)
	defer t.Stop()
	for {
		select {
		case <-b.closed:
			return
		case now := <-t.C:
			b.sweep(now)
		}
	}
}

// sweep 执行一轮巡检（也供测试直接调用）。
func (b *Broker) sweep(now time.Time) {
	// 1) 会话离线队列 TTL + 会话过期
	var expiredSessions []string
	b.sessions.Range(func(sess *session.Session) bool {
		if n := sess.SweepExpired(now); n > 0 {
			b.queuedTotal.Add(-int64(n))
		}
		if sess.ExpireAt(now) {
			log.Printf("broker: session %s expired", sess.ClientID)
			expiredSessions = append(expiredSessions, sess.ClientID)
		}
		return true
	})
	// 过期会话摘除共享组并从仓库删除（保留删除前可被路由识别为过期的语义
	// 由惰性判定覆盖；删除后同 ID 再来即新会话）。
	for _, id := range expiredSessions {
		b.removeExpiredSession(id)
	}
	// 2) 共享组 TTL（全局配额经 onExpired 回调回收）与空闲回收
	reaped, expired := b.groups.Sweep(now)
	if reaped > 0 || expired > 0 {
		log.Printf("broker: sweep reapedGroups=%d expiredGroupMessages=%d", reaped, expired)
	}
}
