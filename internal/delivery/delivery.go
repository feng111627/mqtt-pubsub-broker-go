// Package delivery 定义代理内发布/投递的可区分结果。
//
// 路由一条消息时，每个命中目标（普通订阅会话或共享订阅组）都会产生一个
// Outcome；调用方据此区分“已投递 / 已离线排队 / 无订阅者 / 队列满 /
// 会话过期 / 组内暂无可用成员”等情形，不会被一律当成同一种失败。
package delivery

// Status 投递结果状态。
type Status int

const (
	// DeliveredOnline 目标在线，消息已进入其发送队列。
	DeliveredOnline Status = iota
	// QueuedOffline 目标离线，消息已按发布顺序进入其持久会话离线队列。
	QueuedOffline
	// SharedDelivered 共享组内已分发给某一个在线成员。
	SharedDelivered
	// SharedQueued 共享组暂无在线成员，消息已进入该组的兜底队列。
	SharedQueued
	// NoSubscribers 没有任何订阅（普通或共享）命中该主题。
	NoSubscribers
	// QueueFull 目标队列（会话离线队列 / 共享组兜底队列）已满，消息被拒。
	QueueFull
	// SessionExpired 命中的持久会话已过期，不再接收消息。
	SessionExpired
	// GroupUnavailable 共享组存在但当前没有在线成员，且兜底队列被关闭。
	GroupUnavailable
)

// String 返回状态的可读名称（日志与测试断言用）。
func (s Status) String() string {
	switch s {
	case DeliveredOnline:
		return "delivered-online"
	case QueuedOffline:
		return "queued-offline"
	case SharedDelivered:
		return "shared-delivered"
	case SharedQueued:
		return "shared-queued"
	case NoSubscribers:
		return "no-subscribers"
	case QueueFull:
		return "queue-full"
	case SessionExpired:
		return "session-expired"
	case GroupUnavailable:
		return "group-unavailable"
	default:
		return "status-unknown"
	}
}

// Kind 区分投递目标类型。
type Kind int

const (
	// KindNormal 普通（独占）订阅。
	KindNormal Kind = iota
	// KindShared 共享订阅组。
	KindShared
)

func (k Kind) String() string {
	switch k {
	case KindNormal:
		return "normal"
	case KindShared:
		return "shared"
	default:
		return "kind-unknown"
	}
}

// Outcome 单个命中目标的投递结果。
type Outcome struct {
	// Kind 目标种类：普通会话或共享组。
	Kind Kind
	// Status 结果状态。
	Status Status
	// Target 普通订阅为 clientID；共享订阅为 "$share/<组名>"。
	Target string
	// Dropped 因容量满被丢弃时为 true（Status=QueueFull）。
	Dropped bool
}

// Result 一条消息对全部命中目标的投递结果集合。
type Result struct {
	Topic    string
	Outcomes []Outcome
}

// AnyDropped 是否存在任一目标被丢弃。
func (r *Result) AnyDropped() bool {
	for _, o := range r.Outcomes {
		if o.Dropped {
			return true
		}
	}
	return false
}

// HasStatus 结果中是否存在指定状态。
func (r *Result) HasStatus(s Status) bool {
	for _, o := range r.Outcomes {
		if o.Status == s {
			return true
		}
	}
	return false
}

// Summary 返回“各状态 -> 目标数”的汇总（日志用）。
func (r *Result) Summary() map[Status]int {
	out := make(map[Status]int)
	for _, o := range r.Outcomes {
		out[o.Status]++
	}
	return out
}
