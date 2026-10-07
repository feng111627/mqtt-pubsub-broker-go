# mqtt-pubsub-broker-go

一个本地可运行的 MQTT 发布/订阅消息代理与配套客户端，核心路由、会话与
QoS 状态机均为自实现（未使用现成代理库）。设备侧与后台侧通过本机回环
（127.0.0.1）即可完成发布订阅。

## 支持的协议版本

| 版本 | 协议级别 | 状态 |
|------|---------|------|
| MQTT 3.1.1 | 4 | 完整支持；共享订阅以 `$share/` 过滤器形式作为本代理扩展可用 |
| MQTT 5.0 | 5 | 接入与基础报文支持；PUBACK/PUBREC 携带投递结果原因码；共享订阅可用 |
| MQTT 3.1 (MQIsdp) | 3 | 明确拒绝（CONNACK 返回码 1） |

同一代理端口可同时接入两个版本的客户端，版本按每条连接的 CONNECT
协商，连接之间互不影响。

> 说明：MQTT 3.1.1 的 PUBACK/PUBREC 只有报文标识符、没有原因码字段，
> 因此 3.1.1 无法在线上协议层区分投递结果；需要区分时请使用进程内发布
> 接口（`broker.Broker.Publish` 返回的 `Report`），或使用 MQTT 5.0 连接
> 读取 PUBACK/PUBREC 原因码。

## 主题与订阅语义

- 发布主题名不允许包含通配符与 NUL 字符，否则按协议错误断开。
- 订阅过滤器支持 `+`（单级）与 `#`（多级，必须独占一层且在末尾）。
- 匹配判定是纯函数（`internal/topic.Match`），结果确定：
  - `+` 匹配恰好一个层级（可为空层级）；
  - `#` 匹配自身及之后任意层级，`a/#` 可命中 `a`；
  - 以 `$` 开头的系统主题不被通配符开头的过滤器命中。
- 同一客户端多条过滤器命中同一主题时，只投递一次，取最高订阅 QoS。
- 非法过滤器（如 `a/#/b`）在 SUBACK 中以 0x80 单独拒绝，不影响同报文中的其他过滤器。

## 共享订阅（订阅组）

多个客户端以同一“组”订阅时，一条消息在组内**只投递给其中一个成员**，
而不是组内全员各收一份。语法为：

```
$share/<组名>/<主题过滤器>
```

例如三个客户端都订阅 `$share/workers/jobs/+`，发布到 `jobs/1` 的消息
只会到达其中一个客户端。

- **负载均衡**：成员选择在“当前在线成员”集合上做确定性轮询（按 ClientID
  排序后轮转），成员陆续接入/退出会自动重新平衡，新消息只分给仍在线的成员。
- **掉线接管（不丢、不重）**：成员收到 QoS>0 消息但尚未确认就掉线时，
  该消息以 `DUP=1` 转交给组内其他在线成员处理；同 ClientID 新连接接管旧
  连接时不算掉线，消息仍由该客户端继续持有。摘出按组内投递令牌做了幂等，
  连接读写协程退出与主动关闭竞态不会造成重复转交。
- **无在线成员兜底**：组内暂时没有任何在线成员时，消息进入**组级兜底队列**
  （按发布顺序、有容量上限与过期规则），等任意成员重新上线后补发；
  兜底队列满时新消息被明确拒绝（见“投递结果区分”），不会凭空消失也不会静默丢弃。
- 组成员身份持久化在持久会话中；CleanStart 客户端断开即退组。
- 共享订阅不触发保留消息下发（符合 MQTT 共享订阅约定）。
- 该语法同时对 MQTT 3.1.1 与 5.0 连接开放（3.1.1 协议本身无此原生特性，
  属本代理扩展）。

## 会话语义

- 会话按 ClientID 索引（`internal/session`）。
- `CleanStart=true`：丢弃旧会话（含订阅、在途消息、离线队列），
  断开时会话删除。
- `CleanStart=false`：持久会话。断开后订阅保留，命中它订阅的消息（含 QoS0
  与 QoS>0）在离线期间按发布顺序堆积，重连后按各自投递语义补发；
  CONNACK 的 SessionPresent 置位表示会话被恢复。
- 同一 ClientID 新连接接入时，旧连接被踢除（会话接管），不会串号。
- 并发连接状态完全隔离：每个连接独立的会话指针与发送队列。

### 离线堆积的容量与过期策略

每个持久会话的离线队列相互独立，**一个会话积压再多也不会挤掉别的会话**，
不同会话的消息也不会串到一起。

| 策略 | 默认值 | 满/过期行为 |
|------|--------|-------------|
| 单会话队列容量 `MaxQueue` | 1000 条 | 满了采用“拒新”：拒绝最新消息并返回 `QueueFull`，**不淘汰队首旧消息**，保证先发布的不被后发布的挤掉 |
| 离线消息 TTL `MessageTTL` | 24h | 补发前惰性清理队首所有过期消息；过期消息不下发，计入诊断计数 |
| 持久会话离线 TTL `SessionTTL` | 168h（7 天） | 离线超过 TTL 的会话过期：订阅与队列立即释放（不占堆积内存），进入“墓碑”保留窗；窗口内仍可把命中识别为“会话已过期” |
| 墓碑保留窗 | 1 × `SessionTTL` | 期满后墓碑清除，此后该 ClientID 等同从未订阅；客户端在此期间重连则撤销墓碑 |
| 后台扫描间隔 `SweepInterval` | 30s | 周期性把过期会话转墓碑并清除期满墓碑；消息过期则在入队/补发时惰性清理 |

共享组无在线成员时的兜底队列使用同一套“有界 + TTL + 满了拒新”策略，
其容量与 TTL 单独配置（`Group.MaxPending` 默认 1000，TTL 默认 24h）。

## QoS 投递语义

| 等级 | 语义 | 实现要点 |
|------|------|---------|
| QoS 0 | 至多一次 | 直接转发，无确认 |
| QoS 1 | 至少一次 | PUBACK 确认；未确认的在途消息在重连后以 DUP=1 重发，PacketID 不变 |
| QoS 2 | 恰好一次 | PUBREC/PUBREL/PUBCOMP 四步握手；入方向按 PacketID 去重，重复投递只路由一次 |

- 投递 QoS = min(发布 QoS, 订阅 QoS)。
- 报文标识符分配跳过在途中的 ID，且不为 0。
- 中途断开时未确认消息保留在会话中，不会丢失也不会被重复消费
  （订阅方收到 DUP 重发时可用 PacketID 识别）。

## 保留消息

- PUBLISH 带 RETAIN 时，代理保存该主题最新一条消息。
- 新订阅命中保留消息时立即下发（Retain 标志置位）。
- 以零字节载荷发布的 RETAIN 消息清除对应主题的保留消息。

## 投递结果区分

一次发布对每个命中目标都会产生一个可区分的结果，不再一律当成同一种失败，
也不会静默吞掉。进程内接口 `(*broker.Broker).Publish(topic, qos, retain, payload)`
返回 `*broker.Report`，其中每个 `Target` 的 `Status` 取值：

| 状态 | 含义 |
|------|------|
| `StatusDelivered` | 已投递给在线订阅者（普通订阅在线，或共享组某在线成员） |
| `StatusQueuedOffline` | 订阅者离线，消息已按序进入其持久会话离线队列 |
| `StatusQueueFull` | 订阅者离线且离线队列已满，消息被拒（拒新保旧） |
| `StatusSessionExpired` | 命中的持久会话已过期，处于墓碑保留窗内 |
| `StatusGroupBuffered` | 共享组暂无在线成员，消息已进入组兜底队列，成员上线即补发 |
| `StatusGroupNoMember` | 共享组暂无可用成员，且兜底队列已满，消息被拒 |

`Report` 还提供聚合判定：`NoSubscriber()`（无任何命中）、`Accepted()`
（至少一个目标在线投递/离线排队/组缓冲成功）、`Rejected()`（存在被拒目标）。

MQTT 5.0 连接通过 PUBACK（QoS1）/PUBREC（QoS2）原因码获得结果：

| v5 原因码 | 触发情形 |
|-----------|----------|
| `0x00` Success | 全部命中目标投递/排队/缓冲成功 |
| `0x10` No matching subscribers | 没有任何订阅者命中 |
| `0x97` Quota exceeded | 命中目标队列满/组兜底满，或部分目标被拒 |
| `0x8E` Session taken over | 命中目标均为已过期会话 |

客户端库以 `*client.DeliveryError{ReasonCode}` 返回非成功原因码，可用
`errors.As` 取出并区分。网络层只能给出汇总原因码；需要**逐目标明细**
（区分队列满 / 会话过期 / 组缓冲等）请使用进程内 `Report`。

## 错误分类

操作失败可区分三类错误（`internal/packet`）：

- **协议错误** `ErrProtocol`：畸形报文、非法主题、保留位错误、
  不支持的协议级别等，携带具体原因（`*ProtocolError`）。
- **状态错误** `ErrState`：与当前会话状态冲突（如未 CONNECT 先发其他报文、
  同一连接重复 CONNECT）。
- **连接错误** `ErrConn`：底层网络读写失败。

畸形报文不会被静默丢弃：代理记录原因并断开该连接，不影响其他连接。

## 本地验证

### 构建与测试

```sh
go build ./...
go test ./... -v
```

测试覆盖（`go test ./... -v` 输出中可直接看到“业务=… 用例=… 结果=…”日志）：

- `internal/topic`：通配符匹配（`+`/`#`/系统主题/空层级）、过滤器与主题名校验、
  共享过滤器 `$share/<组>/<过滤器>` 的解析与校验
- `internal/packet`：CONNECT/PUBLISH 编解码往返、v5 属性区与 PUBACK/PUBREC
  原因码、各类畸形报文（未知类型、QoS3、保留位、截断报文等）的可区分拒绝
- `internal/session`：CleanStart 语义、QoS2 去重、在途状态机、离线队列、
  队列容量上限（拒新保旧）、消息 TTL 过期清理、会话 TTL 与墓碑生命周期、
  会话间不互相挤占、共享组在途按令牌幂等接管
- `internal/broker`（集成测试，真实 TCP 回环）：
  - 既有能力（无回归）：通配符路由、QoS1 顺序、QoS2 恰好一次、
    QoS1 断线重传、QoS2 去重、持久会话离线恢复、CleanStart 清理、
    保留消息、非法过滤器拒绝、并发隔离、同 ID 接管、MQTT 5.0 接入
  - 本轮新增：
    - **组内分发与重平衡**：同组单选投递、3 成员轮询均衡、成员退出后只投在线成员
    - **掉线在途接管**：不 ACK 崩溃后消息以 DUP=1 转交他人，ACK 后无重复
    - **无成员兜底**：空组期间堆积、成员重连后补发
    - **离线堆积与补发**：QoS0/QoS1 混合按发布顺序补发
    - **容量与过期**：会话队列满返回 `QueueFull`、消息 TTL 过期丢弃、
      会话 TTL 过期标记 `SessionExpired`、组兜底满返回 `GroupNoMember`
    - **结果区分**：无订阅者 `NoSubscriber`、v5 PUBACK 0x10/0x00 原因码
    - **并发进出组**：4 发布者 × 60 条 + 4 成员反复进出不卡死/不 panic、
      2 稳定成员下 100 条恰好一次（`-race` 下通过）

### 手工冒烟

```sh
# 终端 1：启动代理（可带容量/过期参数；不带则用默认值）
go run ./cmd/mqttd -addr 127.0.0.1:1883 \
  -max-queue 1000 -max-group-pending 1000 \
  -message-ttl 24h -session-ttl 168h -sweep-interval 30s

# 终端 2、3：同一共享组的两个工人（注意 sub 默认 -clean=false 持久会话）
go run ./cmd/mqttcli sub -id w1 -group workers -topic 'jobs/+' -qos 1
go run ./cmd/mqttcli sub -id w2 -group workers -topic 'jobs/+' -qos 1

# 终端 4：发布多条，观察每条只被 w1/w2 中的一个处理（轮询分摊）
go run ./cmd/mqttcli pub -id boss -topic jobs/1 -payload J1 -qos 1
go run ./cmd/mqttcli pub -id boss -topic jobs/2 -payload J2 -qos 1

# 普通订阅（支持通配符）
go run ./cmd/mqttcli sub -id dev-1 -topic 'sensors/+/temp' -qos 1
go run ./cmd/mqttcli pub -id back-1 -topic sensors/kitchen/temp -payload '21.5' -qos 1

# 使用 MQTT 5.0 接入
go run ./cmd/mqttcli sub -id dev-2 -topic '#' -v5
```

代理启动参数（均为可选，0 值取默认）：`-max-queue`（单会话离线队列容量，
默认 1000）、`-max-group-pending`（单组兜底队列容量，默认 1000）、
`-message-ttl`（离线/组消息 TTL，默认 24h）、`-session-ttl`（持久会话离线
TTL，默认 168h）、`-sweep-interval`（过期扫描间隔，默认 30s）。

## 目录结构

```
cmd/mqttd          代理服务入口
cmd/mqttcli        发布/订阅命令行客户端
internal/packet    MQTT 报文编解码（3.1.1 + 5.0），错误分类与 v5 原因码
internal/topic     主题校验、通配符匹配（纯函数）、共享过滤器解析
internal/session   会话、订阅、QoS 在途状态机、有界离线队列、TTL 与墓碑
internal/broker    代理核心：连接接入、路由、共享组（group.go）、
                   投递结果报告、保留消息、会话生命周期
internal/client    客户端库（供 mqttcli 与测试使用）
```
