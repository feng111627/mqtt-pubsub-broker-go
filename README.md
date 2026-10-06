# mqtt-pubsub-broker-go

一个在本机回环（loopback）上可运行的 MQTT 消息代理与配套客户端。核心的主题路由、
会话状态、QoS 交付状态机全部自行实现，**没有套用任何现成的完整 Broker**；仅使用 Go
标准库完成 TCP、字节编解码与并发调度。

适用于：设备侧（publisher）与后台侧（subscriber）在同一台机器上通过
`127.0.0.1:1883` 完成发布/订阅联调，并在连接抖动、重复投递、畸形报文下验证
状态可预期。

---

## 1. 目录结构

```
internal/mqerr     错误分类（协议错误 / 状态错误 / 连接错误）与原因码
internal/packet    MQTT 报文类型、变长整数/UTF-8 编解码、v5 属性、主题与通配符
internal/broker    自实现 Broker：连接握手、主题 Trie 路由、会话、retained、QoS 状态机
internal/client    自实现客户端：自动重连、会话感知重订阅、QoS0/1/2 收发状态机
cmd/broker         Broker 可执行程序
cmd/mqttcli        pub/sub 命令行客户端
```

构建：

```bash
go build ./...
```

---

## 2. 支持的协议版本

| 版本      | 协议名      | Level | 支持情况 |
|-----------|-------------|-------|----------|
| MQTT 3.1  | `MQIsdp`    | 3     | ✅ 完整接入 |
| MQTT 3.1.1| `MQTT`      | 4     | ✅ 完整接入（默认） |
| MQTT 5.0  | `MQTT`      | 5     | ✅ 常用语义接入（见下） |

MQTT 5.0 已支持的特性：

- CONNECT/CONNACK 的属性段编解码；服务端在 CONNACK 中回告
  Receive Maximum、Maximum Packet Size、Topic Alias Maximum=0。
- SUBACK/UNSUBACK/PUBACK/PUBREC/PUBREL/PUBCOMP/DISCONNECT 的原因码。
- 订阅选项：No Local、Retain As Published、Retain Handling（0/1/2）。
- 逐消息的拒绝原因（非法主题、共享订阅、畸形过滤器等）。
- 遗嘱属性（Will Properties）。

MQTT 5.0 中**明确不支持并给出可区分拒绝原因**（而不是静默丢弃）的特性：

| 特性 | 拒绝原因码 |
|------|-----------|
| 增强认证 AUTH（Authentication Method） | `0x8C` Bad Authentication Method |
| Topic Alias（服务端 Alias Max=0，客户端仍使用别名） | 解码期畸形报文 `0x81` |
| Shared Subscription `$share/...` | `0x9E` Shared Subscriptions Not Supported |
| 非 0 的 Session Expiry Interval（本实现仅提供“连接级持久会话”） | `0x82` Protocol Error |
| 非 0 的 Will Delay Interval | `0x82` Protocol Error |

> 设计取舍：为保持会话生命周期确定、可解释，本实现不引入 Broker 重启后的
> 磁盘持久化，也不实现“会话在 Broker 进程内停留 N 秒”的 5.0 过期计时器；
> 持久会话的存活范围是 Broker 进程生命周期内的断线重连窗口。

---

## 3. 主题与通配符

- 主题名（PUBLISH）与主题过滤器（SUBSCRIBE）分别做合法性校验。
- `+` 匹配且仅匹配**一个**主题层级（含空层级）。
- `#` 只能作为最后一个层级，匹配其后**零个或多个**层级：
  `a/#` 同时匹配 `a`、`a/`、`a/b/c`。
- 空层级有意义：`a//b` 与 `a/+/b` 匹配。
- 按字节、大小写敏感匹配。
- 非法形式（`a#`、`a+`、`#/b`、含 NUL/控制字符、空主题）在入口被拒绝。

路由结构是一棵确定性的 Trie（`internal/broker/store.go`）。发布时按层级
依次走“精确子节点 / `+` 子节点 / 祖先 `#` 终节点”，因此每一条命中都能解释
成“被哪个过滤器命中”。同一订阅者即使多个过滤器同时命中也只收到**一份**
消息，有效 QoS 取发布 QoS 与各命中过滤器 QoS 的最小值。

---

## 4. 会话语义

### 4.1 建立与恢复（Clean Session / Clean Start）

- `clean=true`：
  - 总是建立全新空会话；同名旧会话（连同旧订阅、队列）被清除。
  - 断开时会话立即销毁。CONNACK 的 Session Present 恒为 0。
- `clean=false`：
  - 首次连接：建立持久会话，CONNACK Session Present=0。
  - 断线后同名重连：恢复订阅与未完成的 QoS1/QoS2 消息，Session Present=1。
  - 客户端主动 DISCONNECT 后会话仍保留（标准 3.1.1 语义）。
- 客户端 ID 为空且 `clean=true` 时由服务端生成 ID；空 ID 配 `clean=false`
  会被拒绝（3.1.1 返回 `0x02 Identifier Rejected`）。

### 4.2 接管（Session Takeover）

同一 Client ID 再次连接时，旧连接被强制关闭：

- 旧连接**不会**发布遗嘱（Will）；
- 旧连接的退出不会摘除新连接已经绑定的会话；
- 新连接继承/替换会话，订阅不串号、无旧会话残留。

### 4.3 订阅恢复

- Broker 侧：持久会话的订阅在断线期间继续保留。
- 客户端侧（`internal/client`）自动重连后根据 CONNACK 的 Session Present：
  - Present=1：Broker 仍持有订阅，客户端只重发未确认的 PUBLISH/PUBREL；
  - Present=0（例如 Broker 重启导致会话丢失）：客户端确定性地重新发起
    SUBSCRIBE，恢复消息流。

---

## 5. 投递语义（QoS 与重复投递）

### 5.1 QoS0（至多一次）

- 即发即忘；离线持久会话不缓存 QoS0。

### 5.2 QoS1（至少一次，去重）

- Broker → 订阅者：发送后进入 inflight；PUBACK 前断线，重连后以 **DUP=1**
  重发同一条消息、复用同一 Packet Identifier，队列中始终只有一份副本。
- 发布客户端 → Broker：
  - 3.1.1 下重复且未完成的 Packet ID 会重发 PUBACK（幂等）；
  - 5.0 下对“仍在使用的 Packet ID”回 `0x91 Packet Identifier In Use`。

### 5.3 QoS2（恰好一次）

完整两段握手（以 Broker 为发送方）：

```
Broker  --PUBLISH(id)-->  Client
Broker  <--PUBREC(id)--   Client
Broker  --PUBREL(id)-->   Client
Broker  <--PUBCOMP(id)--  Client
```

- 在收到 PUBREL 之前，重复的 QoS2 PUBLISH 只重发 PUBREC，**不重复分发**
  应用消息；订阅端因此不会重复消费。
- 已过 PUBREC 的流程若连接中断，重连后 Broker 重发 PUBREL 收敛状态，
  消息不会丢失也不会二次投递给业务。
- 对未知 Packet ID 的 PUBREC：5.0 回 PUBCOMP `0x92 Packet Identifier Not
  Found`，且连接保持可用。

### 5.4 Retained 消息

- 带 Retain 的发布：更新（空 payload 则清除）该主题的保留值。
- 实时分发给当前订阅者的副本**不带** Retain 标志；只有“订阅匹配时下发的
  快照”才带 Retain。
- 订阅时：
  - Retain Handling=0：总是下发匹配的保留消息；
  - Retain Handling=1：仅当订阅是新建时下发（重复 SUBSCRIBE 不下发）；
  - Retain Handling=2：永不下发。
  - 3.1.1 固定等价于 Handling=0；5.0 下 Retain As Published=0 时快照
    以 Retain=0 投递。
- 每个匹配过滤器的保留消息按主题字典序下发。

### 5.5 遗嘱（Last Will & Testament）

- 仅在**非优雅断开**时发布：TCP 异常关闭、Keep Alive 超时（5.0 先发
  DISCONNECT `0x8D`）等。
- 优雅 DISCONNECT、会话被新连接接管时**不**发布遗嘱。

### 5.6 接收上限与队列

- Broker 对每个连接维护 Receive Maximum（默认 64），超出时不再从该会话
  队列取出新消息，待 ACK 释放额度后继续。
- 离线持久会话的 QoS1/QoS2 消息受 `QueueMax`（默认 1000）约束。

---

## 6. 错误分类：三类失败可区分

所有失败都返回 `*mqerr.Error`，包含 `Class` 与 `Code`：

| Class | 含义 | 典型场景 |
|-------|------|---------|
| `ClassProtocol` | 对端违反协议 / 报文畸形 / 不支持的特性 | 非法主题、QoS=3、版本不支持、共享订阅 |
| `ClassState` | 本地/会话当前状态不允许该操作 | 未连接就发布、SUBACK 超时、QoS 超范围 |
| `ClassConnection` | 传输失败或中断 | dial 失败、读 EOF、写失败 |

拒绝原因码同时兼容：

- 3.1.1 CONNACK：`0x01` 版本不支持、`0x02` ID 拒绝、`0x03` 不可用、
  `0x04` 账号密码错误、`0x05` 未授权。
- 5.0 reason code：`0x80/0x81/0x82/0x84/0x86/0x87/0x8E/0x8F/0x90/0x91/0x92/0x94/0x9E` 等。

可用 `mqerr.IsClass(err, ...)` 与 `mqerr.Of(err, code)` 判定，无需字符串匹配。

---

## 7. 本地验证方法

### 7.1 启动 Broker

```bash
go run ./cmd/broker -addr 127.0.0.1:1883
# mqtt-broker@127.0.0.1:1883 listening on 127.0.0.1:1883 (MQTT 3.1/3.1.1/5.0)
```

### 7.2 两个终端做 pub/sub

终端 1（订阅，支持多层级通配符，3.1.1 持久会话）：

```bash
go run ./cmd/mqttcli sub -topic 'sport/+' -qos 1 -id demo-sub
```

终端 2（发布）：

```bash
go run ./cmd/mqttcli pub -topic sport/tennis -qos 1 -m 'hello'
go run ./cmd/mqttcli pub -topic sport/tennis/player1 -qos 0 -m 'no-hit'
```

终端 1 只会收到 `hello`（`sport/tennis/player1` 多了一层，不命中 `sport/+`）。

MQTT 5.0：

```bash
go run ./cmd/mqttcli sub -topic '#' -qos 2 -version 5
go run ./cmd/mqttcli pub -topic a/b -qos 2 -m 'exact-once' -version 5
```

Retained：

```bash
go run ./cmd/mqttcli pub -topic status/door -qos 1 -retain -m open
go run ./cmd/mqttcli sub -topic 'status/+' -qos 1   # 立即收到 open（[retained]）
```

### 7.3 运行测试（带竞态检测）

```bash
go test -race ./...
```

测试输出使用 `-v` 时会逐条列出“业务场景 → 用例 → 结果”，例如：

```bash
go test -race -v ./internal/broker ./internal/client ./internal/packet
```

也可以只跑某个业务：

```bash
go test -v -run 'Wildcard|QoS|Session|Retained|Will|Takeover|Malformed|Reject|Invalid|Shared|TopicAlias' ./...
```

### 7.4 测试覆盖的关键业务

- 主题/通配符：精确、`+`、`#`、空层级、大小写、非法过滤器/主题。
- QoS：QoS1 离线缓存与重连 DUP=1 重传、QoS2 完整握手、重复 QoS2 PUBLISH
  不重复消费、未知 Packet ID 的区分原因码。
- 会话：Session Present 语义、clean 重连清理、接管不发遗嘱、客户端自动
  重连 + 会话丢失后确定性重订阅、多客户端状态隔离。
- Retained：订阅时下发、空 payload 清除、Retain Handling/RAP。
- 遗嘱：异常断开下发、优雅断开/接管不下发。
- 畸形与拒绝：版本不支持、首包非 CONNECT、QoS3、DUP+QoS0、Packet ID=0、
  SUBSCRIBE 保留位、共享订阅、非法 filter、Topic Alias、报文超长、截断。
- 并发：8 客户端并发 pub/sub，`-race` 下无数据竞争。

---

## 8. 主要局限（诚实声明）

- 仅明文 TCP（1883），无 TLS(8883)、WebSocket。
- 会话为**进程内**持久，Broker 进程退出即清空，不落盘。
- 未实现 5.0 的会话过期计时器、Topic Alias、共享订阅、流控增强认证等
  高级特性（均会被明确拒绝而非静默忽略）。
- 内置一个可选的用户名/密码校验钩子（`Config.Auth`），未实现 ACL 与
  TLS 双向认证。
