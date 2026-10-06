# mqtt-pubsub-broker-go

一个本地可运行的 MQTT 发布/订阅消息代理与配套客户端，核心路由、会话与
QoS 状态机均为自实现（未使用现成代理库）。设备侧与后台侧通过本机回环
（127.0.0.1）即可完成发布订阅。

## 支持的协议版本

| 版本 | 协议级别 | 状态 |
|------|---------|------|
| MQTT 3.1.1 | 4 | 完整支持 |
| MQTT 5.0 | 5 | 接入与基础报文支持（属性区解析并跳过，不实现 v5 扩展特性） |
| MQTT 3.1 (MQIsdp) | 3 | 明确拒绝（CONNACK 返回码 1） |

同一代理端口可同时接入两个版本的客户端，版本按每条连接的 CONNECT
协商，连接之间互不影响。

## 主题与订阅语义

- 发布主题名不允许包含通配符与 NUL 字符，否则按协议错误断开。
- 订阅过滤器支持 `+`（单级）与 `#`（多级，必须独占一层且在末尾）。
- 匹配判定是纯函数（`internal/topic.Match`），结果确定：
  - `+` 匹配恰好一个层级（可为空层级）；
  - `#` 匹配自身及之后任意层级，`a/#` 可命中 `a`；
  - 以 `$` 开头的系统主题不被通配符开头的过滤器命中。
- 同一客户端多条过滤器命中同一主题时，只投递一次，取最高订阅 QoS。
- 非法过滤器（如 `a/#/b`）在 SUBACK 中以 0x80 单独拒绝，不影响同报文中的其他过滤器。

## 会话语义

- 会话按 ClientID 索引（`internal/session`）。
- `CleanStart=true`：丢弃旧会话（含订阅、在途消息、离线队列），
  断开时会话删除。
- `CleanStart=false`：持久会话。断开后订阅保留，QoS>0 的离线消息入队，
  重连后补发；CONNACK 的 SessionPresent 置位表示会话被恢复。
- 同一 ClientID 新连接接入时，旧连接被踢除（会话接管），不会串号。
- 并发连接状态完全隔离：每个连接独立的会话指针与发送队列。

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

测试覆盖（输出中可直接看到各业务用例与结果）：

- `internal/topic`：通配符匹配（`+`/`#`/系统主题/空层级）、过滤器与主题名校验
- `internal/packet`：CONNECT/PUBLISH 编解码往返、v5 属性区、
  各类畸形报文（未知类型、QoS3、保留位、截断报文等）的可区分拒绝
- `internal/session`：CleanStart 语义、QoS2 去重、在途状态机、离线队列
- `internal/broker`（集成测试，真实 TCP 回环）：
  通配符路由命中/不命中、QoS1 顺序投递、QoS2 恰好一次、
  QoS1 断线重传（DUP=1）、QoS2 重复投递去重、
  持久会话离线消息恢复、CleanStart 清理、保留消息下发与清除、
  非法订阅过滤器拒绝、并发连接隔离、同 ClientID 会话接管、MQTT 5.0 接入

### 手工冒烟

```sh
# 终端 1：启动代理
go run ./cmd/mqttd -addr 127.0.0.1:1883

# 终端 2：设备侧订阅（支持通配符）
go run ./cmd/mqttcli sub -id dev-1 -topic 'sensors/+/temp' -qos 1

# 终端 3：后台侧发布
go run ./cmd/mqttcli pub -id back-1 -topic sensors/kitchen/temp -payload '21.5' -qos 1

# 使用 MQTT 5.0 接入
go run ./cmd/mqttcli sub -id dev-2 -topic '#' -v5
```

## 目录结构

```
cmd/mqttd          代理服务入口
cmd/mqttcli        发布/订阅命令行客户端
internal/packet    MQTT 报文编解码（3.1.1 + 5.0），错误分类
internal/topic     主题校验与通配符匹配（纯函数）
internal/session   会话、订阅、QoS 在途状态机、离线队列
internal/broker    代理核心：连接接入、路由、保留消息、会话生命周期
internal/client    客户端库（供 mqttcli 与测试使用）
```
