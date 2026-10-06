package broker_test

import (
	"bytes"
	"net"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

// startBroker 在随机端口启动代理。
func startBroker(t *testing.T) *broker.Broker {
	t.Helper()
	b := broker.New()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	ln.Close()
	go b.ListenAndServe(addr)
	for i := 0; i < 100; i++ {
		if b.Addr() != "" {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Cleanup(func() { b.Close() })
	return b
}

func mustConnect(t *testing.T, addr, id string, clean bool) *client.Client {
	t.Helper()
	c, err := client.Connect(addr, client.Options{ClientID: id, CleanStart: clean})
	if err != nil {
		t.Fatalf("connect %s: %v", id, err)
	}
	return c
}

func recvOne(t *testing.T, c *client.Client, timeout time.Duration) client.Message {
	t.Helper()
	select {
	case m := <-c.Messages():
		return m
	case <-time.After(timeout):
		t.Fatal("timeout waiting for message")
		return client.Message{}
	}
}

func expectNone(t *testing.T, c *client.Client, d time.Duration) {
	t.Helper()
	select {
	case m := <-c.Messages():
		t.Fatalf("unexpected message: %+v", m)
	case <-time.After(d):
	}
}

func TestPubSubWildcardRouting(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	sub := mustConnect(t, addr, "sub", true)
	defer sub.Close()
	if _, err := sub.Subscribe("sensors/+/temp", 1); err != nil {
		t.Fatal(err)
	}
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()

	// 命中
	if err := pub.Publish("sensors/kitchen/temp", 1, false, []byte("21")); err != nil {
		t.Fatal(err)
	}
	m := recvOne(t, sub, 2*time.Second)
	if m.Topic != "sensors/kitchen/temp" || string(m.Payload) != "21" {
		t.Fatalf("bad message: %+v", m)
	}
	// 不命中：层级数不同
	if err := pub.Publish("sensors/temp", 1, false, []byte("x")); err != nil {
		t.Fatal(err)
	}
	expectNone(t, sub, 300*time.Millisecond)
}

func TestMultiLevelWildcardAndHash(t *testing.T) {
	b := startBroker(t)
	sub := mustConnect(t, b.Addr(), "sub", true)
	defer sub.Close()
	sub.Subscribe("a/#", 0)

	pub := mustConnect(t, b.Addr(), "pub", true)
	defer pub.Close()
	pub.Publish("a", 0, false, []byte("1"))
	pub.Publish("a/b/c", 0, false, []byte("2"))
	pub.Publish("b/c", 0, false, []byte("3"))

	m1 := recvOne(t, sub, 2*time.Second)
	m2 := recvOne(t, sub, 2*time.Second)
	if m1.Topic != "a" || m2.Topic != "a/b/c" {
		t.Fatalf("got %q %q", m1.Topic, m2.Topic)
	}
	expectNone(t, sub, 300*time.Millisecond) // b/c 不命中
}

func TestQoS1DeliveryOnce(t *testing.T) {
	b := startBroker(t)
	sub := mustConnect(t, b.Addr(), "sub", true)
	defer sub.Close()
	sub.Subscribe("q", 1)
	pub := mustConnect(t, b.Addr(), "pub", true)
	defer pub.Close()

	for i := 0; i < 5; i++ {
		if err := pub.Publish("q", 1, false, []byte{byte(i)}); err != nil {
			t.Fatal(err)
		}
	}
	for i := 0; i < 5; i++ {
		m := recvOne(t, sub, 2*time.Second)
		if m.Payload[0] != byte(i) {
			t.Fatalf("message %d out of order: %v", i, m.Payload)
		}
	}
	expectNone(t, sub, 300*time.Millisecond) // 无重复投递
}

func TestQoS2ExactlyOnce(t *testing.T) {
	b := startBroker(t)
	sub := mustConnect(t, b.Addr(), "sub", true)
	defer sub.Close()
	sub.Subscribe("q2", 2)
	pub := mustConnect(t, b.Addr(), "pub", true)
	defer pub.Close()

	if err := pub.Publish("q2", 2, false, []byte("exactly-once")); err != nil {
		t.Fatal(err)
	}
	m := recvOne(t, sub, 2*time.Second)
	if string(m.Payload) != "exactly-once" {
		t.Fatalf("bad payload: %q", m.Payload)
	}
	expectNone(t, sub, 500*time.Millisecond)
}

func TestSessionResumeOfflineQueue(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	// 持久会话订阅后断开
	sub := mustConnect(t, addr, "persist", false)
	sub.Subscribe("offline/#", 1)
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	// 离线期间发布 QoS1 消息
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	pub.Publish("offline/a", 1, false, []byte("cached-1"))
	pub.Publish("offline/b", 1, false, []byte("cached-2"))
	time.Sleep(100 * time.Millisecond)

	// 重连（cleanStart=false）：应收到离线缓存
	sub2 := mustConnect(t, addr, "persist", false)
	defer sub2.Close()
	got := map[string]bool{}
	for i := 0; i < 2; i++ {
		m := recvOne(t, sub2, 2*time.Second)
		got[string(m.Payload)] = true
	}
	if !got["cached-1"] || !got["cached-2"] {
		t.Fatalf("offline messages not delivered: %v", got)
	}
	expectNone(t, sub2, 300*time.Millisecond) // 不重复
}

func TestCleanStartClearsSession(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	sub := mustConnect(t, addr, "ephemeral", false)
	sub.Subscribe("clean/#", 1)
	sub.Close()
	time.Sleep(100 * time.Millisecond)

	// CleanStart=true 重连：旧订阅被清理，不应收到消息
	sub2 := mustConnect(t, addr, "ephemeral", true)
	defer sub2.Close()
	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	pub.Publish("clean/x", 1, false, []byte("should-not-arrive"))
	expectNone(t, sub2, 400*time.Millisecond)
}

func TestRetainedMessage(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	pub.Publish("retain/topic", 1, true, []byte("retained-value"))
	time.Sleep(100 * time.Millisecond)

	// 后订阅者收到保留消息
	sub := mustConnect(t, addr, "sub", true)
	defer sub.Close()
	sub.Subscribe("retain/#", 1)
	m := recvOne(t, sub, 2*time.Second)
	if string(m.Payload) != "retained-value" || !m.Retain {
		t.Fatalf("bad retained message: %+v", m)
	}

	// 空载荷清除保留消息
	pub.Publish("retain/topic", 1, true, []byte{})
	time.Sleep(100 * time.Millisecond)
	sub2 := mustConnect(t, addr, "sub2", true)
	defer sub2.Close()
	sub2.Subscribe("retain/#", 1)
	expectNone(t, sub2, 400*time.Millisecond)
}

func TestMalformedPacketRejected(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	// 发送畸形 CONNECT（错误协议名），连接应被关闭
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	body := []byte{0x00, 0x04, 'M', 'Q', 'X', 'X', 0x04, 0x02, 0x00, 0x3C, 0x00, 0x01, 'x'}
	raw := append([]byte{0x10, byte(len(body))}, body...)
	conn.Write(raw)
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	// 期望收到 CONNACK 拒绝或连接关闭
	buf := make([]byte, 16)
	_, err = conn.Read(buf)
	if err == nil && buf[0]>>4 == packet.TypeCONNACK && buf[3] == packet.ConnAccepted {
		t.Fatal("malformed CONNECT was accepted")
	}
}

func TestInvalidSubscribeFilterRejected(t *testing.T) {
	b := startBroker(t)
	c := mustConnect(t, b.Addr(), "sub", true)
	defer c.Close()
	// 非法过滤器：# 不在末尾 -> SUBACK 返回 0x80
	if _, err := c.Subscribe("a/#/b", 1); err == nil {
		t.Fatal("expected subscribe rejection")
	}
	// 合法过滤器仍可用
	if _, err := c.Subscribe("a/#", 1); err != nil {
		t.Fatalf("valid filter rejected: %v", err)
	}
}

func TestConcurrentConnectionsIsolation(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	// 两个客户端订阅不同主题，互不污染
	c1 := mustConnect(t, addr, "c1", true)
	defer c1.Close()
	c2 := mustConnect(t, addr, "c2", true)
	defer c2.Close()
	c1.Subscribe("topic/1", 1)
	c2.Subscribe("topic/2", 1)

	pub := mustConnect(t, addr, "pub", true)
	defer pub.Close()
	pub.Publish("topic/1", 1, false, []byte("for-c1"))

	m := recvOne(t, c1, 2*time.Second)
	if string(m.Payload) != "for-c1" {
		t.Fatalf("c1 got %q", m.Payload)
	}
	expectNone(t, c2, 300*time.Millisecond) // c2 不应收到
}

func TestSameClientIDTakeover(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	old := mustConnect(t, addr, "dup-id", true)
	old.Subscribe("takeover", 1)
	time.Sleep(50 * time.Millisecond)

	// 同 ID 新连接接管，旧连接被断开
	newC := mustConnect(t, addr, "dup-id", true)
	defer newC.Close()
	time.Sleep(100 * time.Millisecond)

	// 旧连接应已关闭（读返回错误）
	old.Conn().SetReadDeadline(time.Now().Add(time.Second))
	buf := make([]byte, 1)
	if _, err := old.Conn().Read(buf); err == nil {
		t.Fatal("old connection should be closed after takeover")
	}
}

func TestProtocolVersion5(t *testing.T) {
	b := startBroker(t)
	addr := b.Addr()

	sub, err := client.Connect(addr, client.Options{ClientID: "v5sub", CleanStart: true, Version: packet.Version5})
	if err != nil {
		t.Fatalf("v5 connect: %v", err)
	}
	defer sub.Close()
	if _, err := sub.Subscribe("v5/#", 1); err != nil {
		t.Fatalf("v5 subscribe: %v", err)
	}
	pub, err := client.Connect(addr, client.Options{ClientID: "v5pub", CleanStart: true, Version: packet.Version5})
	if err != nil {
		t.Fatalf("v5 connect: %v", err)
	}
	defer pub.Close()
	if err := pub.Publish("v5/test", 1, false, []byte("v5-msg")); err != nil {
		t.Fatalf("v5 publish: %v", err)
	}
	m := recvOne(t, sub, 2*time.Second)
	if string(m.Payload) != "v5-msg" {
		t.Fatalf("v5 got %q", m.Payload)
	}
}

// 确保未使用的导入被引用
var _ = bytes.Equal
