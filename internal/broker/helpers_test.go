package broker_test

import (
	"net"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
)

// startBrokerWith 以指定配置在随机端口启动代理。
func startBrokerWith(t *testing.T, cfg broker.Config) *broker.Broker {
	t.Helper()
	b := broker.NewWithConfig(cfg)
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

// storeHooks 绑定 broker 与地址的小包装。
type storeHooks struct {
	b    *broker.Broker
	addr string
}

// startBrokerStoreHooks 启动一个会话 TTL 与扫描间隔都很短的代理，便于测试过期。
func startBrokerStoreHooks(t *testing.T) storeHooks {
	t.Helper()
	cfg := broker.Config{
		Session:       limitsForExpiry(),
		SweepInterval: 50 * time.Millisecond,
	}
	b := startBrokerWith(t, cfg)
	return storeHooks{b: b, addr: b.Addr()}
}

func limitsForExpiry() session.Limits {
	return session.Limits{MaxQueue: 100, MessageTTL: time.Hour, SessionTTL: 100 * time.Millisecond}
}

// publishV5Reason 用一条独立的原始 v5 连接发布 QoS1 并读取 PUBACK 原因码。
// 不经过 client.Client，避免其后台 readLoop 抢先消费确认报文。
func publishV5Reason(t *testing.T, addr, topicName string, qos byte) byte {
	t.Helper()
	conn, err := net.Dial("tcp", addr)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()

	// 手工 CONNECT(v5)：MQTT + level5 + cleanStart flags=0x02 + keepalive + 空属性 + cid
	body := []byte{0x00, 0x04, 'M', 'Q', 'T', 'T', 0x05, 0x02, 0x00, 0x3C, 0x00}
	cid := []byte("v5raw")
	body = append(body, byte(len(cid)>>8), byte(len(cid)))
	body = append(body, cid...)
	conn.Write(append([]byte{0x10, byte(len(body))}, body...))
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	if _, _, _, err := packet.ReadRaw(conn); err != nil {
		t.Fatalf("read connack: %v", err)
	}

	id := uint16(1)
	p := &packet.Packet{
		Header:   packet.FixedHeader{Type: packet.TypePUBLISH, Flags: (qos & 3) << 1},
		Topic:    topicName,
		QoS:      qos,
		PacketID: id,
		Payload:  []byte("x"),
	}
	raw, err := p.Encode(packet.Version5)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := conn.Write(raw); err != nil {
		t.Fatal(err)
	}
	conn.SetReadDeadline(time.Now().Add(2 * time.Second))
	ptype, _, b2, err := packet.ReadRaw(conn)
	if err != nil {
		t.Fatalf("read puback: %v", err)
	}
	ack := &packet.Packet{Header: packet.FixedHeader{Type: ptype}}
	if err := ack.Decode(b2, packet.Version5); err != nil {
		t.Fatalf("decode puback: %v", err)
	}
	if ack.Header.Type != packet.TypePUBACK {
		t.Fatalf("expected PUBACK got %d", ack.Header.Type)
	}
	return ack.ReasonCode
}
