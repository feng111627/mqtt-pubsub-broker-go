package client

import (
	"context"
	"fmt"
	"net"
	"sync"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

func startBroker(t *testing.T) (*broker.Broker, string) {
	t.Helper()
	b := broker.New(broker.Config{ListenAddr: "127.0.0.1:0", QueueMax: 100, ReceiveMaximum: 16})
	if err := b.Start(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { b.Close() })
	return b, b.Addr().String()
}

func recvCh() (chan *Message, Handler) {
	ch := make(chan *Message, 32)
	return ch, func(m *Message) { ch <- m }
}

func TestClientPubSubQoS0(t *testing.T) {
	_, addr := startBroker(t)
	ctx := context.Background()

	got, h := recvCh()
	sub := New(Config{Addr: addr, ClientID: "c-sub-0", Clean: true, ProtocolLevel: packet.V311})
	if err := sub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Disconnect()
	if err := sub.Subscribe(ctx, 0, h, "cli/+/x"); err != nil {
		t.Fatal(err)
	}

	pub := New(Config{Addr: addr, ClientID: "c-pub-0", Clean: true, ProtocolLevel: packet.V311})
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Disconnect()
	if err := pub.Publish(ctx, "cli/a/x", 0, false, []byte("hi")); err != nil {
		t.Fatal(err)
	}

	select {
	case m := <-got:
		if m.Topic != "cli/a/x" || string(m.Payload) != "hi" {
			t.Fatalf("message mismatch: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no message received")
	}
}

func TestClientPublishQoS1Ack(t *testing.T) {
	_, addr := startBroker(t)
	ctx := context.Background()
	pub := New(Config{Addr: addr, ClientID: "p1", Clean: true, ProtocolLevel: packet.V311})
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Disconnect()
	done := make(chan error, 1)
	go func() { done <- pub.Publish(ctx, "q1/x", 1, false, []byte("acked")) }()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("qos1 publish: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("PUBACK not received")
	}
}

func TestClientQoS2Roundtrip(t *testing.T) {
	_, addr := startBroker(t)
	ctx := context.Background()
	got, h := recvCh()
	sub := New(Config{Addr: addr, ClientID: "q2s", Clean: true, ProtocolLevel: packet.V311})
	if err := sub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Disconnect()
	if err := sub.Subscribe(ctx, 2, h, "q2/#"); err != nil {
		t.Fatal(err)
	}
	pub := New(Config{Addr: addr, ClientID: "q2p", Clean: true, ProtocolLevel: packet.V311})
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Disconnect()
	if err := pub.Publish(ctx, "q2/a", 2, false, []byte("exact")); err != nil {
		t.Fatalf("qos2 publish: %v", err)
	}
	select {
	case m := <-got:
		if string(m.Payload) != "exact" || m.QoS != 2 {
			t.Fatalf("qos2 message: %+v", m)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("no qos2 message")
	}
}

func TestClientReconnectResubscribesWhenSessionAbsent(t *testing.T) {
	b, addr := startBroker(t)
	ctx := context.Background()
	got, h := recvCh()
	sub := New(Config{
		Addr: addr, ClientID: "rc-sub", Clean: true,
		ProtocolLevel: packet.V311, AutoReconnect: true, MaxReconnectDelayMS: 500,
		MessageHandler: h,
	})
	if err := sub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer sub.Disconnect()
	if err := sub.Subscribe(ctx, 1, nil, "rc/+"); err != nil {
		t.Fatal(err)
	}

	// Force the broker to forget everything, then wait for auto-reconnect.
	b.Close()
	// restart a brand new broker on the same address
	b2 := broker.New(broker.Config{ListenAddr: addr, QueueMax: 100})
	if err := b2.Start(); err != nil {
		t.Fatal(err)
	}
	defer b2.Close()

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if sub.IsConnected() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	if !sub.IsConnected() {
		t.Fatal("client did not reconnect")
	}
	// allow resubscribe to land
	time.Sleep(300 * time.Millisecond)

	pub := New(Config{Addr: addr, ClientID: "rc-pub", Clean: true, ProtocolLevel: packet.V311})
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer pub.Disconnect()
	if err := pub.Publish(ctx, "rc/x", 0, false, []byte("after")); err != nil {
		t.Fatal(err)
	}
	select {
	case m := <-got:
		if string(m.Payload) != "after" {
			t.Fatalf("payload %s", m.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("resubscription did not restore message flow")
	}
}

func TestClientPersistentSessionReceivesQueuedAfterReconnect(t *testing.T) {
	_, addr := startBroker(t)
	ctx := context.Background()
	got, h := recvCh()
	sub := New(Config{
		Addr: addr, ClientID: "persist-1", Clean: false,
		ProtocolLevel: packet.V311, AutoReconnect: true, MaxReconnectDelayMS: 500,
		MessageHandler: h,
	})
	if err := sub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := sub.Subscribe(ctx, 1, nil, "persist/q"); err != nil {
		t.Fatal(err)
	}
	// Hard close the transport without DISCONNECT: broker keeps the session.
	sub.mu.Lock()
	sub.conn.Close()
	sub.conn = nil
	sub.mu.Unlock()

	// publish while subscriber is offline
	pub := New(Config{Addr: addr, ClientID: "persist-pub", Clean: true})
	if err := pub.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	if err := pub.Publish(ctx, "persist/q", 1, false, []byte("queued")); err != nil {
		t.Fatal(err)
	}

	deadline := time.Now().Add(6 * time.Second)
	for time.Now().Before(deadline) {
		if sub.IsConnected() {
			break
		}
		time.Sleep(100 * time.Millisecond)
	}
	select {
	case m := <-got:
		if string(m.Payload) != "queued" {
			t.Fatalf("payload %s", m.Payload)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("queued message not delivered after reconnect")
	}
	_ = sub.Disconnect()
}

func TestClientInvalidTopicErrorClass(t *testing.T) {
	_, addr := startBroker(t)
	pub := New(Config{Addr: addr, ClientID: "bad", Clean: true, ProtocolLevel: packet.V50})
	if err := pub.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer pub.Disconnect()
	err := pub.Publish(context.Background(), "bad/+", 1, false, []byte("x"))
	if err == nil {
		t.Fatal("expected error")
	}
	me, ok := err.(*mqerr.Error)
	if !ok {
		t.Fatalf("want *mqerr.Error, got %T", err)
	}
	if me.Class != mqerr.ClassProtocol || me.Code != mqerr.CodeTopicNameInvalid {
		t.Fatalf("want protocol/TopicNameInvalid, got class=%d code=%s", me.Class, me.Code)
	}
}

func TestClientConnackRejectionDistinguishable(t *testing.T) {
	_, addr := startBroker(t)
	// provoke identifier rejection: MQTT 3.1 empty/clean=false combo or bad
	// credentials; easiest deterministic check is unsupported level: configure
	// a valid broker but send unsupported protocol level via raw socket.
	conn, err := net.DialTimeout("tcp", addr, 2*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	r := packet.NewReader(conn, 1<<20)
	cp := &packet.Packet{
		Fixed:      packet.FixedHeader{Type: packet.TypeCONNECT},
		CleanStart: true,
		ClientID:   "x",
	}
	wire, _ := cp.Encode(packet.V311)
	wire[8] = 9
	conn.Write(wire)
	ack, err := r.ReadOne()
	if err != nil {
		t.Fatal(err)
	}
	if ack.ReasonCode != 0x01 {
		t.Fatalf("want version rejection, got 0x%02x", ack.ReasonCode)
	}
}

func TestConcurrentClientsDoNotInterfere(t *testing.T) {
	_, addr := startBroker(t)
	ctx := context.Background()
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			ch := make(chan *Message, 8)
			topic := fmt.Sprintf("cc/%d", i)
			c := New(Config{Addr: addr, ClientID: fmt.Sprintf("cc-%d", i), Clean: true})
			if err := c.Connect(ctx); err != nil {
				t.Errorf("connect %d: %v", i, err)
				return
			}
			defer c.Disconnect()
			if err := c.Subscribe(ctx, 0, func(m *Message) { ch <- m }, topic); err != nil {
				t.Errorf("sub %d: %v", i, err)
				return
			}
			p := New(Config{Addr: addr, ClientID: fmt.Sprintf("ccp-%d", i), Clean: true})
			if err := p.Connect(ctx); err != nil {
				t.Errorf("pub connect %d: %v", i, err)
				return
			}
			defer p.Disconnect()
			for j := 0; j < 5; j++ {
				if err := p.Publish(ctx, topic, 0, false, []byte(fmt.Sprintf("m%d", j))); err != nil {
					t.Errorf("pub %d/%d: %v", i, j, err)
				}
			}
			for j := 0; j < 5; j++ {
				select {
				case <-ch:
				case <-time.After(3 * time.Second):
					t.Errorf("client %d missing message %d", i, j)
					return
				}
			}
		}(i)
	}
	wg.Wait()
}
