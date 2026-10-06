// Command mqttcli is a tiny pub/sub client for local verification against the
// bundled broker (or any standards-compliant broker):
//
//	# terminal 1: subscribe (3.1.1, persistent session)
//	mqttcli sub -addr 127.0.0.1:1883 -topic 'sport/+' -qos 1 -id cli-sub
//
//	# terminal 2: publish
//	mqttcli pub -addr 127.0.0.1:1883 -topic sport/tennis -qos 1 -m 'hello'
//
//	# MQTT 5.0:
//	mqttcli sub -topic '#' -qos 2 -version 5
//
// Exit status is non-zero on connection/protocol/state failures, which are
// printed with their classified reason.
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	cmd := os.Args[1]
	fs := flag.NewFlagSet(cmd, flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:1883", "broker address host:port")
	topic := fs.String("topic", "", "topic name (pub) or filter (sub)")
	qos := fs.Int("qos", 1, "QoS level 0/1/2")
	msg := fs.String("m", "", "message payload (pub)")
	id := fs.String("id", "", "client id (default: generated)")
	version := fs.Int("version", 4, "protocol version: 3 (MQTT 3.1), 4 (3.1.1), 5 (5.0)")
	retain := fs.Bool("retain", false, "publish as retained message")
	clean := fs.Bool("clean", false, "clean session/start (sub defaults to persistent)")
	keepalive := fs.Int("keepalive", 30, "keep-alive in seconds")
	user := fs.String("user", "", "username")
	pass := fs.String("pass", "", "password")
	if err := fs.Parse(os.Args[2:]); err != nil {
		os.Exit(2)
	}
	if *topic == "" {
		fail("-topic is required")
	}
	level := protocolLevel(*version)

	cfg := client.Config{
		Addr:          *addr,
		ProtocolLevel: level,
		ClientID:      *id,
		Clean:         *clean,
		KeepAlive:     uint16(*keepalive),
		Username:      *user,
		Password:      *pass,
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch cmd {
	case "pub":
		runPub(ctx, cfg, *topic, byte(*qos), *retain, *msg)
	case "sub":
		runSub(ctx, cfg, *topic, byte(*qos))
	default:
		usage()
		os.Exit(2)
	}
}

func runPub(ctx context.Context, cfg client.Config, topic string, qos byte, retain bool, msg string) {
	cfg.Clean = true // short-lived publisher
	c := client.New(cfg)
	if err := c.Connect(ctx); err != nil {
		failErr(err)
	}
	defer c.Disconnect()
	pubCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if err := c.Publish(pubCtx, topic, qos, retain, []byte(msg)); err != nil {
		failErr(err)
	}
	fmt.Printf("published to %s qos=%d bytes=%d\n", topic, qos, len(msg))
}

func runSub(ctx context.Context, cfg client.Config, filter string, qos byte) {
	cfg.AutoReconnect = true
	cfg.MaxReconnectDelayMS = 3000
	c := client.New(cfg)
	if err := c.Connect(ctx); err != nil {
		failErr(err)
	}
	defer c.Disconnect()
	received := int64(0)
	handler := func(m *client.Message) {
		received++
		flag := ""
		if m.Retain {
			flag = " [retained]"
		}
		if m.Dup {
			flag += " [dup]"
		}
		fmt.Printf("%s (qos=%d, id=%d)%s: %s\n",
			m.Topic, m.QoS, m.PacketID, flag, string(m.Payload))
	}
	if err := c.Subscribe(ctx, qos, handler, filter); err != nil {
		failErr(err)
	}
	fmt.Fprintf(os.Stderr, "subscribed to %s at qos=%d (version %s), waiting...\n",
		filter, qos, packet.VersionName(cfg.ProtocolLevel))
	<-ctx.Done()
}

func protocolLevel(v int) byte {
	switch v {
	case 3:
		return packet.V31
	case 5:
		return packet.V50
	default:
		return packet.V311
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, "usage: mqttcli <pub|sub> [flags]")
	flag.PrintDefaults()
}

func fail(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "error: "+format+"\n", args...)
	os.Exit(1)
}

func failErr(err error) {
	if me, ok := err.(*mqerr.Error); ok {
		fmt.Fprintf(os.Stderr, "error [%s] %s: %s\n", className(me.Class), me.Code, me.Reason)
	} else {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
	}
	os.Exit(1)
}

func className(c mqerr.Class) string {
	switch c {
	case mqerr.ClassProtocol:
		return "protocol"
	case mqerr.ClassState:
		return "state"
	case mqerr.ClassConnection:
		return "connection"
	}
	return "unknown"
}
