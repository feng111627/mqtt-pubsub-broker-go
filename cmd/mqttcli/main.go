// mqttcli 是本地验证用的发布/订阅命令行客户端。
//
// 用法：
//
//	mqttcli sub -addr 127.0.0.1:1883 -id dev-1 -topic 'sensors/#' -qos 1
//	mqttcli pub -addr 127.0.0.1:1883 -id back-1 -topic sensors/a/temp -payload '21.5' -qos 1 [-retain]
package main

import (
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/client"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/packet"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "sub":
		cmdSub(os.Args[2:])
	case "pub":
		cmdPub(os.Args[2:])
	default:
		usage()
		os.Exit(2)
	}
}

func usage() {
	fmt.Fprintf(os.Stderr, `usage:
  mqttcli sub -addr ADDR -id ID -topic FILTER [-qos N] [-v5] [-clean]
  mqttcli pub -addr ADDR -id ID -topic TOPIC -payload STR [-qos N] [-retain] [-v5]
`)
}

func versionFlag(v5 bool) byte {
	if v5 {
		return packet.Version5
	}
	return packet.Version311
}

func cmdSub(args []string) {
	fs := flag.NewFlagSet("sub", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:1883", "broker address")
	id := fs.String("id", "mqttcli-sub", "client id")
	topic := fs.String("topic", "#", "topic filter")
	qos := fs.Uint("qos", 1, "subscribe qos")
	clean := fs.Bool("clean", true, "clean start")
	v5 := fs.Bool("v5", false, "use MQTT 5.0")
	fs.Parse(args)

	c, err := client.Connect(*addr, client.Options{
		ClientID:   *id,
		CleanStart: *clean,
		Version:    versionFlag(*v5),
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	granted, err := c.Subscribe(*topic, byte(*qos))
	if err != nil {
		log.Fatalf("subscribe: %v", err)
	}
	log.Printf("subscribed to %q (granted qos=%d), waiting for messages...", *topic, granted)

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, os.Interrupt, syscall.SIGTERM)
	for {
		select {
		case m := <-c.Messages():
			fmt.Printf("[%s] qos=%d retain=%v dup=%v %s\n", m.Topic, m.QoS, m.Retain, m.Dup, m.Payload)
		case <-sig:
			c.Close()
			return
		}
	}
}

func cmdPub(args []string) {
	fs := flag.NewFlagSet("pub", flag.ExitOnError)
	addr := fs.String("addr", "127.0.0.1:1883", "broker address")
	id := fs.String("id", "mqttcli-pub", "client id")
	topic := fs.String("topic", "", "topic name")
	payload := fs.String("payload", "", "message payload")
	qos := fs.Uint("qos", 0, "publish qos")
	retain := fs.Bool("retain", false, "retain message")
	v5 := fs.Bool("v5", false, "use MQTT 5.0")
	fs.Parse(args)

	if *topic == "" {
		log.Fatal("-topic is required")
	}
	c, err := client.Connect(*addr, client.Options{
		ClientID:   *id,
		CleanStart: true,
		Version:    versionFlag(*v5),
	})
	if err != nil {
		log.Fatalf("connect: %v", err)
	}
	defer c.Close()
	if err := c.Publish(*topic, byte(*qos), *retain, []byte(*payload)); err != nil {
		log.Fatalf("publish: %v", err)
	}
	log.Printf("published to %q (qos=%d retain=%v)", *topic, *qos, *retain)
}
