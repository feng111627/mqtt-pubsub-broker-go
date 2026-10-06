// Command mqttbroker runs the local loopback MQTT broker.
//
// Usage:
//
//	mqttbroker [-addr 127.0.0.1:1883]
//
// It supports MQTT 3.1, 3.1.1 and 5.0 on a plain TCP listener (no TLS).
package main

import (
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "listen address")
	maxQ := flag.Int("queue", 1000, "max queued messages per persistent session")
	flag.Parse()

	cfg := broker.DefaultConfig()
	cfg.ListenAddr = *addr
	cfg.QueueMax = *maxQ
	b := broker.New(cfg)
	if err := b.Start(); err != nil {
		log.Fatalf("broker start failed: %v", err)
	}
	log.Printf("%s listening on %s (MQTT 3.1/3.1.1/5.0)", b, b.Addr())

	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	<-sig
	log.Printf("shutting down")
	if err := b.Close(); err != nil {
		log.Printf("close: %v", err)
	}
}
