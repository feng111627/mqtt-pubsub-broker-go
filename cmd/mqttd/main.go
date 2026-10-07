// mqttd 是本地 MQTT 消息代理服务。
//
// 容量与过期策略可通过命令行覆盖（默认值见 broker.DefaultConfig）：
//
//	mqttd -addr 127.0.0.1:1883 \
//	  -session-queue 1000 -message-ttl 1h -session-expiry 0 \
//	  -group-queue 1000 -total-queue 100000 -sweep 1m
package main

import (
	"flag"
	"log"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "listen address")
	sessionQueue := flag.Int("session-queue", 1000, "max offline queued messages per persistent session")
	messageTTL := flag.Duration("message-ttl", time.Hour, "offline/shared message TTL (0=never expire)")
	sessionExpiry := flag.Duration("session-expiry", 0, "persistent session offline expiry (0=never expire)")
	groupQueue := flag.Int("group-queue", 1000, "fallback queue size per shared-subscription group (0=disable fallback)")
	totalQueue := flag.Int64("total-queue", 100000, "global cap on total queued messages across sessions and groups (0=unlimited)")
	sweep := flag.Duration("sweep", time.Minute, "background sweep interval for TTL/session expiry/idle groups")
	flag.Parse()

	cfg := broker.DefaultConfig()
	cfg.Session.MaxQueue = *sessionQueue
	cfg.Session.MessageTTL = *messageTTL
	cfg.Session.SessionExpiry = *sessionExpiry
	cfg.Group.GroupQueueCap = *groupQueue
	cfg.Group.MessageTTL = *messageTTL
	cfg.TotalQueuedMessages = *totalQueue
	cfg.SweepInterval = *sweep

	b := broker.NewWithConfig(cfg)
	log.Printf("mqttd listening on %s (session-queue=%d message-ttl=%s session-expiry=%s group-queue=%d total-queue=%d sweep=%s)",
		*addr, *sessionQueue, *messageTTL, *sessionExpiry, *groupQueue, *totalQueue, *sweep)
	if err := b.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
