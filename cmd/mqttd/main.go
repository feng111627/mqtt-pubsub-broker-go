// mqttd 是本地 MQTT 消息代理服务。
package main

import (
	"flag"
	"log"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/session"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "listen address")
	maxQueue := flag.Int("max-queue", 0, "per-session offline queue cap (0=default 1000)")
	msgTTL := flag.Duration("message-ttl", 0, "offline/group message TTL (0=default 24h)")
	sessionTTL := flag.Duration("session-ttl", 0, "persistent session offline TTL (0=default 168h)")
	maxGroupPending := flag.Int("max-group-pending", 0, "per-group fallback queue cap (0=default 1000)")
	sweep := flag.Duration("sweep-interval", 0, "expiry sweep interval (0=default 30s)")
	flag.Parse()

	cfg := broker.Config{
		Session: session.Limits{MaxQueue: *maxQueue, MessageTTL: *msgTTL, SessionTTL: *sessionTTL},
		Group: broker.GroupConfig{
			MaxPending: *maxGroupPending,
			MessageTTL: *msgTTL,
		},
		SweepInterval: *sweep,
	}

	b := broker.NewWithConfig(cfg)
	log.Printf("mqttd listening on %s (maxQueue=%v msgTTL=%v sessionTTL=%v groupPending=%v)",
		*addr, cfg.Session.MaxQueue, cfg.Session.MessageTTL, cfg.Session.SessionTTL, cfg.Group.MaxPending)
	if err := b.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
