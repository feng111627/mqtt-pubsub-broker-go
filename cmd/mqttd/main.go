// mqttd 是本地 MQTT 消息代理服务。
package main

import (
	"flag"
	"log"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/broker"
)

func main() {
	addr := flag.String("addr", "127.0.0.1:1883", "listen address")
	flag.Parse()

	b := broker.New()
	log.Printf("mqttd listening on %s", *addr)
	if err := b.ListenAndServe(*addr); err != nil {
		log.Fatal(err)
	}
}
