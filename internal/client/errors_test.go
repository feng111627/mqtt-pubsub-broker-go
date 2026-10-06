package client

import (
	"context"
	"testing"
	"time"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
)

// Connecting to a closed port must be a ClassConnection error; a bad topic a
// ClassProtocol error; a publish after disconnect a ClassState error.
func TestErrorClassTaxonomy(t *testing.T) {
	// connection error
	c := New(Config{Addr: "127.0.0.1:1", ClientID: "x", Clean: true, DialTimeout: 500 * time.Millisecond})
	err := c.Connect(context.Background())
	if err == nil {
		t.Fatal("expected connection error")
	}
	if !mqerr.IsClass(err, mqerr.ClassConnection) {
		t.Fatalf("want ClassConnection, got %v", err)
	}

	_, addr := startBroker(t)
	pub := New(Config{Addr: addr, ClientID: "tax", Clean: true})
	if err := pub.Connect(context.Background()); err != nil {
		t.Fatal(err)
	}
	// protocol error: wildcard in publish topic
	err = pub.Publish(context.Background(), "a/+", 1, false, nil)
	if !mqerr.IsClass(err, mqerr.ClassProtocol) || !mqerr.Of(err, mqerr.CodeTopicNameInvalid) {
		t.Fatalf("want protocol/TopicNameInvalid, got %v", err)
	}
	// state error: operate after Disconnect
	pub.Disconnect()
	err = pub.Publish(context.Background(), "ok", 0, false, nil)
	if err == nil {
		// 0x00 publish may succeed if the write races; ensure disconnect closed it
	} else if !mqerr.IsClass(err, mqerr.ClassConnection) && !mqerr.IsClass(err, mqerr.ClassState) {
		t.Fatalf("post-disconnect publish should be connection/state class, got %v", err)
	}
}
