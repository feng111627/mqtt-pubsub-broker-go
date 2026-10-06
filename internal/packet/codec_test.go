package packet

import (
	"bytes"
	"strings"
	"testing"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
)

func decodeOne(t *testing.T, level byte, wire []byte) *Packet {
	t.Helper()
	r := NewReader(bytes.NewReader(wire), 1<<20)
	if level != 0 {
		r.SetLevel(level)
	}
	p, err := r.ReadOne()
	if err != nil {
		t.Fatalf("decode error: %v", err)
	}
	return p
}

func TestConnectRoundtrip311(t *testing.T) {
	p := &Packet{
		Fixed:      FixedHeader{Type: TypeCONNECT},
		Level:      V311,
		CleanStart: true,
		KeepAlive:  60,
		ClientID:   "device-001",
		HasUser:    true,
		HasPass:    true,
		Username:   "user",
		Password:   []byte("pass"),
		WillFlag:   true,
		WillQoS:    1,
		WillTopic:  "lw/device",
		WillMsg:    []byte("offline"),
	}
	wire, err := p.Encode(V311)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeOne(t, 0, wire)
	if got.ProtoName != "MQTT" || got.ProtoLevel != V311 {
		t.Fatalf("proto = %q level=%d", got.ProtoName, got.ProtoLevel)
	}
	if got.ClientID != "device-001" || got.Username != "user" ||
		string(got.Password) != "pass" || got.WillTopic != "lw/device" ||
		!got.WillFlag || got.WillQoS != 1 || !got.CleanStart || got.KeepAlive != 60 {
		t.Fatalf("CONNECT fields mismatch: %+v", got)
	}
}

func TestConnectRoundtripV5WithProps(t *testing.T) {
	exp := uint32(0)
	p := &Packet{
		Fixed:      FixedHeader{Type: TypeCONNECT},
		CleanStart: true,
		KeepAlive:  30,
		ClientID:   "v5-client",
		Props:      Properties{SessionExpiry: &exp, SubscriptionID: -1, UserProps: [][2]string{{"k", "v"}}},
	}
	wire, err := p.Encode(V50)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeOne(t, 0, wire)
	if got.ProtoLevel != V50 || got.ClientID != "v5-client" {
		t.Fatalf("v5 connect mismatch: %+v", got)
	}
	if len(got.Props.UserProps) != 1 || got.Props.UserProps[0] != [2]string{"k", "v"} {
		t.Fatalf("user props mismatch: %+v", got.Props.UserProps)
	}
}

func TestPublishRoundtripQoS2(t *testing.T) {
	p := &Packet{
		Fixed:    FixedHeader{Type: TypePUBLISH, Flags: (QoS2 << 1) | 0x08}, // QoS2 + DUP
		Topic:    "a/b",
		PacketID: 0x1234,
		Payload:  []byte("hello"),
	}
	wire, err := p.Encode(V311)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeOne(t, V311, wire)
	if got.Topic != "a/b" || got.PacketID != 0x1234 || !got.Fixed.Dup() ||
		got.Fixed.QoS() != 2 || string(got.Payload) != "hello" {
		t.Fatalf("PUBLISH mismatch: %+v", got)
	}
}

func TestSubscribeRoundtripV5Options(t *testing.T) {
	p := &Packet{
		Fixed:    FixedHeader{Type: TypeSUBSCRIBE},
		PacketID: 7,
		Props:    NewProperties(),
		Filters: []SubFilter{
			{Filter: "a/+", QoS: 1, NoLocal: true, RetainHandling: 1},
			{Filter: "#", QoS: 2},
		},
	}
	wire, err := p.Encode(V50)
	if err != nil {
		t.Fatal(err)
	}
	got := decodeOne(t, V50, wire)
	if len(got.Filters) != 2 || !got.Filters[0].NoLocal ||
		got.Filters[0].RetainHandling != 1 || got.Filters[1].QoS != 2 {
		t.Fatalf("SUBSCRIBE mismatch: %+v", got.Filters)
	}
}

func TestAckRoundtrip(t *testing.T) {
	for _, typ := range []Type{TypePUBACK, TypePUBREC, TypePUBREL, TypePUBCOMP} {
		p := &Packet{Fixed: FixedHeader{Type: typ}, PacketID: 42}
		wire, err := p.Encode(V311)
		if err != nil {
			t.Fatal(err)
		}
		got := decodeOne(t, V311, wire)
		if got.PacketID != 42 {
			t.Fatalf("%s id mismatch", typ)
		}
	}
}

func TestMalformedPackets(t *testing.T) {
	cases := []struct {
		name  string
		level byte
		wire  []byte
		code  mqerr.Code
	}{
		{
			name: "bad protocol name",
			wire: buildConnectRaw("XXXX", V311, 0x02, "c1"),
			code: mqerr.CodeUnsupportedProtoV,
		},
		{
			name: "unsupported level",
			wire: buildConnectRaw("MQTT", 9, 0x02, "c1"),
			code: mqerr.CodeUnsupportedProtoV,
		},
		{
			name:  "qos3 publish",
			level: V311,
			wire: func() []byte {
				b := putString(nil, "a/b")
				b = putU16(b, 1)
				return frame(TypePUBLISH, 0x06, b)
			}(),
			code: mqerr.CodeMalformedPacket,
		},
		{
			name:  "dup qos0",
			level: V311,
			wire: func() []byte {
				b := putString(nil, "a/b")
				return frame(TypePUBLISH, 0x08, b)
			}(),
			code: mqerr.CodeMalformedPacket,
		},
		{
			name:  "publish zero packet id qos1",
			level: V311,
			wire: func() []byte {
				b := putString(nil, "a/b")
				b = putU16(b, 0)
				return frame(TypePUBLISH, 0x02, b)
			}(),
			code: mqerr.CodeMalformedPacket,
		},
		{
			name:  "subscribe zero id",
			level: V311,
			wire: func() []byte {
				b := putU16(nil, 0)
				b = putString(b, "a/b")
				b = append(b, 1)
				return frame(TypeSUBSCRIBE, 0x02, b)
			}(),
			code: mqerr.CodeMalformedPacket,
		},
		{
			name:  "subscribe bad flags",
			level: V311,
			wire: func() []byte {
				b := putU16(nil, 1)
				b = putString(b, "a/b")
				b = append(b, 1)
				return frame(TypeSUBSCRIBE, 0x00, b)
			}(),
			code: mqerr.CodeMalformedPacket,
		},
		{
			name:  "reserved packet type",
			level: V311,
			wire:  []byte{0x00, 0x00},
			code:  mqerr.CodeMalformedPacket,
		},
		{
			name:  "truncated remaining length body",
			level: V311,
			wire: func() []byte {
				out := []byte{byte(TypePUBLISH) << 4, 10}
				return append(out, 1, 2, 3)
			}(),
			code: mqerr.CodeImplSpecific, // connection class (short read)
		},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			r := NewReader(bytes.NewReader(c.wire), 1<<20)
			if c.level != 0 {
				r.SetLevel(c.level)
			}
			_, err := r.ReadOne()
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			me, ok := err.(*mqerr.Error)
			if !ok {
				t.Fatalf("expected *mqerr.Error, got %T: %v", err, err)
			}
			if me.Code != c.code {
				t.Fatalf("code = %s, want %s (err: %v)", me.Code, c.code, err)
			}
		})
	}
}

func TestVarintRoundtrip(t *testing.T) {
	for _, v := range []uint32{0, 1, 127, 128, 16383, 16384, 2097151, 2097152, 268435455} {
		b := encodeVarint(nil, v)
		got, n, err := (&buffer{b: b}).varint()
		if err != nil || got != v || n != len(b) {
			t.Fatalf("varint %d roundtrip: got %d n=%d err=%v", v, got, n, err)
		}
	}
}

func TestOversizedPacketRejected(t *testing.T) {
	// remaining length claims 100 bytes but reader max is 10.
	wire := []byte{byte(TypePINGREQ) << 4, 100}
	r := NewReader(bytes.NewReader(wire), 10)
	_, err := r.ReadOne()
	if err == nil || !mqerr.Of(err, mqerr.CodePacketTooLarge) {
		t.Fatalf("expected packet too large, got %v", err)
	}
}

// helpers

func frame(t Type, flags byte, body []byte) []byte {
	out := []byte{byte(t)<<4 | flags}
	out = encodeVarint(out, uint32(len(body)))
	return append(out, body...)
}

func buildConnectRaw(name string, level byte, flags byte, clientID string) []byte {
	var body []byte
	body = putString(body, name)
	body = append(body, level, flags)
	body = putU16(body, 0) // keepalive
	if level == V50 {
		body = encodeVarint(body, 0) // empty properties
	}
	body = putString(body, clientID)
	return frame(TypeCONNECT, 0, body)
}

var _ = strings.Repeat
