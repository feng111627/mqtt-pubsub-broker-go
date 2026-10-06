package packet

import (
	"bytes"
	"errors"
	"testing"
)

// 构造一个 v3.1.1 CONNECT 报文
func connect311(clientID string, clean bool) []byte {
	body := appendStr(nil, "MQTT")
	body = append(body, Version311)
	flags := byte(0)
	if clean {
		flags |= 0x02
	}
	body = append(body, flags, 0x00, 0x3C) // keepalive 60
	body = appendStr(body, clientID)
	out := []byte{TypeCONNECT << 4}
	out = appendVarint(out, len(body))
	return append(out, body...)
}

func TestReadConnect311(t *testing.T) {
	p, err := ReadPacket(bytes.NewReader(connect311("dev-1", true)))
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if p.Header.Type != TypeCONNECT || p.ClientID != "dev-1" || !p.CleanStart || p.KeepAlive != 60 {
		t.Fatalf("bad connect decode: %+v", p)
	}
	if p.ProtocolLevel != Version311 {
		t.Fatalf("level = %d", p.ProtocolLevel)
	}
}

func TestReadConnect5(t *testing.T) {
	body := appendStr(nil, "MQTT")
	body = append(body, Version5, 0x02, 0x00, 0x0A)
	body = append(body, 0x00) // properties
	body = appendStr(body, "dev-5")
	raw := append([]byte{TypeCONNECT << 4}, appendVarint(nil, len(body))...)
	raw = append(raw, body...)
	p, err := ReadPacket(bytes.NewReader(raw))
	if err != nil {
		t.Fatalf("ReadPacket: %v", err)
	}
	if p.ProtocolLevel != Version5 || p.ClientID != "dev-5" {
		t.Fatalf("bad v5 connect: %+v", p)
	}
}

func TestMalformedPackets(t *testing.T) {
	cases := []struct {
		name string
		raw  []byte
	}{
		{"unknown type", []byte{0xF0, 0x00}},
		{"reserved type 0", []byte{0x00, 0x00}},
		{"connect bad protocol name", func() []byte {
			body := appendStr(nil, "MQXX")
			body = append(body, 4, 0x02, 0, 0)
			body = appendStr(body, "x")
			return append(append([]byte{0x10}, appendVarint(nil, len(body))...), body...)
		}()},
		{"connect bad level", func() []byte {
			body := appendStr(nil, "MQTT")
			body = append(body, 9, 0x02, 0, 0)
			body = appendStr(body, "x")
			return append(append([]byte{0x10}, appendVarint(nil, len(body))...), body...)
		}()},
		{"connect reserved flag", func() []byte {
			body := appendStr(nil, "MQTT")
			body = append(body, 4, 0x03, 0, 0)
			body = appendStr(body, "x")
			return append(append([]byte{0x10}, appendVarint(nil, len(body))...), body...)
		}()},
		{"connect truncated", []byte{0x10, 0x05, 0x00}},
		{"publish qos3", []byte{0x36, 0x04, 0x00, 0x01, 'a', 0x00}},
		{"subscribe bad flags", []byte{0x80, 0x05, 0x00, 0x01, 0x00, 0x01, 'a'}},
		{"subscribe empty", []byte{0x82, 0x02, 0x00, 0x01}},
		{"subscribe qos3", []byte{0x82, 0x06, 0x00, 0x01, 0x00, 0x01, 'a', 0x03}},
		{"bad varint", []byte{0x10, 0x80, 0x80, 0x80, 0x80, 0x00}},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			_, err := ReadPacket(bytes.NewReader(c.raw))
			if err == nil {
				t.Fatalf("expected error, got nil")
			}
			if !errors.Is(err, ErrProtocol) {
				t.Fatalf("expected protocol error, got %v", err)
			}
		})
	}
}

func TestEncodeDecodePublishRoundTrip(t *testing.T) {
	for _, ver := range []byte{Version311, Version5} {
		orig := &Packet{
			Header:   FixedHeader{Type: TypePUBLISH},
			Topic:    "a/b",
			QoS:      1,
			PacketID: 42,
			Retain:   true,
			Payload:  []byte("hello"),
		}
		raw, err := orig.Encode(ver)
		if err != nil {
			t.Fatalf("encode v%d: %v", ver, err)
		}
		ptype, flags, body, err := ReadRaw(bytes.NewReader(raw))
		if err != nil {
			t.Fatalf("readraw v%d: %v", ver, err)
		}
		got := &Packet{Header: FixedHeader{Type: ptype, Flags: flags}}
		if err := got.Decode(body, ver); err != nil {
			t.Fatalf("decode v%d: %v", ver, err)
		}
		if got.Topic != "a/b" || got.QoS != 1 || got.PacketID != 42 ||
			!got.Retain || string(got.Payload) != "hello" {
			t.Fatalf("v%d roundtrip mismatch: %+v", ver, got)
		}
	}
}

func TestEncodeConnack(t *testing.T) {
	p := &Packet{Header: FixedHeader{Type: TypeCONNACK}, SessionPresent: true}
	raw, err := p.Encode(Version311)
	if err != nil {
		t.Fatal(err)
	}
	want := []byte{0x20, 0x02, 0x01, 0x00}
	if !bytes.Equal(raw, want) {
		t.Fatalf("connack = %v, want %v", raw, want)
	}
	// v5 带空属性区
	raw, err = p.Encode(Version5)
	if err != nil {
		t.Fatal(err)
	}
	want = []byte{0x20, 0x03, 0x01, 0x00, 0x00}
	if !bytes.Equal(raw, want) {
		t.Fatalf("connack v5 = %v, want %v", raw, want)
	}
}
