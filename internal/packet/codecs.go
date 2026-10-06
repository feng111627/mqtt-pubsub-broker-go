package packet

import (
	"unicode/utf8"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
)

// buffer is a simple cursor-based reader over the decoded remaining bytes of
// one packet. All accessors report mqerr protocol errors on truncation.
type buffer struct {
	b   []byte
	pos int
}

func (b *buffer) remaining() int { return len(b.b) - b.pos }

func (b *buffer) byte() (byte, error) {
	if b.pos >= len(b.b) {
		return 0, mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "packet truncated: expected byte at offset %d", b.pos)
	}
	v := b.b[b.pos]
	b.pos++
	return v, nil
}

func (b *buffer) u16() (uint16, error) {
	if b.remaining() < 2 {
		return 0, mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "packet truncated: expected uint16 at offset %d", b.pos)
	}
	v := uint16(b.b[b.pos])<<8 | uint16(b.b[b.pos+1])
	b.pos += 2
	return v, nil
}

func (b *buffer) u32() (uint32, error) {
	if b.remaining() < 4 {
		return 0, mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "packet truncated: expected uint32 at offset %d", b.pos)
	}
	v := uint32(b.b[b.pos])<<24 | uint32(b.b[b.pos+1])<<16 |
		uint32(b.b[b.pos+2])<<8 | uint32(b.b[b.pos+3])
	b.pos += 4
	return v, nil
}

func (b *buffer) bytesN(n int) ([]byte, error) {
	if n < 0 || b.remaining() < n {
		return nil, mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "packet truncated: expected %d bytes at offset %d (have %d)", n, b.pos, b.remaining())
	}
	v := b.b[b.pos : b.pos+n]
	b.pos += n
	return v, nil
}

// binary reads a uint16-length prefixed binary blob.
func (b *buffer) binary() ([]byte, error) {
	n, err := b.u16()
	if err != nil {
		return nil, err
	}
	return b.bytesN(int(n))
}

// stringRaw reads a UTF-8 string prefix without character validation.
func (b *buffer) stringRaw() (string, error) {
	v, err := b.binary()
	return string(v), err
}

// stringValidated reads an MQTT UTF-8 encoded string and enforces MQTT-1.5.3:
// valid UTF-8, no NUL, and (3.1.1/5.0) no other forbidden control characters.
func (b *buffer) stringValidated() (string, error) {
	v, err := b.binary()
	if err != nil {
		return "", err
	}
	if err := ValidateMQTTString(v); err != nil {
		return "", err
	}
	return string(v), nil
}

func (b *buffer) varint() (uint32, int, error) {
	var v uint32
	var mul uint32 = 1
	for i := 0; i < 4; i++ {
		x, err := b.byte()
		if err != nil {
			return 0, 0, err
		}
		v += uint32(x&0x7F) * mul
		if x&0x80 == 0 {
			return v, i + 1, nil
		}
		mul *= 128
	}
	return 0, 0, mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "variable byte integer exceeds 4 bytes")
}

// ValidateMQTTString applies the MQTT 3.1.1/5.0 string rules.
func ValidateMQTTString(b []byte) error {
	if !utf8.Valid(b) {
		return mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "string is not valid UTF-8")
	}
	for i := 0; i < len(b); i++ {
		r, sz := utf8.DecodeRune(b[i:])
		if r == 0 || (r >= 0x01 && r <= 0x1F) || (r >= 0x7F && r <= 0x9F) {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "string contains forbidden control character U+%04X", r)
		}
		i += sz - 1
	}
	return nil
}

// encodeVarint appends a variable byte integer to dst.
func encodeVarint(dst []byte, v uint32) []byte {
	for {
		b := byte(v % 128)
		v /= 128
		if v > 0 {
			b |= 0x80
		}
		dst = append(dst, b)
		if v == 0 {
			return dst
		}
	}
}

// varintLen returns the encoded size of a variable byte integer.
func varintLen(v uint32) int {
	n := 1
	for v >= 128 {
		v /= 128
		n++
	}
	return n
}

func putU16(dst []byte, v uint16) []byte {
	return append(dst, byte(v>>8), byte(v))
}

func putU32(dst []byte, v uint32) []byte {
	return append(dst, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func putString(dst []byte, s string) []byte {
	dst = putU16(dst, uint16(len(s)))
	return append(dst, s...)
}

func putBinary(dst, b []byte) []byte {
	dst = putU16(dst, uint16(len(b)))
	return append(dst, b...)
}
