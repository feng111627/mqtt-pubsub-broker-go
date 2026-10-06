// Package packet 实现 MQTT 控制报文的编解码。
// 同时支持 MQTT 3.1.1（level 4）与 MQTT 5.0（level 5）的接入解析；
// 对畸形报文返回 *ProtocolError，可与其他错误区分。
package packet

import (
	"encoding/binary"
	"errors"
	"fmt"
	"io"
)

// 报文类型（固定报头高 4 位）。
const (
	TypeCONNECT     byte = 1
	TypeCONNACK     byte = 2
	TypePUBLISH     byte = 3
	TypePUBACK      byte = 4
	TypePUBREC      byte = 5
	TypePUBREL      byte = 6
	TypePUBCOMP     byte = 7
	TypeSUBSCRIBE   byte = 8
	TypeSUBACK      byte = 9
	TypeUNSUBSCRIBE byte = 10
	TypeUNSUBACK    byte = 11
	TypePINGREQ     byte = 12
	TypePINGRESP    byte = 13
	TypeDISCONNECT  byte = 14
)

// MQTT 协议版本号。
const (
	Version311 byte = 4
	Version5   byte = 5
)

// CONNACK 返回码（v3.1.1）。
const (
	ConnAccepted             byte = 0
	ConnRefusedProtocol      byte = 1
	ConnRefusedClientID      byte = 2
	ConnRefusedUnavailable   byte = 3
	ConnRefusedBadAuth       byte = 4
	ConnRefusedNotAuthorized byte = 5
)

// 错误分类：协议错误 / 状态错误 / 连接错误。
var (
	// ErrProtocol 表示畸形报文、非法字段等协议层错误。
	ErrProtocol = errors.New("mqtt: protocol error")
	// ErrState 表示与当前会话状态冲突的错误（如未连接先发布）。
	ErrState = errors.New("mqtt: state error")
	// ErrConn 表示底层连接错误。
	ErrConn = errors.New("mqtt: connection error")
)

// ProtocolError 带原因码的协议错误，可区分拒绝原因。
type ProtocolError struct {
	Reason string
}

func (e *ProtocolError) Error() string { return fmt.Sprintf("mqtt: protocol error: %s", e.Reason) }
func (e *ProtocolError) Unwrap() error { return ErrProtocol }

func protoErr(format string, args ...any) *ProtocolError {
	return &ProtocolError{Reason: fmt.Sprintf(format, args...)}
}

// FixedHeader 固定报头。
type FixedHeader struct {
	Type      byte
	Flags     byte
	Remaining int
}

// Packet 解码后的控制报文。
type Packet struct {
	Header FixedHeader

	// CONNECT
	ProtocolName  string
	ProtocolLevel byte
	CleanStart    bool
	KeepAlive     uint16
	ClientID      string
	Username      string
	Password      []byte
	WillTopic     string
	WillPayload   []byte
	WillQoS       byte
	WillRetain    bool
	HasWill       bool

	// PUBLISH
	Topic    string
	PacketID uint16
	Payload  []byte
	QoS      byte
	Retain   bool
	Dup      bool

	// SUBSCRIBE / UNSUBSCRIBE
	Subscriptions []Subscription

	// CONNACK
	SessionPresent bool

	// 通用原因码（v5 及 v3 CONNACK）
	ReasonCode byte
}

// Subscription 单条订阅。
type Subscription struct {
	Topic string
	QoS   byte
}

// ---------- 读取 ----------

// ReadRaw 读取一个报文的原始字节，返回类型、标志位与报文体。
// 调用方按已协商的协议版本自行解码（PUBLISH 在 v5 下含属性区）。
func ReadRaw(r io.Reader) (ptype byte, flags byte, body []byte, err error) {
	var first [1]byte
	if _, err = io.ReadFull(r, first[:]); err != nil {
		return 0, 0, nil, err
	}
	remaining, err := readVarint(r)
	if err != nil {
		return 0, 0, nil, err
	}
	if remaining > 256*1024*1024 {
		return 0, 0, nil, protoErr("remaining length %d too large", remaining)
	}
	body = make([]byte, remaining)
	if _, err = io.ReadFull(r, body); err != nil {
		if errors.Is(err, io.EOF) || errors.Is(err, io.ErrUnexpectedEOF) {
			// 声明了长度但报文体不完整：视为畸形报文
			return 0, 0, nil, protoErr("truncated packet body: %v", err)
		}
		return 0, 0, nil, fmt.Errorf("read packet body: %w", err)
	}
	ptype = first[0] >> 4
	flags = first[0] & 0x0F
	if ptype < TypeCONNECT || ptype > TypeDISCONNECT {
		return 0, 0, nil, protoErr("unknown packet type %d", ptype)
	}
	return ptype, flags, body, nil
}

// ReadPacket 从连接读取并按 MQTT 3.1.1 布局解码一个控制报文。
// v5 会话请使用 ReadRaw + Decode。
func ReadPacket(r io.Reader) (*Packet, error) {
	ptype, flags, body, err := ReadRaw(r)
	if err != nil {
		return nil, err
	}
	p := &Packet{Header: FixedHeader{Type: ptype, Flags: flags, Remaining: len(body)}}
	if err := p.Decode(body, Version311); err != nil {
		return nil, err
	}
	return p, nil
}

// decodeSubscribeV5 解析 v5 SUBSCRIBE（报文ID + 属性区 + 过滤器列表）。
func (p *Packet) decodeSubscribeV5(r *reader) error {
	if p.Header.Flags != 0x02 {
		return protoErr("SUBSCRIBE flags must be 0x2, got %d", p.Header.Flags)
	}
	id, err := r.uint16()
	if err != nil {
		return err
	}
	if id == 0 {
		return protoErr("SUBSCRIBE packet id must not be 0")
	}
	p.PacketID = id
	if err := r.skipProperties(); err != nil {
		return err
	}
	for r.remaining() > 0 {
		filter, err := r.str()
		if err != nil {
			return err
		}
		opts, err := r.byte()
		if err != nil {
			return err
		}
		qos := opts & 0x03
		if qos > 2 || opts&0xC0 != 0 {
			return protoErr("SUBSCRIBE invalid options 0x%02x", opts)
		}
		p.Subscriptions = append(p.Subscriptions, Subscription{Topic: filter, QoS: qos})
	}
	if len(p.Subscriptions) == 0 {
		return protoErr("SUBSCRIBE with no subscriptions")
	}
	return nil
}

// decodeUnsubscribeV5 解析 v5 UNSUBSCRIBE。
func (p *Packet) decodeUnsubscribeV5(r *reader) error {
	if p.Header.Flags != 0x02 {
		return protoErr("UNSUBSCRIBE flags must be 0x2, got %d", p.Header.Flags)
	}
	id, err := r.uint16()
	if err != nil {
		return err
	}
	if id == 0 {
		return protoErr("UNSUBSCRIBE packet id must not be 0")
	}
	p.PacketID = id
	if err := r.skipProperties(); err != nil {
		return err
	}
	for r.remaining() > 0 {
		filter, err := r.str()
		if err != nil {
			return err
		}
		p.Subscriptions = append(p.Subscriptions, Subscription{Topic: filter})
	}
	if len(p.Subscriptions) == 0 {
		return protoErr("UNSUBSCRIBE with no filters")
	}
	return nil
}

// Decode 按指定协议版本解码报文体。
func (p *Packet) Decode(body []byte, version byte) error {
	r := &reader{buf: body}
	switch p.Header.Type {
	case TypePUBLISH:
		if version == Version5 {
			return p.decodePublishV5(r)
		}
		return p.decodePublish(r)
	case TypeSUBSCRIBE:
		if version == Version5 {
			return p.decodeSubscribeV5(r)
		}
		return p.decodeBody(body)
	case TypeUNSUBSCRIBE:
		if version == Version5 {
			return p.decodeUnsubscribeV5(r)
		}
		return p.decodeBody(body)
	case TypeSUBACK:
		id, err := r.uint16()
		if err != nil {
			return err
		}
		p.PacketID = id
		if version == Version5 {
			if err := r.skipProperties(); err != nil {
				return err
			}
		}
		for r.remaining() > 0 {
			q, err := r.byte()
			if err != nil {
				return err
			}
			p.Subscriptions = append(p.Subscriptions, Subscription{QoS: q})
		}
		return nil
	case TypeCONNACK:
		if err := p.decodeBody(body); err != nil {
			return err
		}
		if version == Version5 {
			// decodeBody 已读两个固定字节，剩余为属性区，忽略
			return nil
		}
		return nil
	}
	return p.decodeBody(body)
}

func readVarint(r io.Reader) (int, error) {
	multiplier, value := 1, 0
	var b [1]byte
	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, b[:]); err != nil {
			return 0, err
		}
		value += int(b[0]&0x7F) * multiplier
		if b[0]&0x80 == 0 {
			return value, nil
		}
		multiplier *= 128
	}
	return 0, protoErr("malformed remaining length varint")
}

type reader struct {
	buf []byte
	pos int
}

func (r *reader) remaining() int { return len(r.buf) - r.pos }

func (r *reader) byte() (byte, error) {
	if r.pos >= len(r.buf) {
		return 0, protoErr("unexpected end of packet")
	}
	b := r.buf[r.pos]
	r.pos++
	return b, nil
}

func (r *reader) uint16() (uint16, error) {
	if r.remaining() < 2 {
		return 0, protoErr("unexpected end of packet")
	}
	v := binary.BigEndian.Uint16(r.buf[r.pos:])
	r.pos += 2
	return v, nil
}

func (r *reader) str() (string, error) {
	n, err := r.uint16()
	if err != nil {
		return "", err
	}
	if r.remaining() < int(n) {
		return "", protoErr("string length exceeds packet")
	}
	s := string(r.buf[r.pos : r.pos+int(n)])
	r.pos += int(n)
	return s, nil
}

func (r *reader) bytes() ([]byte, error) {
	n, err := r.uint16()
	if err != nil {
		return nil, err
	}
	if r.remaining() < int(n) {
		return nil, protoErr("binary length exceeds packet")
	}
	b := r.buf[r.pos : r.pos+int(n)]
	r.pos += int(n)
	return b, nil
}

// skipProperties 跳过 v5 属性区（长度前缀 varint）。
func (r *reader) skipProperties() error {
	// 属性长度本身是 varint，但属性区在报文内部，用字节读取
	multiplier, length := 1, 0
	for i := 0; i < 4; i++ {
		b, err := r.byte()
		if err != nil {
			return err
		}
		length += int(b&0x7F) * multiplier
		if b&0x80 == 0 {
			if r.remaining() < length {
				return protoErr("properties length exceeds packet")
			}
			r.pos += length
			return nil
		}
		multiplier *= 128
	}
	return protoErr("malformed properties length")
}

func (p *Packet) decodeBody(body []byte) error {
	r := &reader{buf: body}
	switch p.Header.Type {
	case TypeCONNECT:
		return p.decodeConnect(r)
	case TypePUBLISH:
		return p.decodePublish(r)
	case TypeSUBSCRIBE:
		return p.decodeSubscribe(r)
	case TypeUNSUBSCRIBE:
		return p.decodeUnsubscribe(r)
	case TypePUBACK, TypePUBREC, TypePUBREL, TypePUBCOMP:
		id, err := r.uint16()
		if err != nil {
			return err
		}
		p.PacketID = id
		// v5 允许附带原因码与属性，此处忽略剩余字节
		return nil
	case TypeCONNACK:
		flags, err := r.byte()
		if err != nil {
			return err
		}
		if flags&0xFE != 0 {
			return protoErr("CONNACK reserved flags set")
		}
		p.SessionPresent = flags&0x01 != 0
		code, err := r.byte()
		if err != nil {
			return err
		}
		p.ReasonCode = code
		return nil // v5 属性区忽略
	case TypeSUBACK:
		id, err := r.uint16()
		if err != nil {
			return err
		}
		p.PacketID = id
		// 跳过 v5 属性区：尝试按 varint+剩余判定过于复杂，
		// 由调用方按版本处理；这里统一把剩余字节当作授予码列表，
		// v5 下首字节为属性长度 0，需剥掉。
		rest := r.buf[r.pos:]
		r.pos = len(r.buf)
		p.Subscriptions = make([]Subscription, 0, len(rest))
		for _, q := range rest {
			p.Subscriptions = append(p.Subscriptions, Subscription{QoS: q})
		}
		return nil
	case TypeUNSUBACK:
		id, err := r.uint16()
		if err != nil {
			return err
		}
		p.PacketID = id
		return nil
	case TypePINGREQ, TypePINGRESP, TypeDISCONNECT:
		return nil
	default:
		return protoErr("packet type %d not supported", p.Header.Type)
	}
}

func (p *Packet) decodeConnect(r *reader) error {
	if p.Header.Flags != 0 {
		return protoErr("CONNECT flags must be 0, got %d", p.Header.Flags)
	}
	name, err := r.str()
	if err != nil {
		return err
	}
	if name != "MQTT" && name != "MQIsdp" {
		return protoErr("unsupported protocol name %q", name)
	}
	level, err := r.byte()
	if err != nil {
		return err
	}
	if level != Version311 && level != Version5 {
		return protoErr("unsupported protocol level %d", level)
	}
	if name == "MQIsdp" {
		return protoErr("MQTT 3.1 (MQIsdp) not supported")
	}
	p.ProtocolName = name
	p.ProtocolLevel = level

	flags, err := r.byte()
	if err != nil {
		return err
	}
	if flags&0x01 != 0 {
		return protoErr("CONNECT reserved flag bit set")
	}
	p.CleanStart = flags&0x02 != 0
	willFlag := flags&0x04 != 0
	p.WillQoS = (flags >> 3) & 0x03
	p.WillRetain = flags&0x20 != 0
	hasPassword := flags&0x40 != 0
	hasUsername := flags&0x80 != 0
	if !willFlag && (p.WillQoS != 0 || p.WillRetain) {
		return protoErr("will QoS/retain set without will flag")
	}
	if p.WillQoS == 3 {
		return protoErr("invalid will QoS 3")
	}
	p.HasWill = willFlag

	ka, err := r.uint16()
	if err != nil {
		return err
	}
	p.KeepAlive = ka

	if level == Version5 {
		if err := r.skipProperties(); err != nil {
			return err
		}
	}

	cid, err := r.str()
	if err != nil {
		return err
	}
	p.ClientID = cid

	if willFlag {
		if level == Version5 {
			if err := r.skipProperties(); err != nil {
				return err
			}
		}
		if p.WillTopic, err = r.str(); err != nil {
			return err
		}
		if p.WillPayload, err = r.bytes(); err != nil {
			return err
		}
	}
	if hasUsername {
		if p.Username, err = r.str(); err != nil {
			return err
		}
	}
	if hasPassword {
		if p.Password, err = r.bytes(); err != nil {
			return err
		}
	}
	if r.remaining() != 0 {
		return protoErr("CONNECT has %d trailing bytes", r.remaining())
	}
	return nil
}

func (p *Packet) decodePublish(r *reader) error {
	p.Dup = p.Header.Flags&0x08 != 0
	p.QoS = (p.Header.Flags >> 1) & 0x03
	p.Retain = p.Header.Flags&0x01 != 0
	if p.QoS == 3 {
		return protoErr("PUBLISH with reserved QoS 3")
	}
	topic, err := r.str()
	if err != nil {
		return err
	}
	p.Topic = topic
	if p.QoS > 0 {
		id, err := r.uint16()
		if err != nil {
			return err
		}
		if id == 0 {
			return protoErr("PUBLISH packet id must not be 0")
		}
		p.PacketID = id
	}
	// 属性区（v5）由上层在已知版本后处理；此处按 3.1.1 布局解析，
	// broker 在 v5 会话中先剥离属性再调用 decodePublishV5。
	p.Payload = append([]byte(nil), r.buf[r.pos:]...)
	r.pos = len(r.buf)
	return nil
}

// decodePublishV5 在 v5 会话中解析 PUBLISH（带属性区）。
func (p *Packet) decodePublishV5(r *reader) error {
	p.Dup = p.Header.Flags&0x08 != 0
	p.QoS = (p.Header.Flags >> 1) & 0x03
	p.Retain = p.Header.Flags&0x01 != 0
	if p.QoS == 3 {
		return protoErr("PUBLISH with reserved QoS 3")
	}
	topic, err := r.str()
	if err != nil {
		return err
	}
	p.Topic = topic
	if p.QoS > 0 {
		id, err := r.uint16()
		if err != nil {
			return err
		}
		if id == 0 {
			return protoErr("PUBLISH packet id must not be 0")
		}
		p.PacketID = id
	}
	if err := r.skipProperties(); err != nil {
		return err
	}
	p.Payload = append([]byte(nil), r.buf[r.pos:]...)
	r.pos = len(r.buf)
	return nil
}

func (p *Packet) decodeSubscribe(r *reader) error {
	if p.Header.Flags != 0x02 {
		return protoErr("SUBSCRIBE flags must be 0x2, got %d", p.Header.Flags)
	}
	id, err := r.uint16()
	if err != nil {
		return err
	}
	if id == 0 {
		return protoErr("SUBSCRIBE packet id must not be 0")
	}
	p.PacketID = id
	for r.remaining() > 0 {
		filter, err := r.str()
		if err != nil {
			return err
		}
		qos, err := r.byte()
		if err != nil {
			return err
		}
		if qos > 2 {
			return protoErr("SUBSCRIBE invalid QoS %d", qos)
		}
		p.Subscriptions = append(p.Subscriptions, Subscription{Topic: filter, QoS: qos})
	}
	if len(p.Subscriptions) == 0 {
		return protoErr("SUBSCRIBE with no subscriptions")
	}
	return nil
}

func (p *Packet) decodeUnsubscribe(r *reader) error {
	if p.Header.Flags != 0x02 {
		return protoErr("UNSUBSCRIBE flags must be 0x2, got %d", p.Header.Flags)
	}
	id, err := r.uint16()
	if err != nil {
		return err
	}
	if id == 0 {
		return protoErr("UNSUBSCRIBE packet id must not be 0")
	}
	p.PacketID = id
	for r.remaining() > 0 {
		filter, err := r.str()
		if err != nil {
			return err
		}
		p.Subscriptions = append(p.Subscriptions, Subscription{Topic: filter})
	}
	if len(p.Subscriptions) == 0 {
		return protoErr("UNSUBSCRIBE with no filters")
	}
	return nil
}

// ---------- 编码 ----------

// Encode 将报文编码为字节流。version 决定是否需要 v5 属性区。
func (p *Packet) Encode(version byte) ([]byte, error) {
	var body []byte
	var err error
	flags := p.Header.Flags

	switch p.Header.Type {
	case TypeCONNACK:
		flags = 0
		body, err = p.encodeConnack(version)
	case TypePUBLISH:
		flags = 0
		if p.Dup {
			flags |= 0x08
		}
		flags |= (p.QoS & 0x03) << 1
		if p.Retain {
			flags |= 0x01
		}
		body, err = p.encodePublish(version)
	case TypePUBACK, TypePUBREC, TypePUBREL, TypePUBCOMP:
		if p.Header.Type == TypePUBREL {
			flags = 0x02
		} else {
			flags = 0
		}
		body = p.encodeAck(version)
	case TypeSUBACK:
		flags = 0
		body, err = p.encodeSuback(version)
	case TypeUNSUBACK:
		flags = 0
		body, err = p.encodeUnsuback(version)
	case TypeSUBSCRIBE:
		flags = 0x02
		body, err = p.encodeSubscribe(version)
	case TypeUNSUBSCRIBE:
		flags = 0x02
		body, err = p.encodeUnsubscribe(version)
	case TypePINGREQ, TypePINGRESP:
		flags = 0
		body = nil
	case TypeDISCONNECT:
		flags = 0
		if version == Version5 {
			body = []byte{0x00, 0x00} // reason + properties
		}
	default:
		return nil, fmt.Errorf("encode: unsupported packet type %d", p.Header.Type)
	}
	if err != nil {
		return nil, err
	}

	out := []byte{p.Header.Type<<4 | flags}
	out = appendVarint(out, len(body))
	out = append(out, body...)
	return out, nil
}

func appendVarint(dst []byte, n int) []byte {
	for {
		b := byte(n % 128)
		n /= 128
		if n > 0 {
			dst = append(dst, b|0x80)
		} else {
			return append(dst, b)
		}
	}
}

func appendStr(dst []byte, s string) []byte {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], uint16(len(s)))
	dst = append(dst, n[:]...)
	return append(dst, s...)
}

func (p *Packet) encodeConnack(version byte) ([]byte, error) {
	var body []byte
	if p.SessionPresent {
		body = append(body, 0x01)
	} else {
		body = append(body, 0x00)
	}
	body = append(body, p.ReasonCode)
	if version == Version5 {
		body = append(body, 0x00) // empty properties
	}
	return body, nil
}

func (p *Packet) encodePublish(version byte) ([]byte, error) {
	body := appendStr(nil, p.Topic)
	if p.QoS > 0 {
		var n [2]byte
		binary.BigEndian.PutUint16(n[:], p.PacketID)
		body = append(body, n[:]...)
	}
	if version == Version5 {
		body = append(body, 0x00) // empty properties
	}
	return append(body, p.Payload...), nil
}

func (p *Packet) encodeAck(version byte) []byte {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], p.PacketID)
	body := n[:]
	if version == Version5 {
		body = append(body, 0x00, 0x00) // reason success + empty properties
	}
	return body
}

func (p *Packet) encodeSubscribe(version byte) ([]byte, error) {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], p.PacketID)
	body := n[:]
	if version == Version5 {
		body = append(body, 0x00) // empty properties
	}
	for _, s := range p.Subscriptions {
		body = appendStr(body, s.Topic)
		body = append(body, s.QoS)
	}
	return body, nil
}

func (p *Packet) encodeUnsubscribe(version byte) ([]byte, error) {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], p.PacketID)
	body := n[:]
	if version == Version5 {
		body = append(body, 0x00) // empty properties
	}
	for _, s := range p.Subscriptions {
		body = appendStr(body, s.Topic)
	}
	return body, nil
}

func (p *Packet) encodeSuback(version byte) ([]byte, error) {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], p.PacketID)
	body := n[:]
	if version == Version5 {
		body = append(body, 0x00) // empty properties
	}
	for _, s := range p.Subscriptions {
		body = append(body, s.QoS)
	}
	return body, nil
}

func (p *Packet) encodeUnsuback(version byte) ([]byte, error) {
	var n [2]byte
	binary.BigEndian.PutUint16(n[:], p.PacketID)
	body := n[:]
	if version == Version5 {
		body = append(body, 0x00) // empty properties
		for range p.Subscriptions {
			body = append(body, 0x00) // per-filter reason: success
		}
	}
	return body, nil
}
