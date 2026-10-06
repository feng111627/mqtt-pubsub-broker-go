package packet

import (
	"io"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
)

// Reader decodes MQTT packets from a transport stream.
type Reader struct {
	r       io.Reader
	maxSize int
	// level is the negotiated protocol level. It is zero until CONNECT has
	// been decoded; SetLevel is called by the handshake handler afterwards so
	// that post-CONNECT packets know whether property sections exist.
	level byte
}

// NewReader builds a Reader. maxSize caps the acceptable remaining length;
// values <= 0 mean "use a 256 MiB hard ceiling".
func NewReader(r io.Reader, maxSize int) *Reader {
	if maxSize <= 0 {
		maxSize = 256 * 1024 * 1024
	}
	return &Reader{r: r, maxSize: maxSize}
}

// SetLevel records the negotiated protocol level (V31/V311/V50).
func (r *Reader) SetLevel(level byte) { r.level = level }

// Level returns the currently negotiated protocol level.
func (r *Reader) Level() byte { return r.level }

func (r *Reader) v5() bool { return r.level == V50 }

// ReadOne decodes exactly one packet.
func (r *Reader) ReadOne() (*Packet, error) {
	hdr, body, err := r.readFrame()
	if err != nil {
		return nil, err
	}
	p := &Packet{Fixed: hdr, Level: r.level}
	buf := &buffer{b: body}
	switch hdr.Type {
	case TypeCONNECT:
		if err := p.decodeConnect(buf); err != nil {
			return nil, err
		}
		r.level = p.ProtoLevel
		p.Level = p.ProtoLevel
	case TypeCONNACK:
		if err := p.decodeConnack(buf, r.level); err != nil {
			return nil, err
		}
	case TypePUBLISH:
		if err := p.decodePublish(buf, r.level); err != nil {
			return nil, err
		}
	case TypePUBACK, TypePUBREC, TypePUBREL, TypePUBCOMP:
		if err := p.decodeAck(buf, r.level); err != nil {
			return nil, err
		}
	case TypeSUBSCRIBE:
		if err := p.decodeSubscribe(buf, r.level); err != nil {
			return nil, err
		}
	case TypeSUBACK:
		if err := p.decodeSuback(buf, r.level); err != nil {
			return nil, err
		}
	case TypeUNSUBSCRIBE:
		if err := p.decodeUnsubscribe(buf, r.level); err != nil {
			return nil, err
		}
	case TypeUNSUBACK:
		if err := p.decodeUnsuback(buf, r.level); err != nil {
			return nil, err
		}
	case TypePINGREQ, TypePINGRESP:
		if hdr.Remain != 0 {
			return nil, errProtocol(mqerr.CodeMalformedPacket, "%s must have zero remaining length", hdr.Type)
		}
	case TypeDISCONNECT:
		if err := p.decodeDisconnect(buf, r.level); err != nil {
			return nil, err
		}
	case TypeAUTH:
		if r.level != V50 {
			return nil, errProtocol(mqerr.CodeProtocolError, "AUTH packets are only valid in MQTT 5.0")
		}
		if err := p.decodeAuth(buf); err != nil {
			return nil, err
		}
	default:
		return nil, errProtocol(mqerr.CodeMalformedPacket, "reserved/unknown packet type %d", hdr.Type)
	}
	if buf.remaining() != 0 {
		return nil, errProtocol(mqerr.CodeMalformedPacket, "%s has %d trailing byte(s)", hdr.Type, buf.remaining())
	}
	return p, nil
}

// readFrame reads fixed header (type+flags, remaining length) and body.
func (r *Reader) readFrame() (FixedHeader, []byte, error) {
	var first [1]byte
	if _, err := io.ReadFull(r.r, first[:]); err != nil {
		return FixedHeader{}, nil, wrapIO(err)
	}
	hdr := FixedHeader{
		Type:  Type(first[0] >> 4),
		Flags: first[0] & 0x0F,
	}
	remain, err := r.readRemainingLen()
	if err != nil {
		return hdr, nil, err
	}
	if remain > r.maxSize {
		return hdr, nil, mqerr.New(mqerr.ClassProtocol, mqerr.CodePacketTooLarge,
			"remaining length %d exceeds maximum %d", remain, r.maxSize)
	}
	hdr.Remain = remain
	body := make([]byte, remain)
	if remain > 0 {
		if _, err := io.ReadFull(r.r, body); err != nil {
			return hdr, nil, wrapIO(err)
		}
	}
	return hdr, body, nil
}

func (r *Reader) readRemainingLen() (int, error) {
	var value uint32
	var mul uint32 = 1
	for i := 0; i < 4; i++ {
		var b [1]byte
		if _, err := io.ReadFull(r.r, b[:]); err != nil {
			return 0, wrapIO(err)
		}
		value += uint32(b[0]&0x7F) * mul
		if b[0]&0x80 == 0 {
			return int(value), nil
		}
		mul *= 128
	}
	return 0, mqerr.New(mqerr.ClassProtocol, mqerr.CodeMalformedPacket, "remaining length field exceeds 4 bytes")
}

func wrapIO(err error) error {
	if err == io.EOF || err == io.ErrUnexpectedEOF {
		return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "connection closed while reading packet: %v", err)
	}
	return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "read failure: %v", err)
}

func errProtocol(code mqerr.Code, format string, args ...any) error {
	return mqerr.New(mqerr.ClassProtocol, code, format, args...)
}

// ---- per-type decoders -------------------------------------------------

func (p *Packet) decodeConnect(b *buffer) error {
	if p.Fixed.Flags != 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "CONNECT fixed header flags must be 0, got %04b", p.Fixed.Flags)
	}
	name, err := b.stringRaw()
	if err != nil {
		return err
	}
	switch name {
	case "MQIsdp", "MQTT":
		p.ProtoName = name
	default:
		return errProtocol(mqerr.CodeUnsupportedProtoV, "unknown protocol name %q", name)
	}
	level, err := b.byte()
	if err != nil {
		return err
	}
	p.ProtoLevel = level
	switch {
	case p.ProtoName == "MQIsdp" && level == V31,
		p.ProtoName == "MQTT" && level == V311,
		p.ProtoName == "MQTT" && level == V50:
	default:
		return mqerr.New(mqerr.ClassProtocol, mqerr.CodeUnsupportedProtoV,
			"unsupported protocol version: name=%q level=%d", p.ProtoName, level)
	}
	flags, err := b.byte()
	if err != nil {
		return err
	}
	p.CleanStart = flags&0x02 != 0
	p.WillFlag = flags&0x04 != 0
	p.WillQoS = (flags >> 3) & 0x03
	p.WillRetain = flags&0x20 != 0
	p.HasUser = flags&0x80 != 0
	p.HasPass = flags&0x40 != 0
	if flags&0x01 != 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "CONNECT reserved bit must be 0")
	}
	if !p.WillFlag && (p.WillQoS != 0 || p.WillRetain) {
		return errProtocol(mqerr.CodeMalformedPacket, "will QoS/retain set without Will Flag")
	}
	if p.WillQoS == 3 {
		return errProtocol(mqerr.CodeMalformedPacket, "will QoS must be 0..2")
	}
	p.KeepAlive, err = b.u16()
	if err != nil {
		return err
	}
	if level == V50 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
		if p.Props.AuthMethod != "" {
			return errProtocol(mqerr.CodeBadAuthMethod, "enhanced authentication is not supported")
		}
		if p.Props.SessionExpiry != nil && *p.Props.SessionExpiry != 0 {
			return errProtocol(mqerr.CodeProtocolError, "non-zero Session Expiry Interval is not supported")
		}
		if p.Props.WillDelay != nil && *p.Props.WillDelay != 0 {
			return errProtocol(mqerr.CodeProtocolError, "non-zero Will Delay Interval is not supported")
		}
		if p.Props.RequestProblem != nil && !*p.Props.RequestProblem {
			return errProtocol(mqerr.CodeProtocolError, "Request Problem Information=0 is not supported")
		}
	}
	clientID, err := b.stringValidated()
	if err != nil {
		return err
	}
	p.ClientID = clientID
	if p.WillFlag {
		if level == V50 {
			p.WillProps, err = readProperties(b)
			if err != nil {
				return err
			}
		}
		p.WillTopic, err = b.stringValidated()
		if err != nil {
			return err
		}
		if !ValidTopicName(p.WillTopic) {
			return errProtocol(mqerr.CodeTopicNameInvalid, "will topic invalid: %q", p.WillTopic)
		}
		p.WillMsg, err = b.binary()
		if err != nil {
			return err
		}
	}
	if p.HasUser {
		p.Username, err = b.stringValidated()
		if err != nil {
			return err
		}
	}
	if p.HasPass {
		p.Password, err = b.binary()
		if err != nil {
			return err
		}
	}
	return nil
}

func (p *Packet) decodeConnack(b *buffer, level byte) error {
	ack, err := b.byte()
	if err != nil {
		return err
	}
	p.SessionPresent = ack&0x01 != 0
	if ack&0xFE != 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "CONNACK Connect Acknowledge Flags has reserved bits set: %08b", ack)
	}
	p.ReasonCode, err = b.byte()
	if err != nil {
		return err
	}
	if level == V50 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
	} else if b.remaining() != 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "CONNACK(3.1.1) has %d trailing bytes", b.remaining())
	}
	return nil
}

func (p *Packet) decodePublish(b *buffer, level byte) error {
	qos := p.Fixed.QoS()
	if qos == 3 {
		return errProtocol(mqerr.CodeMalformedPacket, "PUBLISH QoS must be 0..2")
	}
	if p.Fixed.Dup() && qos == QoS0 {
		return errProtocol(mqerr.CodeMalformedPacket, "DUP=1 is illegal for QoS0 PUBLISH")
	}
	var err error
	p.Topic, err = b.stringValidated()
	if err != nil {
		return err
	}
	if qos > 0 {
		p.PacketID, err = b.u16()
		if err != nil {
			return err
		}
		if p.PacketID == 0 {
			return errProtocol(mqerr.CodeMalformedPacket, "PUBLISH QoS%d packet identifier must not be 0", qos)
		}
	}
	if level == V50 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
		if p.Props.TopicAlias != nil {
			return errProtocol(mqerr.CodeTopicAliasInvalid, "topic aliases are not supported (alias %d)", *p.Props.TopicAlias)
		}
	}
	p.Payload = append([]byte(nil), b.b[b.pos:]...)
	b.pos = len(b.b)
	return nil
}

func (p *Packet) decodeAck(b *buffer, level byte) error {
	// PUBREL has fixed flags 0010 like UNSUBSCRIBE; enforced by the caller's
	// flag validation in handle.
	id, err := b.u16()
	if err != nil {
		return err
	}
	p.PacketID = id
	if level == V50 {
		// reason code and properties are both optional; absent means success.
		if b.remaining() > 0 {
			p.ReasonCode, err = b.byte()
			if err != nil {
				return err
			}
		}
		if b.remaining() > 0 {
			p.Props, err = readProperties(b)
			if err != nil {
				return err
			}
		}
	} else if b.remaining() != 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "%s(3.1.1) has %d trailing bytes", p.Fixed.Type, b.remaining())
	}
	return nil
}

func (p *Packet) decodeSubscribe(b *buffer, level byte) error {
	if p.Fixed.Flags != 0x02 {
		return errProtocol(mqerr.CodeMalformedPacket, "SUBSCRIBE fixed flags must be 0010, got %04b", p.Fixed.Flags)
	}
	var err error
	p.PacketID, err = b.u16()
	if err != nil {
		return err
	}
	if p.PacketID == 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "SUBSCRIBE packet identifier must not be 0")
	}
	if level == V50 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
		p.SubID = p.Props.SubscriptionID
	}
	for b.remaining() > 0 {
		f, err := b.stringValidated()
		if err != nil {
			return err
		}
		opt, err := b.byte()
		if err != nil {
			return err
		}
		qos := opt & 0x03
		if qos == 3 {
			return errProtocol(mqerr.CodeMalformedPacket, "SUBSCRIBE filter %q requests invalid QoS 3", f)
		}
		if opt&0xC0 != 0 {
			return errProtocol(mqerr.CodeMalformedPacket, "SUBSCRIBE filter %q has reserved option bits set", f)
		}
		if level != V50 && opt&0xFC != 0 {
			return errProtocol(mqerr.CodeMalformedPacket, "SUBSCRIBE(3.1.1) filter %q options must be QoS only, got %02x", f, opt)
		}
		sf := SubFilter{Filter: f, QoS: qos}
		if level == V50 {
			sf.NoLocal = opt&0x04 != 0
			sf.RetainAsPub = opt&0x08 != 0
			sf.RetainHandling = (opt >> 4) & 0x03
			if sf.RetainHandling == 3 {
				return errProtocol(mqerr.CodeProtocolError, "SUBSCRIBE filter %q has invalid Retain Handling 3", f)
			}
		}
		p.Filters = append(p.Filters, sf)
	}
	if len(p.Filters) == 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "SUBSCRIBE must contain at least one filter")
	}
	return nil
}

func (p *Packet) decodeSuback(b *buffer, level byte) error {
	var err error
	p.PacketID, err = b.u16()
	if err != nil {
		return err
	}
	if level == V50 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
	} else if b.remaining() == 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "SUBACK must contain at least one return code")
	}
	for b.remaining() > 0 {
		rc, err := b.byte()
		if err != nil {
			return err
		}
		p.ReturnCodes = append(p.ReturnCodes, rc)
	}
	return nil
}

func (p *Packet) decodeUnsubscribe(b *buffer, level byte) error {
	if p.Fixed.Flags != 0x02 {
		return errProtocol(mqerr.CodeMalformedPacket, "UNSUBSCRIBE fixed flags must be 0010, got %04b", p.Fixed.Flags)
	}
	var err error
	p.PacketID, err = b.u16()
	if err != nil {
		return err
	}
	if p.PacketID == 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "UNSUBSCRIBE packet identifier must not be 0")
	}
	if level == V50 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
	}
	for b.remaining() > 0 {
		f, err := b.stringValidated()
		if err != nil {
			return err
		}
		p.Filters = append(p.Filters, SubFilter{Filter: f})
	}
	if len(p.Filters) == 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "UNSUBSCRIBE must contain at least one filter")
	}
	return nil
}

func (p *Packet) decodeUnsuback(b *buffer, level byte) error {
	var err error
	p.PacketID, err = b.u16()
	if err != nil {
		return err
	}
	if level == V50 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
		for b.remaining() > 0 {
			rc, err := b.byte()
			if err != nil {
				return err
			}
			p.ReturnCodes = append(p.ReturnCodes, rc)
		}
	} else if b.remaining() != 0 {
		return errProtocol(mqerr.CodeMalformedPacket, "UNSUBACK(3.1.1) has %d trailing bytes", b.remaining())
	}
	return nil
}

func (p *Packet) decodeDisconnect(b *buffer, level byte) error {
	if b.remaining() == 0 {
		p.Reason = 0
		return nil
	}
	var err error
	p.Reason, err = b.byte()
	if err != nil {
		return err
	}
	if level == V50 && b.remaining() > 0 {
		p.Props, err = readProperties(b)
		if err != nil {
			return err
		}
	}
	if level != V50 {
		return errProtocol(mqerr.CodeMalformedPacket, "DISCONNECT(3.1.1) must have zero remaining length")
	}
	return nil
}

func (p *Packet) decodeAuth(b *buffer) error {
	if b.remaining() == 0 {
		p.Reason = 0
		return nil
	}
	var err error
	p.Reason, err = b.byte()
	if err != nil {
		return err
	}
	p.Props, err = readProperties(b)
	if err != nil {
		return err
	}
	if p.Props.AuthMethod == "" {
		return errProtocol(mqerr.CodeProtocolError, "AUTH requires Authentication Method property")
	}
	return nil
}
