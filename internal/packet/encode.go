package packet

import (
	"io"

	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
)

// Encode returns the packet's wire bytes for the given protocol level.
func (p *Packet) Encode(level byte) ([]byte, error) {
	var varHeader []byte
	var payload []byte
	first := byte(p.Fixed.Type) << 4
	var flags byte

	switch p.Fixed.Type {
	case TypeCONNECT:
		varHeader, payload = p.buildConnect(level)
		flags = 0
	case TypeCONNACK:
		varHeader = p.buildConnack(level)
	case TypePUBLISH:
		varHeader, payload = p.buildPublish(level)
		flags = p.Fixed.Flags // caller sets DUP/QoS/RETAIN bits
	case TypePUBACK, TypePUBREC, TypePUBREL, TypePUBCOMP:
		varHeader = p.buildAck(level)
		if p.Fixed.Type == TypePUBREL {
			flags = 0x02
		}
	case TypeSUBSCRIBE:
		varHeader, payload = p.buildSubscribe(level)
		flags = 0x02
	case TypeSUBACK:
		varHeader, payload = p.buildSuback(level)
	case TypeUNSUBSCRIBE:
		varHeader, payload = p.buildUnsubscribe(level)
		flags = 0x02
	case TypeUNSUBACK:
		varHeader, payload = p.buildUnsuback(level)
	case TypePINGREQ, TypePINGRESP:
		// empty body
	case TypeDISCONNECT:
		varHeader = p.buildDisconnect(level)
	case TypeAUTH:
		if level != V50 {
			return nil, mqerr.New(mqerr.ClassState, mqerr.CodeProtocolError, "AUTH requires MQTT 5.0")
		}
		varHeader = p.buildAuth()
	default:
		return nil, mqerr.New(mqerr.ClassState, mqerr.CodeProtocolError, "cannot encode packet type %d", p.Fixed.Type)
	}

	remain := len(varHeader) + len(payload)
	if remain > 268435455 {
		return nil, mqerr.New(mqerr.ClassState, mqerr.CodePacketTooLarge, "packet too large: %d bytes", remain)
	}
	out := make([]byte, 0, 1+varintLen(uint32(remain))+remain)
	out = append(out, first|flags)
	out = encodeVarint(out, uint32(remain))
	out = append(out, varHeader...)
	out = append(out, payload...)
	return out, nil
}

// WriteTo writes the packet's wire bytes to w.
func (p *Packet) WriteTo(w io.Writer, level byte) error {
	b, err := p.Encode(level)
	if err != nil {
		return err
	}
	for len(b) > 0 {
		n, err := w.Write(b)
		if err != nil {
			return mqerr.New(mqerr.ClassConnection, mqerr.CodeImplSpecific, "write failure: %v", err)
		}
		b = b[n:]
	}
	return nil
}

func (p *Packet) buildConnect(level byte) (vh, payload []byte) {
	name := "MQTT"
	if level == V31 {
		name = "MQIsdp"
	}
	vh = putString(nil, name)
	vh = append(vh, level)
	var flags byte
	if p.CleanStart {
		flags |= 0x02
	}
	if p.WillFlag {
		flags |= 0x04
		flags |= (p.WillQoS & 0x03) << 3
		if p.WillRetain {
			flags |= 0x20
		}
	}
	if p.HasUser {
		flags |= 0x80
	}
	if p.HasPass {
		flags |= 0x40
	}
	vh = append(vh, flags)
	vh = putU16(vh, p.KeepAlive)
	if level == V50 {
		vh = encodeProperties(vh, p.Props)
	}
	payload = putString(payload, p.ClientID)
	if p.WillFlag {
		if level == V50 {
			payload = encodeProperties(payload, p.WillProps)
		}
		payload = putString(payload, p.WillTopic)
		payload = putBinary(payload, p.WillMsg)
	}
	if p.HasUser {
		payload = putString(payload, p.Username)
	}
	if p.HasPass {
		payload = putBinary(payload, p.Password)
	}
	return vh, payload
}

func (p *Packet) buildConnack(level byte) []byte {
	var flags byte
	if p.SessionPresent {
		flags = 0x01
	}
	out := []byte{flags, p.ReasonCode}
	if level == V50 {
		out = encodeProperties(out, p.Props)
	}
	return out
}

func (p *Packet) buildPublish(level byte) (vh, payload []byte) {
	vh = putString(nil, p.Topic)
	if p.Fixed.QoS() > 0 {
		vh = putU16(vh, p.PacketID)
	}
	if level == V50 {
		vh = encodeProperties(vh, p.Props)
	}
	return vh, append([]byte(nil), p.Payload...)
}

func (p *Packet) buildAck(level byte) []byte {
	out := putU16(nil, p.PacketID)
	if level == V50 {
		// Emit reason code + properties when they carry information, otherwise
		// the compact two-byte form is legal and preferred.
		if p.ReasonCode != 0 || len(p.Props.UserProps) > 0 || p.Props.hasReasonString {
			out = append(out, p.ReasonCode)
			out = encodeProperties(out, p.Props)
		}
	}
	return out
}

func (p *Packet) buildSubscribe(level byte) (vh, payload []byte) {
	vh = putU16(nil, p.PacketID)
	if level == V50 {
		vh = encodeProperties(vh, p.Props)
	}
	for _, f := range p.Filters {
		payload = putString(payload, f.Filter)
		var opt byte = f.QoS & 0x03
		if level == V50 {
			if f.NoLocal {
				opt |= 0x04
			}
			if f.RetainAsPub {
				opt |= 0x08
			}
			opt |= (f.RetainHandling & 0x03) << 4
		}
		payload = append(payload, opt)
	}
	return vh, payload
}

func (p *Packet) buildSuback(level byte) (vh, payload []byte) {
	vh = putU16(nil, p.PacketID)
	if level == V50 {
		vh = encodeProperties(vh, p.Props)
	}
	payload = append(payload, p.ReturnCodes...)
	return vh, payload
}

func (p *Packet) buildUnsubscribe(level byte) (vh, payload []byte) {
	vh = putU16(nil, p.PacketID)
	if level == V50 {
		vh = encodeProperties(vh, p.Props)
	}
	for _, f := range p.Filters {
		payload = putString(payload, f.Filter)
	}
	return vh, payload
}

func (p *Packet) buildUnsuback(level byte) (vh, payload []byte) {
	vh = putU16(nil, p.PacketID)
	if level == V50 {
		vh = encodeProperties(vh, p.Props)
		payload = append(payload, p.ReturnCodes...)
	}
	return vh, payload
}

func (p *Packet) buildDisconnect(level byte) []byte {
	if level != V50 {
		return nil
	}
	if p.Reason == 0 && len(p.Props.UserProps) == 0 && !p.Props.hasReasonString {
		return nil // compact form
	}
	out := []byte{p.Reason}
	out = encodeProperties(out, p.Props)
	return out
}

func (p *Packet) buildAuth() []byte {
	if p.Reason == 0 && p.Props.AuthMethod == "" {
		return nil
	}
	out := []byte{p.Reason}
	out = encodeProperties(out, p.Props)
	return out
}
