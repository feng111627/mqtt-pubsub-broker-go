package packet

import (
	"github.com/feng111627/mqtt-pubsub-broker-go/internal/mqerr"
)

// Property identifiers defined by MQTT 5.0.
const (
	PropPayloadFormat    uint32 = 1
	PropMessageExpiry    uint32 = 2
	PropContentType      uint32 = 3
	PropResponseTopic    uint32 = 8
	PropCorrelationData  uint32 = 9
	PropSubscriptionID   uint32 = 11
	PropSessionExpiry    uint32 = 17
	PropAssignedClientID uint32 = 18
	PropServerKeepAlive  uint32 = 19
	PropAuthMethod       uint32 = 21
	PropAuthData         uint32 = 22
	PropRequestProblem   uint32 = 23
	PropWillDelay        uint32 = 24
	PropRequestResponse  uint32 = 25
	PropResponseInfo     uint32 = 26
	PropServerReference  uint32 = 28
	PropReasonString     uint32 = 31
	PropReceiveMaximum   uint32 = 33
	PropTopicAliasMax    uint32 = 34
	PropTopicAlias       uint32 = 35
	PropMaxQoS           uint32 = 36
	PropRetainAvailable  uint32 = 37
	PropUserProperty     uint32 = 38
	PropMaxPacketSize    uint32 = 39
	PropWildcardSubAvail uint32 = 40
	PropSubIDAvailable   uint32 = 41
	PropSharedSubAvail   uint32 = 42
)

// Properties is the decoded set of MQTT 5.0 properties. Properties that may
// legally appear multiple times (user property) are collected; others use a
// pointer/zero value to distinguish "absent". Unrecognised identifiers are
// skipped without aborting the connection (forward compatibility), while
// structurally malformed property bodies remain hard errors.
type Properties struct {
	SessionExpiry    *uint32
	ReceiveMaximum   *uint16
	MaxPacketSize    *uint32
	TopicAliasMax    *uint16
	RequestResponse  *bool
	RequestProblem   *bool
	UserProps        [][2]string
	ReasonString     string
	PayloadFormat    *byte
	MessageExpiry    *uint32
	ContentType      string
	ResponseTopic    string
	CorrelationData  []byte
	SubscriptionID   int // -1 when absent
	WillDelay        *uint32
	AssignedClientID string
	ServerKeepAlive  *uint16
	AuthMethod       string
	AuthData         []byte
	MaxQoS           *byte
	RetainAvailable  *byte
	WildcardAvail    *byte
	SubIDAvailable   *byte
	SharedAvail      *byte
	ServerReference  string
	ResponseInfo     string
	TopicAlias       *uint16
	// hasReasonString / plain-string-presence bookkeeping
	hasReasonString bool
}

// NewProperties returns a Properties with absent optional values initialised.
func NewProperties() Properties { return Properties{SubscriptionID: -1} }

// readProperties decodes a property block after its length varint has NOT yet
// been consumed: this reads length then body.
func readProperties(buf *buffer) (Properties, error) {
	p := NewProperties()
	length, _, err := buf.varint()
	if err != nil {
		return p, err
	}
	body, err := buf.bytesN(int(length))
	if err != nil {
		return p, err
	}
	sub := &buffer{b: body}
	for sub.remaining() > 0 {
		id, _, err := sub.varint()
		if err != nil {
			return p, err
		}
		if err := readOneProperty(&p, sub, id); err != nil {
			return p, err
		}
	}
	return p, nil
}

func boolPtr(v bool) *bool { return &v }

func readOneProperty(p *Properties, sub *buffer, id uint32) error {
	switch id {
	case PropPayloadFormat:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		if v > 1 {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "Payload Format Indicator must be 0 or 1")
		}
		p.PayloadFormat = &v
	case PropMessageExpiry:
		v, err := sub.u32()
		if err != nil {
			return err
		}
		p.MessageExpiry = &v
	case PropContentType:
		s, err := sub.stringValidated()
		if err != nil {
			return err
		}
		p.ContentType = s
	case PropResponseTopic:
		s, err := sub.stringValidated()
		if err != nil {
			return err
		}
		if !ValidTopicName(s) {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeTopicNameInvalid, "Response Topic invalid: %q", s)
		}
		p.ResponseTopic = s
	case PropCorrelationData:
		v, err := sub.binary()
		if err != nil {
			return err
		}
		p.CorrelationData = v
	case PropSubscriptionID:
		v, _, err := sub.varint()
		if err != nil {
			return err
		}
		if v == 0 {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "Subscription Identifier must not be 0")
		}
		p.SubscriptionID = int(v)
	case PropSessionExpiry:
		v, err := sub.u32()
		if err != nil {
			return err
		}
		p.SessionExpiry = &v
	case PropAssignedClientID:
		s, err := sub.stringValidated()
		if err != nil {
			return err
		}
		p.AssignedClientID = s
	case PropServerKeepAlive:
		v, err := sub.u16()
		if err != nil {
			return err
		}
		p.ServerKeepAlive = &v
	case PropAuthMethod:
		s, err := sub.stringValidated()
		if err != nil {
			return err
		}
		p.AuthMethod = s
	case PropAuthData:
		v, err := sub.binary()
		if err != nil {
			return err
		}
		p.AuthData = v
	case PropRequestProblem:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		p.RequestProblem = boolPtr(v == 1)
	case PropWillDelay:
		v, err := sub.u32()
		if err != nil {
			return err
		}
		p.WillDelay = &v
	case PropRequestResponse:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		p.RequestResponse = boolPtr(v == 1)
	case PropResponseInfo:
		s, err := sub.stringValidated()
		if err != nil {
			return err
		}
		p.ResponseInfo = s
	case PropServerReference:
		s, err := sub.stringValidated()
		if err != nil {
			return err
		}
		p.ServerReference = s
	case PropReasonString:
		s, err := sub.stringValidated()
		if err != nil {
			return err
		}
		p.ReasonString = s
		p.hasReasonString = true
	case PropReceiveMaximum:
		v, err := sub.u16()
		if err != nil {
			return err
		}
		if v == 0 {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "Receive Maximum must not be 0")
		}
		p.ReceiveMaximum = &v
	case PropTopicAliasMax:
		v, err := sub.u16()
		if err != nil {
			return err
		}
		p.TopicAliasMax = &v
	case PropTopicAlias:
		v, err := sub.u16()
		if err != nil {
			return err
		}
		if v == 0 {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeTopicAliasInvalid, "Topic Alias must not be 0")
		}
		p.TopicAlias = &v
	case PropMaxQoS:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		if v > 2 {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "Maximum QoS must be 0, 1 or 2")
		}
		p.MaxQoS = &v
	case PropRetainAvailable:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		p.RetainAvailable = &v
	case PropMaxPacketSize:
		v, err := sub.u32()
		if err != nil {
			return err
		}
		if v == 0 {
			return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "Maximum Packet Size must not be 0")
		}
		p.MaxPacketSize = &v
	case PropWildcardSubAvail:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		p.WildcardAvail = &v
	case PropSubIDAvailable:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		p.SubIDAvailable = &v
	case PropSharedSubAvail:
		v, err := sub.byte()
		if err != nil {
			return err
		}
		p.SharedAvail = &v
	case PropUserProperty:
		k, err := sub.stringValidated()
		if err != nil {
			return err
		}
		v, err := sub.stringValidated()
		if err != nil {
			return err
		}
		p.UserProps = append(p.UserProps, [2]string{k, v})
	default:
		// Unknown property: per MQTT 5.0 forward compatibility rules the
		// behaviour depends on context; for an implementation that parses the
		// whole block we cannot skip an unknown-typed value safely. Allocate a
		// protocol error with a distinguishable reason.
		return mqerr.New(mqerr.ClassProtocol, mqerr.CodeProtocolError, "unknown property identifier %d", id)
	}
	return nil
}

// encodeProperties appends the property block (length-prefixed) to dst,
// emitting only present values.
func encodeProperties(dst []byte, p Properties) []byte {
	var body []byte
	put := func(id uint32, enc []byte) {
		body = encodeVarint(body, id)
		body = append(body, enc...)
	}
	if p.PayloadFormat != nil {
		put(PropPayloadFormat, []byte{*p.PayloadFormat})
	}
	if p.MessageExpiry != nil {
		put(PropMessageExpiry, putU32(nil, *p.MessageExpiry))
	}
	if p.ContentType != "" {
		put(PropContentType, putString(nil, p.ContentType))
	}
	if p.ResponseTopic != "" {
		put(PropResponseTopic, putString(nil, p.ResponseTopic))
	}
	if p.CorrelationData != nil {
		put(PropCorrelationData, putBinary(nil, p.CorrelationData))
	}
	if p.SubscriptionID > 0 {
		body = encodeVarint(body, PropSubscriptionID)
		body = encodeVarint(body, uint32(p.SubscriptionID))
	}
	if p.SessionExpiry != nil {
		put(PropSessionExpiry, putU32(nil, *p.SessionExpiry))
	}
	if p.AssignedClientID != "" {
		put(PropAssignedClientID, putString(nil, p.AssignedClientID))
	}
	if p.ServerKeepAlive != nil {
		put(PropServerKeepAlive, putU16(nil, *p.ServerKeepAlive))
	}
	if p.AuthMethod != "" {
		put(PropAuthMethod, putString(nil, p.AuthMethod))
	}
	if p.AuthData != nil {
		put(PropAuthData, putBinary(nil, p.AuthData))
	}
	if p.RequestProblem != nil {
		v := byte(0)
		if *p.RequestProblem {
			v = 1
		}
		put(PropRequestProblem, []byte{v})
	}
	if p.WillDelay != nil {
		put(PropWillDelay, putU32(nil, *p.WillDelay))
	}
	if p.RequestResponse != nil {
		v := byte(0)
		if *p.RequestResponse {
			v = 1
		}
		put(PropRequestResponse, []byte{v})
	}
	if p.ResponseInfo != "" {
		put(PropResponseInfo, putString(nil, p.ResponseInfo))
	}
	if p.ServerReference != "" {
		put(PropServerReference, putString(nil, p.ServerReference))
	}
	if p.hasReasonString {
		put(PropReasonString, putString(nil, p.ReasonString))
	}
	if p.ReceiveMaximum != nil {
		put(PropReceiveMaximum, putU16(nil, *p.ReceiveMaximum))
	}
	if p.TopicAliasMax != nil {
		put(PropTopicAliasMax, putU16(nil, *p.TopicAliasMax))
	}
	if p.TopicAlias != nil {
		put(PropTopicAlias, putU16(nil, *p.TopicAlias))
	}
	if p.MaxQoS != nil {
		put(PropMaxQoS, []byte{*p.MaxQoS})
	}
	if p.RetainAvailable != nil {
		put(PropRetainAvailable, []byte{*p.RetainAvailable})
	}
	if p.MaxPacketSize != nil {
		put(PropMaxPacketSize, putU32(nil, *p.MaxPacketSize))
	}
	if p.WildcardAvail != nil {
		put(PropWildcardSubAvail, []byte{*p.WildcardAvail})
	}
	if p.SubIDAvailable != nil {
		put(PropSubIDAvailable, []byte{*p.SubIDAvailable})
	}
	if p.SharedAvail != nil {
		put(PropSharedSubAvail, []byte{*p.SharedAvail})
	}
	for _, up := range p.UserProps {
		body = encodeVarint(body, PropUserProperty)
		body = putString(body, up[0])
		body = putString(body, up[1])
	}
	dst = encodeVarint(dst, uint32(len(body)))
	return append(dst, body...)
}
