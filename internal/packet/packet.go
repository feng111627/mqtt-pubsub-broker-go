// Package packet implements a self-contained reader/encoder for the MQTT wire
// protocol (fixed header, remaining length, UTF-8 and binary fields) and the
// concrete packet structures used by the broker and client.
//
// Protocol versions:
//   - MQTT 3.1   ("MQIsdp", level 4)
//   - MQTT 3.1.1 ("MQTT",    level 4)
//   - MQTT 5.0   ("MQTT",    level 5)
package packet

// Type is an MQTT control packet type (high nibble of byte 1).
type Type byte

const (
	TypeRESERVED    Type = 0
	TypeCONNECT     Type = 1
	TypeCONNACK     Type = 2
	TypePUBLISH     Type = 3
	TypePUBACK      Type = 4
	TypePUBREC      Type = 5
	TypePUBREL      Type = 6
	TypePUBCOMP     Type = 7
	TypeSUBSCRIBE   Type = 8
	TypeSUBACK      Type = 9
	TypeUNSUBSCRIBE Type = 10
	TypeUNSUBACK    Type = 11
	TypePINGREQ     Type = 12
	TypePINGRESP    Type = 13
	TypeDISCONNECT  Type = 14
	TypeAUTH        Type = 15
)

// String returns the MQTT name of the control packet type.
func (t Type) String() string {
	switch t {
	case TypeRESERVED:
		return "RESERVED0"
	case TypeCONNECT:
		return "CONNECT"
	case TypeCONNACK:
		return "CONNACK"
	case TypePUBLISH:
		return "PUBLISH"
	case TypePUBACK:
		return "PUBACK"
	case TypePUBREC:
		return "PUBREC"
	case TypePUBREL:
		return "PUBREL"
	case TypePUBCOMP:
		return "PUBCOMP"
	case TypeSUBSCRIBE:
		return "SUBSCRIBE"
	case TypeSUBACK:
		return "SUBACK"
	case TypeUNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case TypeUNSUBACK:
		return "UNSUBACK"
	case TypePINGREQ:
		return "PINGREQ"
	case TypePINGRESP:
		return "PINGRESP"
	case TypeDISCONNECT:
		return "DISCONNECT"
	case TypeAUTH:
		return "AUTH"
	default:
		return "UNKNOWN"
	}
}

// QoS levels.
const (
	QoS0 byte = 0
	QoS1 byte = 1
	QoS2 byte = 2
)

// Protocol version indicators.
const (
	V31  byte = 3 // "MQIsdp"
	V311 byte = 4 // "MQTT"
	V50  byte = 5
)

// VersionName returns the human-readable name of a protocol level.
func VersionName(level byte) string {
	switch level {
	case V31:
		return "MQTT 3.1"
	case V311:
		return "MQTT 3.1.1"
	case V50:
		return "MQTT 5.0"
	default:
		return "unknown"
	}
}

// FixedHeader holds the first 1..5 bytes of every control packet.
type FixedHeader struct {
	Type   Type
	Flags  byte
	Remain int
}

// DUP flag accessor helpers for PUBLISH flags (bit 3).
func (h FixedHeader) Dup() bool    { return h.Flags&0x08 != 0 }
func (h FixedHeader) QoS() byte    { return (h.Flags >> 1) & 0x03 }
func (h FixedHeader) Retain() bool { return h.Flags&0x01 != 0 }

// Packet is the unified in-memory representation of every MQTT packet.
// Fields not applicable to a given type are left at zero values.
type Packet struct {
	Fixed FixedHeader

	// Level is the protocol version used to decode/encode this packet.
	Level byte

	// CONNECT
	ProtoName  string
	ProtoLevel byte
	CleanStart bool
	WillFlag   bool
	WillQoS    byte
	WillRetain bool
	HasUser    bool
	HasPass    bool
	KeepAlive  uint16
	ClientID   string
	WillTopic  string
	WillMsg    []byte
	WillProps  Properties // MQTT 5.0 will properties
	Username   string
	Password   []byte
	Props      Properties // CONNECT/PUBLISH/outgoing reason properties in 5.0

	// CONNACK
	SessionPresent bool
	ReasonCode     byte

	// PUBLISH
	Topic    string
	Payload  []byte
	PacketID uint16

	// SUBSCRIBE / UNSUBACK property list
	SubID int // subscription identifier (v5), -1 when absent

	// SUBACK
	ReturnCodes []byte // per-filter granted QoS (3.1.1) or reason codes (5.0)

	// Filters for SUBSCRIBE/UNSUBSCRIBE
	Filters []SubFilter

	// DISCONNECT / AUTH (v5)
	Reason byte
}

// SubFilter is one topic filter in a SUBSCRIBE/UNSUBSCRIBE packet plus the
// requested maximum QoS and (v5) subscription options.
type SubFilter struct {
	Filter string
	QoS    byte
	// v5 options
	NoLocal        bool
	RetainAsPub    bool
	RetainHandling byte
}

// IsResponse reports whether the packet type is a server-to-client or
// client-to-server acknowledgement/response.
func (p *Packet) IsResponse() bool {
	switch p.Fixed.Type {
	case TypeCONNACK, TypePUBACK, TypePUBREC, TypePUBREL, TypePUBCOMP,
		TypeSUBACK, TypeUNSUBACK, TypePINGRESP, TypeAUTH:
		return true
	default:
		return false
	}
}
