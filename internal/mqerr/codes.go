package mqerr

// Code is a machine readable reason code. MQTT 5.0 reason codes are used with
// their on-the-wire numeric values (0x80+); MQTT 3.1.1 CONNACK codes use the
// low values (0x01..0x05) and are named with the 311 suffix.
type Code byte

const (
	CodeOK Code = 0x00
	// MQTT 5.0 reason codes
	CodeUnspecifiedError       Code = 0x80
	CodeMalformedPacket        Code = 0x81
	CodeProtocolError          Code = 0x82
	CodeImplSpecific           Code = 0x83
	CodeUnsupportedProtoV      Code = 0x84
	CodeNotAuthorized          Code = 0x87
	CodeServerBusy             Code = 0x89
	CodeBadAuthMethod          Code = 0x8C
	CodeKeepAliveTimeout       Code = 0x8D
	CodeSessionTakenOver       Code = 0x8E
	CodeBadUsernameOrPwd       Code = 0x86
	CodeTopicFilterInvalid     Code = 0x8F
	CodeTopicNameInvalid       Code = 0x90
	CodePacketIDInUse          Code = 0x91
	CodePacketIDNotFound       Code = 0x92
	CodeReceiveMaxExceeded     Code = 0x93
	CodeTopicAliasInvalid      Code = 0x94
	CodePacketTooLarge         Code = 0x95
	CodeQuotaExceeded          Code = 0x97
	CodeSharedSubUnsupported   Code = 0x9E
	CodeSubIDUnsupported       Code = 0xA1
	CodeWildcardSubUnsupported Code = 0xA2
	// MQTT 3.1.1 CONNACK return codes
	CodeUnsupportedProto311 Code = 0x01
	CodeIdentifierRejected  Code = 0x02
	CodeServerUnavailable   Code = 0x03
	CodeBadCred311          Code = 0x04
	CodeNotAuthorized311    Code = 0x05
)

// String returns a human readable name of the code.
func (c Code) String() string {
	if n, ok := codeName[c]; ok {
		return n
	}
	return "UnknownCode"
}

var codeName = map[Code]string{
	CodeOK:                     "Success",
	CodeUnspecifiedError:       "UnspecifiedError",
	CodeMalformedPacket:        "MalformedPacket",
	CodeProtocolError:          "ProtocolError",
	CodeImplSpecific:           "ImplementationSpecificError",
	CodeUnsupportedProtoV:      "UnsupportedProtocolVersion",
	CodeNotAuthorized:          "NotAuthorized",
	CodeServerBusy:             "ServerBusy",
	CodeBadAuthMethod:          "BadAuthenticationMethod",
	CodeKeepAliveTimeout:       "KeepAliveTimeout",
	CodeSessionTakenOver:       "SessionTakenOver",
	CodeBadUsernameOrPwd:       "BadUserNameOrPassword",
	CodeTopicFilterInvalid:     "TopicFilterInvalid",
	CodeTopicNameInvalid:       "TopicNameInvalid",
	CodePacketIDInUse:          "PacketIdentifierInUse",
	CodePacketIDNotFound:       "PacketIdentifierNotFound",
	CodeReceiveMaxExceeded:     "ReceiveMaximumExceeded",
	CodeTopicAliasInvalid:      "TopicAliasInvalid",
	CodePacketTooLarge:         "PacketTooLarge",
	CodeQuotaExceeded:          "QuotaExceeded",
	CodeSharedSubUnsupported:   "SharedSubscriptionNotSupported",
	CodeSubIDUnsupported:       "SubscriptionIdentifiersNotSupported",
	CodeWildcardSubUnsupported: "WildcardSubscriptionsNotSupported",
	CodeUnsupportedProto311:    "UnacceptableProtocolVersion(3.1.1)",
	CodeIdentifierRejected:     "IdentifierRejected(3.1.1)",
	CodeServerUnavailable:      "ServerUnavailable(3.1.1)",
	CodeBadCred311:             "BadUserNameOrPassword(3.1.1)",
	CodeNotAuthorized311:       "NotAuthorized(3.1.1)",
}
