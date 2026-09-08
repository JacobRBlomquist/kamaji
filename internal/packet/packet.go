// Package packet implements MQTT 3.1.1 control packet encoding and decoding.
package packet

// Type is the MQTT control packet type, carried in the top 4 bits of the
// fixed header's first byte (MQTT 3.1.1 spec section 2.2.1).
type KamajiType byte

const (
	Reserved0   KamajiType = 0
	CONNECT     KamajiType = 1
	CONNACK     KamajiType = 2
	PUBLISH     KamajiType = 3
	PUBACK      KamajiType = 4
	PUBREC      KamajiType = 5
	PUBREL      KamajiType = 6
	PUBCOMP     KamajiType = 7
	SUBSCRIBE   KamajiType = 8
	SUBACK      KamajiType = 9
	UNSUBSCRIBE KamajiType = 10
	UNSUBACK    KamajiType = 11
	PINGREQ     KamajiType = 12
	PINGRESP    KamajiType = 13
	DISCONNECT  KamajiType = 14
	Reserved15  KamajiType = 15
)

func (t KamajiType) String() string {
	switch t {
	case CONNECT:
		return "CONNECT"
	case CONNACK:
		return "CONNACK"
	case PUBLISH:
		return "PUBLISH"
	case PUBACK:
		return "PUBACK"
	case PUBREC:
		return "PUBREC"
	case PUBREL:
		return "PUBREL"
	case PUBCOMP:
		return "PUBCOMP"
	case SUBSCRIBE:
		return "SUBSCRIBE"
	case SUBACK:
		return "SUBACK"
	case UNSUBSCRIBE:
		return "UNSUBSCRIBE"
	case UNSUBACK:
		return "UNSUBACK"
	case PINGREQ:
		return "PINGREQ"
	case PINGRESP:
		return "PINGRESP"
	case DISCONNECT:
		return "DISCONNECT"
	default:
		return "RESERVED"
	}
}
