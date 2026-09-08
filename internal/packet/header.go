// Package packet implements MQTT 3.1.1 fixed header encoding and decoding.
package packet

import (
	"errors"
	"io"
)

// KamajiMQTTControlPacketType is the MQTT control packet type, carried in
// the top 4 bits of the fixed header's first byte (MQTT 3.1.1 spec section
// 2.2.1).
type KamajiMQTTControlPacketType byte

const (
	Reserved0   KamajiMQTTControlPacketType = 0
	CONNECT     KamajiMQTTControlPacketType = 1
	CONNACK     KamajiMQTTControlPacketType = 2
	PUBLISH     KamajiMQTTControlPacketType = 3
	PUBACK      KamajiMQTTControlPacketType = 4
	PUBREC      KamajiMQTTControlPacketType = 5
	PUBREL      KamajiMQTTControlPacketType = 6
	PUBCOMP     KamajiMQTTControlPacketType = 7
	SUBSCRIBE   KamajiMQTTControlPacketType = 8
	SUBACK      KamajiMQTTControlPacketType = 9
	UNSUBSCRIBE KamajiMQTTControlPacketType = 10
	UNSUBACK    KamajiMQTTControlPacketType = 11
	PINGREQ     KamajiMQTTControlPacketType = 12
	PINGRESP    KamajiMQTTControlPacketType = 13
	DISCONNECT  KamajiMQTTControlPacketType = 14
	Reserved15  KamajiMQTTControlPacketType = 15
)

func (t KamajiMQTTControlPacketType) String() string {
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

// KamajiMQTTFlags holds the fixed header flags nibble (the low 4 bits of the
// header's first byte, MQTT 3.1.1 spec section 2.2.2).
type KamajiMQTTFlags struct {
	dup    bool
	qos    int8
	retain bool
}

// ParseFlags decodes the fixed header flags nibble.
func (f *KamajiMQTTFlags) ParseFlags(flags byte) {
	f.dup = (flags & 0x8) != 0
	f.qos = int8((flags >> 1) & 0x03)
	f.retain = (flags & 0x1) != 0
}

// MaxRemainingLength is the largest value representable by the MQTT 3.1.1
// variable byte integer encoding (four 7-bit groups), spec section 2.2.3.
const MaxRemainingLength = 268435455 // 0x0FFFFFFF

// ErrMalformedRemainingLength indicates the continuation bit was still set
// on the fourth byte, which the spec forbids (section 2.2.3).
var ErrMalformedRemainingLength = errors.New("malformed remaining length: exceeds 4 bytes")

// ReadRemainingLength decodes the fixed header's Remaining Length field from
// r: up to four bytes, each carrying 7 bits of value plus a continuation bit
// in the MSB (MQTT 3.1.1 spec section 2.2.3).
func ReadRemainingLength(r io.Reader) (int, error) {
	buf := make([]byte, 1)
	value := 0
	multiplier := 1

	for i := 0; i < 4; i++ {
		if _, err := io.ReadFull(r, buf); err != nil {
			return 0, err
		}

		encodedByte := buf[0]
		value += int(encodedByte&0x7f) * multiplier

		if encodedByte&0x80 == 0 {
			return value, nil
		}

		multiplier *= 128
	}

	return 0, ErrMalformedRemainingLength
}
