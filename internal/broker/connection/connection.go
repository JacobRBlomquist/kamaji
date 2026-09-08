package connection

import (
	"fmt"
	"io"
	"log"
	"net"

	"github.com/JacobRBlomquist/kamaji/internal/packet"
)

type KamajiConnection struct {
	flags           packet.KamajiMQTTFlags
	packetType      packet.KamajiMQTTControlPacketType
	remainingLength int
	conn            net.Conn
}

func (k *KamajiConnection) handleConnection() {
	fmt.Printf("HANDLE %+v\n", k)
}

// parseFixedHeader reads the MQTT fixed header (packet type, flags, and
// remaining length) from conn, per MQTT 3.1.1 spec section 2.2.
func parseFixedHeader(conn net.Conn) (KamajiConnection, error) {
	buf := make([]byte, 1)
	if _, err := io.ReadFull(conn, buf); err != nil {
		return KamajiConnection{}, fmt.Errorf("failed to read first byte: %w", err)
	}

	var firstByte = buf[0]
	var rawControlPacketType = firstByte >> 4
	var rawFlags = firstByte & 0x0f

	var flags packet.KamajiMQTTFlags
	flags.ParseFlags(rawFlags)
	var controlPacketType packet.KamajiMQTTControlPacketType = packet.KamajiMQTTControlPacketType(rawControlPacketType)

	remainingLength, err := packet.ReadRemainingLength(conn)
	if err != nil {
		return KamajiConnection{}, fmt.Errorf("failed to read remaining length: %w", err)
	}

	return KamajiConnection{flags, controlPacketType, remainingLength, conn}, nil
}

func SetUpConnection(conn net.Conn) {
	connection, err := parseFixedHeader(conn)
	if err != nil {
		log.Printf("%v", err)
		return
	}
	connection.handleConnection()
}
