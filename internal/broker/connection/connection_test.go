package connection

import (
	"net"
	"reflect"
	"testing"

	"github.com/JacobRBlomquist/kamaji/internal/packet"
)

// pipeWith returns the server side of a net.Pipe after writing b on the
// client side in the background, so parseFixedHeader can read it.
func pipeWith(t *testing.T, b []byte) net.Conn {
	t.Helper()
	client, server := net.Pipe()
	go func() {
		client.Write(b)
		client.Close()
	}()
	t.Cleanup(func() { server.Close() })
	return server
}

func TestParseFixedHeader(t *testing.T) {
	cases := []struct {
		name       string
		in         []byte
		wantType   packet.KamajiMQTTControlPacketType
		wantRemLen int
	}{
		{"CONNECT, no flags, empty payload", []byte{0x10, 0x00}, packet.CONNECT, 0},
		{"PUBLISH with dup+qos1+retain, 2 byte remaining length", []byte{0x3b, 0x80, 0x01}, packet.PUBLISH, 128},
		{"PINGREQ", []byte{0xc0, 0x00}, packet.PINGREQ, 0},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			conn := pipeWith(t, c.in)
			got, err := parseFixedHeader(conn)
			if err != nil {
				t.Fatalf("parseFixedHeader() returned unexpected error: %v", err)
			}
			if got.packetType != c.wantType {
				t.Errorf("packetType = %v, want %v", got.packetType, c.wantType)
			}
			if got.remainingLength != c.wantRemLen {
				t.Errorf("remainingLength = %d, want %d", got.remainingLength, c.wantRemLen)
			}
			if got.conn != conn {
				t.Errorf("conn = %v, want the connection passed in", got.conn)
			}
		})
	}
}

func TestParseFixedHeaderFlags(t *testing.T) {
	// 0x3b = PUBLISH (3) << 4 | flags 0xb (1011: dup=1, qos=01, retain=1)
	conn := pipeWith(t, []byte{0x3b, 0x00})
	got, err := parseFixedHeader(conn)
	if err != nil {
		t.Fatalf("parseFixedHeader() returned unexpected error: %v", err)
	}

	var want packet.KamajiMQTTFlags
	want.ParseFlags(0x0b)
	if !reflect.DeepEqual(got.flags, want) {
		t.Errorf("flags = %+v, want %+v", got.flags, want)
	}
}

func TestParseFixedHeaderNoBytes(t *testing.T) {
	client, server := net.Pipe()
	client.Close()

	_, err := parseFixedHeader(server)
	if err == nil {
		t.Fatal("expected error reading from a closed connection, got nil")
	}
}

func TestParseFixedHeaderMalformedRemainingLength(t *testing.T) {
	conn := pipeWith(t, []byte{0x10, 0xff, 0xff, 0xff, 0xff})
	_, err := parseFixedHeader(conn)
	if err == nil {
		t.Fatal("expected error for malformed remaining length, got nil")
	}
}

func TestParseFixedHeaderTruncatedRemainingLength(t *testing.T) {
	// First byte present, but the connection closes before the
	// continuation byte of the remaining length arrives.
	conn := pipeWith(t, []byte{0x10, 0x80})
	_, err := parseFixedHeader(conn)
	if err == nil {
		t.Fatal("expected error for truncated remaining length, got nil")
	}
}
