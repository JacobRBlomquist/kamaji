package packet

import (
	"bytes"
	"errors"
	"testing"
)

func TestControlPacketTypeString(t *testing.T) {
	cases := []struct {
		in   KamajiMQTTControlPacketType
		want string
	}{
		{Reserved0, "RESERVED"},
		{CONNECT, "CONNECT"},
		{CONNACK, "CONNACK"},
		{PUBLISH, "PUBLISH"},
		{PUBACK, "PUBACK"},
		{PUBREC, "PUBREC"},
		{PUBREL, "PUBREL"},
		{PUBCOMP, "PUBCOMP"},
		{SUBSCRIBE, "SUBSCRIBE"},
		{SUBACK, "SUBACK"},
		{UNSUBSCRIBE, "UNSUBSCRIBE"},
		{UNSUBACK, "UNSUBACK"},
		{PINGREQ, "PINGREQ"},
		{PINGRESP, "PINGRESP"},
		{DISCONNECT, "DISCONNECT"},
		{Reserved15, "RESERVED"},
		{KamajiMQTTControlPacketType(99), "RESERVED"},
	}

	for _, c := range cases {
		if got := c.in.String(); got != c.want {
			t.Errorf("KamajiMQTTControlPacketType(%d).String() = %q, want %q", c.in, got, c.want)
		}
	}
}

func TestParseFlags(t *testing.T) {
	cases := []struct {
		name       string
		raw        byte
		wantDup    bool
		wantQos    int8
		wantRetain bool
	}{
		{"all clear", 0x0, false, 0, false},
		{"retain only", 0x1, false, 0, true},
		{"qos 1", 0x2, false, 1, false},
		{"qos 2", 0x4, false, 2, false},
		{"dup only", 0x8, true, 0, false},
		{"dup + qos2 + retain", 0xd, true, 2, true},
		{"upper bits ignored", 0xf1, false, 0, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			var f KamajiMQTTFlags
			f.ParseFlags(c.raw)
			if f.dup != c.wantDup {
				t.Errorf("dup = %v, want %v", f.dup, c.wantDup)
			}
			if f.qos != c.wantQos {
				t.Errorf("qos = %v, want %v", f.qos, c.wantQos)
			}
			if f.retain != c.wantRetain {
				t.Errorf("retain = %v, want %v", f.retain, c.wantRetain)
			}
		})
	}
}

func TestReadRemainingLength(t *testing.T) {
	cases := []struct {
		name    string
		in      []byte
		want    int
		wantErr bool
	}{
		{"zero", []byte{0x00}, 0, false},
		{"one byte max", []byte{0x7f}, 127, false},
		{"two byte min", []byte{0x80, 0x01}, 128, false},
		{"two byte max", []byte{0xff, 0x7f}, 16383, false},
		{"three byte min", []byte{0x80, 0x80, 0x01}, 16384, false},
		{"three byte max", []byte{0xff, 0xff, 0x7f}, 2097151, false},
		{"four byte min", []byte{0x80, 0x80, 0x80, 0x01}, 2097152, false},
		{"four byte max", []byte{0xff, 0xff, 0xff, 0x7f}, MaxRemainingLength, false},
		{"malformed, 5th byte still has continuation bit", []byte{0xff, 0xff, 0xff, 0xff}, 0, true},
		{"truncated", []byte{0x80}, 0, true},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			got, err := ReadRemainingLength(bytes.NewReader(c.in))
			if c.wantErr {
				if err == nil {
					t.Fatalf("ReadRemainingLength(%x) = %d, nil, want error", c.in, got)
				}
				return
			}
			if err != nil {
				t.Fatalf("ReadRemainingLength(%x) returned unexpected error: %v", c.in, err)
			}
			if got != c.want {
				t.Errorf("ReadRemainingLength(%x) = %d, want %d", c.in, got, c.want)
			}
		})
	}
}

func TestReadRemainingLengthMalformedErrorType(t *testing.T) {
	_, err := ReadRemainingLength(bytes.NewReader([]byte{0xff, 0xff, 0xff, 0xff}))
	if !errors.Is(err, ErrMalformedRemainingLength) {
		t.Errorf("expected ErrMalformedRemainingLength, got %v", err)
	}
}

func TestReadRemainingLengthOnlyConsumesItsOwnBytes(t *testing.T) {
	r := bytes.NewReader([]byte{0x01, 0xAB})
	got, err := ReadRemainingLength(r)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if got != 1 {
		t.Fatalf("got %d, want 1", got)
	}
	rest := make([]byte, 1)
	if _, err := r.Read(rest); err != nil {
		t.Fatalf("failed to read remaining byte: %v", err)
	}
	if rest[0] != 0xAB {
		t.Errorf("ReadRemainingLength over-consumed the reader, next byte = %x, want %x", rest[0], 0xAB)
	}
}
