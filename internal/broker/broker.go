// Package broker implements a standalone MQTT 3.1.1 broker.
package broker

import (
	"log"
	"net"

	"github.com/JacobRBlomquist/kamaji/internal/broker/connection"
)

// Broker accepts client connections and routes MQTT packets between them.
type Broker struct {
	addr string
}

// New returns a Broker that will listen on addr (e.g. ":1883").
func New(addr string) *Broker {
	return &Broker{addr: addr}
}

// ListenAndServe opens addr and accepts client connections until the
// listener is closed or accept fails.
func (b *Broker) ListenAndServe() error {
	ln, err := net.Listen("tcp", b.addr)
	if err != nil {
		return err
	}
	defer ln.Close()

	for {
		conn, err := ln.Accept()
		if err != nil {
			return err
		}
		go b.handleConn(conn)
	}
}

func (b *Broker) handleConn(conn net.Conn) {
	defer conn.Close()
	log.Printf("accepted connection from %s", conn.RemoteAddr())
	connection.SetUpConnection(conn)
}
