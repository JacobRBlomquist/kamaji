package connection

import (
	"io"
	"log"
	"net"
)

func HandleConnection(conn net.Conn) {
	buf := make([]byte, 1)
	for {
		n, err := io.ReadFull(conn, buf)
		if err != nil {
			log.Printf("Failed to read: %v", err.Error())
			break
		}
		log.Printf("Read %v bytes", n)

		log.Printf("READ: %v 0x%x", buf[0], buf[0])
	}
}
