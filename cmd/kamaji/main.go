// Command kamaji runs a standalone MQTT 3.1.1 broker.
package main

import (
	"flag"
	"log"

	"github.com/JacobRBlomquist/kamaji/internal/broker"
)

func main() {
	addr := flag.String("addr", ":1883", "address for the broker to listen on")
	flag.Parse()

	b := broker.New(*addr)
	log.Printf("kamaji broker listening on %s", *addr)
	log.Fatal(b.ListenAndServe())
}
