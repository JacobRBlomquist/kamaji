# Kamaji

- An MQTT 3.1.1 implementation in golang

## Project layout

- `/` — public client library (`package kamaji`)
- `internal/packet` — MQTT control packet encoding/decoding
- `internal/broker` — broker implementation
- `cmd/kamaji` — standalone broker CLI

## Building

```sh
go build ./...
```

## Running the broker

```sh
go run ./cmd/kamaji -addr :1883
```

`-addr` defaults to `:1883` if omitted.
