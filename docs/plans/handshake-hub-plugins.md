# Kamaji: CONNECT/CONNACK handshake, session watchdog, Hub, plugin extension points

## Context

Kamaji currently only parses the MQTT fixed header (packet type, flags, remaining length) and then stubs out — `handleConnection` just prints and returns, so every connection processes one packet's header and dies. To make this a working (if minimal) broker, we need the actual CONNECT/CONNACK handshake, a way to detect stale connections, a home for cross-connection state (starting with the "same ClientID kicks the old connection" rule), and the extension points discussed for pluggable auth/ACL/observability — without building out pub/sub (SUBSCRIBE/PUBLISH) yet, since that's a separate, later phase.

This phase is scoped tightly: **CONNECT/CONNACK handshake, watchdog, session registry, AuthPlugin wired for real, ACLPlugin/Observer interfaces defined with no-ops (Observer's OnConnect/OnDisconnect wired now, the rest reserved).** SUBSCRIBE/PUBLISH codecs, ACL enforcement, retained messages, subscription routing, persistent sessions, and the RPC/subprocess plugin transport are all explicitly out of scope — noted as follow-up work, not attempted here.

## Package layout

```
internal/packet/
  header.go            existing, untouched
  strings.go           NEW — MQTT UTF-8 string / binary data field decoding
  connect.go           NEW — CONNECT decode
  connack.go           NEW — CONNACK encode + return codes
  pingresp.go          NEW — PINGRESP encode

internal/plugin/
  plugin.go            NEW — AuthPlugin, ACLPlugin, Observer interfaces + no-op defaults

internal/broker/
  hub/hub.go           NEW — single-owner actor: ClientID -> session registry
  guard/guard.go        NEW — timeout + FailMode wrapper around AuthPlugin
  connection/connection.go   MODIFIED — export ParseFixedHeader + accessors; delete SetUpConnection/handleConnection stub
  session/session.go    NEW — per-connection handshake + watchdog + read loop
  broker.go             MODIFIED — functional options, wires session.Deps

cmd/kamaji/main.go       untouched — broker.New(*addr) still compiles with zero opts
```

No import cycles: `packet` and `plugin` are leaves. `hub` declares its own minimal `Session` interface so it never imports `session`. `guard` imports only `plugin`. `session` imports `connection`, `hub`, `guard`, `packet`, `plugin`. `broker` imports `session`, `hub`, `guard`, `plugin`.

## Packet layer (internal/packet)

**strings.go** — shared field decoders reused by CONNECT (and later PUBLISH/SUBSCRIBE):
- `ReadUTF8String(r io.Reader) (string, error)` — 2-byte big-endian length prefix + UTF-8 text (spec 1.5.3), validated via `utf8.Valid`, errors wrapped as `ErrMalformedUTF8String`.
- `ReadBinaryData(r io.Reader) ([]byte, error)` — 2-byte length prefix + raw bytes (spec 1.5.6), used for Will Message/Password.

**connect.go** — `ConnectFlags` (UserName, Password, WillRetain, WillFlag, CleanSession bools + WillQoS uint8), `ConnectPacket` (ProtocolName, ProtocolLevel, Flags, KeepAlive, ClientID, WillTopic, WillMessage, UserName, Password), and:
```go
func DecodeConnect(r io.Reader) (ConnectPacket, error)
```
Caller must bound `r` to exactly the fixed header's Remaining Length (an `*io.LimitedReader`), and after a successful decode check `lr.N != 0` — nonzero means trailing bytes beyond the declared fields, which is malformed and must be rejected (otherwise the next `ParseFixedHeader` call desyncs on the leftover bytes).

Decode order per spec 3.1: protocol name → level (reject ≠4 → `ErrUnacceptableProtocolLevel`) → connect flags (reject reserved bit 0 set → `ErrReservedFlagSet`) → keep alive → ClientID (reject empty when `!CleanSession` → `ErrZeroLengthClientID`) → conditional WillTopic+WillMessage → conditional UserName → conditional Password. Other truncation/invalid-UTF8 errors wrap as `ErrMalformedConnectPacket`.

**connack.go** — return code constants (`ConnackAccepted`=0 through `ConnackNotAuthorized`=5) and:
```go
func WriteConnack(w io.Writer, sessionPresent bool, returnCode byte) error // writes 0x20 0x02 <ackFlags> <returnCode>
```
`sessionPresent` stays `false` always this phase (no persistent sessions).

**pingresp.go** — `func WritePingResp(w io.Writer) error` writes `0xd0 0x00` (spec 3.13).

Each new file gets full table-driven test coverage following `header_test.go`'s conventions (subtests, sentinel errors via `errors.Is`).

## Plugin interfaces (internal/plugin/plugin.go)

```go
type Decision struct {
    Allow      bool
    ReturnCode byte   // CONNACK code to use when Allow is false; ignored for ACL
    Reason     string // logging only
}

type ConnectRequest struct {
    ClientID, UserName string
    Password           []byte
    CleanSession       bool
    RemoteAddr         string
}

type AuthPlugin interface {
    Authenticate(ctx context.Context, req ConnectRequest) (Decision, error)
}

// ACLRequest / ACLPlugin: defined now, not invoked yet (no PUBLISH/SUBSCRIBE codec exists).
type ACLRequest struct{ ClientID, Topic string }
type ACLPlugin interface {
    Authorize(ctx context.Context, req ACLRequest) (Decision, error)
}

// Observer: only OnConnect/OnDisconnect are wired this phase.
type Observer interface {
    OnConnect(ctx context.Context, clientID, remoteAddr string)
    OnDisconnect(ctx context.Context, clientID string, err error)
    // Future: OnPublish, OnSubscribe, OnUnsubscribe once those packets decode.
}

type AllowAllAuth struct{} // Authenticate always returns Decision{Allow: true}, nil
type AllowAllACL  struct{} // Authorize always returns Decision{Allow: true}, nil
type NoOpObserver struct{} // both methods no-op
```
Both `AuthPlugin` and `ACLPlugin` are transport-agnostic by design — an in-process Go type satisfies them today, and a future RPC/subprocess adapter would too, with no interface changes needed then.

## Hub (internal/broker/hub/hub.go)

Single-owner actor — state lives only inside its `run()` goroutine, mutated only via channel messages, so there's no shared-map locking to get wrong:

```go
// Session is the minimal handle Hub needs to enforce "kick the existing
// connection with the same ClientID" (spec 3.1.4). Must be implemented by
// pointer so == comparison reflects identity, not value equality.
type Session interface {
    ClientID() string
    Kick() // idempotent; closes the underlying connection
}

type Hub struct { /* register, unregister channels */ }

func New() *Hub // starts run() internally

// Register stores s under s.ClientID(), returning the previous session it
// replaced (nil if none). Does NOT call Kick itself — callers must do that
// outside the Hub goroutine so a slow Kick can't stall run().
func (h *Hub) Register(s Session) (previous Session)

// Unregister removes s only if it is still the current holder of its
// ClientID — a no-op otherwise. This is the race guard (see edge case #1).
func (h *Hub) Unregister(s Session)
```

Subscription tables and retained-message storage are explicitly **not** modeled yet — add as further actor state + message types when SUBSCRIBE/PUBLISH decoding lands, not as separately-locked structures bolted on now.

## Guard (internal/broker/guard/guard.go)

```go
type FailMode int
const ( FailOpen FailMode = iota; FailClosed )

type AuthGuard struct { /* wrapped plugin, timeout, failMode */ }

func New(p plugin.AuthPlugin, timeout time.Duration, failMode FailMode) *AuthGuard

// Authenticate matches plugin.AuthPlugin's signature, so callers can treat
// *AuthGuard and a raw plugin.AuthPlugin interchangeably. On timeout or
// plugin error, returns the FailMode's fallback Decision (Allow:true for
// FailOpen; Allow:false + ConnackServerUnavailable for FailClosed) — the
// returned error is for logging only, the Decision is already final.
func (g *AuthGuard) Authenticate(ctx context.Context, req plugin.ConnectRequest) (plugin.Decision, error)
```
Implementation: `context.WithTimeout` + run the plugin call in a goroutine writing to a **size-1 buffered** result channel, `select` against `ctx.Done()`. The buffering matters: if the plugin ignores context cancellation, the abandoned goroutine can still complete its send and get garbage collected instead of leaking forever.

## Connection layer changes (internal/broker/connection/connection.go)

- Rename `parseFixedHeader` → exported `ParseFixedHeader` (needed by `session`).
- Add read-only accessors so `session` doesn't need unexported-field access across packages: `PacketType()`, `Flags()`, `RemainingLength()`.
- Delete `handleConnection()` and `SetUpConnection()` entirely — replaced by `session.Handle`. Drop the now-unused `fmt` import.
- Update `connection_test.go` call sites (`parseFixedHeader` → `ParseFixedHeader`); tests stay in `package connection` and keep asserting on unexported fields directly.

## Session (internal/broker/session/session.go, new)

```go
// Deps bundles a Session's collaborators, assembled by broker.New.
type Deps struct {
    Hub      *hub.Hub
    Auth     plugin.AuthPlugin // AllowAllAuth{} or a *guard.AuthGuard — same interface either way
    ACL      plugin.ACLPlugin  // carried for continuity; NOT invoked this phase
    Observer plugin.Observer
}

type Session struct { /* conn, clientID, deps, watchdog timer + duration, sync.Once for close */ }

func (s *Session) ClientID() string { return s.clientID } // hub.Session
func (s *Session) Kick()                                  // hub.Session; sync.Once-guarded conn.Close()

// Handle drives one accepted connection end-to-end (handshake, then the
// post-handshake read loop) and returns when the connection ends for any
// reason. Takes ownership of conn — guarantees it's closed exactly once.
func Handle(conn net.Conn, deps Deps)
```

`Handle` flow:
1. `defer s.Kick()` immediately — guarantees close on every return path.
2. `fixed, err := connection.ParseFixedHeader(conn)`; error → log, return.
3. `fixed.PacketType() != packet.CONNECT` → log "first packet must be CONNECT", return (bare close, no response — spec leaves this undefined).
4. `pkt, ok := s.handleConnect(fixed)`; `!ok` → return (handleConnect already sent any applicable CONNACK).
5. `s.clientID = pkt.ClientID`; `if prev := deps.Hub.Register(s); prev != nil { prev.Kick() }`; `defer deps.Hub.Unregister(s)`.
6. `deps.Observer.OnConnect(...)`, and via `defer`, `deps.Observer.OnDisconnect(...)` capturing the read loop's terminal error.
7. `s.armWatchdog(pkt.KeepAlive)` / `defer s.stopWatchdog()`.
8. `loopErr := s.readLoop()`.

`handleConnect` — decode via a `*io.LimitedReader` bounded to `fixed.RemainingLength()`, then branch on error type to decide CONNACK-vs-bare-close (this is the fiddliest spec detail — see edge case #4):
- `ErrUnacceptableProtocolLevel` → CONNACK code 1, return false.
- `ErrZeroLengthClientID` → CONNACK code 2, return false.
- any other decode error, or `lr.N != 0` (trailing bytes) → bare close, no CONNACK, return false.
- empty ClientID with `CleanSession==true` → synthesize one (`crypto/rand`-based, e.g. `"kamaji-" + hex`); MQTT 3.1.1 has no wire mechanism to tell the client its assigned ID, which is fine since persistent sessions are out of scope.
- call `deps.Auth.Authenticate(...)`; `!decision.Allow` → CONNACK with `decision.ReturnCode` (default `ConnackNotAuthorized` if unset), return false.
- otherwise → CONNACK code 0 (accepted), return `(pkt, true)`.

`readLoop`:
```go
for {
    fixed, err := connection.ParseFixedHeader(s.conn)
    if err != nil { return err }
    s.resetWatchdog()
    switch fixed.PacketType() {
    case packet.PINGREQ:
        if err := packet.WritePingResp(s.conn); err != nil { return err }
    case packet.DISCONNECT:
        return nil // spec 3.14: clean, client-initiated close
    default:
        if _, err := io.CopyN(io.Discard, s.conn, int64(fixed.RemainingLength())); err != nil { return err }
        log.Printf("session: %s from %s not yet implemented, dropped", fixed.PacketType(), s.conn.RemoteAddr())
    }
}
```

Watchdog: `time.AfterFunc` sized at `1.5 * keepAlive` (spec 3.1.2.10), reset on every successful read; `keepAlive == 0` disables it entirely (spec-mandated, not a bug). Use a package-level `var keepAliveUnit = time.Second` so tests can shrink it to `time.Millisecond` for fast subtests without touching the 1.5× multiplier logic.

## Broker (internal/broker/broker.go, modified)

Functional-options constructor so plugins/timeouts/fail-mode are configurable without breaking the zero-config case:
```go
func New(addr string, opts ...Option) *Broker
func WithAuthPlugin(p plugin.AuthPlugin) Option
func WithACLPlugin(p plugin.ACLPlugin) Option
func WithObserver(o plugin.Observer) Option
func WithAuthTimeout(d time.Duration) Option
func WithFailMode(m guard.FailMode) Option
```
Defaults: `ACLPlugin` = `AllowAllACL{}`, `Observer` = `NoOpObserver{}`, `authTimeout` = 5s, `failMode` = `FailOpen`. If no `AuthPlugin` is configured, use `AllowAllAuth{}` directly (unwrapped — nothing to time out or fail over on); only wrap in `guard.New(...)` when a real `AuthPlugin` is supplied. `handleConn` drops its old `defer conn.Close()` (ownership moves to `session.Handle`) and just calls `session.Handle(conn, b.deps)`.

`cmd/kamaji/main.go` needs no changes — `broker.New(*addr)` still compiles against the variadic-opts signature.

## Sequence of work

1. `internal/packet/strings.go` + tests
2. `internal/packet/connect.go` + tests
3. `internal/packet/connack.go` + tests
4. `internal/packet/pingresp.go` + tests
5. `internal/plugin/plugin.go` + tests
6. `internal/broker/hub/hub.go` + tests
7. `internal/broker/guard/guard.go` + tests
8. `internal/broker/connection/connection.go` changes + update `connection_test.go`
9. `internal/broker/session/session.go` + tests
10. `internal/broker/broker.go` functional options + `broker_test.go`
11. `go build ./... && go vet ./... && gofmt -l . && go test ./...`

## Trickiest correctness edge cases

1. **Duplicate ClientID race.** `Hub.Register`/`Unregister` are both synchronous calls into the single actor goroutine, and `Unregister` is compare-then-delete keyed on session identity. This specifically guards against: session A registers, session B registers with the same ClientID (kicking A), but A's own deferred `Unregister` runs *after* B has taken the slot — without the identity check, A's cleanup would wrongly evict B.
2. **CONNECT remaining-length mismatches both directions.** Too-short surfaces as `io.ErrUnexpectedEOF` from the bounded `LimitedReader` (wrap as `ErrMalformedConnectPacket`). Too-long must be caught explicitly via `lr.N != 0` after a successful decode, or the leftover bytes desync the next `ParseFixedHeader` call and corrupt the whole session.
3. **keep-alive = 0 disables the watchdog entirely (spec-compliant)** — a client that sends a fixed header with a large declared Remaining Length and then stalls mid-packet can block a session goroutine indefinitely via the read loop's `io.CopyN` drain, with no independent timeout. Acceptable for this phase (matches spec semantics exactly); call it out as a known limitation rather than silently adding an undocumented deadline that would violate spec-mandated infinite keep-alive=0 behavior.
4. **Reserved bit (bit 0) set in CONNECT flags → bare close, no CONNACK at all** (spec 3.1.2.3) — distinct from protocol-level mismatch and zero-length-ClientID-with-CleanSession-0, both of which *do* get a CONNACK before closing (codes 1 and 2). Enumerate these three failure paths explicitly in `handleConnect` rather than collapsing to one generic "malformed → close" branch.
5. **Auth plugin timeout must not leak a goroutine** even if the plugin ignores `ctx` cancellation — use a size-1 buffered result channel in `guard.AuthGuard.Authenticate`.
6. **Zero-length ClientID + CleanSession=1 is valid and requires synthesizing an ID** (spec 3.1.3.7) — use `crypto/rand`, not a predictable counter, to avoid Hub registry collisions.
7. **Double-close safety.** `Session.Kick()` is called from the watchdog, from `Handle`'s top-level defer, and from a duplicate-ClientID kick by another session — guard with `sync.Once`.

## Verification

- `go build ./... && go vet ./... && gofmt -l .` must be clean.
- `go test ./...` — full coverage of new packet decoders (valid/malformed/truncated cases per field), Hub race behavior (duplicate ClientID registration/unregistration ordering), guard timeout/fail-mode branches, and session handshake/watchdog/read-loop behavior via `net.Pipe`, following existing table-driven + sentinel-error conventions.
- Manual smoke test: run `go run ./cmd/kamaji`, then use `nc localhost 1883` or a small Go/Python script to send a raw CONNECT byte sequence (e.g. `10 0C 00 04 4D 51 54 54 04 02 00 3C 00 00` — protocol name "MQTT", level 4, clean-session flag, keep-alive 60, zero-length ClientID) and confirm a CONNACK (`20 02 00 00`) comes back, then send a PINGREQ (`C0 00`) and confirm a PINGRESP (`D0 00`).