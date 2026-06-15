# attn-agnostic

A clean-room, agent-agnostic, cross-platform [attn](https://github.com/s0nderlabs/attn)
client in Go. The goal: let **any** agent harness (pi, opencode, hermes, Claude
Code, …) use the full attn network — interoperating with the **existing
s0nderlabs relay + Base mainnet name registry** — with one-repo setup.

> attn today is Claude-Code-only (its plugin is bolted into CC's proprietary
> channel system). The relay, crypto, and Base contract are harness-neutral, but
> no other harness can speak to them. This project reimplements the wire protocol
> as a reusable Go core (and, later, a daemon + CLI + MCP server + per-harness
> inbound adapters).

**Status: M4 — complete. The full cross-platform stack ships from one repo.**
A daemon + CLI + MCP server + three harness adapters (pi, opencode, hermes),
all on loopback, plus **one-line installers** and **per-OS daemon services**.
The whole tree is **pure Go (no CGO)**, so it cross-compiles cleanly to
`linux/{amd64,arm64}`, `darwin/{amd64,arm64}`, and `windows/amd64` from a single
host — see [`docs/INSTALL.md`](docs/INSTALL.md) to stand up the stack on any of
the three. Paid Base name writes stay **gated**; every server binds loopback only.

> **M3** added the three realtime **inbound** adapters: a pi TypeScript extension,
> an opencode Go bridge (`attn-opencode`, inject via `prompt_async`), and a hermes
> Go bridge + Python plugin (`attn-hermes-bridge`, HMAC-signed → stable session).

> **M2** fronted the daemon's single `agent.Dispatch` seam with four localhost
> interfaces: a **REST API** (`127.0.0.1:9742`, all 29 ops + pi-compat endpoints),
> a **WS inbound-event stream** (pi-compatible frame shape + `steer`/`followUp`
> hint), the **`attn` CLI**, and the **`attn-mcp` MCP server** (stdio +
> streamable-HTTP/SSE) re-exposing the 29-tool surface. REST/CLI/MCP hold no
> business logic — every op flows through the one daemon.

> **M1** added the long-running `attnd` daemon (persistent relay connection with
> reconnect + backoff + watchdogs), a pure-Go SQLite state store (contacts,
> groups, history, inbox/outbox, mutes, key cache), a Base mainnet name-registrar
> client, and the complete **outbound** 29-tool surface — driven + verified live.

> **M0** proved the foundational, interop-critical layer: secp256k1 identity,
> ECIES E2E encryption, the EIP-191 challenge-response handshake, and the relay
> WebSocket client — all wire-compatible with the live relay and the existing
> Claude-Code attn plugin.

## Install

```sh
# Linux / macOS
curl -fsSL https://raw.githubusercontent.com/TopengDev/attn-agnostic/main/scripts/install.sh | sh

# Windows (PowerShell)
irm https://raw.githubusercontent.com/TopengDev/attn-agnostic/main/scripts/install.ps1 | iex
```

The installer detects your OS + arch, installs the binaries to a PATH dir,
generates a loopback-only identity (key `0600`, never printed), and sets up a
per-user daemon service. It is idempotent. **From a checkout** (the path until
releases are published — the installer auto-detects it and builds from source):

```sh
git clone https://github.com/TopengDev/attn-agnostic && cd attn-agnostic
./scripts/install.sh
```

Then: `attn status` · point an MCP harness at `attn-mcp` · wire an inbound
adapter. **Full per-OS guide, env vars, name registration, and the three
adapters: [`docs/INSTALL.md`](docs/INSTALL.md).**

## Why clean-room?

The upstream `s0nderlabs/attn` is Apache-2.0 TypeScript/Bun. We **read** it to
understand the exact wire protocol and **reimplement** it in Go — we do not copy
code. We reuse the *network* (the live relay + Base contract), not the codebase.

## Wire protocol (as reimplemented here)

| Layer | Scheme |
|---|---|
| **Identity** | secp256k1 keypair → Ethereum address (lowercased), matching viem's `privateKeyToAccount`. |
| **E2E crypto** | ECIES over secp256k1 (`eciesjs`-compatible): AES-256-GCM, 16-byte nonce, uncompressed 65-byte ephemeral key, HKDF-SHA256 over `ephPub‖sharedPoint`. Wire layout `[ephPub:65][nonce:16][tag:16][ct]`, base64. |
| **Auth** | EIP-191 (`personal_sign`) challenge-response on WS connect. Relay sends a `challenge` nonce; client signs it; relay recovers the address + stores the recovered public key. |
| **Envelope** | `encrypted = base64(ECIES(recipientPubKey, plaintext))`; signature = `personal_sign(JSON.stringify({id, to, encrypted}))`. The recipient verifies the signature recovers to the sender. |
| **Transport** | WebSocket to `wss://attn.s0nderlabs.xyz/ws?address=<addr>`; JSON frames; raw `ping`/`pong` keepalive. |

The crypto compatibility is **proven empirically** against the exact upstream
versions (`eciesjs@0.4.18`, `viem@2.47.6`) — see Testing below.

## Layout

```
internal/crypto      ECIES encrypt/decrypt (eciesjs-compatible) + raw-binary (files)
internal/identity    secp256k1 keys, ETH address, EIP-191 sign/verify, envelope sign/verify
internal/relay       relay WS: M0 single-shot Client + M1 persistent Session
                     (reconnect/backoff/watchdog, reactions, presence, resolve, groups)
internal/store       SQLite state (modernc.org/sqlite, CGO-free): contacts, groups,
                     history, pending/outbox, mutes, key cache, meta
internal/names       Base mainnet AttnNames registrar client (read live; writes gated)
internal/config      portable platform paths (XDG/%AppData%/~Library) + key load
internal/agent       orchestrator: the 29 attn ops + inbound policy/persistence
                     + the SurfaceEvent sink (inbound → adapters) + inbound files
internal/httpapi     M2 product interface: localhost REST (all 29 ops + pi-compat)
                     + WS inbound-event stream (pi frame shape + delivery-mode hint)
internal/control     local Unix-socket JSON control plane (attnd ⇄ attnctl)
internal/buildinfo   version/commit/date, injected at link time by scripts/build.sh
cmd/attnd            daemon: persistent connection + state + REST/WS interface
cmd/attn             M2 user-facing CLI (thin client over the daemon's REST API)
cmd/attn-mcp         M2 MCP server (stdio + streamable-HTTP/SSE), 29-tool surface
cmd/attn-opencode    M3 opencode adapter bridge (WS subscriber → prompt_async)
cmd/attnctl          low-level control client to drive/verify the daemon's ops
cmd/attn-smoke       M0 smoke + interop CLI
adapters/pi          M3 pi adapter (TypeScript extension)
adapters/opencode    M3 opencode adapter engine (client + bridge + render)
adapters/hermes      M3 hermes adapter (Go bridge cmd + Python platform plugin)
scripts/build.sh     M4 cross-compile matrix → dist/<os>-<arch>/
scripts/install.sh   M4 one-line installer (Linux/macOS); install.ps1 (Windows)
scripts/services     M4 per-OS daemon service files (systemd / launchd) + README
testdata/interop     bun harness that generates real eciesjs/viem test vectors
docs/INSTALL.md      full-stack install + per-OS setup + adapter wiring (M4)
docs/ideation        vision, architecture, build plan, prototype findings
```

## Build

Requires Go 1.25+ (the pure-Go SQLite driver `modernc.org/sqlite` sets the floor).

```sh
go build ./...
go build -o bin/attnd ./cmd/attnd        # daemon (REST + WS interface)
go build -o bin/attn ./cmd/attn          # user-facing CLI
go build -o bin/attn-mcp ./cmd/attn-mcp  # MCP server (stdio / HTTP)
go build -o bin/attnctl ./cmd/attnctl    # low-level control client
go build -o bin/attn-smoke ./cmd/attn-smoke
```

**Cross-compile matrix** — `scripts/build.sh` builds every shippable binary for
all targets (no CGO, so no C toolchain needed), embedding the version:

```sh
./scripts/build.sh                 # linux/{amd64,arm64} darwin/{amd64,arm64} windows/amd64
./scripts/build.sh linux/arm64     # a single target → dist/linux-arm64/
ATTN_VERSION=v0.1.0 ./scripts/build.sh
```

Output lands in `dist/<os>-<arch>/` with a `SHA256SUMS.txt`; each binary reports
`--version`. To install (build-or-download + service setup), see
[`docs/INSTALL.md`](docs/INSTALL.md).

## Quickstart (`attnd` daemon + `attnctl`)

```sh
# Start the daemon (loads the identity from ATTN_HOME, generating one with -gen-key).
# Platform config dir: $XDG_CONFIG_HOME/attn (Linux) — override with ATTN_HOME.
ATTN_HOME=~/.config/attn ./bin/attnd -gen-key &

# Drive any of the 29 ops through the daemon's control socket:
./bin/attnctl _info                                   # address + relay readiness
./bin/attnctl send to=alice.attn message="hello"       # name- or 0x-addressed
./bin/attnctl add_contact address=0x… name=alice
./bin/attnctl create_group name=devs members=0xaaa…,0xbbb…
./bin/attnctl history with=0x… limit=20
./bin/attnctl lookup query=alice.attn                  # live Base/relay resolve
./bin/attnctl status state=away message="auditing"
```

`register_name` / `transfer_name` / `set_primary_name` are **gated**: the daemon
encodes + simulates the on-chain write and returns the calldata, but never
broadcasts a paid mainnet transaction (registration costs 0.001 ETH, irreversible).

## Interfaces (M2) — REST + WS + CLI + MCP

The daemon serves a localhost-only product interface on `127.0.0.1:9742`
(`ATTN_HTTP_ADDR` / `-http`; binds loopback only, refuses any public address).

**HTTP REST** — every op via `POST /op/{name}` (`{ok,text,data}`), plus pi-compat
endpoints (`POST /send`, `GET /status|/peers|/history|/local-peers`):

```sh
curl -s localhost:9742/status
curl -s -XPOST localhost:9742/op/send -d '{"to":"alice.attn","message":"hi"}'
```

**WS inbound stream** — connect to `ws://127.0.0.1:9742/?session=<name>`; each
inbound attn frame is pushed as JSON (`{type,from,message,trust,groupId,…}` +
a `deliveryMode` hint of `steer`/`followUp`). The shape matches pi-setup's
`extensions/attn/index.ts`, so its adapter is drop-in. (Inbound is **not** done
over MCP — server-initiated MCP notifications don't reach the model.)

**CLI** — a thin, scriptable client over the REST API:

```sh
./bin/attn send alice.attn "hello"      ./bin/attn contacts     ./bin/attn status
./bin/attn history alice.attn --limit 20  ./bin/attn lookup alice.attn  --json
```

**MCP server** — re-exposes the 29 tools to MCP-native harnesses:

```sh
./bin/attn-mcp -transport stdio                 # for Claude Code / opencode
./bin/attn-mcp -transport http -addr 127.0.0.1:9743   # /mcp (streamable) + /sse
```

## Quickstart (`attn-smoke`)

```sh
# Generate an identity (saved to ./attn-key.hex, chmod 600)
./bin/attn-smoke gen-key

# Show your address / public key
./bin/attn-smoke addr
./bin/attn-smoke pubkey

# Send an E2E message via the live relay
./bin/attn-smoke -v send 0x<recipient-address> "hello over attn"

# Listen for inbound messages (decrypt + verify + ack)
./bin/attn-smoke -v listen

# Full round-trip: send, then wait for a reply
./bin/attn-smoke -v interop 0x<recipient-address> "ping"
```

Key resolution order: `-key <hex>` → `ATTN_PRIVATE_KEY` env → `-keyfile` (default
`./attn-key.hex`, override with `ATTN_KEYFILE`).

## Testing

Unit + interop tests prove byte-level compatibility with the upstream libraries.
Generate the test vectors once (needs [bun](https://bun.sh)):

```sh
cd testdata/interop && bun install && bun gen-vectors.ts && cd ../..
go test ./...
```

The vectors are produced by the **real** `eciesjs@0.4.18` and `viem@2.47.6`, so
the tests assert that:

- our address + public-key derivation matches viem,
- our EIP-191 signature is byte-identical to viem's `signMessage`,
- ciphertext from real `eciesjs` decrypts in our Go core (and vice versa).

## Security notes

- Private keys live in a `0o600` keyfile (or `ATTN_PRIVATE_KEY`) and are
  **never** committed (`.gitignore` excludes `*.key`, `.env`, `attn-key*.hex`).
- All external messages are end-to-end encrypted; the relay sees only opaque
  ciphertext. Inbound DM signatures are verified against the claimed sender.

## License

TBD by the project owner. Upstream `s0nderlabs/attn` is Apache-2.0; this is an
independent clean-room reimplementation.
