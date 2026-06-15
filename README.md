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

**Status: M1 — daemon + SQLite state + full outbound 29-tool parity.** On top of
the M0 protocol core, M1 adds the long-running `attnd` daemon (persistent relay
connection with reconnect + backoff + watchdogs), a pure-Go SQLite state store
(contacts, groups, history, inbox/outbox, mutes, key cache), a Base mainnet
name-registrar client, and the complete **outbound** operation surface — all 29
attn tools — driven and verified live against the real relay + Base. Inbound is
received/decrypted/verified/stored/acked; realtime push to a harness and the
HTTP/CLI/MCP interfaces are M2–M3. See `docs/ideation/05-build-plan.md`.

> **M0** proved the foundational, interop-critical layer: secp256k1 identity,
> ECIES E2E encryption, the EIP-191 challenge-response handshake, and the relay
> WebSocket client — all wire-compatible with the live relay and the existing
> Claude-Code attn plugin.

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
internal/control     local Unix-socket JSON control plane (attnd ⇄ attnctl)
cmd/attnd            M1 daemon (persistent connection + state + outbound surface)
cmd/attnctl          control client to drive/verify the daemon's ops
cmd/attn-smoke       M0 smoke + interop CLI
testdata/interop     bun harness that generates real eciesjs/viem test vectors
docs/ideation        vision, architecture, build plan, prototype findings
```

## Build

Requires Go 1.25+ (the pure-Go SQLite driver `modernc.org/sqlite` sets the floor).

```sh
go build ./...
go build -o bin/attnd ./cmd/attnd
go build -o bin/attnctl ./cmd/attnctl
go build -o bin/attn-smoke ./cmd/attn-smoke
```

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
