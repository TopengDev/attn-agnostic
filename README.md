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

**Status: M0 — protocol / relay / crypto core.** This milestone builds and proves
the foundational, interop-critical layer: secp256k1 identity, ECIES E2E
encryption, the EIP-191 challenge-response handshake, and the relay WebSocket
client — all wire-compatible with the live relay and the existing Claude-Code
attn plugin. Later milestones (M1–M4) add the daemon, full tool surface,
interfaces, and per-harness inbound adapters (see `docs/ideation/05-build-plan.md`).

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
internal/crypto      ECIES encrypt/decrypt (eciesjs-compatible)
internal/identity    secp256k1 keys, ETH address, EIP-191 sign/verify, envelope sign/verify
internal/relay       relay WebSocket client: handshake, get_key, send, listen
cmd/attn-smoke       M0 smoke + interop CLI
testdata/interop     bun harness that generates real eciesjs/viem test vectors
docs/ideation        vision, architecture, build plan, prototype findings
```

## Build

Requires Go 1.24+.

```sh
go build ./...
go build -o bin/attn-smoke ./cmd/attn-smoke
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
