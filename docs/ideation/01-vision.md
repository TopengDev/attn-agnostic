# attn-agnostic-client — Vision (01)

**Status:** Discovery (Phase 1) — round 1 answered, round 2 pending. **L3 gated build** (Opus 4.8 workers).
**Date:** 2026-06-15 · Parallel to the (now-complete) fitest closeout.

## One-liner
A clean-room, agent-agnostic, cross-platform attn client (daemon + CLI + MCP + local API) that lets ANY agent harness (pi, opencode, hermes, Claude Code, …) use the FULL attn network — interoperating with the EXISTING s0nderlabs relay + Base mainnet name registry — with easy one-repo setup.

## Problem
attn today is Claude-Code-only: `packages/plugin` is an MCP server bolted into CC's proprietary `--dangerously-load-development-channels` channel system (it pushes inbound straight into the live model context). The relay, crypto, and Base contract are already harness-neutral, but no other harness can use attn. Goal: attn usable by pi/opencode/hermes/etc., on Linux/macOS/Windows, easy setup.

## Architecture recon (github.com/s0nderlabs/attn + local plugin 0.6.4)
- TS/Bun + Solidity monorepo: `packages/{relay, plugin, shared, contracts}` + CC `skills/`.
- **relay:** Cloudflare Workers + Durable Objects, hosted `wss://attn.s0nderlabs.xyz/ws` (self-hostable via wrangler). One DO per agent (mailbox) + per group + R2 for files; offline queue; pubkey store.
- **crypto:** ECIES over secp256k1, E2E (relay sees opaque blobs). Auth: EIP-191 challenge-response per WS connect. Identity = ETH address from secp256k1 keypair, auto-gen first run, stored `~/.claude/channels/attn/.env` (chmod 600). Overrides: `ATTN_PRIVATE_KEY`, `ATTN_RELAY_URL`.
- **names:** Base-mainnet ERC-721 registrar `0x5caDD2F7d8fC6B35bb220cC3DB8DBc187E02dC7A`, `<label>.attn`, 0.001 ETH + gas.
- **29 tools:** send/reply/send_file/history · add_contact/remove_contact/block/contacts · create_group/send_group/add_to_group/leave_group/accept_group/groups · peers (local Unix-socket sessions; send("all")) · react · register_name/lookup/names/transfer_name/set_primary_name · mute/unmute/mutes/status/status_of.
- **local sessions:** Unix domain sockets, `ATTN_SESSION` env, derived keys, per-session SQLite history.
- **CC-specific:** the plugin pushes inbound straight into the live model context (channel hook) — NO other harness has this.

## Round-1 decisions (Toper, 2026-06-15)
1. **Interface:** standalone DAEMON (owns key + relay WS + inbox) exposed 3 ways — standard MCP server (stdio+HTTP/SSE, NOT CC's channel system) + CLI + thin local API. [my lean, approved]
2. **Inbound delivery:** (b) daemon writes inbound to a watched file/queue + (c) per-harness adapters. **GOAL: realtime inbound forwarding to pi + opencode + hermes.** Toper flagged this as the tricky crux — each harness forwards inbound to its CLI differently.
3. **Relay + mainnet:** REUSE the existing s0nderlabs relay + Base contract (interop with the live attn network = true "connect to mainnet").
4. **Repo:** CLEAN-ROOM reimplementation (new repo, our code; reuse the NETWORK = relay+contract, not the code).
5. **v1 scope:** FULL 29-tool parity; must work with pi + opencode + hermes OUT OF THE BOX.

## Riskiest assumption (→ Phase-2 prototype)
**Realtime inbound-message forwarding to pi, opencode, AND hermes.** Each harness has a different inbound/MCP/hook mechanism. PASS criterion (finalize in Phase 2): the daemon delivers an inbound attn message into a live pi/opencode/hermes session in realtime (not just on a manual poll), for all three.

## Round-2 decisions (Toper, 2026-06-15)
6. **Per-harness inbound:** Toper → "you go research" → THIS is the **Phase-2 prototype target** (validate realtime inbound to pi/opencode/hermes).
7. **Language/runtime:** **Go** (single binary per OS, `go-ethereum` for Base, `gorilla/websocket`, zero runtime dep).
8. **Identity on one machine:** one daemon primary identity + optional named sub-identities per agent (like `ATTN_SESSION`). [lean, approved]
9. **Install UX:** one-line `curl … | sh` + per-OS release binaries first; package managers fast-follow. [lean, approved]
10. **Mainnet onboarding:** user-funded name-reg (show address, "fund to register"); `.attn` names OPTIONAL (agent works via raw address). [lean, approved]

## ≥10-question floor: MET (5 round-1 + 5 round-2, all answered).

## Target harnesses (pointers — Toper, 2026-06-15)
- **pi** — https://pi.dev/ + local `~/claude/Git/repositories/pi-setup`.
- **opencode** — https://opencode.ai/ + public `sst/opencode`.
- **hermes** — https://hermes-agent.nousresearch.com/ (Nous Research Hermes Agent).

## Phase 2 (PROTOTYPE GATE) — delegated
Opus recon worker (`attn-proto-inbound-2026-06-15`): validate realtime attn-inbound delivery into a live pi/opencode/hermes session. PASS = all 3 have a viable realtime-inbound path; findings → architecture + plan inputs.

## Resolved at Architect (post-prototype)
- Cross-platform IPC (Unix sockets → Windows named pipes), key storage (platform config dirs), full daemon design — shaped by the prototype's inbound-mechanism findings.
