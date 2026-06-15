# attn-agnostic-client — Build Plan (05)

**Pre-sign-off.** Each milestone = a delegated **Opus** worker (L3 build = delegate; NO build worker spawns until Toper's explicit sign-off). Verify-gated; one milestone closes before the next opens.

## M0 — Protocol / relay / crypto core (Go)  [foundational + riskiest BUILD step]
Reimplement the attn wire protocol in Go against the LIVE relay: secp256k1 identity, ECIES encrypt/decrypt, EIP-191 auth handshake, WS connect, send + receive an E2E message.
**Acceptance:** a Go program sends an encrypted message to a known attn address AND receives one back via the live relay, **interoperating with the existing Claude-Code attn plugin** (cross-client round-trip proven). This is the build's internal prototype — prove interop before building up.

## M1 — Daemon + state + outbound parity
`attnd`: key mgmt, persistent relay connection, SQLite (contacts/groups/history/inbox/mutes), R2 file send/receive, Base name reg/lookup. Full OUTBOUND tool surface.
**Acceptance:** daemon runs persistently; every outbound op works against the live network; contacts/groups/names/files/mute/status at parity with the plugin.

## M2 — Interfaces (HTTP API + WS stream + CLI + MCP server)
Expose `attnd` via: local REST (outbound+mgmt), WS inbound stream (9742-contract), CLI (`attn …`), MCP server (stdio+HTTP/SSE) for the tool surface.
**Acceptance:** CLI + HTTP + MCP all drive the daemon; WS streams inbound live; MCP = 29-tool parity; an MCP-native harness (Claude Code / opencode) uses the tool surface.

## M3 — Per-harness inbound adapters  [the agent-agnostic payoff]
pi extension (adapt pi-setup's), opencode adapter (`prompt_async` + live-session discovery), hermes adapter (gateway webhook + same-session). Productize the prototype for all 3 + resolve session-targeting.
**Acceptance:** a realtime inbound attn message lands in a LIVE session of **pi AND opencode AND hermes** (evidence per harness). hermes same-session continuity verified.

## M4 — Cross-platform + install + docs
Cross-compile per OS; platform key storage; one-line installer (`curl|sh` + `.ps1`) + service setup; README/quickstart so "anyone stands up the full stack from the repo."
**Acceptance:** clean install + working daemon + ≥1 harness adapter on Linux/macOS/Windows from the repo; documented setup.

## Sequencing
M0 → M1 → M2 serial (each builds on the last). M3's 3 adapters can parallelize (≤3 workers) once M2's interfaces exist. M4 closes it. `/audit` (backend-service type) at M1/M2/M3 boundaries; `/ship` per the architecture. Repo scaffolded via `/project-init` (Go) after sign-off; docs/ideation move into it.

## L3 gate status
- [x] ≥10 clarifying questions asked + answered (5 round-1 + 5 round-2)
- [x] Prototype-First gate PASSED — Phase-2 (realtime inbound, opencode POC'd) + Phase-2b (local multi-session mesh PASS: opencode live POC no-leak+broadcast, pi source-proven, cross-harness POC'd; hermes feasible/needs-adapter)
- [x] Written plan presented (this doc + 04-architecture.md)
- [ ] **Toper's explicit sign-off** ← BLOCKING the build
