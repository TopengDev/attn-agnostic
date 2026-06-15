# attn-agnostic-client — Architecture (04)

**Status:** post-prototype (Phase-2 = **PASS**). Informed by `~/claude/notes/attn-proto-inbound-2026-06-15/report.md`. Pre-sign-off.

## Components
1. **`attnd` — the daemon (Go, one per machine/identity).** Owns: secp256k1 key + identity; the relay WS connection (`wss://attn.s0nderlabs.xyz/ws`); local state (SQLite: contacts, groups, history, inbox, mutes); the Base contract client (name reg/lookup). Long-running (systemd-user / launchd / Windows service).
2. **Interfaces the daemon exposes:**
   - **Local HTTP REST API** — outbound + management (send/reply/contacts/groups/names/mute/status/files).
   - **Local WS / event stream** — inbound push; adapters subscribe. Model on pi-setup's existing `127.0.0.1:9742` contract (REST out + WS in).
   - **MCP server (stdio + HTTP/SSE)** — the 29-tool surface for MCP-native harnesses (opencode/hermes/Claude Code). **NOT** used for inbound push (prototype proved MCP notifications don't reach the model).
   - **CLI (`attn …`)** — wraps the HTTP API for scripting/manual use.
3. **Per-harness inbound adapters (2 shapes cover all 3):**
   - **pi** — TS extension (adapt pi-setup's existing `extensions/attn/index.ts`): subscribes daemon WS → `pi.sendUserMessage(text,{deliverAs:'steer'})`. [harness-subscribes-WS]
   - **opencode** — daemon-drives-HTTP → `POST /session/:id/prompt_async` (or a tiny opencode plugin subscribing to the daemon WS). Needs live-session discovery (`GET /session` / `/event`). [daemon-drives-HTTP]
   - **hermes** — daemon HMAC-POSTs inbound → gateway webhook `127.0.0.1:8644/webhooks/attn`, + a small same-session adapter (caveat: a raw webhook run is isolated). [daemon-drives-HTTP]

## Key decisions
- **Language:** Go (single binary per OS · `go-ethereum` for Base · `gorilla/websocket` · zero runtime dep).
- **Identity:** one daemon primary identity + optional named sub-identities (ATTN_SESSION-style).
- **Crypto (clean-room, Go):** ECIES secp256k1 E2E + EIP-191 challenge-response auth. Reimplement the relay wire protocol (read from the Apache-2.0 `s0nderlabs/attn` source + README — reimplement, don't copy).
- **Network:** REUSE the live s0nderlabs relay + Base registrar `0x5caDD2F7d8fC6B35bb220cC3DB8DBc187E02dC7A`. True mainnet, interoperable with the existing attn network.
- **Cross-platform:** all IPC over localhost HTTP/WS (sidesteps the Unix-socket ↔ Windows-named-pipe split). Key storage in the platform config dir (XDG / %APPDATA% / ~/Library). Go cross-compiles to Linux/macOS/Windows.
- **Install:** one-line `curl … | sh` (+ Windows `.ps1`) → per-OS binary + daemon service setup. Package managers fast-follow.
- **Names:** user-funded (0.001 ETH on Base); `.attn` names OPTIONAL (an agent works fully via its raw address).

## Inbound delivery model (from the prototype)
- Per-harness adapters; **no universal push**.
- `prompt_async` (opencode) / `deliverAs:'steer'` (pi) / `busy_input_mode:steer` (hermes) = the same realtime primitive 3 ways → the daemon's adapter contract exposes a **delivery-mode hint** (steer vs followUp/idle).
- **Session-targeting is THE hazard** (not "is realtime possible" — it is). The adapter contract must define how it learns *which live session to inject into* per harness (pi sidesteps it by running in-session; opencode needs the session id; hermes needs a stable session key).

## Local mesh (Phase-2b — validated PASS)
The Claude-Code local mesh decomposes into TWO layers — the key reuse:
- **Layer A — discovery + routing (HARNESS-AGNOSTIC):** a named-session registry + local socket/map (port CC's `local.ts`: peers-dir + per-session Unix socket + NDJSON), relay-bypassed. `peers` = enumerate; `send(name)` = lookup + inject into that ONE session (proven NO-LEAK across 3 live opencode sessions); `send("all")` = fan-out (sender excluded).
- **Layer B — per-harness injection:** the SAME adapters as Phase-2 (opencode `prompt_async` / pi `sendUserMessage`-steer / hermes webhook). So local mesh = Layer A (trivial, harness-blind) + Layer B (already validated).
- **Session-targeting resolution:** daemon holds `localName → (harness, live-handle)`; pi/CC self-register (WS `?session=` / peers-dir sock), opencode discovered/owned (session_id via `/session`+`/event`), hermes mapped (session_key via `_active_sessions`). Cross-harness routing POC'd (opencode↔pi both ways + broadcast).
- **Caveats (→ M3):** hermes adapter (localName→session_key + live-session targeting) not yet POC'd; pi `send("all")` is a trivial daemon-fan-out add.

## Open risks (addressed by build milestones)
- Relay wire-protocol reimplementation interop → **M0 acceptance**.
- hermes same-session continuity → **M3**.
- opencode live-session discovery → **M3**.
- Harness API drift (all 3 fast-moving; opencode's API already drifted vs its dev branch) → pin versions + runtime-verify (M3/M4).
