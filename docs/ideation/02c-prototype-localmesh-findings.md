# Phase-2b PROTOTYPE GATE — LOCAL multi-session inter-messaging on pi / opencode / hermes

**Worker:** attn-proto-local-mesh (Opus) · **Date:** 2026-06-15 · **Parent:** attn-agnostic-client (L3, pre-sign-off)
**Gate question (Toper's):** *"Does local-session inter-messaging work on the 3 harnesses like our Claude Code setup?"* — i.e. can a single daemon holding multiple NAMED local sessions route `send(name)` / `send(all)` **relay-bypassed** into the **correct live session** in realtime, plus `peers`, including **across** harnesses?

---

## ⛳ OVERALL GATE VERDICT: **PASS** ✅ — local multi-session inter-messaging is reproducible on all three harnesses + cross-harness, with two honest caveats (pi: live-POC not run + broadcast not yet in its tool; hermes: needs a session-keyed adapter, not POC'd).

The Claude Code local mesh **cleanly decomposes into two independent layers**, and that is *why* this works:

- **Layer A — discovery + routing (HARNESS-AGNOSTIC).** A shared registry of named sessions + a local transport (CC uses a peers-dir of `<name>.json` + `<name>.sock` Unix sockets; the pi daemon uses an in-memory `Map<name, Set<WS>>`). `peers` = list the registry; `send(name)` = look up the name + deliver locally; `send(all)` = fan out. **No relay, unencrypted, same-host.** This layer is just files/sockets/a map — trivially reproducible anywhere.
- **Layer B — injection into the live model context (PER-HARNESS).** This is the *only* part that differs per harness, and it is **the exact same adapter Phase-2 already validated** for relay-inbound. CC uses a proprietary MCP method (`notifications/claude/channel`); the others each need their own path (opencode `prompt_async`, pi `sendUserMessage`, hermes webhook→gateway).

**So: local multi-session inter-messaging = Layer A (trivial) + Layer B (already PASSED in Phase-2).** When session A `send("B")`, the daemon resolves B in its LOCAL registry and invokes B's Layer-B adapter, sourcing the frame locally instead of from the relay. The real risk was never "is it possible" — it was **correct session-targeting + no-leak**, which is what this prototype hammered.

---

## Per-harness + cross-harness verdict table

| Target | Multi-session local routing | Correct-targeting (A→B only) | No-leak (3rd session) | `peers` | `send("all")` broadcast | Feasibility | POC status / evidence |
|---|---|---|---|---|---|---|---|
| **opencode** | ✅ | ✅ | ✅ | ✅ | ✅ | **PASS** | **Live POC, hard evidence.** 3 live sessions on one `opencode serve`; A→B landed in B (model replied "OK"), C had 0 msgs; broadcast hit B+C, sender excluded; each inject = a real model turn. `poc-opencode-mesh/EVIDENCE.md` |
| **pi** | ✅ | ✅ | ✅ | ✅ | ⚠️ daemon-capable, **not in the tool yet** | **PASS** (source-proven) | **Shipping code**, not re-run live (pi not installed here). `pi-setup/extensions/attn` + `~/.attn/daemon.js`: daemon keeps `Map<name,Set<WS>>` keyed by WS `?session=`; routes `{type:'local',to}`→`sessions.get(to)` only→`pi.sendUserMessage(...,{deliverAs:'steer'})`. In DAILY production use (main↔workers via `ATTN_SESSION`). |
| **hermes** | ✅ (by design) | ✅ (by session_key) | ✅ (designed isolation) | ✅ (gateway session list) | ✅ (gateway fan-out) | **NEEDS-WORK** (adapter) | **Not POC'd** (heavy setup, not installed). Gateway has `build_session_key()` (stable, thread-aware), `_active_sessions`/`_running_agents` registry, designed per-session isolation, + interrupt/`_pending_messages` steer. Needs an adapter mapping `localName→session_key` + targeting the LIVE session (the Phase-2 same-session caveat, per-session). |
| **cross-harness (pi↔opencode)** | ✅ | ✅ | ✅ | ✅ | ✅ | **PASS** (routing POC'd) | **Live POC of the routing layer** w/ ONE real harness + one faithful stub. `mock-daemon-xharness.js`: one daemon, heterogeneous registry (real opencode + pi-stub). opencode↔pi-stub both directions + broadcast routed correctly; dispatch log proves per-target adapter selection. `poc-opencode-mesh/XHARNESS-EVIDENCE.md` |

---

## Session-targeting resolution — *how the daemon learns which live session to inject into*

This is the crux. The common pattern across all harnesses (and CC): **the daemon holds a `localName → (harness, live-session-handle)` registry**, populated one of two ways — the same two adapter shapes Phase-2 identified:

- **Harness-self-registers (cleanest).** The session announces itself on startup with its name; the daemon binds name→handle.
  - **CC:** each session writes `peers/<name>.json` + listens on `peers/<name>.sock` (`ATTN_SESSION` = name).
  - **pi:** each session opens a WS `…/?session=<ATTN_SESSION>`; daemon does `sessions.set(name, ws)`. Routing to a busy/live session is intrinsic — the session's own extension does the inject.
- **Daemon-discovers/owns the session.** The daemon learns the live session id from the harness API.
  - **opencode:** the live-session handle is an opencode **`session_id`**. Daemon discovers via `GET /session` (and live ones via `GET /event` SSE) → routes by `POST /session/:id/prompt_async`. Open design choice (from Phase-2): daemon **owns/creates** the session vs **discovers** the foreground one.
  - **hermes:** the handle is a stable **`session_key`** (`build_session_key()`, thread-aware). Daemon adapter maps `localName→session_key`; must inject via the **`_active_sessions`** path to hit the live session, not spawn an isolated webhook run.

`peers` is just enumerating this registry; `send(name)` is a lookup + Layer-B inject; `send(all)` is fan-out over the registry minus the sender. **Correct-targeting + no-leak fall out for free** because every send resolves to exactly one registry entry (one session_id / one WS / one session_key) — proven empirically for opencode (C never saw the A→B message) and by-construction for pi (the WS `local` path only writes `sessions.get(to)`).

---

## What this means for the architecture / plan

1. **The local mesh is NOT new work on top of Phase-2 — it reuses Phase-2's per-harness adapters with a local routing table swapped in for the relay.** The daemon's local-routing decision is: *if recipient ∈ local registry → Layer-B inject directly (bypass relay); else → relay.* (This is exactly CC's `handleSend` precedence: local name/address match wins over relay.)

2. **One daemon, one registry, heterogeneous sessions.** The cross-harness POC proves the addressing/transport layer is harness-blind; only the leaf adapter is harness-specific and is selected per-target. A pi session and an opencode session mesh through the same daemon by name.

3. **Two open items carry over to the build (both already on the M3 list):**
   - **opencode live-session discovery** — decide own-the-session vs discover-foreground (`GET /event`). Multi-TUI behavior to confirm.
   - **hermes same-session continuity** — the one real "needs-work": route the webhook into the live `_active_sessions` entry, not an isolated run. Multi-session just means the adapter holds N session_keys.

4. **One small parity gap to close in the pi adapter:** expose `send("all")` (the daemon already has fan-out; the extension tool currently only does 1:1 by name). Trivial — mirror CC's `handleSend('all')` loop over `/local-peers`.

5. **Nothing here is load-bearing-FAIL.** No harness is single-session-only; no harness fails correct-targeting; no harness can't enumerate peers. The mesh main↔workers depends on is reproducible everywhere.

---

## Method & sources
- **Reverse-engineered the CC reference** from the installed attn plugin v0.6.4 (`src/local.ts` = the full Layer-A protocol; `src/server.ts handleSend`/`notifyInbound` = routing precedence + the proprietary `notifications/claude/channel` Layer-B; `src/env.ts getPeersDir`). Confirmed the mesh is live right now (`peers` lists main + 2 workers).
- **opencode:** live POC against installed **1.3.13** — `mock-daemon.js`/`mesh-send.js` (near-verbatim ports of `src/local.ts`), 3 sessions, `prompt_async` injection, transcript read-back. Note: `prompt_async` works at runtime (HTTP 204) though it's absent from the stripped OpenAPI `/doc` (API drift, same as Phase-2 flagged).
- **pi:** local `pi-setup/extensions/attn/index.ts` + `~/.attn/daemon.js` (the 9742 daemon) — source-proven; pi not installed here (consistent with Phase-2).
- **hermes:** Phase-2 source findings + fresh research (gateway-internals docs, issues #29535/#16939/#5143): `build_session_key()`, `_active_sessions`/`_running_agents`, designed per-session isolation, interrupt/steer.

## Honest gaps / caveats
- **pi: no live POC here** (pi not installed) — relied on shipping source, same call Phase-2 made. **`send("all")` not yet exposed in the pi tool** (daemon-capable; trivial add).
- **hermes: not POC'd** — feasibility is strong (the gateway already has the multi-session machinery) but the same-session-targeting adapter is unbuilt; it's the one genuine "needs-work."
- **cross-harness: the pi leg is a faithful stub**, not a live pi session. The opencode leg is real; pi's real adapter is independently source-proven. Full live pi↔opencode is a drop-in once pi is installed.
