# Phase-2 PROTOTYPE GATE — Realtime attn-inbound delivery to pi / opencode / hermes

**Worker:** attn-proto-inbound (Opus) · **Date:** 2026-06-15 · **Parent:** attn-agnostic-client (L3, pre-plan)
**Gate question:** Can our external attn **daemon** deliver an inbound message into a **LIVE session** of a non-Claude-Code harness **in realtime** (not just on a manual poll), for pi + opencode + hermes?

---

## ⛳ OVERALL GATE VERDICT: **PASS** ✅ — all three harnesses have a viable realtime-inbound path.

One harness (opencode) was POC'd end-to-end with hard evidence (a real model turn triggered by an external process). pi's path is **already shipping** as working code in the local `pi-setup` repo. hermes's path is documented + self-hostable, with one honest caveat on session-continuity. **No harness is poll-only.** The design does NOT need a poll-only fallback — though hermes warrants a small adapter to guarantee same-session continuity (see caveat).

---

## Per-harness verdict table

| Harness | MCP support | Realtime-inbound mechanism (BEST path) | Feasibility | POC status + evidence | Key risk / gotcha |
|---|---|---|---|---|---|
| **opencode** | ✅ Full MCP **client**, 3 transports (stdio / streamable-HTTP / SSE), official `@modelcontextprotocol/sdk`. **But** server-initiated MCP `notifications/*` are logged, NOT surfaced to the model — useless for waking a session. | **(d) IPC/API the daemon drives.** `opencode serve` (HTTP, default `127.0.0.1:4096`) → **`POST /session/:id/prompt_async`** with `{parts:[{type:text,text:…}]}` → fire-and-forget `204`, runs a real model turn on the existing session. Alt: `/tui/append-prompt`+`/tui/submit-prompt` to drive the foreground TUI. Observe live via `GET /event` (SSE). | **PASS** | ✅ **POC'd end-to-end.** Mock daemon injected → HTTP 204 → model answered "42" to the injected question, streamed live over SSE, persisted in session. `poc-opencode/EVIDENCE.md` + `sse.log`. | Daemon must know the **live session ID** (`GET /session` / `/event` / own-the-session). Server unsecured on localhost by default (fine same-host; `OPENCODE_SERVER_PASSWORD` to lock). Fast-moving — pin version, verify `/doc` at runtime (API already drifted vs dev branch: routes exist but aren't all in the global OpenAPI doc). |
| **pi** | ❌ No built-in MCP client (issue #563 open). MCP only via a custom extension; the `opencode`-profile sibling uses a stdio MCP server instead. | **(b) hook/event/plugin trigger.** A pi **TypeScript extension**: on `session_start` open a WS to the daemon (`127.0.0.1:9742`), and on each inbound frame call **`pi.sendUserMessage(text,{deliverAs:'steer'})`** — steers the live turn after current tool calls, before next LLM call. (`deliverAs:'followUp'` to wait for idle; `pi.sendMessage(...,{triggerTurn:false})` for context-only.) | **PASS** | ✅ **Already shipping** as working code: `~/claude/Git/repositories/pi-setup/extensions/attn/index.ts` (WS client + `sendUserMessage` injection) + the `pi-remote` Telegram bridge as the mirror. Not re-run here (no need — it's the exact pattern, on documented APIs). | `pi.sendUserMessage()` silently dropped right after `ctx.newSession()` (bug #2860). Don't open WS/timers from the extension factory — defer to `session_start`, close on `session_shutdown`. Young, fast-moving extension API — pin pi version. pi is `earendil-works/pi` (MIT, Node ≥18) — **not** Inflection's "Pi" chatbot. |
| **hermes** | ✅ Strong, **both directions.** MCP **client** (stdio+HTTP, OAuth, handles `tools/list_changed`, supports **sampling**). MCP **server** (`hermes mcp serve`, stdio) exposing `messages_send`, `events_poll`, **`events_wait`** (long-poll). | **(b) hook/event trigger — Gateway webhook adapter.** Run the hermes **gateway daemon**, enable the webhook adapter (`127.0.0.1:8644`), define a route, and the attn daemon **HMAC-signs + POSTs each inbound** to `/webhooks/attn` → routed through `GatewayRunner._handle_message()` → agent run → delivered into the live chat thread. | **PASS** (caveat → NEEDS-WORK for strict same-session) | 📄 Documented + self-hostable; **not** POC'd (would need full gateway+platform setup — out of time-box once opencode proved the pattern). POC recipe in findings. | **Caveat:** a webhook run is an **isolated** agent execution — realtime ingest + delivery into the live *thread*, but NOT guaranteed to share the exact in-memory session the user is mid-chat with. Close the gap with a small custom/platform adapter keyed to a stable session id. Config bug #10206 (keys must nest under `extra:`); HMAC auth required; idempotency cache (unique `X-Request-ID` per msg or messages drop). `NousResearch/hermes-agent`, self-hostable — not hosted-only. |

---

## What this means for the architecture / plan

1. **The inbound mechanism is per-harness, by design — there is no single universal push.** Confirms Round-1 decision #2 (per-harness adapters). The daemon needs a thin **adapter per harness** that translates "inbound attn frame" → that harness's realtime-injection call.

2. **Two adapter shapes cover all three:**
   - **Daemon-drives-HTTP** (opencode `prompt_async`; hermes webhook POST): the daemon, on inbound, makes an HTTP call into the harness's local server. Stateless from the harness side.
   - **Harness-subscribes-to-daemon** (pi extension WS client): the harness runs a small plugin that holds a WS/socket to the daemon and injects on each frame. This is the cleanest "realtime, in-session" model and is **already proven** for pi.
   - opencode can use **either** (drive its HTTP API directly, or ship a tiny opencode plugin that subscribes — opencode plugins hold the SDK `client` and can call the same API).

3. **The daemon should expose BOTH a local HTTP API and a WS/socket event stream** so adapters can pick whichever the harness needs. This matches `pi-setup`'s existing daemon contract (`127.0.0.1:9742`: REST for outbound + WS for inbound) — a strong template to reuse.

4. **MCP server-initiated notifications are NOT a reliable universal inbound path.** opencode logs them (never reaches the model); pi has no MCP client; only hermes acts on some of them, and even there it's wrong-direction (`events_wait` is the *client* watching hermes). **Do not architect inbound around MCP `notifications/*`.** MCP is great for the *tool surface* (send/contacts/groups) but not the realtime *inbound push*.

5. **`prompt_async`-style fire-and-forget + a "steer/queue" delivery mode are the realtime primitives.** opencode `prompt_async`, pi `deliverAs:'steer'`, hermes `display.busy_input_mode:steer` are the same concept three ways: inject into a possibly-busy live turn without blocking. The daemon's adapter contract should expose a delivery-mode hint (`steer` vs `followUp`/idle).

6. **Session-targeting is the real cross-harness hazard** (not whether realtime is possible — it is). opencode needs the live session id; hermes webhook needs a stable session key to avoid an isolated run. pi sidesteps it (the extension runs *inside* the session). The daemon design must define how an adapter learns "which live session to inject into."

---

## Method & sources (per CLAUDE.md thorough-research rule)
- 3 parallel deep-research passes (docs + source), then a hands-on POC I ran myself.
- **opencode:** `sst/opencode` source (`packages/opencode/src/mcp/index.ts`, `routes/instance/httpapi/handlers/session.ts`, `handlers/tui.ts`), docs opencode.ai/docs/{mcp-servers,plugins}; **+ live POC against installed v1.3.13** (runtime OpenAPI verified — endpoints drifted vs the dev branch but `prompt_async`/`/event`/`/session` all present).
- **pi:** local `~/claude/Git/repositories/pi-setup` (working `extensions/attn/index.ts`), `earendil-works/pi` `docs/extensions.md`, issues #563/#2714/#2715/#2860.
- **hermes:** `NousResearch/hermes-agent` (`gateway/run.py`, `mcp_serve.py`), docs `/user-guide/messaging/webhooks` + `/features/mcp`, PR #16279 (steer), issue #10206.

## POC evidence (opencode)
`poc-opencode/EVIDENCE.md` — injected `"INBOUND-ATTN-POC from alice.attn: what is 7 multiplied by 6? Reply with ONLY the number"` via `POST /session/:id/prompt_async` (HTTP 204, fire-and-forget); live SSE `/event` stream captured the full model turn (`message.part.delta`, `step-start/finish`, `session.idle`); model answered **42**; conversation persisted in the session. Server cleanly stopped, port 4096 free.

## Honest gaps / follow-ups for the Architect phase
- **hermes same-session continuity** — verify (small POC or a custom adapter) that an inbound can land in the user's *active* session, not just an isolated webhook run. This is the one place a naive "yes" would be wrong.
- **opencode live-session discovery** — decide whether the daemon owns/creates the opencode session or discovers the foreground one via `/event`. (TUI append+submit acts on the foreground session — confirm multi-TUI behavior.)
- **Version pinning** across all three (all are fast-moving; opencode's API already drifted between the researched dev branch and installed 1.3.13).
