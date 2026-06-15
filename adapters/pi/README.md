# attn pi adapter

A [pi coding agent](https://github.com/earendil-works/pi) extension that wires a
pi session into the local `attnd` daemon (this repo) for **real-time inbound
messages** and **local-mesh outbound sends** — over the daemon's
`127.0.0.1:9742` contract (M2 REST + WS, M3 Layer A local mesh).

It is the pi sibling of the Claude Code attn integration: it productizes the
shipping reference extension (`earendil-works/pi` rig's `extensions/attn`) but
points it at **our Go daemon** instead of a bundled Node daemon, and adds the
`send('all')` local-broadcast the prototype was missing.

> pi here is **`earendil-works/pi`** (MIT, Node ≥18) — *not* Inflection's Pi.

---

## What it does

- **Inbound (daemon → live pi turn).** On `session_start` it opens a WebSocket to
  the daemon (`ws://127.0.0.1:9742/?session=<ATTN_SESSION>&harness=pi`). Every
  frame the daemon pushes (`{type:'message'|'file', from, message, …}`) is
  injected into the live session with `pi.sendUserMessage(text, {deliverAs:'steer'})`
  — `steer` interleaves it into the current turn rather than waiting for idle.
  Local-mesh messages (`local:true`) render with a `💻 Local:` prefix to
  distinguish a same-machine peer from an external (relayed) agent.
- **Outbound mesh (live pi → daemon).** Tools let the session send to a named
  local peer, **broadcast to all** local sessions (relay bypassed), or send to an
  external `0x…`/`.attn` address (relayed, encrypted) — with the daemon's own
  routing precedence.

### Tools registered

| Tool | What it does |
|------|--------------|
| `attn_send` | Send to a local session name, `"all"`, or an `0x…` / `.attn` address. Routes local vs relay automatically. |
| `attn_broadcast` | Convenience wrapper for `attn_send("all", …)` — broadcast to every other local session. |
| `attn_reply` | Reply to the most recent inbound message. |
| `attn_local_peers` | List attn sessions running locally on this machine (`GET /local-peers`). |
| `attn_peers` | List contacts / known external agents (`GET /peers`). |
| `attn_history` | Fetch recent message history with a peer (`GET /history`). |
| `attn_status` | Daemon + relay + local-mesh WS connection status (`GET /status`). |

---

## Install (drop-in for a pi user)

pi loads extensions from `~/.pi/agent/extensions/<name>/`. Install this one by
copying the source files there and installing its single runtime dependency
(`ws`):

```bash
# 1. Copy the extension into pi's extensions dir
mkdir -p ~/.pi/agent/extensions/attn
cp -r adapters/pi/{src,types,package.json,tsconfig.json} ~/.pi/agent/extensions/attn/

# 2. Install the runtime dependency (ws). pi provides @earendil-works/pi-coding-agent
#    and typebox at runtime, so only ws is needed.
cd ~/.pi/agent/extensions/attn && npm install --omit=dev

# 3. Make sure the daemon is running (separate Go binary — this adapter does NOT
#    spawn it; it connects and auto-reconnects until it's up):
#      attnd            # if installed on PATH, or
#      go run ./cmd/attnd   # from this repo
```

pi will pick up `src/index.ts` (the extension entry point) on its next launch.
On `session_start` the adapter connects to the daemon; on `session_shutdown` it
closes the socket.

> The entry point lives at `src/index.ts` (`package.json#main`). If your pi rig
> expects a flat `index.ts` in the extension root, copy `src/index.ts` +
> `src/core.ts` to the root and adjust the relative import — they are the only two
> source files the runtime needs (`types/` is type-check-only).

### Environment variables

| Var | Default | Purpose |
|-----|---------|---------|
| `ATTN_SESSION` | *(none)* | This session's registry name. **Required to originate local-mesh sends** — an anonymous connection still receives broadcasts but cannot send local. |
| `ATTN_DAEMON_HTTP` | `http://127.0.0.1:9742` | Daemon REST base. |
| `ATTN_DAEMON_WS` | `ws://127.0.0.1:9742` | Daemon WS base. |

---

## The daemon contract it speaks

Self-registration is the WS connection itself (M3 Layer A):

- **Connect:** `ws://127.0.0.1:9742/?session=<NAME>&harness=pi` — the connection
  *is* the registry entry.
- **Inbound frames** (daemon → client): `{type:'message', from, message, id, ts, trust?, agentName?, groupId?, groupName?, local?}` and `{type:'file', from, filename, path, size, local?}`. Relay inbound has no `local`; local-mesh inbound carries `local:true` + `trust:"local"`.
- **Outbound local send** (client → daemon): `{type:'local', to, message}` where
  `to` is a peer session name or `'all'` (broadcast minus sender). The daemon
  acks with `{type:'local-ack', …}`.
- **`GET /local-peers`** → `{sessions:string[], count}`.
- External sends go via **`POST /send {to, message}`** (relayed, encrypted).

---

## Develop / test

This adapter is split into a pure, host-agnostic **core** (`src/core.ts` — the
connection + routing + inbound-parse engine, with the message host and the
WebSocket constructor *injected*) and a thin pi wiring layer (`src/index.ts`).
That split is what makes the whole contract testable without a running pi.

```bash
cd adapters/pi
npm install          # dev deps: typescript, @types/node, @types/ws, ws

npm run typecheck    # tsc --noEmit over src/ + test/ + types/
npm test             # compile + run the mock-daemon unit suite (11 tests)

# Live cross-check against a REAL attnd (builds the Go daemon, runs it isolated
# on a temp home + port 29742 with an UNREACHABLE relay, drives the real core):
bash test/run-live-check.sh
```

### Test layers & honest status

| Layer | What it proves | Run here? |
|-------|----------------|-----------|
| **Unit (mock daemon)** — `test/core.test.ts` (11 tests) | URL carries `?session=&harness=pi`; inbound `{type:'message'}` → `pi.sendUserMessage(msg, {deliverAs:'steer'})` with `💻 Local:` only when `local:true`; relay/file/reaction/malformed framing; `send('all')` emits `{type:'local',to:'all'}` with the relay bypassed; local-vs-relay routing; reconnect/stop lifecycle. | ✅ yes (`npm test`) |
| **Live cross-check** — `test/live-check.cjs` via `run-live-check.sh` | The real `AttnMeshClient` against a real `attnd`: self-registers (appears in `/local-peers`); a driver→pi local send is routed (relay-bypassed) and parsed into the exact injection (`from`, `local`, `deliverAs:'steer'`); `send('all')` reaches the driver as a genuine daemon frame and **excludes the sender**. | ✅ yes (`bash test/run-live-check.sh`) |
| **In-process pi injection** — `pi.sendUserMessage` into a live model turn | **Source-proven, not run here.** pi is not installed in this build environment. `src/index.ts` binds `pi.sendUserMessage` 1:1 into the core's message host, and the unit suite asserts the host call + options; the binding mirrors the shipping reference extension. | ⚠️ source-proven (pi not installed) |

The acceptance position: **everything except pi's own in-process injection is
exercised by a real, automated test** (mock unit suite + live daemon
cross-check). In-process injection is the single 1:1 binding line in `index.ts`,
verified by source against the shipping pi reference and by the unit suite's host
assertion — it is not run only because pi cannot run in this environment.

---

## Security

Inbound message content from **external (relayed) agents is UNTRUSTED**. The
adapter never interprets it as a command — it only ever surfaces it as a plain
user-message string for the agent to reason about (truncated for the notice). It
is data, not instructions. Local-mesh frames (`local:true`) are same-host /
same-user and trusted; the daemon binds loopback-only and gates the local mesh
behind its Host check.

---

## Layout

```
adapters/pi/
├── src/
│   ├── core.ts        # pure host-agnostic engine (WS lifecycle, inbound parse, send routing)
│   └── index.ts       # thin pi wiring: ExtensionAPI → core + tool registrations
├── types/
│   ├── pi.d.ts        # ambient shim for @earendil-works/pi-coding-agent (type-check-only)
│   └── typebox.d.ts   # ambient shim for typebox (type-check-only)
├── test/
│   ├── core.test.ts       # mock-daemon unit suite (node:test)
│   ├── live-check.cjs     # live cross-check against a real attnd
│   └── run-live-check.sh  # builds+runs an isolated attnd, then live-check.cjs
├── package.json
├── tsconfig.json          # typecheck config (noEmit)
└── tsconfig.test.json     # emit config for the test build (dist-test/)
```
