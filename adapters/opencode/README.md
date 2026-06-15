# attn-opencode adapter (M3 Stage-2)

A **WS-subscriber bridge** that connects one [opencode](https://opencode.ai)
session to an `attnd` daemon. It delivers realtime attn inbound (relay +
local-mesh) into a **live** opencode session and relays outbound local-mesh sends
from that session.

```
            attnd (127.0.0.1:9742)                 opencode serve (127.0.0.1:4096)
  inbound ── WS /?session=oc-a&harness=opencode ──► attn-opencode ──► POST /session/<id>/prompt_async
  (relay/local)         ▲   the WS conn IS the                 (real model turn)
  outbound  ◄── {type:"local",to,message} over the same WS ◄── bridge (oc-a → session_id)
```

## Why a WS-subscriber bridge

The daemon's WS endpoint doubles as the **Layer-A local-mesh registry**: a
subscriber that connects with `?session=<name>` *is* a registry entry (see
`internal/httpapi/ws.go`). So one WS connection gives the bridge, uniformly:

- **inbound** — the daemon pushes every surfaced frame (external relay messages
  *and* routed local-mesh sends) to the subscriber; the bridge injects each into
  its opencode session via `prompt_async`.
- **outbound** — the bridge writes `{type:"local",to,message}` back on the same
  connection; the daemon attributes the sender from the connection's `?session=`
  name (no client-supplied `from`), so a session can't spoof another and
  per-session attribution is correct without running one daemon per session.
- **discovery** — `peers` is just the daemon's `GET /local-peers` over the shared
  registry; `send(name)` routes by name to exactly one subscriber.

The bridge holds the `name → opencode session_id` mapping locally — the daemon
never needs the opencode session id.

## Runtime contract (opencode v1.3.13, verified live)

opencode's API has **drifted**: the global `GET /doc` only lists `/global/*`,
`/auth/*`, `/log`. The session routes are **live at runtime but absent from the
doc**. The bridge therefore *probes the real routes* (`VerifyRoutes`) instead of
trusting `/doc`, and enforces a version-prefix pin (`--version-pin`, default
`1.3`). Routes used:

| route | use |
|---|---|
| `GET /global/health` | version + liveness |
| `GET /session?directory=` | discover / list sessions |
| `POST /session?directory=` | create (own) a session |
| `POST /session/:id/prompt_async` `{parts:[{type:"text",text}]}` | **Layer-B inject** → 204, real model turn |
| `GET /session/:id/message` | transcript read-back (verification) |
| `GET /event` (SSE) | live turn observation |

## Run

```sh
go build -o ./bin/attnd ./cmd/attnd
go build -o ./bin/attn-opencode ./cmd/attn-opencode

# 1. opencode server (unsecured loopback by default — same-host OK)
opencode serve --port 4096 --hostname 127.0.0.1

# 2. attnd daemon (owns the relay + the local-mesh registry)
./bin/attnd --gen-key            # http interface on 127.0.0.1:9742

# 3. one bridge per opencode session
./bin/attn-opencode --name oc-a \
  --opencode http://127.0.0.1:4096 --daemon http://127.0.0.1:9742 \
  --discover \                       # or --new, or --session-id ses_…
  --control 127.0.0.1:7997           # optional: lets the session originate sends
```

Session resolution precedence: `--session-id` > `--new` (create) > `--discover`
(most-recently-updated existing session). Default when none given: discover,
falling back to create.

### Control listener (optional, loopback-only)

Lets the opencode session originate mesh ops (e.g. from a custom command/tool):

- `POST /mesh/send {to,message}` — `to:"all"` broadcasts; relayed over the WS so
  attribution is correct.
- `GET /mesh/peers` — the other local sessions (self filtered).

## Security

- **Inbound is untrusted.** `Render` labels each frame as `📨 attn inbound …`
  data with explicit provenance, never as instructions; bodies are size-capped.
- **Loopback only.** The control listener binds loopback and rejects non-loopback
  `Host` (DNS-rebinding defense), mirroring the daemon interface.
- **opencode auth.** Unsecured on loopback by default. If you set
  `OPENCODE_SERVER_PASSWORD`, pass `--password` / the env var and the client
  sends `Authorization: Bearer <pw>`; verify the header against your opencode
  version before relying on it.
