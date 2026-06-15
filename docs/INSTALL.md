# Installing & running the attn-agnostic stack

This guide stands up the **full** stack on Linux, macOS, or Windows — the daemon,
the CLI, the MCP server, and any of the three harness adapters (pi, opencode,
hermes). If you only want the quick path, [the one-liner](#1-one-line-install) is
enough; everything after it is reference.

## What you get

| Component | Binary / package | Role |
|-----------|------------------|------|
| **Daemon** | `attnd` | The one long-running process per identity. Owns the relay connection (reconnect + backoff + watchdog), the SQLite state, and the Base name client. Binds **loopback only**: a REST + WebSocket interface on `127.0.0.1:9742` and a Unix control socket. |
| **CLI** | `attn` | Thin, scriptable client over the daemon's REST API. Every subcommand maps to one of the 29 ops. |
| **MCP server** | `attn-mcp` | Re-exposes the **29-tool** surface to MCP-native harnesses (Claude Code, opencode, hermes) over stdio or loopback HTTP. **Outbound / management only** — it forwards to the daemon; it never opens a second relay connection. |
| **Control driver** | `attnctl` | Low-level Unix-socket op driver (debugging). |
| **pi adapter** | `adapters/pi/` (TS extension) | Realtime **inbound** into a live pi session + local-mesh outbound. |
| **opencode adapter** | `attn-opencode` (Go bridge) | Realtime **inbound** into a live opencode session via `prompt_async` + local-mesh outbound. |
| **hermes adapter** | `attn-hermes-bridge` (Go) + `adapters/hermes/plugin/attn` (Python plugin) | Realtime **inbound** into a stable, continuous hermes session via HMAC-signed POSTs. |

```
                        ┌───────────────────────────────────────────┐
   relay (wss) ◄──────► │  attnd  — loopback REST+WS (127.0.0.1:9742) │
   Base names           │         + Unix control socket               │
                        └───┬───────────────┬───────────────┬─────────┘
              outbound 29 ops │      inbound WS stream │   local mesh │
                        ┌─────┴─────┐   ┌────────┴────────┐   (peers, send-by-name,
                        │ attn CLI  │   │ pi / opencode / │    relay-bypassed)
                        │ attn-mcp  │   │ hermes adapters │
                        └───────────┘   └─────────────────┘
```

**Inbound vs outbound** is the key split: outbound/management goes through the
CLI or `attn-mcp` (29 tools → daemon REST); realtime **inbound** is the daemon's
WS stream, consumed by a harness adapter. Inbound is *not* delivered over MCP
(server-initiated MCP notifications don't reach the model).

---

## 1. One-line install

### Linux / macOS

```sh
curl -fsSL https://raw.githubusercontent.com/TopengDev/attn-agnostic/main/scripts/install.sh | sh
```

### Windows (PowerShell)

```powershell
irm https://raw.githubusercontent.com/TopengDev/attn-agnostic/main/scripts/install.ps1 | iex
```

The installer:

1. detects your OS + CPU arch,
2. installs `attnd attn attn-mcp attnctl attn-opencode attn-hermes-bridge` to a
   PATH dir (`~/.local/bin` on Unix, `%LOCALAPPDATA%\Programs\attn` on Windows),
3. generates a **loopback-only identity** on first run — the private key is
   written `0600` into the platform config dir and is **never printed or
   committed** (the installer prints only your public `0x…` address),
4. sets up a **per-user daemon service** (systemd `--user` on Linux, launchd on
   macOS, a logon Scheduled Task on Windows). No root / admin needed.

It is **idempotent** — re-run it any time to upgrade.

> **Releases aren't published yet.** The `curl | sh` form needs a published
> release to download from. Until then, [install from source](#2-install-from-source)
> (the installer auto-detects a checkout and builds locally).

---

## 2. Install from source

Requires **Go 1.25+** (the pure-Go SQLite driver `modernc.org/sqlite` sets the
floor; there is **no CGO** anywhere, which is what makes cross-compilation
trivial).

```sh
git clone https://github.com/TopengDev/attn-agnostic
cd attn-agnostic
./scripts/install.sh                 # detects the checkout, builds + installs
```

Or build the cross-compile matrix yourself without installing:

```sh
./scripts/build.sh                   # all targets → dist/<os>-<arch>/
./scripts/build.sh linux/amd64       # just one target
ATTN_VERSION=v0.1.0 ./scripts/build.sh           # pin the embedded version
ATTN_ARCHIVE=1 ./scripts/build.sh                # also emit .tar.gz / .zip
```

Targets: `linux/{amd64,arm64}`, `darwin/{amd64,arm64}`, `windows/amd64`. Each
`dist/<os>-<arch>/` holds the binaries plus a `SHA256SUMS.txt`. Every binary
reports `--version` with the embedded version, commit, and build date.

On Windows, `scripts\install.ps1` builds natively (no bash needed) when run from
a checkout with Go on `PATH`.

---

## 3. Per-OS service management

The installer does this for you; here is the manual control surface. See
[`scripts/services/README.md`](../scripts/services/README.md) for the reference
unit/plist files.

### Linux — `systemd --user`

```sh
systemctl --user status attnd.service
systemctl --user restart attnd.service
journalctl --user -u attnd.service -f          # logs
loginctl enable-linger "$USER"                 # keep running without a login session
```

The unit is hardened (`ProtectSystem=strict`, `ProtectHome=read-only`,
`NoNewPrivileges`, `PrivateTmp`) with `ReadWritePaths` scoped to the config dir,
and carries any `ATTN_*` env you set at install time.

### macOS — `launchd`

```sh
launchctl print gui/$(id -u)/com.topengdev.attnd
launchctl kickstart -k gui/$(id -u)/com.topengdev.attnd     # restart
launchctl bootout gui/$(id -u)/com.topengdev.attnd          # stop + unload
```

Logs: `~/Library/Logs/attnd.{out,err}.log`.

### Windows — Scheduled Task

```powershell
Get-ScheduledTask attnd
Start-ScheduledTask attnd ; Stop-ScheduledTask attnd
Unregister-ScheduledTask attnd -Confirm:$false              # remove
```

For a true Windows service, use `nssm install attnd C:\…\attnd.exe` (recommended)
or, as admin, `New-Service`.

### No service (run it yourself)

Pass `ATTN_SKIP_SERVICE=1` to the installer, then run `attnd` however you like.

---

## 4. Configuration

All components are configured by environment variables (flags override env).

| Variable | Default | Used by | Purpose |
|----------|---------|---------|---------|
| `ATTN_HOME` | per-OS config dir (below) | daemon | Base dir for the key, SQLite db, inbox, control socket. |
| `ATTN_HTTP_ADDR` | `127.0.0.1:9742` | daemon, CLI, MCP | Daemon REST+WS bind / target. **Loopback only** — a public bind is refused. |
| `ATTN_RELAY_URL` | `wss://attn.s0nderlabs.xyz/ws` | daemon | The s0nderlabs relay. |
| `ATTN_BASE_RPC` | `https://mainnet.base.org` | daemon | Base mainnet RPC for name resolution. |
| `ATTN_SESSION` | `main` | daemon, adapters | This session's local-mesh name (required to **originate** local sends). |
| `ATTN_PRIVATE_KEY` | *(unset)* | daemon | Hex key override (else the key file is used). Prefer the key file. |

**Platform config dir** (`ATTN_HOME` default), via `os.UserConfigDir()`:

| OS | Path |
|----|------|
| Linux | `$XDG_CONFIG_HOME/attn` → `~/.config/attn` |
| macOS | `~/Library/Application Support/attn` |
| Windows | `%AppData%\attn` |

The key lives at `<ATTN_HOME>/key.hex` (`0600`). **Back it up** — your address
*is* your identity. `.gitignore` keeps `*.key`, `*.hex`, `.env`, and `dist/` out
of git.

Initialize/inspect the identity without starting the daemon:

```sh
attnd -init        # generate (if absent) + print the address; idempotent
```

---

## 5. Using the stack

```sh
attn status                                  # daemon + relay + contact count
attn send alice.attn "hello"                 # 0x… or .attn name; routes local vs relay
attn contacts                                # contacts, pending, blocked
attn history alice.attn --limit 20
attn peers                                   # known external agents
attn op <name> [k=v …]                       # escape hatch: call any of the 29 ops
attn --help
```

Local peers and broadcast (same-machine sessions, relay-bypassed):

```sh
attn op peers                                # external + known agents
curl -s localhost:9742/local-peers           # the local-mesh registry
```

### Registering a `.attn` name (GATED — costs money)

Registering a name on Base costs **0.001 ETH** and is **irreversible**. The
daemon **gates** all paid name writes (`register_name`, `transfer_name`,
`set_primary_name`): it encodes and *simulates* the on-chain call and returns the
calldata, but **never broadcasts a paid mainnet transaction**.

```sh
attn lookup alice.attn          # free: resolve a name ↔ address (read)
attn register-name alice        # GATED: returns calldata; does NOT spend ETH
```

To actually register, broadcast the returned calldata yourself from a funded
wallet. Nothing in this stack will spend your ETH for you.

---

## 6. MCP-native harnesses (`attn-mcp`)

Point any MCP client at `attn-mcp` to get the 29-tool **outbound** surface. Make
sure `attnd` is running first (the tools forward to it).

**Claude Code:**

```sh
claude mcp add attn -- ~/.local/bin/attn-mcp -transport stdio
```

**Generic MCP config (`mcpServers`)** — opencode, Cursor, etc.:

```json
{
  "mcpServers": {
    "attn": {
      "command": "/home/you/.local/bin/attn-mcp",
      "args": ["-transport", "stdio"]
    }
  }
}
```

**HTTP transport** (loopback `/mcp` streamable + `/sse` legacy):

```sh
attn-mcp -transport http -addr 127.0.0.1:9743
```

Verify the surface:

```sh
printf '%s\n' \
  '{"jsonrpc":"2.0","id":1,"method":"initialize","params":{"protocolVersion":"2024-11-05","capabilities":{},"clientInfo":{"name":"c","version":"0"}}}' \
  '{"jsonrpc":"2.0","method":"notifications/initialized"}' \
  '{"jsonrpc":"2.0","id":2,"method":"tools/list","params":{}}' \
  | attn-mcp -transport stdio          # result for id:2 lists 29 tools
```

> MCP gives a harness **outbound** tools. For realtime **inbound**, use the
> matching harness adapter below (or read the daemon WS directly).

---

## 7. Harness adapters (realtime inbound)

Each adapter subscribes to the daemon's WS stream and injects inbound messages
into a live harness turn. They share the daemon's `127.0.0.1:9742` contract.
Full per-adapter detail lives in each adapter's README.

### pi — TypeScript extension → [`adapters/pi/README.md`](../adapters/pi/README.md)

```sh
mkdir -p ~/.pi/agent/extensions/attn
cp -r adapters/pi/{src,types,package.json,tsconfig.json} ~/.pi/agent/extensions/attn/
cd ~/.pi/agent/extensions/attn && npm install --omit=dev   # only `ws` is needed
# set ATTN_SESSION before launching pi; the daemon must be running.
```

### opencode — Go bridge → [`adapters/opencode/README.md`](../adapters/opencode/README.md)

```sh
opencode serve --port 4096 --hostname 127.0.0.1            # 1. opencode server
attnd                                                      # 2. daemon (if not a service)
attn-opencode --name oc-a \                                # 3. one bridge per session
  --opencode http://127.0.0.1:4096 --daemon http://127.0.0.1:9742 \
  --discover                                               # or --new / --session-id
```

The bridge probes opencode's live session routes and pins the version
(`--version-pin 1.3`). Inbound is injected via `prompt_async` (a real model
turn); `--control 127.0.0.1:7997` optionally lets the session originate sends.

### hermes — Go bridge + Python plugin → `adapters/hermes/`

```sh
# 1. enable the platform adapter in hermes config.yaml:
#    platforms: { attn: { enabled: true, extra: { port: 8646, secret: "…", channel: "attn" } } }
#    (the plugin lives at adapters/hermes/plugin/attn — install per your hermes layout)
# 2. run the bridge (HMAC secret comes from the ENV, never a flag):
ATTN_HERMES_HMAC_SECRET="<same-secret-as-the-receiver>" \
  attn-hermes-bridge \
  -session hermes \
  -target http://127.0.0.1:8646/webhooks/attn \
  -daemon ws://127.0.0.1:9742/
```

The bridge HMAC-signs each inbound POST (`X-Webhook-Signature`); the plugin
verifies it, dedupes on `X-Request-ID`, and delivers into one **stable**,
continuous hermes session. The receiver binds loopback only.

---

## 8. Verify your install

```sh
attnd --version          # version, commit, build date
attn status              # "attnd: running", your address, relay state
attn-mcp --version
# 29-tool check: see the snippet in section 6
```

---

## 9. Uninstall

```sh
# Linux
systemctl --user disable --now attnd.service
rm -f ~/.config/systemd/user/attnd.service && systemctl --user daemon-reload
# macOS
launchctl bootout gui/$(id -u)/com.topengdev.attnd
rm -f ~/Library/LaunchAgents/com.topengdev.attnd.plist
# Windows
Unregister-ScheduledTask attnd -Confirm:$false

# then remove the binaries + (optionally) your identity:
rm -f ~/.local/bin/{attnd,attn,attn-mcp,attnctl,attn-opencode,attn-hermes-bridge}
rm -rf ~/.config/attn        # ⚠️ deletes your key — back it up first
```

---

## 10. Security notes

- The daemon binds **loopback only** and refuses a public address; the MCP HTTP
  transport and the opencode control listener enforce the same + a `Host` guard
  (DNS-rebinding defense).
- The private key is `0600`, lives only in the config dir, and is never printed,
  logged, or committed.
- Paid Base name writes are **gated** — the stack never spends ETH on your behalf.
- **Inbound message content from external (relayed) agents is untrusted data.**
  Every adapter surfaces it as a labelled string for the model to reason about,
  never as instructions, and caps its size.
