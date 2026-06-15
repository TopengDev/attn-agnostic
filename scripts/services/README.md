# Daemon service files

Per-OS service definitions that keep `attnd` running in the background as a
**per-user** service (no root / admin). The installers (`scripts/install.sh`,
`scripts/install.ps1`) generate the concrete versions with your real install
paths; the files here are the reference templates.

`attnd` owns its own relay connection with reconnect + backoff + a watchdog, so
none of these add network-ordering dependencies — the daemon tolerates a down
relay at start and recovers on its own. It binds loopback only.

| OS | Mechanism | File / location |
|----|-----------|-----------------|
| **Linux** | `systemd --user` unit | `attnd.service` → `~/.config/systemd/user/attnd.service` |
| **macOS** | `launchd` LaunchAgent | `com.topengdev.attnd.plist` → `~/Library/LaunchAgents/` |
| **Windows** | logon Scheduled Task | registered by `install.ps1` (no file here) |

## Linux — systemd --user

```sh
# install.sh does this for you; manual equivalent:
cp scripts/services/attnd.service ~/.config/systemd/user/attnd.service
# (edit ExecStart if attnd is not at ~/.local/bin, and ReadWritePaths if you
#  set a non-default ATTN_HOME)
systemctl --user daemon-reload
systemctl --user enable --now attnd.service
systemctl --user status attnd.service     # check
journalctl --user -u attnd.service -f      # logs
```

The unit uses the `%h` specifier for the home dir and is hardened
(`ProtectSystem=strict`, `ProtectHome=read-only`, `NoNewPrivileges`,
`PrivateTmp`); `ReadWritePaths` is scoped to the daemon's config dir. To run at
boot without an interactive login: `loginctl enable-linger $USER`.

## macOS — launchd

```sh
# install.sh substitutes the __ATTND_BIN__ / __LOG_OUT__ / __LOG_ERR__
# placeholders (launchd does not expand ~), then:
launchctl bootstrap gui/$(id -u) ~/Library/LaunchAgents/com.topengdev.attnd.plist
launchctl print gui/$(id -u)/com.topengdev.attnd     # check
# stop / unload:
launchctl bootout gui/$(id -u)/com.topengdev.attnd
```

`RunAtLoad` starts it at login; `KeepAlive{SuccessfulExit=false}` restarts it
only after a crash (a clean shutdown stays down). Logs go to `~/Library/Logs/`.

## Windows — Scheduled Task

`install.ps1` registers a per-user **logon** Scheduled Task named `attnd` (no
admin), with restart-on-failure. Manage it:

```powershell
Get-ScheduledTask attnd
Start-ScheduledTask attnd ; Stop-ScheduledTask attnd
Unregister-ScheduledTask attnd -Confirm:$false   # remove
```

Alternatives (documented by the installer on failure):

- **Windows service (needs admin):** `New-Service -Name attnd -BinaryPathName 'C:\path\attnd.exe'`
  (note: `attnd` is a plain console app, not an SCM-aware service — prefer nssm).
- **nssm (recommended for a true service):** `nssm install attnd C:\path\attnd.exe`
