#!/bin/sh
#
# install.sh — one-line installer for the attn-agnostic stack (Linux + macOS).
#
#   curl -fsSL https://raw.githubusercontent.com/TopengDev/attn-agnostic/main/scripts/install.sh | sh
#
# or, from a checkout:
#
#   ./scripts/install.sh
#
# It detects your OS + arch, installs the daemon + CLI + MCP server + the Go
# harness bridges to a PATH dir, generates a loopback-only identity on first run
# (the private key is written 0600 to the platform config dir and NEVER printed),
# and sets up a per-user daemon service (systemd --user on Linux, launchd on
# macOS). It is idempotent — safe to re-run to upgrade.
#
# Two install methods, auto-selected:
#   1. from source — when run inside a checkout (or ATTN_REPO_DIR is set) AND Go
#      1.25+ is available: cross-builds via scripts/build.sh.
#   2. download    — fetch a prebuilt release archive from ATTN_RELEASE_BASE.
#      (No public releases are published yet, so this needs ATTN_RELEASE_BASE.)
#
# Env knobs:
#   ATTN_BIN_DIR       install dir for binaries        (default: ~/.local/bin)
#   ATTN_HOME          daemon state/key dir            (default: per-OS config dir)
#   ATTN_VERSION       version to embed/fetch          (default: git describe / latest)
#   ATTN_RELEASE_BASE  base URL for prebuilt archives  (enables the download path)
#   ATTN_REPO_DIR      path to a checkout to build from
#   ATTN_SKIP_SERVICE  if set (=1), do not create/enable the daemon service
#
# POSIX sh on purpose (curl | sh runs under /bin/sh). `local` is used and is
# supported by every real /bin/sh (dash, ash, busybox, bash).
# shellcheck disable=SC3043
set -eu
unset CDPATH

BINARIES="attnd attn attn-mcp attnctl attn-opencode attn-hermes-bridge"

ATTN_BIN_DIR="${ATTN_BIN_DIR:-$HOME/.local/bin}"
ATTN_RELEASE_BASE="${ATTN_RELEASE_BASE:-}"
ATTN_VERSION="${ATTN_VERSION:-}"

say()  { printf '%s\n' "$*"; }
step() { printf '\033[1m==>\033[0m %s\n' "$*"; }
warn() { printf 'install.sh: %s\n' "$*" >&2; }
die()  { warn "$*"; exit 1; }
have() { command -v "$1" >/dev/null 2>&1; }

detect_os() {
	case "$(uname -s)" in
	Linux) echo linux ;;
	Darwin) echo darwin ;;
	MINGW* | MSYS* | CYGWIN*) die "Windows detected — use install.ps1 instead:
    irm https://raw.githubusercontent.com/TopengDev/attn-agnostic/main/scripts/install.ps1 | iex" ;;
	*) die "unsupported OS: $(uname -s)" ;;
	esac
}

detect_arch() {
	case "$(uname -m)" in
	x86_64 | amd64) echo amd64 ;;
	aarch64 | arm64) echo arm64 ;;
	*) die "unsupported architecture: $(uname -m) (supported: x86_64/amd64, aarch64/arm64)" ;;
	esac
}

# find_repo prints a checkout root containing this module, or returns 1.
find_repo() {
	if [ -n "${ATTN_REPO_DIR:-}" ] && [ -f "$ATTN_REPO_DIR/go.mod" ]; then
		echo "$ATTN_REPO_DIR"
		return 0
	fi
	# $0 is a real file only when run as a script (not piped via curl | sh).
	if [ -f "$0" ]; then
		local d
		d=$(cd -- "$(dirname -- "$0")/.." 2>/dev/null && pwd) || d=""
		if [ -n "$d" ] && grep -q 'module github.com/TopengDev/attn-agnostic' "$d/go.mod" 2>/dev/null; then
			echo "$d"
			return 0
		fi
	fi
	return 1
}

fetch() {
	# fetch <url> <out>
	if have curl; then
		curl -fsSL -o "$2" "$1"
	elif have wget; then
		wget -qO "$2" "$1"
	else
		return 1
	fi
}

# build_from_source <repo> <os> <arch> → sets SRC_DIR to the built dist dir.
build_from_source() {
	local repo="$1" os="$2" arch="$3"
	have go || die "building from source needs Go 1.25+ (https://go.dev/dl).
  Or set ATTN_RELEASE_BASE to install prebuilt binaries."
	[ -f "$repo/scripts/build.sh" ] || die "missing $repo/scripts/build.sh"
	step "building from source ($repo) for $os/$arch"
	( cd "$repo" && ATTN_VERSION="$ATTN_VERSION" sh scripts/build.sh "$os/$arch" >&2 )
	SRC_DIR="$repo/dist/$os-$arch"
	[ -d "$SRC_DIR" ] || die "build produced no $SRC_DIR"
}

# download_release <os> <arch> → sets SRC_DIR to a temp dir of extracted binaries.
download_release() {
	local os="$1" arch="$2"
	[ -n "$ATTN_RELEASE_BASE" ] || die "no source checkout found and ATTN_RELEASE_BASE is not set.
  Prebuilt releases are not published yet. Install from source instead:
    git clone https://github.com/TopengDev/attn-agnostic
    cd attn-agnostic && ./scripts/install.sh
  (or set ATTN_RELEASE_BASE=<release-url> once binaries are published)."
	have curl || have wget || die "need curl or wget to download"
	local ver tmp archive url
	ver="${ATTN_VERSION:-latest}"
	archive="attn-agnostic_${ver}_${os}-${arch}.tar.gz"
	url="$ATTN_RELEASE_BASE/$archive"
	tmp=$(mktemp -d)
	step "downloading $url"
	fetch "$url" "$tmp/$archive" || die "download failed: $url"
	tar -C "$tmp" -xzf "$tmp/$archive" || die "extract failed: $archive"
	SRC_DIR="$tmp"
}

install_binaries() {
	local src="$1" b
	step "installing binaries → $ATTN_BIN_DIR"
	mkdir -p "$ATTN_BIN_DIR"
	for b in $BINARIES; do
		if [ -f "$src/$b" ]; then
			cp "$src/$b" "$ATTN_BIN_DIR/$b"
			chmod 0755 "$ATTN_BIN_DIR/$b"
			say "  $b"
		else
			warn "  $b missing in $src (skipped)"
		fi
	done
}

resolve_home() {
	local os="$1"
	if [ -n "${ATTN_HOME:-}" ]; then
		echo "$ATTN_HOME"
	elif [ "$os" = darwin ]; then
		echo "$HOME/Library/Application Support/attn"
	else
		echo "${XDG_CONFIG_HOME:-$HOME/.config}/attn"
	fi
}

init_identity() {
	step "initializing identity (key written 0600, never printed)"
	# attnd -init is idempotent: generates the key only if absent, then exits.
	"$ATTN_BIN_DIR/attnd" -init
}

# systemd_env / launchd_env emit extra service env lines for any passthrough
# vars the user set at install time, so the managed daemon runs with the SAME
# config the installer used (custom relay, port, session). ATTN_HOME is always
# set by the caller, so it is not repeated here.
systemd_env() {
	if [ -n "${ATTN_HTTP_ADDR:-}" ]; then printf 'Environment=ATTN_HTTP_ADDR=%s\n' "$ATTN_HTTP_ADDR"; fi
	if [ -n "${ATTN_RELAY_URL:-}" ]; then printf 'Environment=ATTN_RELAY_URL=%s\n' "$ATTN_RELAY_URL"; fi
	if [ -n "${ATTN_SESSION:-}" ]; then printf 'Environment=ATTN_SESSION=%s\n' "$ATTN_SESSION"; fi
	return 0
}
launchd_env() {
	if [ -n "${ATTN_HTTP_ADDR:-}" ]; then printf '        <key>ATTN_HTTP_ADDR</key>\n        <string>%s</string>\n' "$ATTN_HTTP_ADDR"; fi
	if [ -n "${ATTN_RELAY_URL:-}" ]; then printf '        <key>ATTN_RELAY_URL</key>\n        <string>%s</string>\n' "$ATTN_RELAY_URL"; fi
	if [ -n "${ATTN_SESSION:-}" ]; then printf '        <key>ATTN_SESSION</key>\n        <string>%s</string>\n' "$ATTN_SESSION"; fi
	return 0
}

setup_service_linux() {
	local attn_home="$1" unit_dir unit
	unit_dir="$HOME/.config/systemd/user"
	unit="$unit_dir/attnd.service"
	if ! have systemctl; then
		warn "systemctl not found — skipping service. Start manually: $ATTN_BIN_DIR/attnd"
		return 0
	fi
	step "installing systemd --user service (attnd.service)"
	mkdir -p "$unit_dir"
	cat >"$unit" <<EOF
[Unit]
Description=attn-agnostic daemon (attnd) — agent messaging network client
Documentation=https://github.com/TopengDev/attn-agnostic

[Service]
Type=simple
Environment=ATTN_HOME=$attn_home
$(systemd_env)
ExecStart=$ATTN_BIN_DIR/attnd
Restart=on-failure
RestartSec=3
NoNewPrivileges=true
ProtectSystem=strict
ProtectHome=read-only
ReadWritePaths=$attn_home
PrivateTmp=true
ProtectControlGroups=true
RestrictSUIDSGID=true

[Install]
WantedBy=default.target
EOF
	if systemctl --user daemon-reload 2>/dev/null &&
		systemctl --user enable --now attnd.service 2>/dev/null; then
		say "  enabled + started (systemctl --user status attnd)"
	else
		warn "  could not start via systemd (no active --user session / D-Bus?)."
		warn "  unit written to $unit — enable it in a login session with:"
		warn "    systemctl --user enable --now attnd.service"
		warn "  or just run: $ATTN_BIN_DIR/attnd"
	fi
}

setup_service_darwin() {
	local attn_home="$1" label plist log_dir
	label="com.topengdev.attnd"
	plist="$HOME/Library/LaunchAgents/$label.plist"
	log_dir="$HOME/Library/Logs"
	step "installing launchd LaunchAgent ($label)"
	mkdir -p "$HOME/Library/LaunchAgents" "$log_dir"
	cat >"$plist" <<EOF
<?xml version="1.0" encoding="UTF-8"?>
<!DOCTYPE plist PUBLIC "-//Apple//DTD PLIST 1.0//EN" "http://www.apple.com/DTDs/PropertyList-1.0.dtd">
<plist version="1.0">
<dict>
    <key>Label</key>
    <string>$label</string>
    <key>ProgramArguments</key>
    <array>
        <string>$ATTN_BIN_DIR/attnd</string>
    </array>
    <key>EnvironmentVariables</key>
    <dict>
        <key>ATTN_HOME</key>
        <string>$attn_home</string>
$(launchd_env)
    </dict>
    <key>RunAtLoad</key>
    <true/>
    <key>KeepAlive</key>
    <dict>
        <key>SuccessfulExit</key>
        <false/>
    </dict>
    <key>ProcessType</key>
    <string>Background</string>
    <key>StandardOutPath</key>
    <string>$log_dir/attnd.out.log</string>
    <key>StandardErrorPath</key>
    <string>$log_dir/attnd.err.log</string>
</dict>
</plist>
EOF
	if have launchctl; then
		launchctl bootout "gui/$(id -u)/$label" 2>/dev/null || true
		if launchctl bootstrap "gui/$(id -u)" "$plist" 2>/dev/null; then
			say "  loaded ($label) — launchctl print gui/$(id -u)/$label"
		elif launchctl load -w "$plist" 2>/dev/null; then
			# Fallback for older macOS (pre-bootstrap).
			say "  loaded ($label) via legacy launchctl load"
		else
			warn "  plist written to $plist — load with: launchctl bootstrap gui/$(id -u) \"$plist\""
		fi
	else
		warn "  launchctl not found — plist written to $plist (load it manually)"
	fi
}

setup_service() {
	local os="$1" attn_home="$2"
	if [ -n "${ATTN_SKIP_SERVICE:-}" ]; then
		step "skipping service setup (ATTN_SKIP_SERVICE set) — run: $ATTN_BIN_DIR/attnd"
		return 0
	fi
	case "$os" in
	linux) setup_service_linux "$attn_home" ;;
	darwin) setup_service_darwin "$attn_home" ;;
	esac
}

check_path() {
	case ":$PATH:" in
	*":$ATTN_BIN_DIR:"*) : ;;
	*)
		warn "note: $ATTN_BIN_DIR is not on your PATH. Add this to your shell rc:"
		say  "    export PATH=\"$ATTN_BIN_DIR:\$PATH\""
		;;
	esac
}

main() {
	local os arch repo attn_home
	os=$(detect_os)
	arch=$(detect_arch)
	step "attn-agnostic installer — target $os/$arch"

	if repo=$(find_repo); then
		build_from_source "$repo" "$os" "$arch"
	else
		download_release "$os" "$arch"
	fi

	install_binaries "$SRC_DIR"
	attn_home=$(resolve_home "$os")
	init_identity
	setup_service "$os" "$attn_home"
	check_path

	say ""
	step "done. next steps:"
	say "  • check the daemon:    attn status"
	say "  • set your handle:     attn register-name <you>   (GATED — costs 0.001 ETH on Base)"
	say "  • MCP-native harness:  point it at '$ATTN_BIN_DIR/attn-mcp' (stdio) — see docs/INSTALL.md"
	say "  • pi / opencode / hermes adapters: see docs/INSTALL.md"
	say ""
	say "  config dir (key + db): $attn_home   (key.hex is 0600 — keep it private)"
}

main "$@"
