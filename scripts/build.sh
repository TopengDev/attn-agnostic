#!/usr/bin/env bash
#
# scripts/build.sh — cross-compile every attn-agnostic binary for the full
# OS/arch matrix into versioned per-target dirs under dist/.
#
# The whole stack is pure-Go (modernc.org/sqlite, no CGO), so cross-compilation
# needs no C toolchain: we force CGO_ENABLED=0 and let `go build` target any
# GOOS/GOARCH from a single Linux/macOS/Windows host.
#
# Usage:
#   scripts/build.sh                      # build all targets, version from git
#   ATTN_VERSION=v0.1.0 scripts/build.sh  # pin the embedded version
#   scripts/build.sh linux/amd64 darwin/arm64   # build only the given targets
#   ATTN_ARCHIVE=1 scripts/build.sh       # also produce per-target archives
#
# Env knobs:
#   ATTN_VERSION   embedded version string (default: `git describe`, else "dev")
#   ATTN_DIST_DIR  output root (default: dist)
#   ATTN_ARCHIVE   if "1", create .tar.gz (unix) / .zip (windows) per target
#
# Output: $ATTN_DIST_DIR/<os>-<arch>/<binary>[.exe] + SHA256SUMS.txt
set -euo pipefail

# ── locate the repo root (this script lives in scripts/) ────────────────────
unset CDPATH
SCRIPT_DIR=$(cd -- "$(dirname -- "$0")" && pwd)
REPO_ROOT=$(cd -- "$SCRIPT_DIR/.." && pwd)
cd "$REPO_ROOT"

if ! command -v go >/dev/null 2>&1; then
	echo "build.sh: 'go' not found on PATH (need Go 1.25+)" >&2
	exit 1
fi

# ── version metadata (embedded via -ldflags -X into internal/buildinfo) ─────
VERSION="${ATTN_VERSION:-}"
if [ -z "$VERSION" ]; then
	VERSION=$(git describe --tags --always --dirty 2>/dev/null || echo "dev")
fi
COMMIT=$(git rev-parse --short HEAD 2>/dev/null || echo "none")
DATE=$(date -u +%Y-%m-%dT%H:%M:%SZ)

BUILDINFO_PKG="github.com/TopengDev/attn-agnostic/internal/buildinfo"
LDFLAGS="-s -w"
LDFLAGS="$LDFLAGS -X ${BUILDINFO_PKG}.Version=${VERSION}"
LDFLAGS="$LDFLAGS -X ${BUILDINFO_PKG}.Commit=${COMMIT}"
LDFLAGS="$LDFLAGS -X ${BUILDINFO_PKG}.Date=${DATE}"

# ── the matrix ──────────────────────────────────────────────────────────────
DEFAULT_TARGETS="linux/amd64 linux/arm64 darwin/amd64 darwin/arm64 windows/amd64"
if [ "$#" -gt 0 ]; then
	TARGETS="$*"
else
	TARGETS="$DEFAULT_TARGETS"
fi

# Shippable binaries: pkg-path -> output base name. attnctl is the low-level
# control driver (handy for debugging a live daemon); the rest are the user-
# facing daemon + CLI + MCP server + the two Go harness bridges.
BINARIES="
./cmd/attnd:attnd
./cmd/attn:attn
./cmd/attn-mcp:attn-mcp
./cmd/attnctl:attnctl
./cmd/attn-opencode:attn-opencode
./adapters/hermes/cmd/attn-hermes-bridge:attn-hermes-bridge
"

DIST="${ATTN_DIST_DIR:-dist}"

echo "attn-agnostic build matrix"
echo "  version : $VERSION"
echo "  commit  : $COMMIT"
echo "  date    : $DATE"
echo "  dist    : $DIST"
echo "  targets : $TARGETS"
echo

fail=0
built=0

for target in $TARGETS; do
	os="${target%%/*}"
	arch="${target##*/}"
	if [ "$os" = "$target" ] || [ -z "$arch" ]; then
		echo "  ✗ malformed target '$target' (want os/arch)" >&2
		fail=$((fail + 1))
		continue
	fi

	outdir="$DIST/$os-$arch"
	rm -rf "$outdir"
	mkdir -p "$outdir"

	ext=""
	if [ "$os" = "windows" ]; then
		ext=".exe"
	fi

	echo "── $os/$arch ──"
	target_ok=1
	for entry in $BINARIES; do
		pkg="${entry%%:*}"
		name="${entry##*:}"
		out="$outdir/${name}${ext}"
		if CGO_ENABLED=0 GOOS="$os" GOARCH="$arch" \
			go build -trimpath -ldflags "$LDFLAGS" -o "$out" "$pkg" 2>build.err; then
			size=$(wc -c <"$out" | tr -d ' ')
			printf '  ✓ %-22s %10s bytes\n' "$name$ext" "$size"
			built=$((built + 1))
		else
			printf '  ✗ %-22s FAILED\n' "$name$ext"
			sed 's/^/      /' build.err >&2
			target_ok=0
			fail=$((fail + 1))
		fi
	done
	rm -f build.err

	# Per-target checksums (verifiable, reproducible artifact). Best effort:
	# never fail the build over a missing sha256sum.
	if [ "$target_ok" = "1" ]; then
		(cd "$outdir" && sha256sum -- * >SHA256SUMS.txt 2>/dev/null) || true
	fi

	# Optional archive (download path for install.sh / install.ps1).
	if [ "${ATTN_ARCHIVE:-0}" = "1" ] && [ "$target_ok" = "1" ]; then
		archive_base="attn-agnostic_${VERSION}_${os}-${arch}"
		if [ "$os" = "windows" ]; then
			if command -v zip >/dev/null 2>&1; then
				(cd "$outdir" && zip -q -r "../${archive_base}.zip" .)
				echo "  → $DIST/${archive_base}.zip"
			elif command -v python3 >/dev/null 2>&1; then
				(cd "$outdir" && python3 -c 'import shutil,sys; shutil.make_archive(sys.argv[1],"zip",".")' "../${archive_base}")
				echo "  → $DIST/${archive_base}.zip"
			else
				echo "  (no zip/python3 — skipping windows archive)" >&2
			fi
		else
			tar -C "$outdir" -czf "$DIST/${archive_base}.tar.gz" .
			echo "  → $DIST/${archive_base}.tar.gz"
		fi
	fi
	echo
done

echo "built $built binaries; $fail failure(s)"
if [ "$fail" -ne 0 ]; then
	exit 1
fi
