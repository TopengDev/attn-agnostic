#!/usr/bin/env bash
# Live cross-check runner: builds attnd, starts it ISOLATED (temp home, own port,
# unreachable relay so the local mesh is proven relay-independent), compiles the
# adapter core, runs live-check.cjs against it, and tears the daemon down.
# Exits non-zero if the build or the check fails. Idempotent + self-cleaning.
set -uo pipefail
export PATH="$HOME/.local/go/bin:$PATH"

ADAPTER_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
REPO_DIR="$(cd "$ADAPTER_DIR/../.." && pwd)"
PORT="${LIVE_PORT:-29742}"
HOME_DIR="$(mktemp -d /tmp/attn-pi-livecheck.XXXXXX)"
BIN="$HOME_DIR/attnd"
LOG="$HOME_DIR/attnd.log"
DAEMON_PID=""

cleanup() {
  [ -n "$DAEMON_PID" ] && kill "$DAEMON_PID" 2>/dev/null
  wait "$DAEMON_PID" 2>/dev/null
  rm -rf "$HOME_DIR"
}
trap cleanup EXIT

echo "== build attnd =="
go -C "$REPO_DIR" build -o "$BIN" ./cmd/attnd || { echo "BUILD FAILED"; exit 1; }

echo "== compile adapter core =="
( cd "$ADAPTER_DIR" && npx tsc -p tsconfig.test.json ) || { echo "TSC FAILED"; exit 1; }

echo "== start isolated attnd on 127.0.0.1:$PORT (relay unreachable) =="
ATTN_HOME="$HOME_DIR" \
ATTN_HTTP_ADDR="127.0.0.1:$PORT" \
ATTN_RELAY_URL="ws://127.0.0.1:1" \
ATTN_SESSION="daemon-self" \
"$BIN" --gen-key >"$LOG" 2>&1 &
DAEMON_PID=$!

echo "== wait for /healthz =="
for i in $(seq 1 50); do
  if curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1; then
    echo "daemon healthy (pid $DAEMON_PID)"; break
  fi
  if ! kill -0 "$DAEMON_PID" 2>/dev/null; then echo "DAEMON DIED:"; cat "$LOG"; exit 1; fi
  sleep 0.2
done
curl -fsS "http://127.0.0.1:$PORT/healthz" >/dev/null 2>&1 || { echo "HEALTHZ TIMEOUT:"; cat "$LOG"; exit 1; }

echo "== run live-check.cjs =="
( cd "$ADAPTER_DIR" && LIVE_PORT="$PORT" node test/live-check.cjs )
RC=$?

echo "== daemon log (tail) =="
tail -n 15 "$LOG"
exit $RC
