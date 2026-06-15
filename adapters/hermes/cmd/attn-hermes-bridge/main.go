// Command attn-hermes-bridge subscribes to the local attn daemon and forwards
// inbound messages (relay + local-mesh) to a hermes receiver as HMAC-signed
// POSTs, turning each into a real hermes agent run.
//
// The HMAC secret is read from the environment (ATTN_HERMES_HMAC_SECRET), never
// a flag, so it never appears in the process table.
//
// Usage:
//
//	ATTN_HERMES_HMAC_SECRET=… attn-hermes-bridge \
//	  -session hermes \
//	  -target http://127.0.0.1:8644/webhooks/attn \
//	  -daemon ws://127.0.0.1:9742/
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	"github.com/TopengDev/attn-agnostic/adapters/hermes/bridge"
)

const hmacEnv = "ATTN_HERMES_HMAC_SECRET"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("")

	daemon := flag.String("daemon", envOr("ATTN_HERMES_DAEMON_WS", "ws://127.0.0.1:9742/"),
		"attnd WS base URL")
	session := flag.String("session", envOr("ATTN_SESSION", "hermes"),
		"local-mesh session name to register as (the hermes attn identity)")
	harness := flag.String("harness", "hermes", "harness label reported on the WS handshake")
	target := flag.String("target", envOr("ATTN_HERMES_TARGET", "http://127.0.0.1:8644/webhooks/attn"),
		"hermes receiver endpoint (webhook route or attn plugin adapter)")
	sigHeader := flag.String("sig-header", "X-Webhook-Signature",
		"header carrying the hex HMAC-SHA256 of the POST body")
	sessionKey := flag.String("session-key", envOr("ATTN_HERMES_SESSION_KEY", ""),
		"override the `session` field sent to the receiver (stable session_key source); defaults to -session")
	flag.Parse()

	secret := strings.TrimSpace(os.Getenv(hmacEnv))
	if secret == "" {
		log.Fatalf("%s is required (export it; do not pass secrets as flags)", hmacEnv)
	}

	b, err := bridge.New(bridge.Config{
		DaemonWS:           *daemon,
		Session:            *session,
		Harness:            *harness,
		TargetURL:          *target,
		HMACSecret:         secret,
		SignatureHeader:    *sigHeader,
		SessionKeyOverride: *sessionKey,
		Logger:             log.Default(),
	})
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	log.Printf("[attn-hermes] starting bridge (session=%q target=%q)", *session, *target)
	if err := b.Run(ctx); err != nil && ctx.Err() == nil {
		log.Fatalf("bridge exited: %v", err)
	}
	log.Printf("[attn-hermes] shutdown")
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
