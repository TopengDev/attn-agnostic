// Command attnd is the attn-agnostic daemon: one long-running process per
// identity that owns the persistent relay connection (with reconnect + backoff
// + watchdog), the SQLite state, and the Base names client. It receives,
// decrypts, verifies, stores, and acks inbound messages, and exposes the full
// outbound operation surface over a local Unix control socket (driven by attnctl).
//
// Inbound realtime push to a harness (pi/opencode/hermes) and the HTTP/MCP
// interfaces are M2/M3. M1 = daemon + state + outbound parity.
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/agent"
	"github.com/TopengDev/attn-agnostic/internal/config"
	"github.com/TopengDev/attn-agnostic/internal/control"
	"github.com/TopengDev/attn-agnostic/internal/httpapi"
	"github.com/TopengDev/attn-agnostic/internal/mesh"
	"github.com/TopengDev/attn-agnostic/internal/store"
)

func main() {
	var (
		keyHex   = flag.String("key", "", "hex private key (overrides ATTN_PRIVATE_KEY and the key file)")
		genKey   = flag.Bool("gen-key", false, "generate + persist a new identity if none is configured")
		sockPath = flag.String("sock", "", "control socket path (default: <home>/attnd.sock)")
		httpAddr = flag.String("http", "", "localhost REST+WS bind (default: 127.0.0.1:9742 / ATTN_HTTP_ADDR)")
		noHTTP   = flag.Bool("no-http", false, "disable the REST+WS interface (control socket only)")
	)
	flag.Parse()

	logger := log.New(os.Stderr, "attnd ", log.LstdFlags|log.Lmsgprefix)

	cfg, err := config.Load(*keyHex, *genKey)
	if err != nil {
		logger.Fatalf("config: %v", err)
	}
	if *sockPath != "" {
		cfg.SockPath = *sockPath
	}
	if *httpAddr != "" {
		cfg.HTTPAddr = *httpAddr
	}
	if err := os.MkdirAll(cfg.InboxDir, 0o700); err != nil {
		logger.Fatalf("mkdir inbox: %v", err)
	}

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		logger.Fatalf("store: %v", err)
	}
	defer st.Close()

	ag := agent.New(cfg, st, logger)
	defer ag.Close()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	if err := ag.Start(ctx); err != nil {
		logger.Fatalf("agent start: %v", err)
	}

	logger.Printf("identity %s", cfg.ID.Address())
	logger.Printf("relay %s · base %s", cfg.RelayURL, cfg.BaseRPC)
	logger.Printf("db %s", cfg.DBPath)
	logger.Printf("control socket %s", cfg.SockPath)

	// Product interface: localhost REST API (outbound + management) + WS inbound
	// event stream. The agent surfaces inbound events to the WS hub. The Layer-A
	// local-mesh registry is shared between the HTTP/WS interface (WS
	// self-registration + http-target registration + local routing) and the agent
	// (Send precedence + peers + send-all), so a local recipient is delivered
	// same-host, relay-bypassed. Without the interface there is no local mesh.
	if !*noHTTP {
		reg := mesh.New()
		ag.SetMesh(reg, cfg.SelfName)
		httpSrv := httpapi.New(ag, cfg.HTTPAddr, logger, reg)
		ag.OnSurface(httpSrv.Broadcast)
		go func() {
			if err := httpSrv.Run(ctx); err != nil {
				logger.Printf("http interface stopped: %v", err)
				cancel() // a refused bind (e.g. non-loopback) is fatal — don't run blind
			}
		}()
		logger.Printf("http interface %s (local-mesh session name %q)", cfg.HTTPAddr, cfg.SelfName)
	}

	// Control plane.
	handler := makeHandler(ag, cancel, logger)
	go func() {
		if err := control.Serve(ctx, cfg.SockPath, handler); err != nil {
			logger.Printf("control server stopped: %v", err)
		}
	}()

	// Graceful shutdown on SIGINT/SIGTERM.
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	select {
	case <-ctx.Done():
	case s := <-sig:
		logger.Printf("received %s — shutting down", s)
		cancel()
	}
	// Give in-flight goroutines a moment to unwind.
	time.Sleep(200 * time.Millisecond)
	logger.Printf("stopped")
}

// makeHandler builds the control handler: a few daemon-control ops (prefixed _)
// plus the full agent operation surface.
func makeHandler(ag *agent.Agent, shutdown context.CancelFunc, logger *log.Logger) control.Handler {
	return func(ctx context.Context, req control.Request) control.Response {
		// Per-request timeout so a hung relay op never wedges the control conn.
		opCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()

		switch req.Op {
		case "_ping":
			return control.Response{OK: true, Text: "pong"}
		case "_info":
			return control.Response{OK: true, Text: fmt.Sprintf("address=%s relay_ready=%v", ag.Address(), ag.Session().IsReady()),
				Data: map[string]any{"address": ag.Address(), "relay_ready": ag.Session().IsReady()}}
		case "_drop_conn":
			ag.Session().DropConn()
			logger.Printf("control: forced connection drop (reconnect test)")
			return control.Response{OK: true, Text: "dropped relay connection — auto-reconnect will follow"}
		case "_shutdown":
			logger.Printf("control: shutdown requested")
			shutdown()
			return control.Response{OK: true, Text: "shutting down"}
		}

		res, err := ag.Dispatch(opCtx, req.Op, req.Args)
		if err != nil {
			return control.Response{OK: false, Error: err.Error()}
		}
		return control.Response{OK: true, Text: res.Text, Data: res.Data}
	}
}
