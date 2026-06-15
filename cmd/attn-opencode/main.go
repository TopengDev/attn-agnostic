// Command attn-opencode is the M3 opencode adapter: a WS-subscriber bridge that
// connects one opencode session to an attnd daemon. It delivers realtime attn
// inbound (relay + local-mesh) into a LIVE opencode session via prompt_async, and
// relays outbound local-mesh sends from that session back over the same WS.
//
// The WS connection (ws://<daemon>/?session=<name>&harness=opencode) IS the
// Layer-A local-mesh registry entry, so `peers` lists this session and send(name)
// routes to it. The bridge holds name→opencode-session_id locally.
//
// Usage:
//
//	attn-opencode --name oc-a \
//	  --opencode http://127.0.0.1:4096 --daemon http://127.0.0.1:9742 \
//	  [--session-id ses_… | --new | --discover] [--directory <dir>] \
//	  [--control 127.0.0.1:7997]
//
// Session resolution precedence: --session-id > --new (create) > --discover (most
// recently updated existing session). Default when none given: discover, falling
// back to create.
//
// Security: inbound is UNTRUSTED (rendered as labelled data, never instructions).
// The optional --control listener binds loopback-only with a Host guard. opencode
// is unsecured on loopback by default (same-host OK); set --password /
// OPENCODE_SERVER_PASSWORD to authenticate if your server requires it.
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/TopengDev/attn-agnostic/adapters/opencode"
	"github.com/TopengDev/attn-agnostic/internal/buildinfo"
)

func main() {
	var (
		name      = flag.String("name", envOr("ATTN_SESSION", ""), "local-mesh session name to register as (?session=); required")
		harness   = flag.String("harness", "opencode", "harness label reported to the daemon (?harness=)")
		ocURL     = flag.String("opencode", envOr("OPENCODE_URL", "http://127.0.0.1:4096"), "opencode serve base URL")
		daemonURL = flag.String("daemon", envOr("ATTN_HTTP_URL", "http://127.0.0.1:9742"), "attnd HTTP/WS interface base URL")
		sessionID = flag.String("session-id", "", "explicit opencode session_id to inject into")
		newSess   = flag.Bool("new", false, "create (own) a fresh opencode session")
		discover  = flag.Bool("discover", false, "attach to the most recently updated existing opencode session")
		directory = flag.String("directory", "", "opencode project directory to scope create/discover (default: cwd)")
		control   = flag.String("control", "", "loopback addr for the outbound-send control listener (e.g. 127.0.0.1:7997); empty disables")
		password  = flag.String("password", os.Getenv("OPENCODE_SERVER_PASSWORD"), "opencode server password (Authorization: Bearer); default unsecured same-host")
		pin       = flag.String("version-pin", "1.3", "required opencode version prefix (runtime route/pin guard); empty disables")
		title     = flag.String("title", "attn-opencode", "title for a --new session")
		showVer   = flag.Bool("version", false, "print version and exit")
	)
	flag.Parse()

	if *showVer {
		fmt.Println("attn-opencode " + buildinfo.String())
		return
	}

	logger := log.New(os.Stderr, "attn-opencode ", log.LstdFlags|log.Lmsgprefix)

	if strings.TrimSpace(*name) == "" {
		logger.Fatalf("--name is required (the local-mesh session name; also settable via ATTN_SESSION)")
	}
	if *directory == "" {
		if cwd, err := os.Getwd(); err == nil {
			*directory = cwd
		}
	}

	ctx, cancel := signalContext()
	defer cancel()

	client := opencode.NewClient(*ocURL, *password)

	// Runtime route + pin verification (the 02b drift guard: /doc omits the session
	// routes, so probe them live).
	ver, err := client.VerifyRoutes(ctx, *pin)
	if err != nil {
		logger.Fatalf("opencode route/pin verification failed: %v", err)
	}
	logger.Printf("opencode %s verified at %s (session route live)", ver, *ocURL)

	// Resolve the opencode session_id to inject into.
	sid, err := resolveSession(ctx, client, *sessionID, *newSess, *discover, *directory, *title, logger)
	if err != nil {
		logger.Fatalf("resolve opencode session: %v", err)
	}
	logger.Printf("bound to opencode session %s (name=%q harness=%q)", sid, *name, *harness)

	bridge := &opencode.Bridge{
		Name:       *name,
		Harness:    *harness,
		SessionID:  sid,
		DaemonHTTP: *daemonURL,
		Inject:     client,
		Log:        logger,
	}

	// Optional loopback control listener: lets the opencode session originate
	// mesh sends (POST /mesh/send {to,message}) + read peers (GET /mesh/peers).
	if strings.TrimSpace(*control) != "" {
		go func() {
			if err := serveControl(ctx, *control, bridge, *daemonURL, *name, logger); err != nil {
				logger.Printf("control listener stopped: %v", err)
			}
		}()
	}

	logger.Printf("bridge starting (daemon=%s)", *daemonURL)
	if err := bridge.Run(ctx); err != nil {
		logger.Fatalf("bridge: %v", err)
	}
	logger.Printf("stopped")
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	sig := make(chan os.Signal, 1)
	signal.Notify(sig, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sig
		cancel()
	}()
	return ctx, cancel
}

// resolveSession picks the opencode session_id per the documented precedence.
func resolveSession(ctx context.Context, c *opencode.Client, explicit string, create, discover bool, directory, title string, logger *log.Logger) (string, error) {
	if explicit != "" {
		return explicit, nil
	}
	if create {
		s, err := c.CreateSession(ctx, directory, title)
		if err != nil {
			return "", err
		}
		logger.Printf("created opencode session %s in %s", s.ID, directory)
		return s.ID, nil
	}
	// discover (explicit or as the default fallback): most-recently-updated session
	// in the directory, else create one.
	sessions, err := c.ListSessions(ctx, directory)
	if err != nil {
		return "", err
	}
	if best := mostRecent(sessions); best != "" {
		logger.Printf("discovered foreground opencode session %s", best)
		return best, nil
	}
	if discover {
		logger.Printf("no existing session to discover — creating one")
	}
	s, err := c.CreateSession(ctx, directory, title)
	if err != nil {
		return "", err
	}
	logger.Printf("created opencode session %s in %s", s.ID, directory)
	return s.ID, nil
}

func mostRecent(sessions []opencode.Session) string {
	best := ""
	var bestT int64
	for _, s := range sessions {
		t := s.Time.Updated
		if t == 0 {
			t = s.Time.Created
		}
		if best == "" || t > bestT {
			best, bestT = s.ID, t
		}
	}
	return best
}

// ── control listener (loopback-only) ───────────────────────────────────────

func serveControl(ctx context.Context, addr string, bridge *opencode.Bridge, daemonURL, self string, logger *log.Logger) error {
	if err := loopbackOnly(addr); err != nil {
		return err
	}
	mux := http.NewServeMux()
	mux.HandleFunc("POST /mesh/send", func(w http.ResponseWriter, r *http.Request) {
		var body struct {
			To      string `json:"to"`
			Message string `json:"message"`
		}
		if err := json.NewDecoder(io.LimitReader(r.Body, 1<<20)).Decode(&body); err != nil && err != io.EOF {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		if body.To == "" {
			writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "to is required"})
			return
		}
		if err := bridge.SendLocal(body.To, body.Message); err != nil {
			writeJSON(w, http.StatusServiceUnavailable, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "to": body.To})
	})
	mux.HandleFunc("GET /mesh/peers", func(w http.ResponseWriter, r *http.Request) {
		peers, err := fetchPeers(r.Context(), daemonURL, self)
		if err != nil {
			writeJSON(w, http.StatusBadGateway, map[string]any{"ok": false, "error": err.Error()})
			return
		}
		writeJSON(w, http.StatusOK, map[string]any{"ok": true, "peers": peers, "count": len(peers)})
	})

	srv := &http.Server{
		Handler:           hostGuard(mux),
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	go func() {
		<-ctx.Done()
		sctx, c := context.WithTimeout(context.Background(), 2*time.Second)
		defer c()
		_ = srv.Shutdown(sctx)
	}()
	logger.Printf("control listener on http://%s (POST /mesh/send, GET /mesh/peers)", addr)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// fetchPeers reads the daemon's local-mesh registry and filters out self (pi
// contract: GET /local-peers → {sessions:[],count}).
func fetchPeers(ctx context.Context, daemonURL, self string) ([]string, error) {
	base, err := httpBase(daemonURL)
	if err != nil {
		return nil, err
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, base+"/local-peers", nil)
	if err != nil {
		return nil, err
	}
	resp, err := (&http.Client{Timeout: 5 * time.Second}).Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("daemon /local-peers: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Sessions []string `json:"sessions"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&out); err != nil {
		return nil, err
	}
	peers := make([]string, 0, len(out.Sessions))
	for _, s := range out.Sessions {
		if s != self {
			peers = append(peers, s)
		}
	}
	return peers, nil
}

// httpBase normalises a ws/http daemon URL to an http(s) base with no path.
func httpBase(raw string) (string, error) {
	u, err := url.Parse(raw)
	if err != nil {
		return "", err
	}
	switch u.Scheme {
	case "ws", "":
		u.Scheme = "http"
	case "wss":
		u.Scheme = "https"
	}
	u.Path, u.RawQuery, u.Fragment = "", "", ""
	return strings.TrimRight(u.String(), "/"), nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// hostGuard rejects non-loopback Host headers (DNS-rebinding defense), mirroring
// the daemon's interface guard.
func hostGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !loopbackHost(r.Host) {
			http.Error(w, "forbidden: host not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

func loopbackHost(authority string) bool {
	host := strings.TrimSpace(authority)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]")
	host = strings.ToLower(host)
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func loopbackOnly(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid control addr %q: %w", addr, err)
	}
	host = strings.TrimSpace(host)
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("refusing control bind %q: host must be a loopback IP or \"localhost\"", addr)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("refusing non-loopback control bind %q (localhost only)", addr)
	}
	return nil
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
