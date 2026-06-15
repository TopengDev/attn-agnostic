// Package httpapi is attnd's product interface: a localhost-only HTTP REST API
// (outbound + management, all 29 ops via agent.Dispatch) plus a WebSocket
// inbound-event stream, both on one port. It deliberately mirrors pi-setup's
// existing `127.0.0.1:9742` daemon contract (extensions/attn/index.ts: REST out
// + WS in) so the M3 per-harness adapters are drop-in.
//
// Invariant: the REST/WS layer contains NO business logic. Every one of the 29
// ops flows through agent.Dispatch; the pi-compat read endpoints
// (/status, /peers, /history, /local-peers) only re-shape read-only store data
// into pi's JSON contract. The single relay connection lives in the daemon.
package httpapi

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"strings"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/agent"
	"github.com/TopengDev/attn-agnostic/internal/mesh"
)

// Server is the localhost REST + WS interface in front of one Agent.
type Server struct {
	ag   *agent.Agent
	addr string
	log  *log.Logger
	hub  *Hub
	mesh *mesh.Registry
}

// New builds the interface server. addr MUST be a loopback bind (enforced in Run).
// reg is the shared Layer-A local-mesh registry (the WS hub self-registers
// subscribers into it; the same registry must be wired into the agent via
// ag.SetMesh so Send/peers see the live sessions). A nil reg disables local mesh.
func New(ag *agent.Agent, addr string, logger *log.Logger, reg *mesh.Registry) *Server {
	if logger == nil {
		logger = log.Default()
	}
	if reg == nil {
		reg = mesh.New()
	}
	return &Server{ag: ag, addr: addr, log: logger, hub: newHub(logger, reg, ag), mesh: reg}
}

// Broadcast pushes a surfaced inbound event to all WS subscribers. Wire this as
// the agent's surface sink: ag.OnSurface(srv.Broadcast).
func (s *Server) Broadcast(ev agent.SurfaceEvent) { s.hub.Broadcast(ev) }

// Run binds the (loopback-only) listener and serves until ctx is cancelled.
func (s *Server) Run(ctx context.Context) error {
	if err := loopbackOnly(s.addr); err != nil {
		return err
	}
	srv := &http.Server{
		Handler:           s.handler(),
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       15 * time.Second,
		WriteTimeout:      60 * time.Second, // > opTimeout; WS is hijacked so unaffected
		IdleTimeout:       120 * time.Second,
	}
	ln, err := net.Listen("tcp", s.addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", s.addr, err)
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
		s.hub.CloseAll()
	}()
	s.log.Printf("[http] REST+WS interface on http://%s (localhost-only)", s.addr)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

// handler builds the route mux (shared by Run and tests), wrapped in the
// Host-header guard.
func (s *Server) handler() http.Handler {
	mux := http.NewServeMux()
	s.routes(mux)
	return hostGuard(mux)
}

// hostGuard rejects requests whose Host header is not a loopback name (audit
// M-csrf — DNS-rebinding / local CSRF defense). The listener already binds
// loopback, but a browser page on attacker.com whose DNS rebinds to 127.0.0.1
// would still carry a foreign Host; this blocks it before any handler runs.
func hostGuard(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if !allowedHost(r.Host) {
			http.Error(w, "forbidden: host not allowed", http.StatusForbidden)
			return
		}
		next.ServeHTTP(w, r)
	})
}

// allowedHost reports whether a Host (or Origin) authority is a loopback name.
func allowedHost(authority string) bool {
	host := strings.TrimSpace(authority)
	if h, _, err := net.SplitHostPort(host); err == nil {
		host = h
	}
	host = strings.TrimSuffix(strings.TrimPrefix(host, "["), "]") // strip IPv6 brackets
	host = strings.ToLower(host)
	if host == "localhost" {
		return true
	}
	ip := net.ParseIP(host)
	return ip != nil && ip.IsLoopback()
}

func (s *Server) routes(mux *http.ServeMux) {
	// Root: WS upgrade (pi connects to ws://127.0.0.1:9742/?session=…) or info.
	mux.HandleFunc("/", s.handleRoot)
	mux.HandleFunc("/ws", s.handleWS)
	mux.HandleFunc("GET /healthz", s.handleHealthz)
	// pi-setup contract endpoints (drop-in for the M3 pi adapter).
	mux.HandleFunc("GET /status", s.handleStatus)
	mux.HandleFunc("GET /local-peers", s.handleLocalPeers)
	mux.HandleFunc("GET /peers", s.handlePeers)
	mux.HandleFunc("GET /history", s.handleHistory)
	mux.HandleFunc("POST /send", s.handleSend)
	// Layer-A local-mesh control: http-target self-registration (opencode/hermes
	// adapters that the daemon drives over HTTP, vs pi-style WS self-registration).
	mux.HandleFunc("POST /local/register", s.handleLocalRegister)
	mux.HandleFunc("POST /local/deregister", s.handleLocalDeregister)
	// Canonical 29-op surface — every op via the single agent.Dispatch seam.
	mux.HandleFunc("POST /op/{op}", s.handleOp)
}

func (s *Server) handleRoot(w http.ResponseWriter, r *http.Request) {
	if isWebSocketUpgrade(r) {
		s.handleWS(w, r)
		return
	}
	if r.URL.Path != "/" {
		http.NotFound(w, r)
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"service": "attnd",
		"address": s.ag.Address(),
		"interfaces": map[string]any{
			"rest": "POST /op/{name} (29 ops), POST /send, GET /status|/peers|/history|/local-peers",
			"ws":   "ws on / (and /ws) — inbound event stream",
		},
	})
}

func (s *Server) handleHealthz(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

// loopbackOnly refuses any bind that is not a loopback address (defense in depth:
// the inbound message content is untrusted, so the interface must never be
// reachable off-host). An empty host (e.g. ":9742" → all interfaces) is rejected.
func loopbackOnly(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid http addr %q: %w", addr, err)
	}
	host = strings.TrimSpace(host)
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		return fmt.Errorf("refusing http bind %q: host must be a loopback IP or \"localhost\"", addr)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("refusing non-loopback http bind %q (localhost only)", addr)
	}
	return nil
}
