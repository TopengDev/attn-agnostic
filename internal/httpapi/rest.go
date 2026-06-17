package httpapi

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/mesh"
)

const (
	opTimeout       = 30 * time.Second
	maxHistoryLimit = 1000 // cap GET /history?limit= (local-DoS guard)
)

// handleOp is the canonical surface: POST /op/{name} with a JSON args object,
// dispatched verbatim through agent.Dispatch. This is the ONLY path the CLI and
// MCP server use, so all business logic stays behind the single seam.
func (s *Server) handleOp(w http.ResponseWriter, r *http.Request) {
	op := r.PathValue("op")
	args, err := decodeArgs(r)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "bad json body: " + err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), opTimeout)
	defer cancel()
	res, err := s.ag.Dispatch(ctx, op, args)
	if err != nil {
		writeJSON(w, http.StatusOK, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "text": res.Text, "data": res.Data})
}

// handleSend is the pi-setup contract endpoint: POST /send {to,message} →
// {id,status}. It routes through Dispatch("send") (no logic duplicated) and
// re-shapes the result to pi's expected fields.
func (s *Server) handleSend(w http.ResponseWriter, r *http.Request) {
	var body struct {
		To      string `json:"to"`
		Message string `json:"message"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), opTimeout)
	defer cancel()
	res, err := s.ag.Dispatch(ctx, "send", map[string]any{"to": body.To, "message": body.Message})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	id, _ := res.Data["id"].(string)
	status, _ := res.Data["status"].(string)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "text": res.Text})
}

// handleSendFile is the Telegram bridge endpoint:
//
//	POST /send-file {to, filename?, data (base64), caption?, type?}
//
// It decodes the base64 payload to a temp file, calls the send_file op with
// that path, then removes the temp file. caption and type are accepted but not
// forwarded — SendFile(ctx, to, path) does not yet accept them; note as followup.
func (s *Server) handleSendFile(w http.ResponseWriter, r *http.Request) {
	var body struct {
		To       string `json:"to"`
		Filename string `json:"filename"`
		Data     string `json:"data"` // base64-encoded file bytes
	}
	if err := decodeBody(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error()})
		return
	}
	if body.To == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "to is required"})
		return
	}
	if body.Data == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "data is required"})
		return
	}
	raw, err := base64.StdEncoding.DecodeString(body.Data)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "invalid base64 data: " + err.Error()})
		return
	}
	// Preserve the original extension so the recipient sees a sensible filename.
	pattern := "attn-send-*"
	if ext := filepath.Ext(body.Filename); ext != "" {
		pattern = "attn-send-*" + ext
	}
	tmp, err := os.CreateTemp("", pattern)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "temp file: " + err.Error()})
		return
	}
	defer os.Remove(tmp.Name())
	if _, err := tmp.Write(raw); err != nil {
		tmp.Close()
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "write temp file: " + err.Error()})
		return
	}
	tmp.Close()

	ctx, cancel := context.WithTimeout(r.Context(), opTimeout)
	defer cancel()
	res, err := s.ag.Dispatch(ctx, "send_file", map[string]any{"to": body.To, "path": tmp.Name()})
	if err != nil {
		writeJSON(w, http.StatusBadGateway, map[string]any{"error": err.Error()})
		return
	}
	id, _ := res.Data["id"].(string)
	status, _ := res.Data["status"].(string)
	writeJSON(w, http.StatusOK, map[string]any{"id": id, "status": status, "text": res.Text})
}

// handleStatus → {address, relayConnected, peers} (pi contract).
func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	contacts, err := s.ag.ContactsView()
	if err != nil {
		// /status is a health endpoint — stay up, but log + report 0 rather than
		// silently implying an authoritative empty contact set.
		s.log.Printf("[http] /status: contacts read failed: %v", err)
	}
	writeJSON(w, http.StatusOK, map[string]any{
		"address":        s.ag.Address(),
		"relayConnected": s.ag.RelayReady(),
		"peers":          len(contacts),
	})
}

// handleLocalPeers → {sessions,count} (pi contract: sessions is a []string of
// names; the pi adapter filters its own ATTN_SESSION out). Enumerates the live
// Layer-A registry.
func (s *Server) handleLocalPeers(w http.ResponseWriter, _ *http.Request) {
	names := s.mesh.Names()
	writeJSON(w, http.StatusOK, map[string]any{"sessions": names, "count": len(names)})
}

// handleLocalRegister registers an http-target local session (opencode/hermes
// adapters the daemon drives over HTTP — vs pi's WS self-registration). The
// daemon delivers routed local frames by POSTing them to the registered
// `endpoint`, which MUST be loopback (the mesh is same-host only; an off-host
// endpoint would turn the daemon into an SSRF relay). Last-registration-wins.
//
//	POST /local/register {name, harness?, endpoint, sessionId?}
//
// NOTE: any `address` field in the body is intentionally IGNORED. The local mesh
// routes by NAME only — honoring a self-asserted relay address here would let a
// local session shadow the encrypted relay path for a 0x-addressed send (M3
// audit M2 — address-shadowing). A session's relay identity is the daemon's own.
func (s *Server) handleLocalRegister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name      string `json:"name"`
		Harness   string `json:"harness"`
		Endpoint  string `json:"endpoint"`
		SessionID string `json:"sessionId"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Name == "" || body.Endpoint == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "name and endpoint are required"})
		return
	}
	if err := loopbackEndpoint(body.Endpoint); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	s.mesh.Register(&mesh.Entry{
		Name: body.Name, Harness: body.Harness, Transport: mesh.TransportHTTP,
	}, &httpDeliverer{endpoint: body.Endpoint, sessionID: body.SessionID, client: localInjectClient})
	s.log.Printf("[mesh] http-target registered: %q (harness=%q endpoint=%q)", body.Name, body.Harness, body.Endpoint)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": body.Name, "transport": string(mesh.TransportHTTP)})
}

// handleLocalDeregister removes an http-target (or any) local session by name.
//
//	POST /local/deregister {name}
func (s *Server) handleLocalDeregister(w http.ResponseWriter, r *http.Request) {
	var body struct {
		Name string `json:"name"`
	}
	if err := decodeBody(r, &body); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	if body.Name == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"ok": false, "error": "name is required"})
		return
	}
	removed := s.mesh.DeregisterByName(body.Name)
	writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": body.Name, "removed": removed})
}

// handlePeers → {peers:[{address,name,added_at}]} (pi contract; = contacts).
func (s *Server) handlePeers(w http.ResponseWriter, _ *http.Request) {
	contacts, err := s.ag.ContactsView()
	if err != nil {
		s.log.Printf("[http] /peers: contacts read failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to read contacts"})
		return
	}
	peers := make([]map[string]any, 0, len(contacts))
	for _, c := range contacts {
		var name any
		if c.Name != "" {
			name = c.Name
		}
		peers = append(peers, map[string]any{"address": c.Address, "name": name, "added_at": c.AddedAt})
	}
	writeJSON(w, http.StatusOK, map[string]any{"peers": peers})
}

// handleHistory → {messages:[{id,peer,direction,content,ts}]} (pi contract).
func (s *Server) handleHistory(w http.ResponseWriter, r *http.Request) {
	with := r.URL.Query().Get("with")
	if with == "" {
		writeJSON(w, http.StatusBadRequest, map[string]any{"error": "with is required"})
		return
	}
	limit := 20
	if l := r.URL.Query().Get("limit"); l != "" {
		if n, err := strconv.Atoi(l); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > maxHistoryLimit { // cap to avoid a local DoS via limit=2147483647
		limit = maxHistoryLimit
	}
	msgs, err := s.ag.HistoryView(with, limit)
	if err != nil {
		s.log.Printf("[http] /history: read failed: %v", err)
		writeJSON(w, http.StatusInternalServerError, map[string]any{"error": "failed to read history"})
		return
	}
	out := make([]map[string]any, 0, len(msgs))
	for _, m := range msgs {
		out = append(out, map[string]any{
			"id": m.ID, "peer": m.Peer, "direction": m.Direction, "content": m.Content, "ts": m.Ts,
		})
	}
	writeJSON(w, http.StatusOK, map[string]any{"messages": out})
}

// ── helpers ────────────────────────────────────────────────────────────────

const maxBody = 1 << 20 // 1 MiB

func decodeArgs(r *http.Request) (map[string]any, error) {
	args := map[string]any{}
	if r.Body == nil {
		return args, nil
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBody))
	if err := dec.Decode(&args); err != nil && err != io.EOF {
		return nil, err
	}
	if args == nil {
		args = map[string]any{}
	}
	return args, nil
}

func decodeBody(r *http.Request, v any) error {
	if r.Body == nil {
		return nil
	}
	dec := json.NewDecoder(io.LimitReader(r.Body, maxBody))
	if err := dec.Decode(v); err != nil && err != io.EOF {
		return err
	}
	return nil
}

func writeJSON(w http.ResponseWriter, code int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}
