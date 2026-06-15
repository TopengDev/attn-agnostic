package httpapi

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strconv"
	"time"
)

const opTimeout = 30 * time.Second

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

// handleStatus → {address, relayConnected, peers} (pi contract).
func (s *Server) handleStatus(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{
		"address":        s.ag.Address(),
		"relayConnected": s.ag.RelayReady(),
		"peers":          len(s.ag.ContactsView()),
	})
}

// handleLocalPeers → {sessions,count}. Local mesh (Layer A) is M3; the endpoint
// exists so the pi adapter's routing probe degrades cleanly to relay-only.
func (s *Server) handleLocalPeers(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"sessions": []string{}, "count": 0})
}

// handlePeers → {peers:[{address,name,added_at}]} (pi contract; = contacts).
func (s *Server) handlePeers(w http.ResponseWriter, _ *http.Request) {
	contacts := s.ag.ContactsView()
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
	msgs := s.ag.HistoryView(with, limit)
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
