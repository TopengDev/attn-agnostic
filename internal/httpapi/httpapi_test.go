package httpapi

import (
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"

	"github.com/TopengDev/attn-agnostic/internal/agent"
	"github.com/TopengDev/attn-agnostic/internal/config"
	"github.com/TopengDev/attn-agnostic/internal/identity"
	"github.com/TopengDev/attn-agnostic/internal/store"
)

func newTestServer(t *testing.T) *Server {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "test.db"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := &config.Config{
		ID:       id,
		RelayURL: "wss://relay.invalid/ws",
		BaseRPC:  "https://base.invalid",
		InboxDir: t.TempDir(),
	}
	ag := agent.New(cfg, st, log.New(io.Discard, "", 0))
	return New(ag, "127.0.0.1:0", log.New(io.Discard, "", 0))
}

func TestLoopbackOnly(t *testing.T) {
	ok := []string{"127.0.0.1:9742", "localhost:9742", "[::1]:9742"}
	bad := []string{"0.0.0.0:9742", ":9742", "192.168.1.5:9742", "8.8.8.8:80"}
	for _, a := range ok {
		if err := loopbackOnly(a); err != nil {
			t.Errorf("loopbackOnly(%q) = %v, want nil", a, err)
		}
	}
	for _, a := range bad {
		if err := loopbackOnly(a); err == nil {
			t.Errorf("loopbackOnly(%q) = nil, want refusal", a)
		}
	}
}

func TestRESTEndpoints(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	// /healthz
	if got := getJSON(t, ts.URL+"/healthz"); got["ok"] != true {
		t.Errorf("/healthz = %v, want ok:true", got)
	}

	// /status — pi contract shape.
	st := getJSON(t, ts.URL+"/status")
	for _, k := range []string{"address", "relayConnected", "peers"} {
		if _, ok := st[k]; !ok {
			t.Errorf("/status missing key %q (got %v)", k, st)
		}
	}
	if st["relayConnected"] != false {
		t.Errorf("/status relayConnected = %v, want false (no relay in test)", st["relayConnected"])
	}

	// /local-peers — M3 stub: empty list.
	lp := getJSON(t, ts.URL+"/local-peers")
	if lp["count"].(float64) != 0 {
		t.Errorf("/local-peers count = %v, want 0", lp["count"])
	}

	// /op/contacts — generic op surface (reads the store, no relay needed).
	resp := postJSON(t, ts.URL+"/op/contacts", `{}`)
	if resp["ok"] != true {
		t.Errorf("/op/contacts ok = %v, want true (%v)", resp["ok"], resp)
	}

	// /op/{unknown} — dispatch error surfaces as ok:false, not a 5xx.
	bad := postJSON(t, ts.URL+"/op/does_not_exist", `{}`)
	if bad["ok"] != false {
		t.Errorf("/op/does_not_exist ok = %v, want false", bad["ok"])
	}

	// /peers — contact list shape (empty initially).
	pe := getJSON(t, ts.URL+"/peers")
	if _, ok := pe["peers"]; !ok {
		t.Errorf("/peers missing peers key (got %v)", pe)
	}

	// /history requires `with`.
	r, err := http.Get(ts.URL + "/history")
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusBadRequest {
		t.Errorf("/history without `with` = %d, want 400", r.StatusCode)
	}
}

// TestWSInboundContract is the WS frame contract test: it drives a real surface
// event through the hub over a live WS connection and asserts the frame matches
// what pi-setup's extensions/attn/index.ts parser expects.
func TestWSInboundContract(t *testing.T) {
	s := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()
	s.ag.OnSurface(s.Broadcast)

	wsURL := "ws" + strings.TrimPrefix(ts.URL, "http") + "/?session=tester"
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial: %v", err)
	}
	defer conn.Close()

	// Give the server a moment to register the subscriber before broadcasting.
	deadline := time.Now().Add(2 * time.Second)
	for s.hub.count() == 0 && time.Now().Before(deadline) {
		time.Sleep(5 * time.Millisecond)
	}

	s.Broadcast(agent.SurfaceEvent{
		Type: "message", From: "0xabc", FromName: "alice.attn",
		Message: "hello there", MessageID: "m1", Ts: 1700000000000, DeliveryMode: "steer",
	})

	_ = conn.SetReadDeadline(time.Now().Add(3 * time.Second))
	_, data, err := conn.ReadMessage()
	if err != nil {
		t.Fatalf("ws read: %v", err)
	}
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("ws frame not JSON: %v", err)
	}
	// Fields the pi extension reads.
	checks := map[string]any{
		"type": "message", "from": "0xabc", "message": "hello there",
		"agentName": "alice.attn", "id": "m1", "deliveryMode": "steer",
	}
	for k, want := range checks {
		if f[k] != want {
			t.Errorf("ws frame[%q] = %v, want %v", k, f[k], want)
		}
	}
}

// TestSurfaceToFrame pins the SurfaceEvent → pi-frame mapping for each kind.
func TestSurfaceToFrame(t *testing.T) {
	cases := []struct {
		name string
		ev   agent.SurfaceEvent
		want map[string]any
	}{
		{
			name: "dm",
			ev:   agent.SurfaceEvent{Type: "message", From: "0x1", Message: "hi", MessageID: "a", DeliveryMode: "steer"},
			want: map[string]any{"type": "message", "from": "0x1", "message": "hi", "deliveryMode": "steer"},
		},
		{
			name: "pending",
			ev:   agent.SurfaceEvent{Type: "message", Trust: "pending", From: "0x2", Message: "yo", DeliveryMode: "followUp"},
			want: map[string]any{"type": "message", "trust": "pending", "deliveryMode": "followUp"},
		},
		{
			name: "group",
			ev:   agent.SurfaceEvent{Type: "message", From: "0x3", Message: "gm", GroupID: "g1", GroupName: "team", DeliveryMode: "followUp"},
			want: map[string]any{"type": "message", "groupId": "g1", "groupName": "team"},
		},
		{
			name: "reaction",
			ev:   agent.SurfaceEvent{Type: "message", Trust: "reaction", From: "0x4", Message: "🔥", ReactionFor: "x9", DeliveryMode: "steer"},
			want: map[string]any{"type": "message", "trust": "reaction", "message": "🔥", "reactionMessageId": "x9"},
		},
		{
			name: "file",
			ev:   agent.SurfaceEvent{Type: "file", From: "0x5", Filename: "a.txt", Path: "/inbox/a.txt", Size: 42, DeliveryMode: "steer"},
			want: map[string]any{"type": "file", "filename": "a.txt", "path": "/inbox/a.txt"},
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			f := surfaceToFrame(tc.ev)
			for k, want := range tc.want {
				if f[k] != want {
					t.Errorf("frame[%q] = %v, want %v", k, f[k], want)
				}
			}
			// file frames must not carry a message key, and vice-versa.
			if tc.ev.Type == "file" {
				if _, ok := f["message"]; ok {
					t.Errorf("file frame should not have message key")
				}
				if f["size"] != int64(42) {
					t.Errorf("file size = %v, want 42", f["size"])
				}
			}
		})
	}
}

// ── helpers ──────────────────────────────────────────────────────────────

func getJSON(t *testing.T, url string) map[string]any {
	t.Helper()
	r, err := http.Get(url)
	if err != nil {
		t.Fatalf("GET %s: %v", url, err)
	}
	defer r.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return out
}

func postJSON(t *testing.T, url, body string) map[string]any {
	t.Helper()
	r, err := http.Post(url, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatalf("POST %s: %v", url, err)
	}
	defer r.Body.Close()
	var out map[string]any
	if err := json.NewDecoder(r.Body).Decode(&out); err != nil {
		t.Fatalf("decode %s: %v", url, err)
	}
	return out
}
