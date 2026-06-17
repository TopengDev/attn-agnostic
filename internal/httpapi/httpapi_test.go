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
	"github.com/TopengDev/attn-agnostic/internal/mesh"
	"github.com/TopengDev/attn-agnostic/internal/store"
)

func newTestServer(t *testing.T) (*Server, *store.Store) {
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
	reg := mesh.New()
	ag.SetMesh(reg, "daemon")
	return New(ag, "127.0.0.1:0", log.New(io.Discard, "", 0), reg), st
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
	s, _ := newTestServer(t)
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
	s, _ := newTestServer(t)
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

// TestHostGuard is the M-csrf regression test: a foreign Host header (DNS
// rebinding) is rejected; loopback Host is allowed.
func TestHostGuard(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	// Foreign Host → 403.
	req, _ := http.NewRequest(http.MethodGet, ts.URL+"/status", nil)
	req.Host = "attacker.com"
	r, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	r.Body.Close()
	if r.StatusCode != http.StatusForbidden {
		t.Errorf("foreign Host → %d, want 403", r.StatusCode)
	}

	// Loopback Host → 200 (the default httptest client already sends 127.0.0.1).
	r2, err := http.Get(ts.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	r2.Body.Close()
	if r2.StatusCode != http.StatusOK {
		t.Errorf("loopback Host → %d, want 200", r2.StatusCode)
	}

	// Unit-level allowlist.
	for _, h := range []string{"127.0.0.1:9742", "localhost:9742", "[::1]:9742", "localhost"} {
		if !allowedHost(h) {
			t.Errorf("allowedHost(%q) = false, want true", h)
		}
	}
	for _, h := range []string{"attacker.com", "attacker.com:9742", "8.8.8.8:80", ""} {
		if allowedHost(h) {
			t.Errorf("allowedHost(%q) = true, want false", h)
		}
	}
}

// TestCheckLoopbackOrigin pins the WS CheckOrigin policy (M-csrf).
func TestCheckLoopbackOrigin(t *testing.T) {
	mk := func(origin string) *http.Request {
		r, _ := http.NewRequest(http.MethodGet, "/", nil)
		if origin != "" {
			r.Header.Set("Origin", origin)
		}
		return r
	}
	if !checkLoopbackOrigin(mk("")) {
		t.Error("no-Origin (non-browser client) should be allowed")
	}
	if !checkLoopbackOrigin(mk("http://127.0.0.1:9742")) {
		t.Error("loopback Origin should be allowed")
	}
	if checkLoopbackOrigin(mk("https://attacker.com")) {
		t.Error("cross-origin browser request should be rejected")
	}
}

// TestPeersDBErrorIs500 is the H4 regression test: a DB read failure surfaces as
// 5xx, not an empty-but-200 result that masks data loss.
func TestPeersDBErrorIs500(t *testing.T) {
	s, st := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	st.Close() // force every subsequent store read to error

	for _, path := range []string{"/peers", "/history?with=0xabc"} {
		r, err := http.Get(ts.URL + path)
		if err != nil {
			t.Fatalf("GET %s: %v", path, err)
		}
		r.Body.Close()
		if r.StatusCode != http.StatusInternalServerError {
			t.Errorf("%s with broken DB → %d, want 500", path, r.StatusCode)
		}
	}
}

// TestSendFileHandler covers POST /send-file input validation and the
// decode→tempfile→Dispatch path (no relay needed for the validation cases).
func TestSendFileHandler(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	// missing to → 400
	r, code := postJSONStatus(t, ts.URL+"/send-file", `{"filename":"f.txt","data":"aGVsbG8="}`)
	if code != http.StatusBadRequest || r["error"] == nil {
		t.Errorf("/send-file missing to: want 400+error, got %d %v", code, r)
	}

	// missing data → 400
	r, code = postJSONStatus(t, ts.URL+"/send-file", `{"to":"0x1234567890123456789012345678901234567890","filename":"f.txt"}`)
	if code != http.StatusBadRequest || r["error"] == nil {
		t.Errorf("/send-file missing data: want 400+error, got %d %v", code, r)
	}

	// invalid base64 → 400
	r, code = postJSONStatus(t, ts.URL+"/send-file", `{"to":"0x1234567890123456789012345678901234567890","data":"not!!valid!!b64"}`)
	if code != http.StatusBadRequest || r["error"] == nil {
		t.Errorf("/send-file invalid base64: want 400+error, got %d %v", code, r)
	}

	// valid input passes all validation and reaches Dispatch (relay absent → relay
	// error, not a 400); no panic is the key invariant.
	r, code = postJSONStatus(t, ts.URL+"/send-file", `{"to":"0x1234567890123456789012345678901234567890","filename":"hello.txt","data":"aGVsbG8="}`)
	if code == http.StatusBadRequest {
		t.Errorf("/send-file valid input: got 400 (input rejected), want relay-level error: %v", r)
	}
	if r["error"] == nil {
		t.Errorf("/send-file valid input: expected relay error (no relay in test), got %v", r)
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
	out, _ := postJSONStatus(t, url, body)
	return out
}

func postJSONStatus(t *testing.T, url, body string) (map[string]any, int) {
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
	return out, r.StatusCode
}
