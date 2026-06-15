package httpapi

import (
	"encoding/json"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

// dialSession opens a WS subscriber with ?session=<name> against the test server.
func dialSession(t *testing.T, tsURL, name string) *websocket.Conn {
	t.Helper()
	wsURL := "ws" + strings.TrimPrefix(tsURL, "http") + "/?session=" + name
	conn, _, err := websocket.DefaultDialer.Dial(wsURL, nil)
	if err != nil {
		t.Fatalf("ws dial %s: %v", name, err)
	}
	return conn
}

// readFrame reads one JSON frame within timeout.
func readFrame(t *testing.T, conn *websocket.Conn, timeout time.Duration) (map[string]any, error) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := conn.ReadMessage()
	if err != nil {
		return nil, err
	}
	var f map[string]any
	if err := json.Unmarshal(data, &f); err != nil {
		t.Fatalf("frame not JSON: %v", err)
	}
	return f, nil
}

// expectNoFrame asserts the connection receives NOTHING within timeout — the
// core no-leak assertion.
func expectNoFrame(t *testing.T, conn *websocket.Conn, who string, timeout time.Duration) {
	t.Helper()
	_ = conn.SetReadDeadline(time.Now().Add(timeout))
	_, data, err := conn.ReadMessage()
	if err == nil {
		t.Fatalf("NO-LEAK VIOLATION: %s received a frame it must not have: %s", who, string(data))
	}
	ne, ok := err.(net.Error)
	if !ok || !ne.Timeout() {
		t.Fatalf("%s read error = %v, want timeout (no frame)", who, err)
	}
}

func sendLocal(t *testing.T, conn *websocket.Conn, to, msg string) {
	t.Helper()
	if err := conn.WriteJSON(map[string]any{"type": "local", "to": to, "message": msg}); err != nil {
		t.Fatalf("send local: %v", err)
	}
}

func waitMeshCount(s *Server, want int, timeout time.Duration) bool {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if s.mesh.Count() == want {
			return true
		}
		time.Sleep(5 * time.Millisecond)
	}
	return s.mesh.Count() == want
}

// readUntilMessage drains acks and returns the first type:"message" frame (or
// fails on timeout). Used where a connection may interleave local-ack frames.
func readUntilMessage(t *testing.T, conn *websocket.Conn, who string, timeout time.Duration) map[string]any {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		f, err := readFrame(t, conn, time.Until(deadline))
		if err != nil {
			t.Fatalf("%s expected a message frame, got read error: %v", who, err)
		}
		if f["type"] == "message" {
			return f
		}
	}
	t.Fatalf("%s: no message frame within %s", who, timeout)
	return nil
}

// TestLocalMeshNoLeak is the #1-risk proof: with 3 mock WS sessions, send(B)
// lands ONLY in B (C sees nothing), and send("all") reaches B+C but never the
// sender A. All local frames are relay-bypassed, local:true, trust:local.
func TestLocalMeshNoLeak(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	a := dialSession(t, ts.URL, "A")
	defer a.Close()
	b := dialSession(t, ts.URL, "B")
	defer b.Close()
	c := dialSession(t, ts.URL, "C")
	defer c.Close()

	if !waitMeshCount(s, 3, 2*time.Second) {
		t.Fatalf("registry count = %d, want 3", s.mesh.Count())
	}

	// NOTE: a gorilla read that hits its deadline puts the conn's read side into a
	// failed state, so mid-stream "expect nothing" timeout-reads would break later
	// reads on that same conn. We therefore do timeout-reads only TERMINALLY, and
	// prove no-leak structurally: C's first-ever frame must be the broadcast (it
	// must NEVER have seen hi-B), and C must then have exactly one frame.

	// ── A → B (1:1) ──────────────────────────────────────────────────────────
	sendLocal(t, a, "B", "hi-B")

	bf := readUntilMessage(t, b, "B", 2*time.Second)
	if bf["message"] != "hi-B" {
		t.Errorf("B message = %v, want hi-B", bf["message"])
	}
	if bf["from"] != "A" {
		t.Errorf("B frame from = %v, want A (sender session name)", bf["from"])
	}
	if bf["local"] != true {
		t.Errorf("B frame local = %v, want true", bf["local"])
	}
	if bf["trust"] != "local" {
		t.Errorf("B frame trust = %v, want local", bf["trust"])
	}
	// A receives a local-ack (delivered=true), never the message itself.
	af, err := readFrame(t, a, 1*time.Second)
	if err != nil {
		t.Fatalf("A expected a local-ack: %v", err)
	}
	if af["type"] != "local-ack" || af["delivered"] != true {
		t.Errorf("A ack = %v, want type:local-ack delivered:true", af)
	}

	// ── A → all (broadcast, sender excluded) ─────────────────────────────────
	sendLocal(t, a, "all", "broadcast-msg")

	// B's next message is the broadcast.
	bf2 := readUntilMessage(t, b, "B", 2*time.Second)
	if bf2["message"] != "broadcast-msg" {
		t.Errorf("B broadcast = %v, want broadcast-msg", bf2["message"])
	}
	// C's FIRST EVER message is the broadcast — proving it NEVER saw hi-B (no-leak).
	cf := readUntilMessage(t, c, "C", 2*time.Second)
	if cf["message"] != "broadcast-msg" {
		t.Errorf("C first frame = %v, want broadcast-msg (NO-LEAK: C must never have seen hi-B)", cf["message"])
	}
	if cf["local"] != true {
		t.Errorf("C broadcast local = %v, want true", cf["local"])
	}
	// A must get an ack for the broadcast but NOT the broadcast message itself.
	af2, err := readFrame(t, a, 1*time.Second)
	if err != nil {
		t.Fatalf("A expected a broadcast ack: %v", err)
	}
	if af2["type"] != "local-ack" {
		t.Errorf("A post-broadcast frame = %v, want local-ack (sender excluded from own broadcast)", af2)
	}

	// ── Terminal no-leak assertions (last op per conn) ───────────────────────
	// C received EXACTLY ONE frame ever (the broadcast) — had A→B leaked, C would
	// have a second, earlier hi-B frame.
	expectNoFrame(t, c, "C (exactly-one-frame)", 400*time.Millisecond)
	// A never receives its own broadcast.
	expectNoFrame(t, a, "A (self-broadcast)", 400*time.Millisecond)
}

// TestLocalPeersAndPeersOpEnumerate: /local-peers (pi shape) and the peers op
// both list the live registered sessions.
func TestLocalPeersAndPeersOpEnumerate(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	a := dialSession(t, ts.URL, "alpha")
	defer a.Close()
	b := dialSession(t, ts.URL, "bravo")
	defer b.Close()
	if !waitMeshCount(s, 2, 2*time.Second) {
		t.Fatalf("registry count = %d, want 2", s.mesh.Count())
	}

	// /local-peers — pi contract: {sessions:[]string, count}.
	lp := getJSON(t, ts.URL+"/local-peers")
	if lp["count"].(float64) != 2 {
		t.Errorf("/local-peers count = %v, want 2", lp["count"])
	}
	sessions, _ := lp["sessions"].([]any)
	got := map[string]bool{}
	for _, v := range sessions {
		got[v.(string)] = true
	}
	if !got["alpha"] || !got["bravo"] {
		t.Errorf("/local-peers sessions = %v, want alpha+bravo", sessions)
	}

	// peers op (CLI/MCP surface) via /op/peers.
	resp := postJSON(t, ts.URL+"/op/peers", `{}`)
	if resp["ok"] != true {
		t.Fatalf("/op/peers ok = %v (%v)", resp["ok"], resp)
	}
	data, _ := resp["data"].(map[string]any)
	if data["count"].(float64) != 2 {
		t.Errorf("peers op count = %v, want 2", data["count"])
	}
}

// TestLocalMeshHTTPTarget proves an http-target registered via POST
// /local/register receives a routed local send (opencode/hermes shape).
func TestLocalMeshHTTPTarget(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	// Stand up the http-target's inject endpoint (a fake adapter).
	var mu sync.Mutex
	var received map[string]any
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		body, _ := io.ReadAll(r.Body)
		mu.Lock()
		_ = json.Unmarshal(body, &received)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()

	// Register it as local session "opencode-1".
	reg := postJSON(t, ts.URL+"/local/register",
		`{"name":"opencode-1","harness":"opencode","endpoint":"`+target.URL+`","sessionId":"sess-xyz"}`)
	if reg["ok"] != true {
		t.Fatalf("/local/register ok = %v (%v)", reg["ok"], reg)
	}

	// Route a local send to it via the agent send op (precedence → local).
	send := postJSON(t, ts.URL+"/op/send", `{"to":"opencode-1","message":"hello-http-target"}`)
	if send["ok"] != true {
		t.Fatalf("/op/send to http-target ok = %v (%v)", send["ok"], send)
	}
	sd, _ := send["data"].(map[string]any)
	if sd["local"] != true {
		t.Errorf("send to http-target local = %v, want true (relay bypassed)", sd["local"])
	}

	// The fake adapter must have received the injected frame.
	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := received
		mu.Unlock()
		if got != nil {
			if got["message"] != "hello-http-target" {
				t.Errorf("http-target message = %v, want hello-http-target", got["message"])
			}
			if got["local"] != true || got["trust"] != "local" {
				t.Errorf("http-target frame local/trust = %v/%v, want true/local", got["local"], got["trust"])
			}
			if got["sessionId"] != "sess-xyz" {
				t.Errorf("http-target sessionId = %v, want sess-xyz", got["sessionId"])
			}
			// Deregister + verify gone.
			d := postJSON(t, ts.URL+"/local/deregister", `{"name":"opencode-1"}`)
			if d["removed"] != true {
				t.Errorf("/local/deregister removed = %v, want true", d["removed"])
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("http-target never received the routed local frame")
}

// TestLocalRegisterRejectsNonLoopback is the SSRF guard: an off-host inject
// endpoint is refused (the mesh is same-host only).
func TestLocalRegisterRejectsNonLoopback(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	bad := []string{
		`{"name":"x","endpoint":"http://169.254.169.254/latest/meta-data"}`, // cloud IMDS
		`{"name":"x","endpoint":"http://10.0.0.5:8080/inject"}`,             // LAN
		`{"name":"x","endpoint":"http://evil.example.com/inject"}`,          // external host
		`{"name":"x","endpoint":"ftp://127.0.0.1/x"}`,                       // bad scheme
		`{"name":"x","endpoint":"http://8.8.8.8/inject"}`,                   // public IP
	}
	for _, body := range bad {
		r, err := http.Post(ts.URL+"/local/register", "application/json", strings.NewReader(body))
		if err != nil {
			t.Fatalf("POST: %v", err)
		}
		code := r.StatusCode
		r.Body.Close()
		if code != http.StatusBadRequest {
			t.Errorf("register %s → %d, want 400 (SSRF guard)", body, code)
		}
	}
	if s.mesh.Count() != 0 {
		t.Errorf("registry count = %d after rejected registers, want 0", s.mesh.Count())
	}

	// Unit-level: loopbackEndpoint accepts loopback, rejects the rest.
	for _, ok := range []string{"http://127.0.0.1:9000/x", "http://localhost:8080/y", "https://[::1]:9000/z"} {
		if err := loopbackEndpoint(ok); err != nil {
			t.Errorf("loopbackEndpoint(%q) = %v, want nil", ok, err)
		}
	}
	for _, bad := range []string{"http://169.254.169.254/x", "http://10.0.0.1/x", "http://evil.com/x", "ws://127.0.0.1/x"} {
		if err := loopbackEndpoint(bad); err == nil {
			t.Errorf("loopbackEndpoint(%q) = nil, want rejection", bad)
		}
	}
}

// TestWSDisconnectCleansUpRegistry: a subscriber's registry entry is removed on
// disconnect (no stale entries).
func TestWSDisconnectCleansUpRegistry(t *testing.T) {
	s, _ := newTestServer(t)
	ts := httptest.NewServer(s.handler())
	defer ts.Close()

	conn := dialSession(t, ts.URL, "ephemeral")
	if !waitMeshCount(s, 1, 2*time.Second) {
		t.Fatalf("count = %d after connect, want 1", s.mesh.Count())
	}
	conn.Close()
	if !waitMeshCount(s, 0, 2*time.Second) {
		t.Errorf("count = %d after disconnect, want 0 (stale entry)", s.mesh.Count())
	}
}
