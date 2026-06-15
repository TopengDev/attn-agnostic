package bridge

import (
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"runtime"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func testCfg() Config {
	return Config{
		DaemonWS:   "ws://127.0.0.1:9742/",
		Session:    "hermes",
		TargetURL:  "http://127.0.0.1:8644/webhooks/attn",
		HMACSecret: "s3cr3t",
		Logger:     log.New(io.Discard, "", 0),
	}
}

func TestNewValidation(t *testing.T) {
	tests := []struct {
		name    string
		mut     func(*Config)
		wantErr bool
	}{
		{"ok", func(c *Config) {}, false},
		{"no daemon", func(c *Config) { c.DaemonWS = "" }, true},
		{"no session", func(c *Config) { c.Session = "" }, true},
		{"no target", func(c *Config) { c.TargetURL = "" }, true},
		{"no secret", func(c *Config) { c.HMACSecret = "" }, true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			cfg := testCfg()
			tt.mut(&cfg)
			_, err := New(cfg)
			if (err != nil) != tt.wantErr {
				t.Fatalf("New() err=%v wantErr=%v", err, tt.wantErr)
			}
		})
	}
}

func TestNewDefaults(t *testing.T) {
	cfg := testCfg()
	cfg.Harness = ""
	cfg.SignatureHeader = ""
	b, err := New(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if b.cfg.Harness != defaultHarness {
		t.Errorf("Harness default = %q want %q", b.cfg.Harness, defaultHarness)
	}
	if b.cfg.SignatureHeader != defaultSignatureHeader {
		t.Errorf("SignatureHeader default = %q want %q", b.cfg.SignatureHeader, defaultSignatureHeader)
	}
}

func TestWSURL(t *testing.T) {
	b, _ := New(testCfg())
	got, err := b.wsURL()
	if err != nil {
		t.Fatal(err)
	}
	// order of query params is deterministic (url.Values.Encode sorts keys).
	want := "ws://127.0.0.1:9742/?harness=hermes&session=hermes"
	if got != want {
		t.Errorf("wsURL() = %q want %q", got, want)
	}
}

func TestSignMatchesHMAC(t *testing.T) {
	b, _ := New(testCfg())
	body := []byte(`{"hello":"world"}`)
	got := b.sign(body)
	mac := hmac.New(sha256.New, []byte("s3cr3t"))
	mac.Write(body)
	want := hex.EncodeToString(mac.Sum(nil))
	if got != want {
		t.Errorf("sign() = %q want %q", got, want)
	}
}

func TestShouldForward(t *testing.T) {
	b, _ := New(testCfg())
	cases := []struct {
		name string
		f    inboundFrame
		want bool
	}{
		{"message ok", inboundFrame{Type: "message", Message: "hi", From: "alice"}, true},
		{"local message ok", inboundFrame{Type: "message", Message: "hi", From: "opencode", Local: true}, true},
		{"file with filename ok", inboundFrame{Type: "file", Filename: "doc.pdf", Path: "/tmp/doc.pdf", From: "alice"}, true},
		{"file with path only ok", inboundFrame{Type: "file", Path: "/tmp/x", From: "alice"}, true},
		{"file without filename or path", inboundFrame{Type: "file", Message: "hi", From: "alice"}, false},
		{"reaction type dropped", inboundFrame{Type: "reaction", Message: "👍", From: "alice"}, false},
		{"empty text", inboundFrame{Type: "message", Message: "   ", From: "alice"}, false},
		{"self echo", inboundFrame{Type: "message", Message: "hi", From: "hermes"}, false},
		{"self echo file", inboundFrame{Type: "file", Filename: "x", From: "hermes"}, false},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			if got := b.shouldForward(tt.f); got != tt.want {
				t.Errorf("shouldForward(%+v) = %v want %v", tt.f, got, tt.want)
			}
		})
	}
}

func TestBuildPostSessionOverride(t *testing.T) {
	cfg := testCfg()
	b, _ := New(cfg)
	p := b.buildPost(inboundFrame{From: "alice", Message: "hi", ID: "1"})
	if p.Session != "hermes" {
		t.Errorf("default Session = %q want hermes", p.Session)
	}
	cfg.SessionKeyOverride = "attn-channel"
	b2, _ := New(cfg)
	p2 := b2.buildPost(inboundFrame{From: "alice", Message: "hi", ID: "1"})
	if p2.Session != "attn-channel" {
		t.Errorf("override Session = %q want attn-channel", p2.Session)
	}
}

func TestIdempotencyGuard(t *testing.T) {
	b, _ := New(testCfg())
	if b.alreadySeen("id-1") {
		t.Fatal("first sighting should be false")
	}
	if !b.alreadySeen("id-1") {
		t.Fatal("second sighting should be true")
	}
	b.forget("id-1")
	if b.alreadySeen("id-1") {
		t.Fatal("after forget, should be false again")
	}
	// empty id is never tracked
	if b.alreadySeen("") || b.alreadySeen("") {
		t.Fatal("empty id must never be marked seen")
	}
}

// TestForwardSignsAndPosts asserts the receiver gets the exact signed body,
// the matching signature header, and X-Request-ID = frame id.
func TestForwardSignsAndPosts(t *testing.T) {
	var (
		mu      sync.Mutex
		gotBody []byte
		gotSig  string
		gotReq  string
		gotCT   string
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		defer mu.Unlock()
		gotBody, _ = io.ReadAll(r.Body)
		gotSig = r.Header.Get("X-Webhook-Signature")
		gotReq = r.Header.Get("X-Request-ID")
		gotCT = r.Header.Get("Content-Type")
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()

	cfg := testCfg()
	cfg.TargetURL = srv.URL
	b, _ := New(cfg)

	f := inboundFrame{Type: "message", From: "alice.attn", Message: "hello hermes", ID: "req-42", Ts: 123, Local: true, Trust: "local"}
	if err := b.forward(context.Background(), f); err != nil {
		t.Fatalf("forward: %v", err)
	}

	mu.Lock()
	defer mu.Unlock()
	if gotReq != "req-42" {
		t.Errorf("X-Request-ID = %q want req-42", gotReq)
	}
	if gotCT != "application/json" {
		t.Errorf("Content-Type = %q want application/json", gotCT)
	}
	// signature must verify against the received body
	want := b.sign(gotBody)
	if gotSig != want {
		t.Errorf("signature %q does not match HMAC of received body %q", gotSig, want)
	}
	var p postBody
	if err := json.Unmarshal(gotBody, &p); err != nil {
		t.Fatalf("body not valid json: %v", err)
	}
	if p.From != "alice.attn" || p.Message != "hello hermes" || p.Session != "hermes" || !p.Local || p.Trust != "local" {
		t.Errorf("post body mismatch: %+v", p)
	}
}

// TestForwardNon2xxIsError ensures a non-2xx receiver response surfaces as an
// error so the caller can forget the id and let a retry re-attempt.
func TestForwardNon2xxIsError(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusUnauthorized)
		_, _ = w.Write([]byte("bad sig"))
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.TargetURL = srv.URL
	b, _ := New(cfg)
	err := b.forward(context.Background(), inboundFrame{Type: "message", From: "x", Message: "y", ID: "z"})
	if err == nil {
		t.Fatal("expected error on 401")
	}
}

// TestHandleFrameIdempotentForward asserts a duplicate id is only forwarded once.
func TestHandleFrameIdempotentForward(t *testing.T) {
	var mu sync.Mutex
	count := 0
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		count++
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.TargetURL = srv.URL
	b, _ := New(cfg)

	frame, _ := json.Marshal(inboundFrame{Type: "message", From: "alice", Message: "hi", ID: "dup-1"})
	b.handleFrame(context.Background(), frame)
	b.handleFrame(context.Background(), frame) // duplicate id

	mu.Lock()
	defer mu.Unlock()
	if count != 1 {
		t.Errorf("forwarded %d times, want 1 (idempotency)", count)
	}
}

func TestHandleFrameSkipsJunk(t *testing.T) {
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		t.Error("junk frame should not POST")
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.TargetURL = srv.URL
	b, _ := New(cfg)
	b.handleFrame(context.Background(), []byte("not json"))
	// file frame with no filename/path is not deliverable (nothing to reference).
	b.handleFrame(context.Background(), []byte(`{"type":"file","message":"x","from":"a","id":"1"}`))
	b.handleFrame(context.Background(), []byte(`{"type":"message","message":"","from":"a","id":"2"}`))
	// reaction frames are dropped (the hermes agent run is message-driven).
	b.handleFrame(context.Background(), []byte(`{"type":"reaction","message":"👍","from":"a","id":"3"}`))
}

// TestHandleFrameForwardsFile asserts a file frame is forwarded as a one-line
// notice referencing the daemon-saved path (M3 audit M4 — hermes previously
// dropped ALL non-message frames; opencode + pi surface files).
func TestHandleFrameForwardsFile(t *testing.T) {
	var (
		mu      sync.Mutex
		gotBody []byte
	)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		mu.Lock()
		gotBody, _ = io.ReadAll(r.Body)
		mu.Unlock()
		w.WriteHeader(http.StatusAccepted)
	}))
	defer srv.Close()
	cfg := testCfg()
	cfg.TargetURL = srv.URL
	b, _ := New(cfg)

	frame, _ := json.Marshal(inboundFrame{
		Type: "file", From: "alice", ID: "file-1",
		Filename: "report.pdf", Path: "/tmp/attn/report.pdf", Size: 2048,
	})
	b.handleFrame(context.Background(), frame)

	mu.Lock()
	defer mu.Unlock()
	if gotBody == nil {
		t.Fatal("file frame was not forwarded (got no POST)")
	}
	var p postBody
	if err := json.Unmarshal(gotBody, &p); err != nil {
		t.Fatalf("body not json: %v", err)
	}
	if p.From != "alice" {
		t.Errorf("From = %q want alice", p.From)
	}
	for _, want := range []string{"report.pdf", "2048", "/tmp/attn/report.pdf"} {
		if !strings.Contains(p.Message, want) {
			t.Errorf("file notice %q missing %q", p.Message, want)
		}
	}
}

// TestRunOnceNoGoroutineLeak is the M3-audit-M1 regression: runOnce's
// conn-watcher goroutine must be bound to a PER-CONNECTION ctx, so it exits when
// the connection ends. Pre-fix it watched the process-lifetime ctx, leaking one
// goroutine per reconnect. We drive many runOnce cycles against a server that
// immediately closes each connection and assert the goroutine count does not
// grow with the number of cycles.
func TestRunOnceNoGoroutineLeak(t *testing.T) {
	upgrader := websocket.Upgrader{}
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		c, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		// Drop the connection immediately so runOnce's read returns and it cycles.
		_ = c.Close()
	}))
	defer srv.Close()

	wsURL := "ws" + strings.TrimPrefix(srv.URL, "http")
	b, _ := New(testCfg())
	ctx := context.Background() // long-lived: a leaked watcher would NOT exit on its own

	// Warm up so one-time runtime goroutines are already started, then baseline.
	_ = b.runOnce(ctx, wsURL)
	runtime.GC()
	time.Sleep(20 * time.Millisecond)
	base := runtime.NumGoroutine()

	const cycles = 30
	for i := 0; i < cycles; i++ {
		_ = b.runOnce(ctx, wsURL)
	}
	runtime.GC()
	time.Sleep(50 * time.Millisecond)
	got := runtime.NumGoroutine()

	// Pre-fix this would grow by ~cycles (one leaked watcher each). Allow a small
	// slack for scheduler/transport goroutines still winding down.
	if got-base > 5 {
		t.Errorf("goroutine leak across %d reconnects: base=%d got=%d (delta=%d, want <=5)", cycles, base, got, got-base)
	}
}
