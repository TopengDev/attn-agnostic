package opencode

import (
	"context"
	"encoding/json"
	"io"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

type fakeInjector struct {
	mu    sync.Mutex
	calls []injectCall
	err   error
	gotCh chan struct{}
}

type injectCall struct{ sid, text string }

func (f *fakeInjector) PromptAsync(_ context.Context, sid, text string) error {
	f.mu.Lock()
	f.calls = append(f.calls, injectCall{sid, text})
	f.mu.Unlock()
	if f.gotCh != nil {
		select {
		case f.gotCh <- struct{}{}:
		default:
		}
	}
	return f.err
}

func (f *fakeInjector) snapshot() []injectCall {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]injectCall, len(f.calls))
	copy(out, f.calls)
	return out
}

func quietLogger() *log.Logger { return log.New(io.Discard, "", 0) }

func newTestBridge(inj Injector) *Bridge {
	return &Bridge{Name: "oc-a", Harness: "opencode", SessionID: "ses_a", DaemonHTTP: "http://127.0.0.1:9742", Inject: inj, Log: quietLogger()}
}

func TestHandleRawInjectsMessage(t *testing.T) {
	inj := &fakeInjector{}
	b := newTestBridge(inj)
	b.HandleRaw(context.Background(), []byte(`{"type":"message","from":"alice","message":"the answer is here","id":"m1","local":true}`))

	calls := inj.snapshot()
	if len(calls) != 1 {
		t.Fatalf("want 1 inject, got %d", len(calls))
	}
	if calls[0].sid != "ses_a" {
		t.Errorf("inject sid = %q; want ses_a", calls[0].sid)
	}
	if !strings.Contains(calls[0].text, "the answer is here") || !strings.Contains(calls[0].text, "from alice") {
		t.Errorf("inject text not rendered correctly: %q", calls[0].text)
	}
}

func TestHandleRawSkipsNonInjectable(t *testing.T) {
	inj := &fakeInjector{}
	acks := 0
	b := newTestBridge(inj)
	b.OnAck = func(InboundFrame) { acks++ }

	cases := []string{
		`{"type":"local-ack","to":"oc-b","delivered":true,"detail":"delivered"}`, // ack → no inject
		`{"type":"message","from":"x","message":""}`,                             // empty body → no inject
		`{"type":"reaction","from":"x","reactionMessageId":"m9"}`,                // reaction → no inject
		`not json at all`, // malformed → no inject, no panic
	}
	for _, c := range cases {
		b.HandleRaw(context.Background(), []byte(c))
	}
	if got := inj.snapshot(); len(got) != 0 {
		t.Fatalf("expected 0 injects for non-injectable frames, got %d: %+v", len(got), got)
	}
	if acks != 1 {
		t.Errorf("OnAck should fire once for the local-ack frame, got %d", acks)
	}
}

// TestHandleRawSkipsSelfEcho is the audit-M3 regression: a frame whose `from`
// equals this session's own name must NOT be injected (a latent local-mesh
// injection loop if a session ever sends to its own name).
func TestHandleRawSkipsSelfEcho(t *testing.T) {
	inj := &fakeInjector{}
	b := newTestBridge(inj) // Name: "oc-a"
	b.HandleRaw(context.Background(), []byte(`{"type":"message","from":"oc-a","message":"echo of my own send","local":true,"id":"e1"}`))
	if got := inj.snapshot(); len(got) != 0 {
		t.Fatalf("self-echo frame was injected (%d) — must be dropped", len(got))
	}
	// A DIFFERENT sender with the same body still injects (guard is exact-name).
	b.HandleRaw(context.Background(), []byte(`{"type":"message","from":"oc-b","message":"from a peer","local":true,"id":"e2"}`))
	if got := inj.snapshot(); len(got) != 1 {
		t.Fatalf("peer frame should inject, got %d", len(got))
	}
}

// TestHandleRawReactionRendersNotice is the audit-M5 regression: a reaction
// frame (type:message + trust:reaction) is surfaced as a brief one-line notice,
// NOT injected as a full message block.
func TestHandleRawReactionRendersNotice(t *testing.T) {
	inj := &fakeInjector{}
	b := newTestBridge(inj)
	b.HandleRaw(context.Background(), []byte(`{"type":"message","from":"alice","agentName":"alice.attn","message":"👍","trust":"reaction","reactionMessageId":"m9","id":"r1"}`))
	calls := inj.snapshot()
	if len(calls) != 1 {
		t.Fatalf("want 1 reaction notice, got %d", len(calls))
	}
	txt := calls[0].text
	if !strings.Contains(txt, "reaction") || !strings.Contains(txt, "👍") || !strings.Contains(txt, "m9") {
		t.Errorf("reaction notice missing parts: %q", txt)
	}
	// A full message injection would carry the Render provenance header
	// "📨 attn inbound"; a reaction must NOT (it is "📨 attn reaction").
	if strings.Contains(txt, "attn inbound") {
		t.Errorf("reaction was injected as a full message block, want a brief notice: %q", txt)
	}
}

func TestHandleRawInjectErrorIsNonFatal(t *testing.T) {
	inj := &fakeInjector{err: io.ErrClosedPipe}
	b := newTestBridge(inj)
	// Must not panic; error is swallowed + logged.
	b.HandleRaw(context.Background(), []byte(`{"type":"message","from":"x","message":"y"}`))
	if len(inj.snapshot()) != 1 {
		t.Error("inject should still have been attempted")
	}
}

func TestSendLocalFrames(t *testing.T) {
	b := newTestBridge(&fakeInjector{})
	if err := b.SendLocal("oc-b", "hi B"); err != nil {
		t.Fatalf("SendLocal error: %v", err)
	}
	if err := b.SendLocal("all", "broadcast"); err != nil {
		t.Fatalf("SendLocal(all) error: %v", err)
	}

	// White-box: drain the buffered frames and assert their shape.
	got := drainFrame(t, b)
	if got.Type != "local" || got.To != "oc-b" || got.Message != "hi B" {
		t.Errorf("frame1 = %+v; want local/oc-b/hi B", got)
	}
	got2 := drainFrame(t, b)
	if got2.To != "all" || got2.Message != "broadcast" {
		t.Errorf("frame2 = %+v; want all/broadcast", got2)
	}
}

func drainFrame(t *testing.T, b *Bridge) localSendFrame {
	t.Helper()
	select {
	case raw := <-b.sendCh:
		var f localSendFrame
		if err := json.Unmarshal(raw, &f); err != nil {
			t.Fatalf("unmarshal queued frame: %v", err)
		}
		return f
	case <-time.After(time.Second):
		t.Fatal("no frame queued")
		return localSendFrame{}
	}
}

func TestSendLocalBufferFull(t *testing.T) {
	b := newTestBridge(&fakeInjector{})
	// Fill the buffer (nothing drains it — no connection running).
	for i := 0; i < bridgeSendBuffer; i++ {
		if err := b.SendLocal("oc-b", "x"); err != nil {
			t.Fatalf("unexpected error at %d: %v", i, err)
		}
	}
	if err := b.SendLocal("oc-b", "overflow"); err == nil {
		t.Error("SendLocal should error when the buffer is full")
	}
}

func TestWSURL(t *testing.T) {
	cases := []struct{ in, wantScheme, wantHost string }{
		{"http://127.0.0.1:9742", "ws", "127.0.0.1:9742"},
		{"https://localhost:9742", "wss", "localhost:9742"},
		{"ws://127.0.0.1:9742", "ws", "127.0.0.1:9742"},
	}
	for _, c := range cases {
		b := &Bridge{Name: "oc-a", Harness: "opencode", DaemonHTTP: c.in}
		got, err := b.wsURL()
		if err != nil {
			t.Fatalf("wsURL(%q) error: %v", c.in, err)
		}
		if !strings.HasPrefix(got, c.wantScheme+"://"+c.wantHost) {
			t.Errorf("wsURL(%q) = %q; want scheme %q host %q", c.in, got, c.wantScheme, c.wantHost)
		}
		if !strings.Contains(got, "session=oc-a") || !strings.Contains(got, "harness=opencode") {
			t.Errorf("wsURL(%q) = %q; missing session/harness query", c.in, got)
		}
	}
}

func TestRunValidation(t *testing.T) {
	cases := []*Bridge{
		{Name: "a", SessionID: "s", Log: quietLogger()},               // nil Inject
		{SessionID: "s", Inject: &fakeInjector{}, Log: quietLogger()}, // empty Name
		{Name: "a", Inject: &fakeInjector{}, Log: quietLogger()},      // empty SessionID
	}
	for i, b := range cases {
		if err := b.Run(context.Background()); err == nil {
			t.Errorf("case %d: Run should reject invalid config", i)
		}
	}
}

// TestBridgeWSRoundtrip exercises the real WS path in-process: a fake daemon
// upgrades the connection, asserts the ?session= self-registration query, pushes
// an inbound message frame, and the bridge injects it via the (fake) Injector.
func TestBridgeWSRoundtrip(t *testing.T) {
	gotSession := make(chan string, 1)
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		up := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
		conn, err := up.Upgrade(w, r, nil)
		if err != nil {
			return
		}
		defer conn.Close()
		select {
		case gotSession <- r.URL.Query().Get("session"):
		default:
		}
		// Push one inbound message frame the bridge should inject.
		_ = conn.WriteMessage(websocket.TextMessage, []byte(`{"type":"message","from":"alice","message":"roundtrip body","id":"m1"}`))
		// Hold the connection open until the client closes.
		for {
			if _, _, err := conn.ReadMessage(); err != nil {
				return
			}
		}
	}))
	defer srv.Close()

	inj := &fakeInjector{gotCh: make(chan struct{}, 1)}
	b := &Bridge{Name: "oc-rt", Harness: "opencode", SessionID: "ses_rt", DaemonHTTP: srv.URL, Inject: inj, Log: quietLogger()}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() { _ = b.Run(ctx); close(done) }()

	select {
	case <-inj.gotCh:
	case <-time.After(5 * time.Second):
		cancel()
		t.Fatal("bridge did not inject the pushed frame within 5s")
	}

	if s := <-gotSession; s != "oc-rt" {
		t.Errorf("daemon saw session=%q; want oc-rt (self-registration query)", s)
	}
	calls := inj.snapshot()
	if len(calls) == 0 || calls[0].sid != "ses_rt" || !strings.Contains(calls[0].text, "roundtrip body") {
		t.Errorf("inject wrong: %+v", calls)
	}

	cancel()
	select {
	case <-done:
	case <-time.After(3 * time.Second):
		t.Error("bridge did not stop after ctx cancel")
	}
}
