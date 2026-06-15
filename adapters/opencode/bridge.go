package opencode

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Injector is the Layer-B sink: inject text into a live harness session. *Client
// implements it via prompt_async; tests use a fake.
type Injector interface {
	PromptAsync(ctx context.Context, sessionID, text string) error
}

const (
	bridgeSendBuffer  = 64
	bridgeWriteWait   = 10 * time.Second
	bridgePingPeriod  = 30 * time.Second
	bridgePongWait    = 90 * time.Second
	bridgeReadLimit   = 1 << 20 // 1 MiB, matches the daemon's maxBody
	backoffInitial    = 500 * time.Millisecond
	backoffMax        = 15 * time.Second
	bridgeInjectGrace = 25 * time.Second // bound a single prompt_async fire-and-forget
)

// Bridge subscribes to attnd's WS event stream as a named local session and
// injects each inbound attn frame into a live opencode session, while relaying
// outbound local-mesh sends from that session over the same WS.
//
// The WS connection (opened with ?session=<Name>&harness=<Harness>) IS the
// Layer-A registry entry — so `peers` lists this session and send(Name) routes
// to it. The Bridge holds Name→SessionID locally; the daemon never needs the
// opencode session_id.
type Bridge struct {
	// Name is the local-mesh session name (?session=) this bridge registers as.
	Name string
	// Harness is reported to the daemon (?harness=); use "opencode".
	Harness string
	// SessionID is the opencode session_id to inject inbound frames into.
	SessionID string
	// DaemonHTTP is the attnd interface base, ws or http scheme accepted
	// (e.g. "ws://127.0.0.1:9742" or "http://127.0.0.1:9742").
	DaemonHTTP string
	// Inject is the Layer-B sink (an opencode *Client in production).
	Inject Injector
	// Renderer turns an inbound frame into injected text; defaults to Render.
	Renderer func(InboundFrame) string
	// Log is required.
	Log *log.Logger
	// OnAck, if set, is called for each local-ack frame (visibility/tests).
	OnAck func(InboundFrame)

	mu      sync.Mutex
	conn    *websocket.Conn
	sendCh  chan []byte
	started bool
}

func (b *Bridge) renderer() func(InboundFrame) string {
	if b.Renderer != nil {
		return b.Renderer
	}
	return Render
}

// wsURL builds ws://host/?session=&harness= from DaemonHTTP (accepts ws/http).
func (b *Bridge) wsURL() (string, error) {
	u, err := url.Parse(b.DaemonHTTP)
	if err != nil {
		return "", fmt.Errorf("parse daemon url %q: %w", b.DaemonHTTP, err)
	}
	switch u.Scheme {
	case "http", "":
		u.Scheme = "ws"
	case "https":
		u.Scheme = "wss"
	case "ws", "wss":
		// already a ws scheme
	default:
		return "", fmt.Errorf("unsupported daemon scheme %q", u.Scheme)
	}
	if u.Host == "" {
		return "", fmt.Errorf("daemon url %q has no host", b.DaemonHTTP)
	}
	q := u.Query()
	q.Set("session", b.Name)
	if b.Harness != "" {
		q.Set("harness", b.Harness)
	}
	u.RawQuery = q.Encode()
	if u.Path == "" {
		u.Path = "/"
	}
	return u.String(), nil
}

func (b *Bridge) initOnce() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !b.started {
		b.sendCh = make(chan []byte, bridgeSendBuffer)
		b.started = true
	}
}

// SendLocal relays an outbound local-mesh send over the WS (to="all" broadcasts).
// The daemon attributes the sender from this connection's session name, so the
// message correctly appears to come from b.Name. Best-effort: buffered, dropped
// with an error if the send buffer is full or the bridge is not running.
func (b *Bridge) SendLocal(to, message string) error {
	b.initOnce()
	frame := newLocalSend(to, message)
	select {
	case b.sendCh <- frame:
		return nil
	default:
		return fmt.Errorf("bridge send buffer full (not connected, or backpressure) — dropped send to %q", to)
	}
}

// Run dials the daemon WS and serves inbound→inject + outbound relay until ctx is
// cancelled, reconnecting with capped backoff on any connection error.
func (b *Bridge) Run(ctx context.Context) error {
	if b.Inject == nil {
		return fmt.Errorf("bridge: Inject is nil")
	}
	if b.Name == "" {
		return fmt.Errorf("bridge: Name is required (it is the local-mesh session name)")
	}
	if b.SessionID == "" {
		return fmt.Errorf("bridge: SessionID is required (the opencode session to inject into)")
	}
	if b.Log == nil {
		b.Log = log.Default()
	}
	b.initOnce()
	wsURL, err := b.wsURL()
	if err != nil {
		return err
	}

	backoff := backoffInitial
	for {
		if ctx.Err() != nil {
			return nil
		}
		err := b.runConn(ctx, wsURL)
		if ctx.Err() != nil {
			return nil
		}
		if err != nil {
			b.Log.Printf("[bridge %s] connection error: %v — reconnecting in %s", b.Name, err, backoff)
		} else {
			b.Log.Printf("[bridge %s] disconnected — reconnecting in %s", b.Name, backoff)
		}
		select {
		case <-ctx.Done():
			return nil
		case <-time.After(backoff):
		}
		if backoff < backoffMax {
			backoff *= 2
			if backoff > backoffMax {
				backoff = backoffMax
			}
		}
	}
}

// runConn manages exactly one WS connection lifecycle.
func (b *Bridge) runConn(ctx context.Context, wsURL string) error {
	dialer := *websocket.DefaultDialer
	dialer.HandshakeTimeout = 10 * time.Second
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		return err
	}
	b.setConn(conn)
	defer b.clearConn(conn)

	b.Log.Printf("[bridge %s] subscribed to %s (harness=%s) → opencode session %s", b.Name, wsURL, b.Harness, b.SessionID)

	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	// Close the conn when the context ends (parent shutdown OR this connection's
	// own teardown) so the blocking readPump unblocks promptly — ReadMessage does
	// not observe ctx. Double Close is harmless. The goroutine exits when connCtx
	// is cancelled by the defer, so it never leaks.
	go func() {
		<-connCtx.Done()
		conn.Close()
	}()

	var wg sync.WaitGroup
	wg.Add(1)
	go func() { defer wg.Done(); b.writePump(connCtx, conn) }()

	b.readPump(ctx, conn) // blocks until the connection closes
	cancel()
	conn.Close()
	wg.Wait()
	return nil
}

func (b *Bridge) writePump(ctx context.Context, conn *websocket.Conn) {
	ticker := time.NewTicker(bridgePingPeriod)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case msg := <-b.sendCh:
			_ = conn.SetWriteDeadline(time.Now().Add(bridgeWriteWait))
			if err := conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				b.Log.Printf("[bridge %s] write failed: %v", b.Name, err)
				return
			}
		case <-ticker.C:
			_ = conn.SetWriteDeadline(time.Now().Add(bridgeWriteWait))
			if err := conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		}
	}
}

func (b *Bridge) readPump(ctx context.Context, conn *websocket.Conn) {
	conn.SetReadLimit(bridgeReadLimit)
	_ = conn.SetReadDeadline(time.Now().Add(bridgePongWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(bridgePongWait))
	})
	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(bridgePongWait))
		b.HandleRaw(ctx, data)
	}
}

// HandleRaw parses one daemon→subscriber frame and injects it into the opencode
// session when it is an injectable message/file. Exposed for unit tests (drive it
// with a fake Injector and no live WS). Errors are logged, never fatal — one bad
// frame must not kill the subscription.
func (b *Bridge) HandleRaw(ctx context.Context, data []byte) {
	var f InboundFrame
	if err := json.Unmarshal(data, &f); err != nil {
		b.Log.Printf("[bridge %s] drop malformed frame: %v", b.Name, err)
		return
	}
	if f.Type == "local-ack" {
		if b.OnAck != nil {
			b.OnAck(f)
		}
		b.Log.Printf("[bridge %s] local-ack to=%q delivered=%v (%s)", b.Name, f.To, f.Delivered, f.Detail)
		return
	}
	// Self-echo guard (audit M3): never inject a frame that claims to originate
	// from THIS session — a latent local-mesh injection loop if a session ever
	// sends to its own name. Broadcast already excludes the sender by name; this
	// is defense in depth (hermes + pi carry the same guard).
	if f.From != "" && f.From == b.Name {
		return
	}
	// Reactions are surfaced as a one-line notice, not a full message injection
	// (audit M5; consistent with the pi adapter). A reaction frame is a
	// type:"message" frame carrying trust:"reaction" + reactionMessageId.
	if f.Trust == "reaction" {
		b.injectReaction(ctx, f)
		return
	}
	if !f.injectable() {
		return
	}
	text := b.renderer()(f)
	ictx, cancel := context.WithTimeout(ctx, bridgeInjectGrace)
	defer cancel()
	if err := b.Inject.PromptAsync(ictx, b.SessionID, text); err != nil {
		b.Log.Printf("[bridge %s] inject failed (from=%s id=%s): %v", b.Name, f.From, f.ID, err)
		return
	}
	scope := "relay"
	if f.Local {
		scope = "local"
	}
	b.Log.Printf("[bridge %s] injected %s frame from %q (id=%s) → opencode %s", b.Name, scope, f.From, f.ID, b.SessionID)
}

// injectReaction surfaces a reaction frame as a brief one-line notice (NOT a full
// message block) so a 👍 doesn't read as a new instruction-bearing message.
func (b *Bridge) injectReaction(ctx context.Context, f InboundFrame) {
	from := f.From
	if f.AgentName != "" {
		from = f.AgentName
	}
	if from == "" {
		from = "unknown"
	}
	notice := fmt.Sprintf("📨 attn reaction · %s reacted %s", from, f.Message)
	if f.ReactionFor != "" {
		notice += fmt.Sprintf(" to message %s", f.ReactionFor)
	}
	ictx, cancel := context.WithTimeout(ctx, bridgeInjectGrace)
	defer cancel()
	if err := b.Inject.PromptAsync(ictx, b.SessionID, notice); err != nil {
		b.Log.Printf("[bridge %s] reaction inject failed (from=%s): %v", b.Name, f.From, err)
	}
}

func (b *Bridge) setConn(c *websocket.Conn) {
	b.mu.Lock()
	b.conn = c
	b.mu.Unlock()
}

// clearConn nils the current conn only if it still points at c (avoid clobbering
// a newer connection from a racing reconnect).
func (b *Bridge) clearConn(c *websocket.Conn) {
	b.mu.Lock()
	if b.conn == c {
		b.conn = nil
	}
	b.mu.Unlock()
}
