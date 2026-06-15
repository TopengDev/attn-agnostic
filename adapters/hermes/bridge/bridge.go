// Package bridge implements the attn → hermes inbound bridge.
//
// The bridge subscribes to the local attn daemon (attnd) over its WS inbound
// stream as a NAMED local-mesh session, and for each inbound message it
// HMAC-signs the payload and POSTs it to a hermes receiver (the stock webhook
// adapter at /webhooks/attn, or our custom `attn` plugin adapter). The hermes
// receiver turns the POST into a real agent run.
//
// Two attn delivery paths converge on the same WS subscription:
//   - relay inbound  — an external attn agent messages this daemon's identity.
//   - local-mesh     — another local session does send("<thisName>", ...).
//
// Both arrive as {"type":"message","from":...,"message":...,"id":...} frames
// (local-mesh frames additionally carry local:true + trust:"local"). The bridge
// is transport-symmetric: holding the WS as ?session=<name> ALSO self-registers
// this name in the daemon's local-mesh registry (Layer A), so send("<name>")
// from any peer routes here — that is the mesh "to the hermes session" path.
//
// Security: the WS is loopback-only (the daemon binds 127.0.0.1). The HMAC
// secret is read from the environment, never a flag (so it never lands in the
// process table). Inbound content is treated as untrusted data — the bridge
// never interprets it, it only forwards the bytes under an HMAC the receiver
// verifies.
package bridge

import (
	"bytes"
	"context"
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

// Config configures a Bridge. Zero values are filled with sane defaults by
// (*Config).withDefaults during New.
type Config struct {
	// DaemonWS is the attnd WS base URL, e.g. "ws://127.0.0.1:9742/".
	DaemonWS string
	// Session is the local-mesh session NAME this bridge registers as
	// (the attn identity of the hermes session). send("<Session>") routes here.
	Session string
	// Harness is reported on the WS handshake (?harness=) for peer display.
	Harness string
	// TargetURL is the hermes receiver endpoint, e.g.
	// "http://127.0.0.1:8644/webhooks/attn".
	TargetURL string
	// HMACSecret is the shared secret used to sign POST bodies. Required.
	HMACSecret string
	// SignatureHeader is the header carrying the hex HMAC-SHA256 of the body.
	// Defaults to "X-Webhook-Signature" (the hermes webhook generic format).
	SignatureHeader string
	// SessionKeyOverride, when non-empty, is sent as the "session" field of the
	// POST body so the receiver can key a STABLE hermes session_key from it
	// (same-session continuity). When empty, Session is used.
	SessionKeyOverride string
	// Logger receives operational logs. Defaults to the std logger.
	Logger *log.Logger
}

const (
	defaultHarness         = "hermes"
	defaultSignatureHeader = "X-Webhook-Signature"
	defaultDialTimeout     = 10 * time.Second
	defaultPOSTTimeout     = 30 * time.Second
	reconnectBaseDelay     = 1 * time.Second
	reconnectMaxDelay      = 30 * time.Second
	// WS read-loop liveness (mirrors the daemon/opencode pattern). The daemon
	// pings every 30s; we reset the read deadline on each ping/pong/data frame so
	// a half-open TCP (server vanished without a FIN) trips the deadline within
	// readWait instead of hanging until OS keepalive.
	readWait      = 90 * time.Second
	writeWait     = 10 * time.Second
	readLimitByte = 1 << 20 // 1 MiB, matches the daemon's maxBody
	// seenCap bounds the local idempotency set so a long-lived bridge never
	// grows unbounded. The receiver enforces real idempotency by X-Request-ID;
	// this is only a defensive guard against reconnect replays.
	seenCap = 4096
)

func (c *Config) withDefaults() {
	if c.Harness == "" {
		c.Harness = defaultHarness
	}
	if c.SignatureHeader == "" {
		c.SignatureHeader = defaultSignatureHeader
	}
	if c.Logger == nil {
		c.Logger = log.Default()
	}
}

func (c *Config) validate() error {
	if strings.TrimSpace(c.DaemonWS) == "" {
		return fmt.Errorf("DaemonWS is required")
	}
	if strings.TrimSpace(c.Session) == "" {
		return fmt.Errorf("Session is required")
	}
	if strings.TrimSpace(c.TargetURL) == "" {
		return fmt.Errorf("TargetURL is required")
	}
	if strings.TrimSpace(c.HMACSecret) == "" {
		return fmt.Errorf("HMACSecret is required (set it via env, never a flag)")
	}
	return nil
}

// inboundFrame is the daemon→subscriber WS frame (see internal/httpapi/ws.go
// surfaceToFrame). We act on type=="message" and type=="file".
type inboundFrame struct {
	Type         string `json:"type"`
	From         string `json:"from"`
	Message      string `json:"message"`
	ID           string `json:"id"`
	Ts           int64  `json:"ts"`
	Local        bool   `json:"local"`
	Trust        string `json:"trust"`
	DeliveryMode string `json:"deliveryMode"`
	AgentName    string `json:"agentName"`
	GroupID      string `json:"groupId"`
	GroupName    string `json:"groupName"`

	// file frames (daemon already downloaded + decrypted + saved to disk).
	Filename string `json:"filename"`
	Path     string `json:"path"`
	Size     int64  `json:"size"`
}

// postBody is the JSON the bridge POSTs to the hermes receiver. The stock
// webhook adapter renders it via a {message} template; our `attn` plugin reads
// `session` to build a stable session_key and `from`/`message` to compose the
// prompt. Field names are stable contract — keep aligned with the plugin.
type postBody struct {
	Session string `json:"session"`
	From    string `json:"from"`
	Message string `json:"message"`
	ID      string `json:"id"`
	Ts      int64  `json:"ts"`
	Local   bool   `json:"local"`
	Trust   string `json:"trust"`
	GroupID string `json:"groupId,omitempty"`
}

// Bridge forwards attn inbound to a hermes receiver.
type Bridge struct {
	cfg    Config
	http   *http.Client
	dialer *websocket.Dialer

	mu   sync.Mutex
	seen map[string]struct{}
	conn *websocket.Conn
}

// New constructs a Bridge from cfg. It returns an error if cfg is invalid.
func New(cfg Config) (*Bridge, error) {
	cfg.withDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &Bridge{
		cfg:    cfg,
		http:   &http.Client{Timeout: defaultPOSTTimeout},
		dialer: &websocket.Dialer{HandshakeTimeout: defaultDialTimeout},
		seen:   make(map[string]struct{}, 64),
	}, nil
}

// wsURL builds the subscription URL: <DaemonWS>?session=<Session>&harness=<Harness>.
func (b *Bridge) wsURL() (string, error) {
	u, err := url.Parse(b.cfg.DaemonWS)
	if err != nil {
		return "", fmt.Errorf("parse DaemonWS: %w", err)
	}
	q := u.Query()
	q.Set("session", b.cfg.Session)
	q.Set("harness", b.cfg.Harness)
	u.RawQuery = q.Encode()
	return u.String(), nil
}

// Run subscribes and forwards until ctx is cancelled, reconnecting with
// bounded exponential backoff on transient failures.
func (b *Bridge) Run(ctx context.Context) error {
	endpoint, err := b.wsURL()
	if err != nil {
		return err
	}
	delay := reconnectBaseDelay
	for {
		if ctx.Err() != nil {
			return ctx.Err()
		}
		err := b.runOnce(ctx, endpoint)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		b.cfg.Logger.Printf("[attn-hermes] ws session ended: %v — reconnecting in %s", err, delay)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(delay):
		}
		delay *= 2
		if delay > reconnectMaxDelay {
			delay = reconnectMaxDelay
		}
	}
}

// runOnce holds one WS connection and pumps inbound frames until it errors.
func (b *Bridge) runOnce(ctx context.Context, endpoint string) error {
	conn, _, err := b.dialer.DialContext(ctx, endpoint, nil)
	if err != nil {
		return fmt.Errorf("dial: %w", err)
	}
	b.mu.Lock()
	b.conn = conn
	b.mu.Unlock()
	defer func() {
		b.mu.Lock()
		b.conn = nil
		b.mu.Unlock()
		_ = conn.Close()
	}()
	b.cfg.Logger.Printf("[attn-hermes] subscribed to %s as session=%q harness=%q → %s",
		b.cfg.DaemonWS, b.cfg.Session, b.cfg.Harness, b.cfg.TargetURL)

	// Per-CONNECTION ctx: the conn-watcher goroutine must observe THIS
	// connection's teardown, not the process-lifetime ctx — otherwise every
	// reconnect leaks one watcher pinning a closed conn (audit M1). The defer
	// cancels connCtx when runOnce returns, so the watcher always exits. We still
	// pass the PARENT ctx to handleFrame so an in-flight forward POST survives a
	// WS reconnect (the message was already received).
	connCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() {
		<-connCtx.Done()
		_ = conn.Close()
	}()

	// Read-loop liveness: bound the frame size + detect a dead peer via the read
	// deadline. The daemon pings us (we send none), so the deadline is refreshed
	// in the ping handler (which also replies with a pong via WriteControl — safe
	// concurrent with SendLocal's writer) as well as on each pong/data frame.
	conn.SetReadLimit(readLimitByte)
	_ = conn.SetReadDeadline(time.Now().Add(readWait))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(readWait))
	})
	conn.SetPingHandler(func(appData string) error {
		_ = conn.SetReadDeadline(time.Now().Add(readWait))
		err := conn.WriteControl(websocket.PongMessage, []byte(appData), time.Now().Add(writeWait))
		if err == websocket.ErrCloseSent {
			return nil
		}
		return err
	})

	for {
		_, data, err := conn.ReadMessage()
		if err != nil {
			return fmt.Errorf("read: %w", err)
		}
		_ = conn.SetReadDeadline(time.Now().Add(readWait))
		b.handleFrame(ctx, data)
	}
}

// handleFrame parses one WS frame and, if it is a deliverable message, forwards
// it. Parse/forward errors are logged, never fatal — one bad frame must not
// drop the subscription.
func (b *Bridge) handleFrame(ctx context.Context, data []byte) {
	var f inboundFrame
	if err := json.Unmarshal(data, &f); err != nil {
		b.cfg.Logger.Printf("[attn-hermes] skipping unparseable frame: %v", err)
		return
	}
	if !b.shouldForward(f) {
		return
	}
	if b.alreadySeen(f.ID) {
		b.cfg.Logger.Printf("[attn-hermes] skipping already-seen id=%q", f.ID)
		return
	}
	if err := b.forward(ctx, f); err != nil {
		b.cfg.Logger.Printf("[attn-hermes] forward failed id=%q from=%q: %v", f.ID, f.From, err)
		// Forget the id so a future retry (e.g. reconnect replay) can re-attempt.
		b.forget(f.ID)
		return
	}
	b.cfg.Logger.Printf("[attn-hermes] forwarded id=%q from=%q local=%v len=%d",
		f.ID, f.From, f.Local, len(f.Message))
}

// shouldForward reports whether a frame is a deliverable inbound. message and
// file frames are forwarded (file as a one-line notice, consistent with the
// opencode + pi adapters); reaction/typing/local-ack and self-originated echoes
// are dropped, as are empty-bodied messages (nothing to run on).
func (b *Bridge) shouldForward(f inboundFrame) bool {
	if f.From == b.cfg.Session { // self-echo guard (applies to every type)
		return false
	}
	switch f.Type {
	case "message":
		return strings.TrimSpace(f.Message) != ""
	case "file":
		return f.Filename != "" || f.Path != ""
	default:
		return false
	}
}

// buildPost composes the POST body for a frame. A file frame carries no message
// text, so we synthesize a one-line notice (mirroring the opencode renderer +
// the pi 📎 notice) referencing the daemon-saved path the agent can read.
func (b *Bridge) buildPost(f inboundFrame) postBody {
	sess := b.cfg.SessionKeyOverride
	if sess == "" {
		sess = b.cfg.Session
	}
	msg := f.Message
	if f.Type == "file" {
		name := f.Filename
		if name == "" {
			name = f.Path
		}
		msg = fmt.Sprintf("[received file: %s (%d bytes) at %s]", name, f.Size, f.Path)
	}
	return postBody{
		Session: sess,
		From:    f.From,
		Message: msg,
		ID:      f.ID,
		Ts:      f.Ts,
		Local:   f.Local,
		Trust:   f.Trust,
		GroupID: f.GroupID,
	}
}

// sign returns the lowercase hex HMAC-SHA256 of body under the configured secret.
func (b *Bridge) sign(body []byte) string {
	mac := hmac.New(sha256.New, []byte(b.cfg.HMACSecret))
	mac.Write(body)
	return hex.EncodeToString(mac.Sum(nil))
}

// forward POSTs the signed body to the hermes receiver and asserts a 2xx.
func (b *Bridge) forward(ctx context.Context, f inboundFrame) error {
	body, err := json.Marshal(b.buildPost(f))
	if err != nil {
		return fmt.Errorf("marshal: %w", err)
	}
	reqID := f.ID
	if reqID == "" {
		// No id on the frame (shouldn't happen for routed frames); fall back to a
		// deterministic id so retries stay idempotent at the receiver.
		reqID = b.sign(body)[:32]
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, b.cfg.TargetURL, bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("new request: %w", err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set(b.cfg.SignatureHeader, b.sign(body))
	req.Header.Set("X-Request-ID", reqID)
	resp, err := b.http.Do(req)
	if err != nil {
		return fmt.Errorf("post: %w", err)
	}
	defer resp.Body.Close()
	// Drain so the connection can be reused.
	snippet, _ := io.ReadAll(io.LimitReader(resp.Body, 512))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("receiver status %d: %s", resp.StatusCode, strings.TrimSpace(string(snippet)))
	}
	return nil
}

// --- local idempotency guard (defensive; receiver is authoritative) ---

func (b *Bridge) alreadySeen(id string) bool {
	if id == "" {
		return false
	}
	b.mu.Lock()
	defer b.mu.Unlock()
	if _, ok := b.seen[id]; ok {
		return true
	}
	if len(b.seen) >= seenCap {
		// Cheap bound: drop the whole set rather than track insertion order.
		b.seen = make(map[string]struct{}, 64)
	}
	b.seen[id] = struct{}{}
	return false
}

func (b *Bridge) forget(id string) {
	if id == "" {
		return
	}
	b.mu.Lock()
	delete(b.seen, id)
	b.mu.Unlock()
}

// SendLocal originates a local-mesh send on behalf of this session
// ({"type":"local","to":to,"message":msg}). This is the mesh "from the hermes
// session" path — the hermes side can reach any local peer by name (or "all")
// through the WS the bridge already holds. Safe to call concurrently; a no-op
// error is returned if the WS is not currently connected.
func (b *Bridge) SendLocal(to, msg string) error {
	b.mu.Lock()
	conn := b.conn
	b.mu.Unlock()
	if conn == nil {
		return fmt.Errorf("not connected")
	}
	frame := map[string]string{"type": "local", "to": to, "message": msg}
	data, err := json.Marshal(frame)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, data)
}
