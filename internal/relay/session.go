package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/gorilla/websocket"

	icrypto "github.com/TopengDev/attn-agnostic/internal/crypto"
	"github.com/TopengDev/attn-agnostic/internal/identity"
)

// InboundEvent is a decrypted, classified inbound frame handed to the agent.
// The Session does the network + crypto (decrypt/verify); the agent applies
// policy (block/contact/mute) and persistence.
type InboundEvent struct {
	Kind        string // "dm" | "group" | "group_invite" | "group_member_update" | "reaction"
	ID          string
	From        string // sender address (lowercase)
	FromName    string // relay-provided .attn name, if any
	Plaintext   string // decrypted text (or emoji for reactions)
	Ts          int64
	Verified    bool // DM/reaction envelope signature recovered to From
	GroupID     string
	GroupName   string
	ReactionFor string   // message_id a reaction targets
	Action      string   // group_member_update: joined | left | admin_transferred
	Members     []string // group_invite/member_update roster
}

// Handlers are the agent-supplied callbacks the Session invokes. All are
// optional except OnInbound; a nil callback is skipped.
type Handlers struct {
	// OnInbound receives every decrypted, classified inbound event.
	OnInbound func(InboundEvent)
	// OnKeyLearned persists a freshly-resolved address→pubkey mapping.
	OnKeyLearned func(address, pubHex string)
	// OnReady fires after each successful (re)authentication — the agent flushes
	// its outbox and re-asserts presence here.
	OnReady func()
	// Logf is the structured logger.
	Logf func(format string, args ...any)
}

// Session is a persistent, self-healing relay connection: it dials, performs the
// EIP-191 handshake, and stays connected with ping keepalive, a pong watchdog,
// an auth-handshake watchdog, and exponential-backoff reconnect. One Session per
// identity. Upgrade of M0's single-shot Client.
type Session struct {
	id       *identity.Identity
	relayURL string
	httpBase string
	h        Handlers

	writeMu  sync.Mutex
	mu       sync.Mutex
	conn     *websocket.Conn
	authed   bool
	lastPong time.Time

	// onReadyActive single-flights OnReady so a reconnect storm can't launch
	// overlapping outbox flushes (which could double-send the same row). wg tracks
	// the in-flight OnReady goroutine so shutdown can join it.
	onReadyActive atomic.Bool
	wg            sync.WaitGroup

	keyCache  map[string]string
	keyWaits  map[string][]chan *string
	resWaits  map[string][]chan *resolveResult
	presWaits map[string][]chan *presenceResult
	delivWait map[string]chan DeliveryResult

	// readyCh is closed each time we reach auth_ok and replaced on disconnect, so
	// WaitReady can block for the next healthy connection.
	readyCh chan struct{}

	http *http.Client
}

// maxFrameBytes bounds a single inbound WebSocket frame. attn frames are small:
// a DM/group ECIES ciphertext (base64) or a control/system JSON object. File
// BYTES travel over HTTP (send_file uploads to the relay and only a small URL
// reference goes over the socket), so 8 MiB is far more than any legitimate
// frame while making the bound intentional + documented and capping per-frame
// memory against a hostile oversized frame (gorilla's default read limit is
// unbounded).
const maxFrameBytes = 8 << 20 // 8 MiB

type resolveResult struct {
	address string
	pubKey  string
}

type presenceResult struct {
	state   string
	message string
}

// NewSession builds a Session. Call Run to start the connect/reconnect loop.
func NewSession(id *identity.Identity, relayURL string, h Handlers) *Session {
	if h.Logf == nil {
		h.Logf = func(string, ...any) {}
	}
	return &Session{
		id:        id,
		relayURL:  relayURL,
		httpBase:  httpBaseFromWS(relayURL),
		h:         h,
		keyCache:  make(map[string]string),
		keyWaits:  make(map[string][]chan *string),
		resWaits:  make(map[string][]chan *resolveResult),
		presWaits: make(map[string][]chan *presenceResult),
		delivWait: make(map[string]chan DeliveryResult),
		readyCh:   make(chan struct{}),
		http:      &http.Client{Timeout: 20 * time.Second},
	}
}

// httpBaseFromWS mirrors env.ts getRelayHttpUrl: wss→https, ws→http, strip /ws.
func httpBaseFromWS(wsURL string) string {
	s := strings.Replace(wsURL, "wss://", "https://", 1)
	s = strings.Replace(s, "ws://", "http://", 1)
	return strings.TrimSuffix(s, "/ws")
}

// Address returns this session's identity address.
func (s *Session) Address() string { return s.id.Address() }

// HTTPBase returns the relay's HTTP base URL.
func (s *Session) HTTPBase() string { return s.httpBase }

// Run drives the connect → auth → serve → reconnect loop until ctx is done.
// Backoff is 1s doubling to 30s, reset to 1s on each successful auth. On exit it
// joins any in-flight OnReady/outbox flush so the goroutine is never leaked
// (the daemon may additionally call Wait() to block its own shutdown on it).
func (s *Session) Run(ctx context.Context) {
	defer s.wg.Wait()
	backoff := time.Second
	for {
		if ctx.Err() != nil {
			return
		}
		authed := s.connectOnce(ctx)
		if ctx.Err() != nil {
			return
		}
		if authed {
			backoff = time.Second // healthy connection → reset backoff
		}
		s.h.Logf("reconnect scheduled in %s", backoff)
		select {
		case <-ctx.Done():
			return
		case <-time.After(backoff):
		}
		backoff *= 2
		if backoff > 30*time.Second {
			backoff = 30 * time.Second
		}
	}
}

// connectOnce dials, authenticates, and serves one connection until it closes.
// Returns whether authentication was reached (drives backoff reset).
func (s *Session) connectOnce(ctx context.Context) (authed bool) {
	u, err := url.Parse(s.relayURL)
	if err != nil {
		s.h.Logf("parse relay url: %v", err)
		return false
	}
	q := u.Query()
	q.Set("address", s.id.Address())
	u.RawQuery = q.Encode()

	s.h.Logf("connecting to %s", s.relayURL)
	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		s.h.Logf("dial failed: %v", err)
		return false
	}
	conn.SetReadLimit(maxFrameBytes) // intentional per-frame bound (see maxFrameBytes)

	s.mu.Lock()
	s.conn = conn
	s.authed = false
	s.lastPong = time.Now()
	s.mu.Unlock()

	// Connection-scoped context: cancelled when this conn dies so the keepalive
	// and watchdog goroutines exit.
	connCtx, connCancel := context.WithCancel(ctx)
	defer connCancel()

	// Auth-handshake watchdog: if auth_ok doesn't arrive in 10s, drop the conn.
	authTimer := time.AfterFunc(10*time.Second, func() {
		s.mu.Lock()
		ok := s.authed
		s.mu.Unlock()
		if !ok {
			s.h.Logf("auth handshake timeout (10s) — dropping conn")
			_ = conn.Close()
		}
	})
	defer authTimer.Stop()

	authedCh := make(chan struct{}, 1)
	go s.keepAlive(connCtx, conn)

	// readLoop blocks until the connection errors/closes.
	s.readLoop(conn, authTimer, authedCh)

	// Mark not-ready and wake any waiters by replacing readyCh.
	s.mu.Lock()
	if s.conn == conn {
		s.conn = nil
	}
	s.authed = false
	old := s.readyCh
	s.readyCh = make(chan struct{})
	s.mu.Unlock()
	_ = old // readyCh is closed only on auth; replacing it resets WaitReady

	// Unblock + clear every pending request waiter orphaned by this disconnect,
	// so callers return immediately (clean retry path) instead of hanging until
	// their per-call timeout, and the maps can't grow unboundedly across
	// reconnect cycles on a flaky network.
	s.drainWaiters()

	select {
	case <-authedCh:
		authed = true
	default:
	}
	s.h.Logf("disconnected")
	return authed
}

// drainWaiters resolves every pending key/resolve/presence waiter with a nil
// result and clears the maps. Idempotent (a second call ranges over empty maps)
// and mutex-guarded. The delivery-status waiters (delivWait) are not drained
// here: each Send caller deletes its own entry via defer and is bounded by its
// own 15s timeout, so that map can't leak.
func (s *Session) drainWaiters() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, waiters := range s.keyWaits {
		for _, w := range waiters {
			select {
			case w <- nil:
			default:
			}
		}
		delete(s.keyWaits, k)
	}
	for k, waiters := range s.resWaits {
		for _, w := range waiters {
			select {
			case w <- nil:
			default:
			}
		}
		delete(s.resWaits, k)
	}
	for k, waiters := range s.presWaits {
		for _, w := range waiters {
			select {
			case w <- nil:
			default:
			}
		}
		delete(s.presWaits, k)
	}
}

// launchOnReady fires the OnReady callback (outbox flush + presence re-assert)
// after a successful (re)auth, single-flighted so a reconnect storm never runs
// two flushes at once, and tracked on wg so Wait() can join it at shutdown.
func (s *Session) launchOnReady() {
	if s.h.OnReady == nil {
		return
	}
	if !s.onReadyActive.CompareAndSwap(false, true) {
		s.h.Logf("OnReady already running — skipping duplicate flush")
		return
	}
	s.wg.Add(1)
	go func() {
		defer s.wg.Done()
		defer s.onReadyActive.Store(false)
		s.h.OnReady()
	}()
}

// Wait blocks until any in-flight OnReady goroutine has finished. The daemon
// calls this during graceful shutdown so an outbox flush isn't cut off mid-run.
func (s *Session) Wait() { s.wg.Wait() }

// removeWaiter removes ch from m[key] (and drops the key if it empties). Callers
// must hold s.mu. Used so a per-call timeout/cancel cleans up its own waiter
// even on a live connection (no disconnect needed).
func removeWaiter[T any](m map[string][]chan T, key string, ch chan T) {
	waiters := m[key]
	for i, w := range waiters {
		if w == ch {
			m[key] = append(waiters[:i:i], waiters[i+1:]...)
			break
		}
	}
	if len(m[key]) == 0 {
		delete(m, key)
	}
}

func (s *Session) readLoop(conn *websocket.Conn, authTimer *time.Timer, authedCh chan struct{}) {
	for {
		mt, data, err := conn.ReadMessage()
		if err != nil {
			s.h.Logf("read error: %v", err)
			return
		}
		if mt != websocket.TextMessage {
			continue
		}
		if string(data) == "pong" {
			s.mu.Lock()
			s.lastPong = time.Now()
			s.mu.Unlock()
			continue
		}
		var msg serverMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			s.h.Logf("non-JSON frame (%d bytes)", len(data))
			continue
		}
		s.handle(conn, &msg, authTimer, authedCh)
	}
}

func (s *Session) handle(conn *websocket.Conn, msg *serverMessage, authTimer *time.Timer, authedCh chan struct{}) {
	switch msg.Type {
	case "challenge":
		sig, err := s.id.SignPersonal(msg.Nonce)
		if err != nil {
			s.h.Logf("sign challenge: %v", err)
			_ = conn.Close()
			return
		}
		if err := s.writeJSON(conn, authFrame{Type: "auth", Address: s.id.Address(), Signature: sig, Presence: "online"}); err != nil {
			s.h.Logf("send auth: %v", err)
			_ = conn.Close()
		}

	case "auth_ok":
		s.h.Logf("authenticated as %s", msg.Address)
		authTimer.Stop()
		s.mu.Lock()
		s.authed = true
		s.lastPong = time.Now()
		close(s.readyCh) // wake WaitReady
		s.mu.Unlock()
		select {
		case authedCh <- struct{}{}:
		default:
		}
		s.launchOnReady()

	case "auth_error":
		s.h.Logf("auth_error: %s — dropping conn", msg.Error)
		_ = conn.Close()

	case "key_response":
		addr := normalize(msg.Address)
		if msg.PublicKey != nil && *msg.PublicKey != "" {
			s.cacheKey(addr, *msg.PublicKey)
		}
		s.mu.Lock()
		waiters := s.keyWaits[addr]
		delete(s.keyWaits, addr)
		s.mu.Unlock()
		for _, w := range waiters {
			w <- msg.PublicKey
		}

	case "resolve_response":
		label := strings.ToLower(msg.Name)
		var res *resolveResult
		if msg.Address != "" {
			r := &resolveResult{address: normalize(msg.Address)}
			if msg.PublicKey != nil {
				r.pubKey = *msg.PublicKey
				s.cacheKey(r.address, r.pubKey)
			}
			res = r
		}
		s.mu.Lock()
		waiters := s.resWaits[label]
		delete(s.resWaits, label)
		s.mu.Unlock()
		for _, w := range waiters {
			w <- res
		}

	case "presence_response":
		addr := normalize(msg.Address)
		res := &presenceResult{state: msg.State, message: msg.Message}
		s.mu.Lock()
		waiters := s.presWaits[addr]
		delete(s.presWaits, addr)
		s.mu.Unlock()
		for _, w := range waiters {
			w <- res
		}

	case "delivery_status":
		res := DeliveryResult{ID: msg.ID, Status: msg.Status, RecipientState: msg.RecipientState}
		if msg.RecipientMsg != nil {
			res.RecipientMsg = *msg.RecipientMsg
		}
		s.mu.Lock()
		w := s.delivWait[msg.ID]
		s.mu.Unlock()
		if w != nil {
			select {
			case w <- res:
			default:
			}
		}

	case "received", "delivered":
		// informational

	case "message":
		s.handleInboundMessage(conn, msg)

	case "reaction":
		s.handleInboundReaction(conn, msg)

	case "error":
		s.h.Logf("relay error: %s", msg.Error)

	default:
		s.h.Logf("unhandled frame type=%s", msg.Type)
	}
}

// ack always fires so the relay clears the message from its queue.
func (s *Session) ack(conn *websocket.Conn, id string) {
	_ = s.writeJSON(conn, ackFrame{Type: "ack", ID: id})
}

func (s *Session) emit(ev InboundEvent) {
	if s.h.OnInbound != nil {
		s.h.OnInbound(ev)
	}
}

func (s *Session) handleInboundMessage(conn *websocket.Conn, msg *serverMessage) {
	defer s.ack(conn, msg.ID)

	// Group system messages are unencrypted JSON ({"type":"group_invite"|...}).
	if msg.GroupID != "" && strings.HasPrefix(strings.TrimSpace(msg.Encrypted), "{") {
		var sys struct {
			Type      string   `json:"type"`
			GroupID   string   `json:"group_id"`
			GroupName string   `json:"group_name"`
			From      string   `json:"from"`
			Action    string   `json:"action"`
			Address   string   `json:"address"`
			Members   []string `json:"members"`
		}
		if err := json.Unmarshal([]byte(msg.Encrypted), &sys); err == nil && sys.Type != "" {
			switch sys.Type {
			case "group_invite":
				s.emit(InboundEvent{
					Kind: "group_invite", ID: msg.ID, From: normalize(sys.From), Ts: msg.Ts,
					GroupID: sys.GroupID, GroupName: sys.GroupName, Members: lowerAll(sys.Members),
				})
				return
			case "group_member_update":
				s.emit(InboundEvent{
					Kind: "group_member_update", ID: msg.ID, From: normalize(sys.Address), Ts: msg.Ts,
					GroupID: sys.GroupID, GroupName: sys.GroupName, Action: sys.Action,
					Members: lowerAll(sys.Members),
				})
				return
			}
		}
	}

	plaintext, err := icrypto.DecryptBase64(s.id.PrivateKeyBytes(), msg.Encrypted)
	if err != nil {
		s.h.Logf("decrypt inbound id=%s failed: %v", msg.ID, err)
		return
	}

	if msg.GroupID != "" {
		// Group message — relay is the trust anchor, no signature verification.
		s.emit(InboundEvent{
			Kind: "group", ID: msg.ID, From: normalize(msg.From), FromName: msg.FromName,
			Plaintext: string(plaintext), Ts: msg.Ts, GroupID: msg.GroupID, GroupName: msg.GroupName,
		})
		return
	}

	verified, _ := identity.VerifyEnvelope(msg.From, msg.ID, s.id.Address(), msg.Encrypted, msg.Signature)
	s.emit(InboundEvent{
		Kind: "dm", ID: msg.ID, From: normalize(msg.From), FromName: msg.FromName,
		Plaintext: string(plaintext), Ts: msg.Ts, Verified: verified,
	})
}

func (s *Session) handleInboundReaction(conn *websocket.Conn, msg *serverMessage) {
	defer s.ack(conn, msg.ID)
	emoji, err := icrypto.DecryptBase64(s.id.PrivateKeyBytes(), msg.Encrypted)
	if err != nil {
		s.h.Logf("decrypt reaction id=%s failed: %v", msg.ID, err)
		return
	}
	verified := true
	if msg.GroupID == "" {
		verified, _ = identity.VerifyEnvelope(msg.From, msg.ID, s.id.Address(), msg.Encrypted, msg.Signature)
	}
	s.emit(InboundEvent{
		Kind: "reaction", ID: msg.ID, From: normalize(msg.From), FromName: msg.FromName,
		Plaintext: string(emoji), Ts: msg.Ts, Verified: verified,
		GroupID: msg.GroupID, GroupName: msg.GroupName, ReactionFor: msg.MessageID,
	})
}

// ── outbound send paths ──────────────────────────────────────────────────

func (s *Session) writeJSON(conn *websocket.Conn, v any) error {
	s.writeMu.Lock()
	defer s.writeMu.Unlock()
	return conn.WriteJSON(v)
}

func (s *Session) currentConn() (*websocket.Conn, bool) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.conn, s.authed && s.conn != nil
}

// IsReady reports whether the session is connected and authenticated.
func (s *Session) IsReady() bool {
	_, ok := s.currentConn()
	return ok
}

// WaitReady blocks until the session is authenticated or ctx/timeout elapses.
func (s *Session) WaitReady(ctx context.Context, timeout time.Duration) bool {
	if s.IsReady() {
		return true
	}
	s.mu.Lock()
	ch := s.readyCh
	s.mu.Unlock()
	select {
	case <-ch:
		return true
	case <-time.After(timeout):
		return s.IsReady()
	case <-ctx.Done():
		return false
	}
}

func (s *Session) cacheKey(addr, pub string) {
	s.mu.Lock()
	s.keyCache[addr] = pub
	s.mu.Unlock()
	if s.h.OnKeyLearned != nil {
		s.h.OnKeyLearned(addr, pub)
	}
}

// CachedKey returns an in-memory cached pubkey for addr, or "".
func (s *Session) CachedKey(addr string) string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.keyCache[normalize(addr)]
}

// PrimeKey seeds the in-memory key cache (e.g. from the DB on startup).
func (s *Session) PrimeKey(addr, pub string) {
	if pub == "" {
		return
	}
	s.mu.Lock()
	s.keyCache[normalize(addr)] = pub
	s.mu.Unlock()
}

// GetKey resolves a recipient's uncompressed public key via the in-memory cache
// or a relay get_key round-trip. Returns "" if the relay has no key on file.
func (s *Session) GetKey(ctx context.Context, address string) (string, error) {
	addr := normalize(address)
	if k := s.CachedKey(addr); k != "" {
		return k, nil
	}
	conn, ready := s.currentConn()
	if !ready {
		return "", fmt.Errorf("relay not ready")
	}
	ch := make(chan *string, 1)
	s.mu.Lock()
	s.keyWaits[addr] = append(s.keyWaits[addr], ch)
	first := len(s.keyWaits[addr]) == 1
	s.mu.Unlock()
	if first {
		if err := s.writeJSON(conn, getKeyFrame{Type: "get_key", Address: addr}); err != nil {
			return "", fmt.Errorf("send get_key: %w", err)
		}
	}
	select {
	case pk := <-ch:
		if pk == nil {
			return "", nil
		}
		return *pk, nil
	case <-time.After(10 * time.Second):
		s.mu.Lock()
		removeWaiter(s.keyWaits, addr, ch)
		s.mu.Unlock()
		return "", fmt.Errorf("get_key timeout for %s", addr)
	case <-ctx.Done():
		s.mu.Lock()
		removeWaiter(s.keyWaits, addr, ch)
		s.mu.Unlock()
		return "", ctx.Err()
	}
}

// Send encrypts plaintext to `to`, signs the envelope, transmits it, and waits
// for delivery_status. recipientPubHex short-circuits the key lookup if set.
func (s *Session) Send(ctx context.Context, to, plaintext, recipientPubHex string) (DeliveryResult, string, error) {
	toLC := normalize(to)
	pub := recipientPubHex
	if pub == "" {
		var err error
		pub, err = s.GetKey(ctx, toLC)
		if err != nil {
			return DeliveryResult{}, "", fmt.Errorf("lookup recipient key: %w", err)
		}
		if pub == "" {
			return DeliveryResult{}, "", fmt.Errorf("no public key on relay for %s (has it ever connected?)", toLC)
		}
	}
	encrypted, err := icrypto.EncryptBase64(pub, []byte(plaintext))
	if err != nil {
		return DeliveryResult{}, "", fmt.Errorf("encrypt: %w", err)
	}
	id := newID()
	sig, err := s.id.SignEnvelope(id, toLC, encrypted)
	if err != nil {
		return DeliveryResult{}, encrypted, fmt.Errorf("sign envelope: %w", err)
	}
	return s.sendPreparedMessage(ctx, id, toLC, encrypted, sig)
}

// sendPreparedMessage transmits an already-encrypted+signed message frame and
// waits for delivery_status. Shared by Send and SendQueued.
func (s *Session) sendPreparedMessage(ctx context.Context, id, toLC, encrypted, sig string) (DeliveryResult, string, error) {
	conn, ready := s.currentConn()
	if !ready {
		return DeliveryResult{}, encrypted, fmt.Errorf("relay not ready")
	}
	ch := make(chan DeliveryResult, 1)
	s.mu.Lock()
	s.delivWait[id] = ch
	s.mu.Unlock()
	defer func() {
		s.mu.Lock()
		delete(s.delivWait, id)
		s.mu.Unlock()
	}()

	if err := s.writeJSON(conn, messageFrame{Type: "message", ID: id, To: toLC, Encrypted: encrypted, Signature: sig}); err != nil {
		return DeliveryResult{}, encrypted, fmt.Errorf("send message: %w", err)
	}
	s.h.Logf("→ message id=%s to=%s (%d-byte ct)", id, toLC, len(encrypted))
	select {
	case res := <-ch:
		return res, encrypted, nil
	case <-time.After(15 * time.Second):
		return DeliveryResult{ID: id, Status: "unknown"}, encrypted, fmt.Errorf("no delivery_status within 15s")
	case <-ctx.Done():
		return DeliveryResult{}, encrypted, ctx.Err()
	}
}

// SendQueued transmits an outbox row (already encrypted+signed) without waiting
// for delivery_status — used during the post-auth outbox flush.
func (s *Session) SendQueued(id, toLC, encrypted, sig string) error {
	conn, ready := s.currentConn()
	if !ready {
		return fmt.Errorf("relay not ready")
	}
	return s.writeJSON(conn, messageFrame{Type: "message", ID: id, To: normalize(toLC), Encrypted: encrypted, Signature: sig})
}

// SignEnvelope exposes envelope signing so the agent can prepare outbox rows.
func (s *Session) SignEnvelope(id, to, encrypted string) (string, error) {
	return s.id.SignEnvelope(id, normalize(to), encrypted)
}

// NewMessageID mints a message id.
func (s *Session) NewMessageID() string { return newID() }

// SendReaction encrypts an emoji to `to`, signs it, and sends a reaction frame
// referencing messageID. recipientPubHex short-circuits the key lookup.
func (s *Session) SendReaction(ctx context.Context, to, messageID, emoji, recipientPubHex string) error {
	toLC := normalize(to)
	pub := recipientPubHex
	if pub == "" {
		var err error
		pub, err = s.GetKey(ctx, toLC)
		if err != nil {
			return fmt.Errorf("lookup recipient key: %w", err)
		}
		if pub == "" {
			return fmt.Errorf("no public key on relay for %s", toLC)
		}
	}
	encrypted, err := icrypto.EncryptBase64(pub, []byte(emoji))
	if err != nil {
		return fmt.Errorf("encrypt: %w", err)
	}
	id := newID()
	sig, err := s.id.SignEnvelope(id, toLC, encrypted)
	if err != nil {
		return fmt.Errorf("sign envelope: %w", err)
	}
	conn, ready := s.currentConn()
	if !ready {
		return fmt.Errorf("relay not ready")
	}
	return s.writeJSON(conn, reactionFrame{Type: "reaction", ID: id, To: toLC, MessageID: messageID, Encrypted: encrypted, Signature: sig})
}

// Resolve asks the relay to resolve a .attn label, returning (address, pubHex).
func (s *Session) Resolve(ctx context.Context, label string) (string, string, error) {
	lbl := strings.ToLower(strings.TrimSuffix(label, ".attn"))
	conn, ready := s.currentConn()
	if !ready {
		return "", "", fmt.Errorf("relay not ready")
	}
	ch := make(chan *resolveResult, 1)
	s.mu.Lock()
	s.resWaits[lbl] = append(s.resWaits[lbl], ch)
	first := len(s.resWaits[lbl]) == 1
	s.mu.Unlock()
	if first {
		if err := s.writeJSON(conn, resolveFrame{Type: "resolve", Name: lbl}); err != nil {
			return "", "", fmt.Errorf("send resolve: %w", err)
		}
	}
	select {
	case r := <-ch:
		if r == nil {
			return "", "", nil
		}
		return r.address, r.pubKey, nil
	case <-time.After(5 * time.Second):
		s.mu.Lock()
		removeWaiter(s.resWaits, lbl, ch)
		s.mu.Unlock()
		return "", "", fmt.Errorf("resolve timeout for %s", lbl)
	case <-ctx.Done():
		s.mu.Lock()
		removeWaiter(s.resWaits, lbl, ch)
		s.mu.Unlock()
		return "", "", ctx.Err()
	}
}

// QueryPresence asks the relay for another agent's availability.
func (s *Session) QueryPresence(ctx context.Context, address string) (state, message string, ok bool, err error) {
	addr := normalize(address)
	conn, ready := s.currentConn()
	if !ready {
		return "", "", false, fmt.Errorf("relay not ready")
	}
	ch := make(chan *presenceResult, 1)
	s.mu.Lock()
	s.presWaits[addr] = append(s.presWaits[addr], ch)
	first := len(s.presWaits[addr]) == 1
	s.mu.Unlock()
	if first {
		if err := s.writeJSON(conn, presenceQueryFrame{Type: "presence_query", Address: addr}); err != nil {
			return "", "", false, fmt.Errorf("send presence_query: %w", err)
		}
	}
	select {
	case r := <-ch:
		if r == nil {
			return "", "", false, nil
		}
		return r.state, r.message, true, nil
	case <-time.After(5 * time.Second):
		s.mu.Lock()
		removeWaiter(s.presWaits, addr, ch)
		s.mu.Unlock()
		return "", "", false, fmt.Errorf("presence_query timeout for %s", addr)
	case <-ctx.Done():
		s.mu.Lock()
		removeWaiter(s.presWaits, addr, ch)
		s.mu.Unlock()
		return "", "", false, ctx.Err()
	}
}

// SetPresence sets this agent's availability on the relay (best effort).
func (s *Session) SetPresence(state, message string) error {
	conn, ready := s.currentConn()
	if !ready {
		return fmt.Errorf("relay not ready")
	}
	var msgPtr *string
	if message != "" {
		msgPtr = &message
	}
	return s.writeJSON(conn, presenceSetFrame{Type: "presence_set", State: state, Message: msgPtr})
}

func (s *Session) keepAlive(ctx context.Context, conn *websocket.Conn) {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			s.mu.Lock()
			last := s.lastPong
			s.mu.Unlock()
			if time.Since(last) > 90*time.Second {
				s.h.Logf("pong watchdog expired (%.0fs) — dropping conn", time.Since(last).Seconds())
				_ = conn.Close()
				return
			}
			s.writeMu.Lock()
			err := conn.WriteMessage(websocket.TextMessage, []byte("ping"))
			s.writeMu.Unlock()
			if err != nil {
				s.h.Logf("keepalive ping failed: %v — dropping conn", err)
				_ = conn.Close()
				return
			}
		}
	}
}

// DropConn force-closes the current connection (used to simulate a network blip
// for reconnect testing). The Run loop reconnects automatically.
func (s *Session) DropConn() {
	s.mu.Lock()
	conn := s.conn
	s.mu.Unlock()
	if conn != nil {
		_ = conn.Close()
	}
}

// ── signed HTTP (group + file ops) ───────────────────────────────────────

// SignedRequest performs an HTTP request to the relay with the X-Attn-* signed
// headers (mirrors env.ts signedFetch: personal_sign("METHOD:path:timestamp")).
func (s *Session) SignedRequest(ctx context.Context, method, path string, body io.Reader, extraHeaders map[string]string) (*http.Response, error) {
	full := s.httpBase + path
	parsed, err := url.Parse(full)
	if err != nil {
		return nil, err
	}
	ts := strconv.FormatInt(time.Now().UnixMilli(), 10)
	nonce := method + ":" + parsed.Path + ":" + ts
	sig, err := s.id.SignPersonal(nonce)
	if err != nil {
		return nil, fmt.Errorf("sign request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, method, full, body)
	if err != nil {
		return nil, err
	}
	req.Header.Set("X-Attn-Address", s.id.Address())
	req.Header.Set("X-Attn-Timestamp", ts)
	req.Header.Set("X-Attn-Signature", sig)
	for k, v := range extraHeaders {
		req.Header.Set(k, v)
	}
	return s.http.Do(req)
}

// PlainGet performs an unsigned GET (for public relay endpoints like /resolve).
func (s *Session) PlainGet(ctx context.Context, path string) (*http.Response, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, s.httpBase+path, nil)
	if err != nil {
		return nil, err
	}
	return s.http.Do(req)
}

func lowerAll(in []string) []string {
	out := make([]string, len(in))
	for i, v := range in {
		out[i] = normalize(v)
	}
	return out
}
