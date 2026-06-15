package relay

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	icrypto "github.com/TopengDev/attn-agnostic/internal/crypto"
	"github.com/TopengDev/attn-agnostic/internal/identity"
)

// Client is a single authenticated connection to the attn relay.
type Client struct {
	id      *identity.Identity
	relay   string
	conn    *websocket.Conn
	verbose bool

	writeMu sync.Mutex // gorilla conns disallow concurrent writers

	authOnce sync.Once
	authCh   chan error // closed/signalled once on auth_ok or auth_error

	mu        sync.Mutex
	keyWaits  map[string][]chan *string      // address(lc) -> waiters
	delivWait map[string]chan DeliveryResult // message id -> waiter

	inbound  func(Inbound)
	closed   chan struct{}
	closeErr error
}

// Dial connects to the relay, performs the EIP-191 challenge-response handshake,
// and returns an authenticated Client. It blocks until auth_ok (or fails).
func Dial(ctx context.Context, relayURL string, id *identity.Identity, verbose bool) (*Client, error) {
	u, err := url.Parse(relayURL)
	if err != nil {
		return nil, fmt.Errorf("parse relay url: %w", err)
	}
	q := u.Query()
	q.Set("address", id.Address())
	u.RawQuery = q.Encode()

	dialer := websocket.Dialer{HandshakeTimeout: 15 * time.Second}
	conn, _, err := dialer.DialContext(ctx, u.String(), nil)
	if err != nil {
		return nil, fmt.Errorf("ws dial %s: %w", relayURL, err)
	}

	c := &Client{
		id:        id,
		relay:     relayURL,
		conn:      conn,
		verbose:   verbose,
		authCh:    make(chan error, 1),
		keyWaits:  make(map[string][]chan *string),
		delivWait: make(map[string]chan DeliveryResult),
		closed:    make(chan struct{}),
	}

	go c.readLoop()

	// Wait for the handshake to complete.
	select {
	case err := <-c.authCh:
		if err != nil {
			c.Close()
			return nil, err
		}
	case <-time.After(20 * time.Second):
		c.Close()
		return nil, fmt.Errorf("auth handshake timeout")
	case <-ctx.Done():
		c.Close()
		return nil, ctx.Err()
	}

	go c.keepAlive()
	return c, nil
}

func (c *Client) logf(format string, args ...any) {
	if c.verbose {
		log.Printf("[relay] "+format, args...)
	}
}

func (c *Client) writeJSON(v any) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteJSON(v)
}

func (c *Client) writeText(s string) error {
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	return c.conn.WriteMessage(websocket.TextMessage, []byte(s))
}

func (c *Client) signalAuth(err error) {
	c.authOnce.Do(func() { c.authCh <- err })
}

func (c *Client) readLoop() {
	defer close(c.closed)
	for {
		mt, data, err := c.conn.ReadMessage()
		if err != nil {
			c.closeErr = err
			c.signalAuth(fmt.Errorf("connection closed before auth: %w", err))
			c.logf("read error: %v", err)
			return
		}
		if mt != websocket.TextMessage {
			continue
		}
		// Heartbeat: the relay DO auto-responds to "ping" with raw "pong".
		if string(data) == "pong" {
			continue
		}

		var msg serverMessage
		if err := json.Unmarshal(data, &msg); err != nil {
			c.logf("non-JSON frame (%d bytes): %q", len(data), string(data))
			continue
		}
		c.handle(&msg)
	}
}

func (c *Client) handle(msg *serverMessage) {
	switch msg.Type {
	case "challenge":
		c.logf("← challenge nonce=%s", msg.Nonce)
		sig, err := c.id.SignPersonal(msg.Nonce)
		if err != nil {
			c.signalAuth(fmt.Errorf("sign challenge: %w", err))
			return
		}
		if err := c.writeJSON(authFrame{
			Type:      "auth",
			Address:   c.id.Address(),
			Signature: sig,
			Presence:  "online",
		}); err != nil {
			c.signalAuth(fmt.Errorf("send auth: %w", err))
			return
		}
		c.logf("→ auth address=%s", c.id.Address())

	case "auth_ok":
		c.logf("← auth_ok address=%s", msg.Address)
		c.signalAuth(nil)

	case "auth_error":
		c.logf("← auth_error: %s", msg.Error)
		c.signalAuth(fmt.Errorf("auth_error: %s", msg.Error))

	case "key_response":
		addr := normalize(msg.Address)
		c.logf("← key_response address=%s hasKey=%v", addr, msg.PublicKey != nil)
		c.mu.Lock()
		waiters := c.keyWaits[addr]
		delete(c.keyWaits, addr)
		c.mu.Unlock()
		for _, w := range waiters {
			w <- msg.PublicKey
		}

	case "received":
		c.logf("← received id=%s", msg.ID)

	case "delivered":
		c.logf("← delivered id=%s", msg.ID)

	case "delivery_status":
		c.logf("← delivery_status id=%s status=%s recipient=%s", msg.ID, msg.Status, msg.RecipientState)
		res := DeliveryResult{ID: msg.ID, Status: msg.Status, RecipientState: msg.RecipientState}
		if msg.RecipientMsg != nil {
			res.RecipientMsg = *msg.RecipientMsg
		}
		c.mu.Lock()
		w := c.delivWait[msg.ID]
		c.mu.Unlock()
		if w != nil {
			select {
			case w <- res:
			default:
			}
		}

	case "message":
		c.handleInbound(msg)

	case "reaction":
		// M0 scope: log only.
		c.logf("← reaction id=%s from=%s", msg.ID, msg.From)
		_ = c.writeJSON(ackFrame{Type: "ack", ID: msg.ID})

	case "error":
		c.logf("← relay error: %s", msg.Error)

	default:
		c.logf("← unhandled frame type=%s", msg.Type)
	}
}

func (c *Client) handleInbound(msg *serverMessage) {
	// Always ack so the relay clears it from the queue.
	defer func() { _ = c.writeJSON(ackFrame{Type: "ack", ID: msg.ID}) }()

	if msg.GroupID != "" {
		c.logf("← group message id=%s (M0: skipping)", msg.ID)
		return
	}

	plaintext, err := icrypto.DecryptBase64(c.id.PrivateKeyBytes(), msg.Encrypted)
	if err != nil {
		c.logf("decrypt inbound id=%s failed: %v", msg.ID, err)
		return
	}

	verified, verr := identity.VerifyEnvelope(msg.From, msg.ID, c.id.Address(), msg.Encrypted, msg.Signature)
	if verr != nil {
		c.logf("verify inbound id=%s error: %v", msg.ID, verr)
	}

	in := Inbound{
		ID:        msg.ID,
		From:      normalize(msg.From),
		FromName:  msg.FromName,
		Plaintext: string(plaintext),
		Ts:        msg.Ts,
		Verified:  verified,
	}
	if c.inbound != nil {
		c.inbound(in)
	}
}

func (c *Client) keepAlive() {
	t := time.NewTicker(30 * time.Second)
	defer t.Stop()
	for {
		select {
		case <-c.closed:
			return
		case <-t.C:
			if err := c.writeText("ping"); err != nil {
				c.logf("keepalive ping failed: %v", err)
				return
			}
		}
	}
}

// Address returns this client's identity address.
func (c *Client) Address() string { return c.id.Address() }

// GetKey looks up a recipient's secp256k1 public key (uncompressed hex) via the
// relay. Returns "" if the relay has no key on file (agent never connected).
func (c *Client) GetKey(ctx context.Context, address string) (string, error) {
	addr := normalize(address)
	ch := make(chan *string, 1)
	c.mu.Lock()
	c.keyWaits[addr] = append(c.keyWaits[addr], ch)
	first := len(c.keyWaits[addr]) == 1
	c.mu.Unlock()

	if first {
		if err := c.writeJSON(getKeyFrame{Type: "get_key", Address: addr}); err != nil {
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
		return "", fmt.Errorf("get_key timeout for %s", addr)
	case <-ctx.Done():
		return "", ctx.Err()
	case <-c.closed:
		return "", fmt.Errorf("connection closed: %w", c.closeErr)
	}
}

// Send encrypts plaintext to `to`, signs the envelope, transmits it, and waits
// for the relay's delivery_status. The recipient's public key is fetched via
// GetKey unless `recipientPubHex` is non-empty (a caching shortcut).
func (c *Client) Send(ctx context.Context, to, plaintext, recipientPubHex string) (DeliveryResult, string, error) {
	toLC := normalize(to)

	pub := recipientPubHex
	if pub == "" {
		var err error
		pub, err = c.GetKey(ctx, toLC)
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
	sig, err := c.id.SignEnvelope(id, toLC, encrypted)
	if err != nil {
		return DeliveryResult{}, "", fmt.Errorf("sign envelope: %w", err)
	}

	ch := make(chan DeliveryResult, 1)
	c.mu.Lock()
	c.delivWait[id] = ch
	c.mu.Unlock()
	defer func() {
		c.mu.Lock()
		delete(c.delivWait, id)
		c.mu.Unlock()
	}()

	if err := c.writeJSON(messageFrame{Type: "message", ID: id, To: toLC, Encrypted: encrypted, Signature: sig}); err != nil {
		return DeliveryResult{}, encrypted, fmt.Errorf("send message: %w", err)
	}
	c.logf("→ message id=%s to=%s (%d-byte ciphertext b64)", id, toLC, len(encrypted))

	select {
	case res := <-ch:
		return res, encrypted, nil
	case <-time.After(15 * time.Second):
		return DeliveryResult{ID: id, Status: "unknown"}, encrypted, fmt.Errorf("no delivery_status within 15s")
	case <-ctx.Done():
		return DeliveryResult{}, encrypted, ctx.Err()
	case <-c.closed:
		return DeliveryResult{}, encrypted, fmt.Errorf("connection closed: %w", c.closeErr)
	}
}

// Listen registers an inbound handler and blocks until the connection closes or
// ctx is cancelled. The handler is invoked for each decrypted DM.
func (c *Client) Listen(ctx context.Context, handler func(Inbound)) error {
	c.mu.Lock()
	c.inbound = handler
	c.mu.Unlock()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-c.closed:
		return c.closeErr
	}
}

// Close tears down the connection.
func (c *Client) Close() error {
	if c.conn == nil {
		return nil
	}
	c.writeMu.Lock()
	defer c.writeMu.Unlock()
	_ = c.conn.WriteMessage(websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, ""))
	return c.conn.Close()
}
