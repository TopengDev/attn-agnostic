package httpapi

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/TopengDev/attn-agnostic/internal/agent"
	"github.com/TopengDev/attn-agnostic/internal/mesh"
)

// checkLoopbackOrigin allows WS upgrades from non-browser clients (no Origin) and
// from loopback origins only; cross-origin browser pages are rejected.
func checkLoopbackOrigin(r *http.Request) bool {
	origin := r.Header.Get("Origin")
	if origin == "" {
		return true
	}
	u, err := url.Parse(origin)
	if err != nil {
		return false
	}
	return allowedHost(u.Host)
}

const (
	wsWriteWait  = 10 * time.Second
	wsPongWait   = 90 * time.Second
	wsPingPeriod = 30 * time.Second
	wsSendBuffer = 64
)

// Hub fans surfaced inbound events out to every connected WS subscriber. Each
// client gets a buffered send channel + a dedicated writer goroutine, so one
// slow subscriber can never block the relay read loop (the broadcast path is
// non-blocking; a full buffer drops the frame with a log line). The send channel
// is never closed — shutdown is signalled via a `done` channel — so Broadcast
// can never send on a closed channel.
type Hub struct {
	log      *log.Logger
	upgrader websocket.Upgrader
	reg      *mesh.Registry // Layer-A local-mesh registry (shared with the agent)
	ag       *agent.Agent   // for minting routed-frame message ids

	mu      sync.Mutex
	clients map[*wsClient]struct{}
}

type wsClient struct {
	conn      *websocket.Conn
	session   string
	harness   string
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
	release   func() // deregisters this client from the mesh (no-op if never registered)
}

func newHub(logger *log.Logger, reg *mesh.Registry, ag *agent.Agent) *Hub {
	if reg == nil {
		reg = mesh.New()
	}
	return &Hub{
		log:     logger,
		reg:     reg,
		ag:      ag,
		clients: make(map[*wsClient]struct{}),
		upgrader: websocket.Upgrader{
			ReadBufferSize:  1024,
			WriteBufferSize: 4096,
			// Reject cross-origin browser upgrades (audit M-csrf). Non-browser
			// clients (the pi `ws` lib, gorilla dialer) send no Origin → allowed;
			// a browser carries Origin → its host must be loopback.
			CheckOrigin: checkLoopbackOrigin,
		},
	}
}

func (s *Server) handleWS(w http.ResponseWriter, r *http.Request) { s.hub.serve(w, r) }

func (h *Hub) serve(w http.ResponseWriter, r *http.Request) {
	conn, err := h.upgrader.Upgrade(w, r, nil)
	if err != nil {
		h.log.Printf("[ws] upgrade failed: %v", err)
		return
	}
	c := &wsClient{
		conn:    conn,
		session: r.URL.Query().Get("session"),
		harness: r.URL.Query().Get("harness"),
		send:    make(chan []byte, wsSendBuffer),
		done:    make(chan struct{}),
		release: func() {},
	}
	h.add(c)
	// Layer-A self-registration (pi-style): a subscriber that connected with a
	// ?session=<name> IS its local-mesh registry entry. The connection is the
	// transport; deregister on disconnect. Last-registration-wins, and the stale
	// old connection's release is a no-op (mesh.Register identity-compare), so a
	// reconnect under the same name never evicts the fresh entry.
	if c.session != "" {
		c.release = h.reg.Register(&mesh.Entry{
			Name: c.session, Harness: c.harness, Transport: mesh.TransportWS,
		}, &wsDeliverer{c: c, hub: h})
	}
	h.log.Printf("[ws] subscriber connected (session=%q harness=%q, total=%d, local-sessions=%d)",
		c.session, c.harness, h.count(), h.reg.Count())
	go h.writePump(c)
	h.readPump(c) // blocks until the connection closes
	c.release()   // deregister from the mesh before removing from the broadcast set
	h.remove(c)
	h.log.Printf("[ws] subscriber disconnected (session=%q, total=%d, local-sessions=%d)",
		c.session, h.count(), h.reg.Count())
}

// Broadcast marshals a surface event into the pi-setup frame shape and pushes it
// to every subscriber. Non-blocking per client.
func (h *Hub) Broadcast(ev agent.SurfaceEvent) {
	data, err := json.Marshal(surfaceToFrame(ev))
	if err != nil {
		h.log.Printf("[ws] marshal surface event: %v", err)
		return
	}
	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		if !c.trySend(data) {
			h.log.Printf("[ws] subscriber (session=%q) send buffer full — dropping frame", c.session)
		}
	}
}

// trySend enqueues data without blocking. Returns false if the buffer was full
// (frame dropped). Safe after close (the done case wins, never a closed-chan send).
func (c *wsClient) trySend(data []byte) bool {
	select {
	case c.send <- data:
		return true
	case <-c.done:
		return true // client gone — not a buffer-full drop
	default:
		return false
	}
}

// surfaceToFrame is the WS inbound contract. It MUST stay aligned with pi-setup's
// extensions/attn/index.ts message parser (type/from/message/filename/path/size/
// id/ts/trust/agentName/groupId/groupName/reactionMessageId/local). `deliveryMode`
// is our additive injection-mode hint (04-architecture.md) — older adapters that
// don't read it are unaffected.
func surfaceToFrame(ev agent.SurfaceEvent) map[string]any {
	f := map[string]any{
		"type":         ev.Type,
		"from":         ev.From,
		"id":           ev.MessageID,
		"ts":           ev.Ts,
		"deliveryMode": ev.DeliveryMode,
	}
	if ev.FromName != "" {
		f["agentName"] = ev.FromName
	}
	if ev.Trust != "" {
		f["trust"] = ev.Trust
	}
	if ev.GroupID != "" {
		f["groupId"] = ev.GroupID
	}
	if ev.GroupName != "" {
		f["groupName"] = ev.GroupName
	}
	if ev.Local {
		f["local"] = true
	}
	if ev.ReactionFor != "" {
		f["reactionMessageId"] = ev.ReactionFor
	}
	switch ev.Type {
	case "file":
		f["filename"] = ev.Filename
		f["path"] = ev.Path
		f["size"] = ev.Size
	default:
		f["message"] = ev.Message
	}
	return f
}

func (h *Hub) writePump(c *wsClient) {
	ticker := time.NewTicker(wsPingPeriod)
	defer func() {
		ticker.Stop()
		c.close()
	}()
	for {
		select {
		case msg := <-c.send:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.TextMessage, msg); err != nil {
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(wsWriteWait))
			if err := c.conn.WriteMessage(websocket.PingMessage, nil); err != nil {
				return
			}
		case <-c.done:
			return
		}
	}
}

func (h *Hub) readPump(c *wsClient) {
	defer c.close()
	c.conn.SetReadLimit(maxBody)
	_ = c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
	})
	for {
		_, data, err := c.conn.ReadMessage()
		if err != nil {
			return
		}
		_ = c.conn.SetReadDeadline(time.Now().Add(wsPongWait))
		// Client→daemon frames carry a local-mesh send (pi-style). pi emits
		// {type:'local', to, message}; CC's local.ts uses `text` — accept both.
		h.handleClientFrame(c, data)
	}
}

// clientFrame is the client→daemon WS frame. Only type:"local" is acted on.
type clientFrame struct {
	Type        string `json:"type"`
	To          string `json:"to"`
	Message     string `json:"message"`
	Text        string `json:"text"` // CC local.ts alias for message
	ReactionFor string `json:"reaction_for"`
}

// handleClientFrame routes a client→daemon local-mesh send (relay-bypassed) and
// replies with a {type:'local-ack'} the way pi-setup's extension expects. Only a
// subscriber that self-registered (has a session name) may originate a local
// send — its name is the authoritative sender identity (no client-supplied
// "from", so a session can't spoof another).
func (h *Hub) handleClientFrame(c *wsClient, data []byte) {
	var in clientFrame
	if json.Unmarshal(data, &in) != nil || in.Type != "local" {
		return
	}
	if c.session == "" {
		c.trySend(localAck("", false, "anonymous subscriber cannot send local messages (connect with ?session=<name>)"))
		return
	}
	body := in.Message
	if body == "" {
		body = in.Text
	}
	frame := mesh.Frame{
		ID: h.newID(), From: c.session, Text: body,
		Ts: time.Now().UnixMilli(), ReactionFor: in.ReactionFor,
	}

	if in.To == "all" {
		frame.Broadcast = true
		sent := h.reg.Broadcast(frame, c.session) // exclude the sender
		c.trySend(localAck("all", len(sent) > 0, fmt.Sprintf("delivered to %d local session(s)", len(sent))))
		h.log.Printf("[ws] local broadcast from %q → %d session(s)", c.session, len(sent))
		return
	}

	name, err := h.reg.Route(in.To, frame)
	if err != nil {
		c.trySend(localAck(in.To, false, fmt.Sprintf("no local session %q", in.To)))
		h.log.Printf("[ws] local send from %q to %q failed: %v", c.session, in.To, err)
		return
	}
	c.trySend(localAck(name, true, "delivered"))
	h.log.Printf("[ws] local send %q → %q delivered", c.session, name)
}

// newID mints a routed-frame id (via the agent's relay session); falls back to a
// timestamp-derived id if no agent is wired (tests).
func (h *Hub) newID() string {
	if h.ag != nil {
		return h.ag.NewMessageID()
	}
	return fmt.Sprintf("local-%d", time.Now().UnixNano())
}

func localAck(to string, delivered bool, detail string) []byte {
	b, _ := json.Marshal(map[string]any{
		"type": "local-ack", "to": to, "delivered": delivered, "detail": detail,
	})
	return b
}

// wsDeliverer delivers a local-mesh frame to one WS subscriber by pushing the
// pi-setup inbound frame onto its buffered send channel (non-blocking; a full
// buffer is reported as an error so the registry's broadcast fan-out can omit it).
type wsDeliverer struct {
	c   *wsClient
	hub *Hub
}

func (d *wsDeliverer) Deliver(f mesh.Frame) error {
	// Reuse the canonical inbound frame contract: a local message is a normal
	// message frame with local=true + trust="local" (pi reads `local`; CC reads
	// `trust`), from = the sender's session NAME, deliveryMode = steer.
	ev := agent.SurfaceEvent{
		Type: "message", From: f.From, Message: f.Text, MessageID: f.ID, Ts: f.Ts,
		Local: true, Trust: "local", DeliveryMode: "steer", ReactionFor: f.ReactionFor,
	}
	data, err := json.Marshal(surfaceToFrame(ev))
	if err != nil {
		return err
	}
	if !d.c.trySend(data) {
		return fmt.Errorf("subscriber %q send buffer full", d.c.session)
	}
	return nil
}

func (h *Hub) add(c *wsClient) {
	h.mu.Lock()
	h.clients[c] = struct{}{}
	h.mu.Unlock()
}

func (h *Hub) remove(c *wsClient) {
	h.mu.Lock()
	delete(h.clients, c)
	h.mu.Unlock()
	c.close()
}

func (h *Hub) count() int {
	h.mu.Lock()
	defer h.mu.Unlock()
	return len(h.clients)
}

// CloseAll closes every subscriber (called on graceful shutdown).
func (h *Hub) CloseAll() {
	h.mu.Lock()
	clients := make([]*wsClient, 0, len(h.clients))
	for c := range h.clients {
		clients = append(clients, c)
	}
	h.mu.Unlock()
	for _, c := range clients {
		c.close()
	}
}

func (c *wsClient) close() {
	c.closeOnce.Do(func() {
		close(c.done)
		_ = c.conn.Close()
	})
}

// isWebSocketUpgrade reports whether r is a WS upgrade request.
func isWebSocketUpgrade(r *http.Request) bool {
	return websocket.IsWebSocketUpgrade(r)
}
