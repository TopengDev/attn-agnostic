package httpapi

import (
	"encoding/json"
	"log"
	"net/http"
	"net/url"
	"sync"
	"time"

	"github.com/gorilla/websocket"

	"github.com/TopengDev/attn-agnostic/internal/agent"
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

	mu      sync.Mutex
	clients map[*wsClient]struct{}
}

type wsClient struct {
	conn      *websocket.Conn
	session   string
	send      chan []byte
	done      chan struct{}
	closeOnce sync.Once
}

func newHub(logger *log.Logger) *Hub {
	return &Hub{
		log:     logger,
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
		send:    make(chan []byte, wsSendBuffer),
		done:    make(chan struct{}),
	}
	h.add(c)
	h.log.Printf("[ws] subscriber connected (session=%q, total=%d)", c.session, h.count())
	go h.writePump(c)
	h.readPump(c) // blocks until the connection closes
	h.remove(c)
	h.log.Printf("[ws] subscriber disconnected (session=%q, total=%d)", c.session, h.count())
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
		// Client→daemon frames are reserved for local-mesh send (M3). Reply with a
		// clear notice so a client that tries it gets feedback rather than silence.
		var in struct {
			Type string `json:"type"`
		}
		if json.Unmarshal(data, &in) == nil && in.Type == "local" {
			notice, _ := json.Marshal(map[string]any{"type": "error", "error": "local-mesh routing is not available yet (M3)"})
			c.trySend(notice)
		}
	}
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
