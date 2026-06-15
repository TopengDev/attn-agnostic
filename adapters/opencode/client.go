// Package opencode is the M3 Stage-2 opencode adapter: a WS-subscriber bridge
// that delivers realtime attn inbound (relay + local-mesh) into a LIVE opencode
// session, and relays outbound local-mesh sends from that session.
//
// It has two halves:
//
//   - Client (this file): a thin, typed HTTP client for `opencode serve`
//     (default 127.0.0.1:4096). Layer-B injection is POST /session/:id/prompt_async.
//   - Bridge (bridge.go): subscribes to attnd's WS event stream
//     (ws://127.0.0.1:9742/?session=&harness=opencode — which IS the Layer-A
//     local-mesh registry entry), injects each inbound frame via the Client, and
//     relays {type:"local",to,message} mesh sends back over the same WS.
//
// Runtime route note (verified against installed opencode v1.3.13): the global
// OpenAPI at GET /doc only lists /global/*, /auth/*, /log — the session routes
// (/session, /session/:id/prompt_async, /session/:id/message) and /event are
// LIVE at runtime but ABSENT from the doc. So VerifyRoutes probes the real
// endpoints instead of trusting /doc (the 02b drift gotcha). Pin v1.3.13.
package opencode

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

// Client is a minimal typed client for an `opencode serve` HTTP API.
//
// opencode is unsecured on loopback by default (same-host OK). If a server
// password is configured (OPENCODE_SERVER_PASSWORD), pass it here and it is sent
// as `Authorization: Bearer <password>` — opencode's server-auth convention.
// Verify the header against your opencode version if you enable it; the default
// (empty) path matches the unsecured same-host default the bridge runs in.
type Client struct {
	baseURL  string
	password string
	hc       *http.Client
}

// NewClient builds a client for baseURL (e.g. http://127.0.0.1:4096). A trailing
// slash is trimmed. password is optional (see Client doc).
func NewClient(baseURL, password string) *Client {
	return &Client{
		baseURL:  strings.TrimRight(strings.TrimSpace(baseURL), "/"),
		password: strings.TrimSpace(password),
		hc:       &http.Client{Timeout: 30 * time.Second},
	}
}

// Session is one opencode conversation. Only the fields the bridge needs are
// modelled; opencode returns more.
type Session struct {
	ID        string `json:"id"`
	Title     string `json:"title"`
	Directory string `json:"directory"`
	ProjectID string `json:"projectID"`
	Time      struct {
		Created int64 `json:"created"`
		Updated int64 `json:"updated"`
	} `json:"time"`
}

// Message is one turn in a session transcript (GET /session/:id/message). Used by
// tests + verification to assert what landed in which session.
type Message struct {
	Info struct {
		ID   string `json:"id"`
		Role string `json:"role"`
	} `json:"info"`
	Parts []struct {
		Type string `json:"type"`
		Text string `json:"text"`
	} `json:"parts"`
}

// Text concatenates the text parts of a message (skips tool/reasoning parts).
func (m Message) Text() string {
	var b strings.Builder
	for _, p := range m.Parts {
		if p.Type == "text" && p.Text != "" {
			if b.Len() > 0 {
				b.WriteByte(' ')
			}
			b.WriteString(p.Text)
		}
	}
	return strings.TrimSpace(b.String())
}

func (c *Client) newReq(ctx context.Context, method, path string, body io.Reader) (*http.Request, error) {
	req, err := http.NewRequestWithContext(ctx, method, c.baseURL+path, body)
	if err != nil {
		return nil, err
	}
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if c.password != "" {
		req.Header.Set("Authorization", "Bearer "+c.password)
	}
	return req, nil
}

// Health returns the opencode version (GET /global/health → {healthy,version}).
func (c *Client) Health(ctx context.Context) (string, error) {
	req, err := c.newReq(ctx, http.MethodGet, "/global/health", nil)
	if err != nil {
		return "", err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return "", fmt.Errorf("opencode health: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", fmt.Errorf("opencode health: HTTP %d", resp.StatusCode)
	}
	var out struct {
		Healthy bool   `json:"healthy"`
		Version string `json:"version"`
	}
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<16)).Decode(&out); err != nil {
		return "", fmt.Errorf("opencode health decode: %w", err)
	}
	return out.Version, nil
}

// VerifyRoutes confirms the routes the bridge depends on are LIVE at runtime,
// rather than trusting GET /doc (which omits the session routes in v1.3.13 — the
// 02b drift gotcha). It checks /global/health and that GET /session responds. A
// non-empty wantVersionPrefix (e.g. "1.3") is enforced as a pin guard (warn-only
// is the caller's choice — here it's a hard mismatch error).
func (c *Client) VerifyRoutes(ctx context.Context, wantVersionPrefix string) (string, error) {
	ver, err := c.Health(ctx)
	if err != nil {
		return "", err
	}
	if wantVersionPrefix != "" && !strings.HasPrefix(ver, wantVersionPrefix) {
		return ver, fmt.Errorf("opencode version %q does not match expected prefix %q (pin mismatch — verify the API contract)", ver, wantVersionPrefix)
	}
	// Probe the session route directly: it is absent from /doc but must be live.
	req, err := c.newReq(ctx, http.MethodGet, "/session", nil)
	if err != nil {
		return ver, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return ver, fmt.Errorf("opencode /session probe: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode != http.StatusOK {
		return ver, fmt.Errorf("opencode /session route not live (HTTP %d) — API drift; verify routes for this opencode version", resp.StatusCode)
	}
	return ver, nil
}

// ListSessions returns the sessions opencode knows about. directory is optional
// (scopes to a project instance); empty lists all.
func (c *Client) ListSessions(ctx context.Context, directory string) ([]Session, error) {
	path := "/session"
	if directory != "" {
		path += "?directory=" + url.QueryEscape(directory)
	}
	req, err := c.newReq(ctx, http.MethodGet, path, nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opencode list sessions: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opencode list sessions: HTTP %d", resp.StatusCode)
	}
	var out []Session
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<22)).Decode(&out); err != nil {
		return nil, fmt.Errorf("opencode list sessions decode: %w", err)
	}
	return out, nil
}

// CreateSession creates (owns) a fresh opencode session in directory and returns
// it. title is optional.
func (c *Client) CreateSession(ctx context.Context, directory, title string) (*Session, error) {
	path := "/session"
	if directory != "" {
		path += "?directory=" + url.QueryEscape(directory)
	}
	body, _ := json.Marshal(map[string]any{"title": title})
	req, err := c.newReq(ctx, http.MethodPost, path, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opencode create session: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, fmt.Errorf("opencode create session: HTTP %d", resp.StatusCode)
	}
	var s Session
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<20)).Decode(&s); err != nil {
		return nil, fmt.Errorf("opencode create session decode: %w", err)
	}
	if s.ID == "" {
		return nil, fmt.Errorf("opencode create session: empty id in response")
	}
	return &s, nil
}

// PromptAsync injects text into a live session as a user turn and returns
// immediately (fire-and-forget). opencode runs a real model turn on the existing
// session. This is the Layer-B injection primitive. Expects 2xx (204 in v1.3.13).
func (c *Client) PromptAsync(ctx context.Context, sessionID, text string) error {
	if sessionID == "" {
		return fmt.Errorf("opencode prompt_async: empty sessionID")
	}
	body, _ := json.Marshal(map[string]any{
		"parts": []map[string]any{{"type": "text", "text": text}},
	})
	req, err := c.newReq(ctx, http.MethodPost, "/session/"+url.PathEscape(sessionID)+"/prompt_async", bytes.NewReader(body))
	if err != nil {
		return err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return fmt.Errorf("opencode prompt_async: %w", err)
	}
	defer resp.Body.Close()
	io.Copy(io.Discard, io.LimitReader(resp.Body, 1<<16))
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return fmt.Errorf("opencode prompt_async: HTTP %d", resp.StatusCode)
	}
	return nil
}

// Messages returns a session's transcript (GET /session/:id/message). Used by
// verification/tests to assert what was injected where.
func (c *Client) Messages(ctx context.Context, sessionID string) ([]Message, error) {
	req, err := c.newReq(ctx, http.MethodGet, "/session/"+url.PathEscape(sessionID)+"/message", nil)
	if err != nil {
		return nil, err
	}
	resp, err := c.hc.Do(req)
	if err != nil {
		return nil, fmt.Errorf("opencode messages: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("opencode messages: HTTP %d", resp.StatusCode)
	}
	var out []Message
	if err := json.NewDecoder(io.LimitReader(resp.Body, 1<<24)).Decode(&out); err != nil {
		return nil, fmt.Errorf("opencode messages decode: %w", err)
	}
	return out, nil
}
