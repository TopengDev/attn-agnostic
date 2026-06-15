package httpapi

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/mesh"
)

// localInjectClient is the bounded HTTP client used to push routed local frames
// to http-target sessions. It is short-timeout so a hung/slow adapter endpoint
// can never wedge the routing path (the broadcast fan-out delivers sequentially).
var localInjectClient = &http.Client{Timeout: 5 * time.Second}

// httpDeliverer delivers a local-mesh frame to an http-target session by POSTing
// the inbound frame to the adapter's registered (loopback) inject endpoint. The
// wire shape mirrors the WS local frame: type:"message", local:true,
// trust:"local", from = sender session name, plus the adapter's own session
// handle (sessionId/sessionKey) so it can target the correct live session.
type httpDeliverer struct {
	endpoint  string
	sessionID string
	client    *http.Client
}

func (d *httpDeliverer) Deliver(f mesh.Frame) error {
	payload := map[string]any{
		"type": "message", "from": f.From, "message": f.Text,
		"id": f.ID, "ts": f.Ts, "local": true, "trust": "local",
		"deliveryMode": "steer",
	}
	if d.sessionID != "" {
		payload["sessionId"] = d.sessionID
	}
	if f.ReactionFor != "" {
		payload["reactionMessageId"] = f.ReactionFor
	}
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, d.endpoint, bytes.NewReader(raw))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := d.client.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return fmt.Errorf("http-target inject status %d", resp.StatusCode)
	}
	return nil
}

// loopbackEndpoint validates that a registered http-target inject endpoint is a
// loopback http(s) URL. The local mesh is same-host + unencrypted by design; an
// off-host endpoint would let a (trusted-but-buggy) local registration turn the
// daemon into an SSRF relay to arbitrary internal/external hosts. Defense in
// depth on top of the loopback bind + Host guard.
func loopbackEndpoint(raw string) error {
	u, err := url.Parse(raw)
	if err != nil {
		return fmt.Errorf("invalid endpoint url: %w", err)
	}
	if u.Scheme != "http" && u.Scheme != "https" {
		return fmt.Errorf("endpoint scheme %q must be http or https", u.Scheme)
	}
	host := u.Hostname()
	if host == "" {
		return fmt.Errorf("endpoint has no host")
	}
	if strings.EqualFold(host, "localhost") {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil {
		// A hostname that isn't "localhost" could resolve anywhere — reject so the
		// loopback guarantee is static (no DNS-time surprise).
		return fmt.Errorf("endpoint host %q must be a loopback IP or \"localhost\"", host)
	}
	if !ip.IsLoopback() {
		return fmt.Errorf("endpoint %q is not loopback (local mesh is same-host only)", host)
	}
	return nil
}
