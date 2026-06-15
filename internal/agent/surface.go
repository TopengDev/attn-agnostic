package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	icrypto "github.com/TopengDev/attn-agnostic/internal/crypto"
	"github.com/TopengDev/attn-agnostic/internal/relay"
	"github.com/TopengDev/attn-agnostic/internal/store"
)

// SurfaceEvent is a "should be shown to the operator now" inbound notification.
// It is the daemon→adapter contract: the agent emits one whenever an inbound
// frame passes policy (block/mute/contact) and should reach a live harness. The
// HTTP/WS layer maps it onto pi-setup's `127.0.0.1:9742` WS frame shape so M3's
// pi adapter is drop-in. The agent owns WHAT to surface; adapters own HOW to
// inject (steer vs follow-up), guided by DeliveryMode.
type SurfaceEvent struct {
	Type         string // "message" | "file"
	From         string // sender address (lowercase)
	FromName     string // relay-provided .attn name, if any (→ frame agentName)
	Message      string // text (or emoji for reactions)
	MessageID    string
	Ts           int64
	Trust        string // "" (normal) | "pending" | "reaction"
	GroupID      string
	GroupName    string
	Local        bool   // local-mesh peer (M3); always false in M2
	DeliveryMode string // "steer" | "followUp" — adapter injection-mode hint
	ReactionFor  string // message_id a reaction targets
	// file fields (Type=="file")
	Filename string
	Path     string // local inbox path after download+decrypt
	Size     int64
}

// Delivery-mode hints (04-architecture.md: steer = inject into a possibly-busy
// live turn; followUp = wait for idle). DMs/reactions are addressed to you →
// steer; group + pending are lower-priority → followUp.
const (
	deliverSteer    = "steer"
	deliverFollowUp = "followUp"
)

// OnSurface registers the inbound surface sink (the WS hub's broadcast). Safe to
// call before Start. A nil sink is a no-op (e.g. headless control-only runs).
func (a *Agent) OnSurface(fn func(SurfaceEvent)) {
	a.mu.Lock()
	a.surfaceSink = fn
	a.mu.Unlock()
}

// surface emits an event to the registered sink, if any. Never blocks the caller
// beyond the sink's own contract (the hub uses per-client buffered channels).
func (a *Agent) surface(ev SurfaceEvent) {
	a.mu.Lock()
	fn := a.surfaceSink
	a.mu.Unlock()
	if fn != nil {
		fn(ev)
	}
}

// ── structured read accessors (for the pi-compat REST endpoints) ───────────
// These are read-only data-access shims so the HTTP layer can emit the exact
// pi-setup JSON shapes (/status, /peers, /history) without duplicating any
// business logic. The 29-op surface still flows exclusively through Dispatch.

// ContactsView returns the raw contact list (for GET /peers).
func (a *Agent) ContactsView() []store.Contact {
	c, _ := a.st.GetContacts()
	return c
}

// HistoryView returns raw messages with a peer/group (for GET /history).
func (a *Agent) HistoryView(with string, limit int) []store.Message {
	m, _ := a.st.GetHistory(with, limit)
	return m
}

// RelayReady reports whether the relay session is connected + authenticated.
func (a *Agent) RelayReady() bool { return a.sess.IsReady() }

// ── inbound surfacing (called from the policy pipeline in agent.go) ────────

// surfaceInboundDM surfaces a DM, detecting an inbound file reference and
// downloading it asynchronously so the relay read loop is never blocked.
func (a *Agent) surfaceInboundDM(ev relay.InboundEvent) {
	if fr, ok := parseFileRef(ev.Plaintext); ok {
		go a.surfaceInboundFile(ev, fr)
		return
	}
	a.surface(SurfaceEvent{
		Type: "message", From: ev.From, FromName: ev.FromName,
		Message: ev.Plaintext, MessageID: ev.ID, Ts: ev.Ts, DeliveryMode: deliverSteer,
	})
}

func (a *Agent) surfaceInboundFile(ev relay.InboundEvent, fr *fileRef) {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	path, size, err := a.downloadInboundFile(ctx, fr)
	if err != nil {
		// Don't lose the message — surface the raw file-ref as a normal message.
		a.log.Printf("[inbound] file from %s: download failed (%v) — surfacing as message", ev.From, err)
		a.surface(SurfaceEvent{
			Type: "message", From: ev.From, FromName: ev.FromName,
			Message: ev.Plaintext, MessageID: ev.ID, Ts: ev.Ts, DeliveryMode: deliverSteer,
		})
		return
	}
	a.log.Printf("[inbound] file from %s saved to %s (%d bytes)", ev.From, path, size)
	a.surface(SurfaceEvent{
		Type: "file", From: ev.From, FromName: ev.FromName,
		Filename: fr.Filename, Path: path, Size: size,
		MessageID: ev.ID, Ts: ev.Ts, DeliveryMode: deliverSteer,
	})
}

// fileRef is the JSON payload SendFile transmits as a message body.
type fileRef struct {
	Type     string `json:"type"`
	URL      string `json:"url"`
	Key      string `json:"key"`
	Filename string `json:"filename"`
	Size     int64  `json:"size"`
	Mime     string `json:"mime"`
}

func parseFileRef(plaintext string) (*fileRef, bool) {
	s := strings.TrimSpace(plaintext)
	if !strings.HasPrefix(s, "{") {
		return nil, false
	}
	var fr fileRef
	if json.Unmarshal([]byte(s), &fr) != nil {
		return nil, false
	}
	if fr.Type != "file" || fr.URL == "" {
		return nil, false
	}
	return &fr, true
}

// downloadInboundFile fetches the encrypted blob, decrypts it with our private
// key, and saves it under the inbox dir. The blob is ECIES-encrypted to us, so a
// plain GET is safe (the relay can serve it openly); we fall back to a signed
// GET if the relay rejects the unsigned request.
func (a *Agent) downloadInboundFile(ctx context.Context, fr *fileRef) (string, int64, error) {
	blob, err := a.fetchFileBlob(ctx, fr.URL)
	if err != nil {
		return "", 0, err
	}
	plain, err := icrypto.DecryptBinary(a.cfg.ID.PrivateKeyBytes(), blob)
	if err != nil {
		return "", 0, fmt.Errorf("decrypt file: %w", err)
	}
	name := filepath.Base(fr.Filename)
	if name == "" || name == "." || name == "/" {
		name = fr.Key
	}
	dst := filepath.Join(a.cfg.InboxDir, name)
	if err := os.WriteFile(dst, plain, 0o600); err != nil {
		return "", 0, fmt.Errorf("save file: %w", err)
	}
	return dst, int64(len(plain)), nil
}

const maxInboundFile = 12 * 1024 * 1024 // 10MB payload + ECIES overhead headroom

func (a *Agent) fetchFileBlob(ctx context.Context, rawURL string) ([]byte, error) {
	read := func(resp *http.Response) ([]byte, error) {
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("download status %d", resp.StatusCode)
		}
		return io.ReadAll(io.LimitReader(resp.Body, maxInboundFile))
	}
	// Absolute URL → direct GET.
	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		return read(resp)
	}
	// Relay-relative path → unsigned GET, then signed GET on rejection.
	if resp, err := a.sess.PlainGet(ctx, rawURL); err == nil {
		if resp.StatusCode/100 == 2 {
			return read(resp)
		}
		resp.Body.Close()
	}
	resp, err := a.sess.SignedRequest(ctx, http.MethodGet, rawURL, nil, nil)
	if err != nil {
		return nil, err
	}
	return read(resp)
}
