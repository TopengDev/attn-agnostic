package agent

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
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
// business logic. They propagate store errors so the HTTP layer can return 5xx
// instead of masking a DB failure as an empty-but-200 result (audit H3/H4).

// ContactsView returns the raw contact list (for GET /peers).
func (a *Agent) ContactsView() ([]store.Contact, error) {
	return a.st.GetContacts()
}

// HistoryView returns raw messages with a peer/group (for GET /history).
func (a *Agent) HistoryView(with string, limit int) ([]store.Message, error) {
	return a.st.GetHistory(with, limit)
}

// RelayReady reports whether the relay session is connected + authenticated.
func (a *Agent) RelayReady() bool { return a.sess.IsReady() }

// ── inbound surfacing (called from the policy pipeline in agent.go) ────────

// surfaceInboundDM surfaces a DM, detecting an inbound file reference and
// downloading it asynchronously so the relay read loop is never blocked. The
// number of concurrent downloads is bounded (audit M-flood) so a contact cannot
// OOM the daemon by flooding file references; on saturation the file-ref is
// surfaced as a plain message instead of spawning an unbounded goroutine.
func (a *Agent) surfaceInboundDM(ev relay.InboundEvent) {
	if fr, ok := parseFileRef(ev.Plaintext); ok {
		select {
		case a.downloadSem <- struct{}{}:
			go func() {
				defer func() { <-a.downloadSem }()
				a.surfaceInboundFile(ev, fr)
			}()
		default:
			a.log.Printf("[inbound] file from %s: download pool saturated — surfacing as message", ev.From)
			a.surface(SurfaceEvent{
				Type: "message", From: ev.From, FromName: ev.FromName,
				Message: ev.Plaintext, MessageID: ev.ID, Ts: ev.Ts, DeliveryMode: deliverSteer,
			})
		}
		return
	}
	a.surface(SurfaceEvent{
		Type: "message", From: ev.From, FromName: ev.FromName,
		Message: ev.Plaintext, MessageID: ev.ID, Ts: ev.Ts, DeliveryMode: deliverSteer,
	})
}

func (a *Agent) surfaceInboundFile(ev relay.InboundEvent, fr *fileRef) {
	// Tie the download to the agent's root lifecycle so it cancels on shutdown
	// (audit M-ctx) rather than surviving SIGTERM via a detached Background ctx.
	parent := a.downloadCtx()
	ctx, cancel := context.WithTimeout(parent, 30*time.Second)
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

func (a *Agent) downloadCtx() context.Context {
	a.mu.Lock()
	ctx := a.rootCtx
	a.mu.Unlock()
	if ctx == nil {
		return context.Background()
	}
	return ctx
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
// key, and saves it under the inbox dir. The destination is computed + validated
// BEFORE any network I/O so a malicious filename fails fast.
func (a *Agent) downloadInboundFile(ctx context.Context, fr *fileRef) (string, int64, error) {
	dst, err := a.inboxDest(fr)
	if err != nil {
		return "", 0, err
	}
	blob, err := a.fetchFileBlob(ctx, fr.URL)
	if err != nil {
		return "", 0, err
	}
	plain, err := icrypto.DecryptBinary(a.cfg.ID.PrivateKeyBytes(), blob)
	if err != nil {
		return "", 0, fmt.Errorf("decrypt file: %w", err)
	}
	saved, err := writeNoClobber(dst, plain)
	if err != nil {
		return "", 0, err
	}
	return saved, int64(len(plain)), nil
}

// inboxDest computes the sanitized destination path for an inbound file and
// asserts it stays inside the inbox dir (audit H1 — path traversal). Both the
// Filename and the Key fallback are run through filepath.Base; the result must
// be a single, non-dotted path element; and the joined path's parent must be
// exactly the inbox dir.
func (a *Agent) inboxDest(fr *fileRef) (string, error) {
	name := filepath.Base(fr.Filename)
	if unsafeFileName(name) {
		name = filepath.Base(fr.Key)
	}
	if unsafeFileName(name) {
		return "", fmt.Errorf("inbound file: no safe filename in ref")
	}
	inboxAbs, err := filepath.Abs(a.cfg.InboxDir)
	if err != nil {
		return "", err
	}
	dst := filepath.Join(inboxAbs, name)
	dstAbs, err := filepath.Abs(dst)
	if err != nil {
		return "", err
	}
	// Containment: the file must land DIRECTLY in the inbox (parent == inbox),
	// and the path must be prefixed by inbox + separator.
	if filepath.Dir(dstAbs) != inboxAbs || !strings.HasPrefix(dstAbs, inboxAbs+string(os.PathSeparator)) {
		return "", fmt.Errorf("inbound file: path %q escapes inbox %q", dstAbs, inboxAbs)
	}
	return dstAbs, nil
}

func unsafeFileName(name string) bool {
	switch name {
	case "", ".", "..", "/":
		return true
	}
	// Any path separator (including the OS separator, e.g. `\` on Windows) means
	// this isn't a single inbox-local filename.
	return strings.ContainsRune(name, '/') || strings.ContainsRune(name, os.PathSeparator)
}

// writeNoClobber writes data to dst (or dst-1, dst-2, … on collision) using
// O_CREATE|O_EXCL so it never overwrites an existing file and never follows a
// symlink at the target (audit H1). Returns the path actually written.
func writeNoClobber(dst string, data []byte) (string, error) {
	ext := filepath.Ext(dst)
	base := strings.TrimSuffix(dst, ext)
	for i := 0; i < 1000; i++ {
		p := dst
		if i > 0 {
			p = fmt.Sprintf("%s-%d%s", base, i, ext)
		}
		f, err := os.OpenFile(p, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			if errors.Is(err, os.ErrExist) {
				continue
			}
			return "", fmt.Errorf("save file: %w", err)
		}
		_, werr := f.Write(data)
		cerr := f.Close()
		if werr != nil {
			return "", fmt.Errorf("save file: %w", werr)
		}
		if cerr != nil {
			return "", fmt.Errorf("save file: %w", cerr)
		}
		return p, nil
	}
	return "", fmt.Errorf("inbound file: too many name collisions for %q", dst)
}

const maxInboundFile = 12 * 1024 * 1024 // ≤10 MiB payload + ECIES overhead headroom

// fetchFileBlob retrieves the encrypted blob. Absolute URLs are constrained to
// the configured relay host (audit H2 — SSRF): a malicious contact authors the
// fileRef, so an unrestricted absolute fetch would reach cloud IMDS
// (169.254.169.254), the daemon's own control plane, or internal services.
func (a *Agent) fetchFileBlob(ctx context.Context, rawURL string) ([]byte, error) {
	read := func(resp *http.Response) ([]byte, error) {
		defer resp.Body.Close()
		if resp.StatusCode/100 != 2 {
			return nil, fmt.Errorf("download status %d", resp.StatusCode)
		}
		// Read one byte past the cap so an over-cap body is rejected, not silently
		// truncated-and-saved.
		data, err := io.ReadAll(io.LimitReader(resp.Body, maxInboundFile+1))
		if err != nil {
			return nil, err
		}
		if len(data) > maxInboundFile {
			return nil, fmt.Errorf("inbound file exceeds %d-byte cap", maxInboundFile)
		}
		return data, nil
	}

	if strings.HasPrefix(rawURL, "http://") || strings.HasPrefix(rawURL, "https://") {
		if err := a.validateAbsoluteFileURL(rawURL); err != nil {
			return nil, err
		}
		req, err := http.NewRequestWithContext(ctx, http.MethodGet, rawURL, nil)
		if err != nil {
			return nil, err
		}
		// Bounded client (not http.DefaultClient).
		client := &http.Client{Timeout: 30 * time.Second}
		resp, err := client.Do(req)
		if err != nil {
			return nil, err
		}
		return read(resp)
	}

	// Relative path → must be a clean relay-scoped path so the session's
	// httpBase+path concatenation can only ever hit the relay (no `//host`,
	// no scheme injection).
	if !strings.HasPrefix(rawURL, "/") || strings.HasPrefix(rawURL, "//") {
		return nil, fmt.Errorf("inbound file: unsupported url %q", rawURL)
	}
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

// validateAbsoluteFileURL enforces that an absolute inbound file URL points at
// the configured relay (scheme+host+port) and does not resolve to a never-legit
// internal target (IMDS/link-local/loopback/unspecified — DNS-rebinding defense).
func (a *Agent) validateAbsoluteFileURL(rawURL string) error {
	if err := relayURLAllowed(rawURL, a.sess.HTTPBase()); err != nil {
		return err
	}
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("inbound file: bad url: %w", err)
	}
	ips, err := net.LookupIP(u.Hostname())
	if err != nil {
		return fmt.Errorf("inbound file: cannot resolve %q: %w", u.Hostname(), err)
	}
	for _, ip := range ips {
		if blockedFetchIP(ip) {
			return fmt.Errorf("inbound file: %q resolves to blocked address %s", u.Hostname(), ip)
		}
	}
	return nil
}

// relayURLAllowed (pure) accepts rawURL only if its scheme + host:port match the
// relay's HTTP base. Host-pinning makes SSRF to anything but the relay impossible
// regardless of how the attacker crafts the URL.
func relayURLAllowed(rawURL, relayBase string) error {
	u, err := url.Parse(rawURL)
	if err != nil {
		return fmt.Errorf("inbound file: bad url: %w", err)
	}
	rb, err := url.Parse(relayBase)
	if err != nil || rb.Host == "" {
		return fmt.Errorf("inbound file: relay base unknown (%q)", relayBase)
	}
	if !strings.EqualFold(u.Scheme, rb.Scheme) {
		return fmt.Errorf("inbound file: url scheme %q != relay scheme %q", u.Scheme, rb.Scheme)
	}
	if !strings.EqualFold(u.Host, rb.Host) {
		return fmt.Errorf("inbound file: url host %q is not the relay %q", u.Host, rb.Host)
	}
	return nil
}

// blockedFetchIP (pure) blocks addresses that are never a legitimate relay:
// loopback, link-local (incl. 169.254.169.254 IMDS), and the unspecified addr.
// RFC-1918/ULA are intentionally NOT blocked here — the host-pin already
// constrains the target to the operator-configured relay, which may legitimately
// be a LAN host in a dev setup.
func blockedFetchIP(ip net.IP) bool {
	return ip.IsLoopback() ||
		ip.IsLinkLocalUnicast() ||
		ip.IsLinkLocalMulticast() ||
		ip.IsUnspecified()
}
