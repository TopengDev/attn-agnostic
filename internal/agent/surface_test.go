package agent

import (
	"context"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/TopengDev/attn-agnostic/internal/config"
	"github.com/TopengDev/attn-agnostic/internal/identity"
	"github.com/TopengDev/attn-agnostic/internal/store"
)

func newDLAgent(t *testing.T, relayURL string) *Agent {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatalf("identity: %v", err)
	}
	st, err := store.Open(filepath.Join(t.TempDir(), "t.db"))
	if err != nil {
		t.Fatalf("store: %v", err)
	}
	t.Cleanup(func() { st.Close() })
	cfg := &config.Config{
		ID:       id,
		RelayURL: relayURL,
		BaseRPC:  "https://base.invalid",
		InboxDir: t.TempDir(),
	}
	return New(cfg, st, log.New(io.Discard, "", 0))
}

// TestInboxDestContainment is the H1 path-traversal regression test. It must FAIL
// on the pre-fix code (which used the raw fr.Key fallback) and PASS after.
func TestInboxDestContainment(t *testing.T) {
	a := newDLAgent(t, "wss://relay.example/ws")
	inbox, _ := filepath.Abs(a.cfg.InboxDir)

	type tc struct {
		name        string
		fr          *fileRef
		wantErr     bool
		wantContain bool // result must sit directly inside the inbox
	}
	cases := []tc{
		{"normal", &fileRef{Filename: "ok.txt"}, false, true},
		// The historic exploit: empty filename → raw Key with traversal.
		{"empty-filename-traversal-key", &fileRef{Filename: "", Key: "../../../../tmp/pwned"}, false, true},
		{"empty-filename-abs-key", &fileRef{Filename: "", Key: "/etc/passwd"}, false, true},
		{"filename-traversal", &fileRef{Filename: "../../../../etc/cron.d/x"}, false, true},
		{"dotdot-key", &fileRef{Filename: "", Key: ".."}, true, false},
		{"empty-both", &fileRef{Filename: "", Key: ""}, true, false},
		{"slash-name", &fileRef{Filename: "a/b/c"}, false, true}, // Base → "c"
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			dst, err := a.inboxDest(c.fr)
			if c.wantErr {
				if err == nil {
					t.Fatalf("expected error, got dst=%q", dst)
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected error: %v", err)
			}
			// The crux: every accepted destination MUST live directly in the inbox.
			if filepath.Dir(dst) != inbox {
				t.Fatalf("path escapes inbox: dst=%q parent=%q inbox=%q", dst, filepath.Dir(dst), inbox)
			}
			if !strings.HasPrefix(dst, inbox+string(os.PathSeparator)) {
				t.Fatalf("path not inbox-prefixed: %q", dst)
			}
		})
	}
}

// TestWriteNoClobber proves O_EXCL no-clobber behavior (H1): a second write to the
// same name does not overwrite the first; it lands at a suffixed path.
func TestWriteNoClobber(t *testing.T) {
	dir := t.TempDir()
	dst := filepath.Join(dir, "f.txt")
	p1, err := writeNoClobber(dst, []byte("first"))
	if err != nil {
		t.Fatal(err)
	}
	p2, err := writeNoClobber(dst, []byte("second"))
	if err != nil {
		t.Fatal(err)
	}
	if p1 == p2 {
		t.Fatalf("clobber: both writes went to %q", p1)
	}
	if b, _ := os.ReadFile(p1); string(b) != "first" {
		t.Fatalf("original file was overwritten: %q", b)
	}
}

// TestRelayURLAllowed pins the SSRF host-allow logic (H2).
func TestRelayURLAllowed(t *testing.T) {
	const relay = "https://relay.example"
	ok := []string{
		"https://relay.example/files/abc",
		"https://Relay.Example/files/abc", // case-insensitive host+scheme
	}
	bad := []string{
		"http://169.254.169.254/latest/meta-data/", // cloud IMDS
		"http://127.0.0.1:9742/op/send",            // confused-deputy on own control plane
		"https://evil.com/x",                       // foreign host
		"https://relay.example:8443/x",             // port mismatch
		"http://relay.example/x",                   // scheme mismatch (plaintext)
	}
	for _, u := range ok {
		if err := relayURLAllowed(u, relay); err != nil {
			t.Errorf("relayURLAllowed(%q) = %v, want allow", u, err)
		}
	}
	for _, u := range bad {
		if err := relayURLAllowed(u, relay); err == nil {
			t.Errorf("relayURLAllowed(%q) = allow, want reject", u)
		}
	}
}

// TestBlockedFetchIP pins the never-legitimate-target IP block (H2 defense-in-depth).
func TestBlockedFetchIP(t *testing.T) {
	blocked := []string{"169.254.169.254", "127.0.0.1", "::1", "0.0.0.0", "fe80::1"}
	allowed := []string{"1.2.3.4", "8.8.8.8", "192.168.1.10"}
	for _, s := range blocked {
		if !blockedFetchIP(net.ParseIP(s)) {
			t.Errorf("blockedFetchIP(%s) = false, want blocked", s)
		}
	}
	for _, s := range allowed {
		if blockedFetchIP(net.ParseIP(s)) {
			t.Errorf("blockedFetchIP(%s) = true, want allowed", s)
		}
	}
}

// TestFetchFileBlobNoSSRF is the end-to-end SSRF regression test: a fileRef whose
// URL points at an internal/foreign server must be rejected WITHOUT the server
// ever being contacted. On the pre-fix code (no validation) the server is hit.
func TestFetchFileBlobNoSSRF(t *testing.T) {
	hit := false
	victim := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		hit = true
		w.Write([]byte("secret-internal-data"))
	}))
	defer victim.Close()

	// Agent's relay is some OTHER host, so the victim URL is not the relay.
	a := newDLAgent(t, "wss://relay.example/ws")
	_, err := a.fetchFileBlob(context.Background(), victim.URL+"/latest/meta-data/")
	if err == nil {
		t.Fatalf("fetchFileBlob accepted a non-relay absolute URL")
	}
	if hit {
		t.Fatalf("SSRF: the internal server was contacted (err=%v)", err)
	}
	if !strings.Contains(err.Error(), "relay") {
		t.Errorf("expected a relay-host rejection, got: %v", err)
	}
}
