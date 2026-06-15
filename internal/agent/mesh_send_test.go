package agent

import (
	"context"
	"strings"
	"sync"
	"testing"

	"github.com/TopengDev/attn-agnostic/internal/mesh"
)

type captureDeliverer struct {
	mu     sync.Mutex
	frames []mesh.Frame
}

func (c *captureDeliverer) Deliver(f mesh.Frame) error {
	c.mu.Lock()
	c.frames = append(c.frames, f)
	c.mu.Unlock()
	return nil
}

func (c *captureDeliverer) last() (mesh.Frame, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if len(c.frames) == 0 {
		return mesh.Frame{}, false
	}
	return c.frames[len(c.frames)-1], true
}

func (c *captureDeliverer) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.frames)
}

// bob's relay identity — a syntactically valid lowercase address.
const bobAddr = "0x00000000000000000000000000000000000000b0"

// TestSendLocalPrecedenceBypassesRelay is the CP4 proof: a send to a recipient
// that is BOTH a local session AND has a relay identity routes LOCAL (relay
// bypassed) — proven by (a) the local deliverer receiving the frame, (b) Send
// reporting local:true, and (c) NO outbox entry (the relay-offline path would
// have queued or errored). The relay is intentionally not ready (invalid URL),
// so a local send that SUCCEEDS could only have bypassed the relay.
func TestSendLocalPrecedenceBypassesRelay(t *testing.T) {
	a := newDLAgent(t, "wss://relay.invalid/ws")
	reg := mesh.New()
	a.SetMesh(reg, "main")
	cap := &captureDeliverer{}
	reg.Register(&mesh.Entry{Name: "bob", Address: bobAddr, Transport: mesh.TransportWS}, cap)

	ctx := context.Background()

	// (1) Send by NAME → local.
	res, err := a.Send(ctx, "bob", "hello-local")
	if err != nil {
		t.Fatalf("Send(bob) errored (should route local): %v", err)
	}
	if res.Data["local"] != true {
		t.Errorf("Send(bob) data.local = %v, want true", res.Data["local"])
	}
	f, ok := cap.last()
	if !ok || f.Text != "hello-local" {
		t.Fatalf("local deliverer did not get hello-local (frames=%d)", cap.count())
	}
	if f.From != "main" {
		t.Errorf("local frame From = %q, want main (selfName)", f.From)
	}

	// (2) Send by ADDRESS that matches a local session → local (CC address-match).
	if _, err := a.Send(ctx, bobAddr, "hello-by-addr"); err != nil {
		t.Fatalf("Send(bobAddr) errored (should route local by address): %v", err)
	}
	if f, _ := cap.last(); f.Text != "hello-by-addr" {
		t.Errorf("address-match local frame = %q, want hello-by-addr", f.Text)
	}

	// (3) NO relay frame was emitted: the outbox is empty (relay path would queue).
	outbox, err := a.st.GetOutbox()
	if err != nil {
		t.Fatalf("GetOutbox: %v", err)
	}
	if len(outbox) != 0 {
		t.Errorf("outbox has %d entries — a relay send leaked (must be 0, fully bypassed)", len(outbox))
	}

	// (4) Explicit ".attn" suffix forces RELAY resolution (never local), even
	// though a like-named local session exists — mirrors CC handleSend precedence.
	if _, err := a.Send(ctx, "bob.attn", "via-relay"); err == nil {
		t.Error("Send(bob.attn) succeeded, want relay-resolution failure (no relay) — .attn must bypass local")
	}
	if got := cap.count(); got != 2 {
		t.Errorf("local deliverer got %d frames, want 2 (bob.attn must NOT route local)", got)
	}

	// (5) A non-local valid address goes to the RELAY path (and errors, since the
	// relay is offline + no cached key) — proving only local-registry matches are
	// bypassed.
	_, err = a.Send(ctx, "0x000000000000000000000000000000000000dead", "to-relay")
	if err == nil {
		t.Error("Send(non-local addr) succeeded, want relay-offline error")
	}
	if !strings.Contains(err.Error(), "relay offline") {
		t.Errorf("non-local send error = %v, want a relay-offline error", err)
	}
}

// TestSendAllBroadcast proves send("all") fans out over the registry minus the
// daemon's own session name.
func TestSendAllBroadcast(t *testing.T) {
	a := newDLAgent(t, "wss://relay.invalid/ws")
	reg := mesh.New()
	a.SetMesh(reg, "main")
	b, c := &captureDeliverer{}, &captureDeliverer{}
	reg.Register(&mesh.Entry{Name: "w1", Transport: mesh.TransportWS}, b)
	reg.Register(&mesh.Entry{Name: "w2", Transport: mesh.TransportWS}, c)
	// A same-named "main" session must be EXCLUDED from a self-broadcast.
	self := &captureDeliverer{}
	reg.Register(&mesh.Entry{Name: "main", Transport: mesh.TransportWS}, self)

	res, err := a.Send(context.Background(), "all", "broadcast")
	if err != nil {
		t.Fatalf("Send(all): %v", err)
	}
	if res.Data["recipients"].(int) != 2 {
		t.Errorf("recipients = %v, want 2 (w1+w2, self excluded)", res.Data["recipients"])
	}
	if b.count() != 1 || c.count() != 1 {
		t.Errorf("w1/w2 got %d/%d frames, want 1/1", b.count(), c.count())
	}
	if self.count() != 0 {
		t.Errorf(`"main" got %d frames, want 0 (sender excluded from own broadcast)`, self.count())
	}
}

// TestSendAllNoPeers errors cleanly when no local peers are registered.
func TestSendAllNoPeers(t *testing.T) {
	a := newDLAgent(t, "wss://relay.invalid/ws")
	a.SetMesh(mesh.New(), "main")
	if _, err := a.Send(context.Background(), "all", "x"); err == nil {
		t.Error("Send(all) with no peers succeeded, want error")
	}
}

// TestPeersOpEnumerates lists registered sessions through the agent op.
func TestPeersOpEnumerates(t *testing.T) {
	a := newDLAgent(t, "wss://relay.invalid/ws")
	reg := mesh.New()
	a.SetMesh(reg, "main")
	reg.Register(&mesh.Entry{Name: "alpha", Harness: "pi", Transport: mesh.TransportWS}, &captureDeliverer{})

	res, err := a.Peers()
	if err != nil {
		t.Fatalf("Peers: %v", err)
	}
	if res.Data["count"].(int) != 1 {
		t.Errorf("peers count = %v, want 1", res.Data["count"])
	}
	if !strings.Contains(res.Text, "alpha") {
		t.Errorf("peers text missing alpha: %q", res.Text)
	}
}

// TestNoMeshControlOnly: with no mesh wired (control-only mode), local routing is
// disabled — "all" errors clearly and a bare name falls through to relay.
func TestNoMeshControlOnly(t *testing.T) {
	a := newDLAgent(t, "wss://relay.invalid/ws")
	// no SetMesh
	if _, err := a.Send(context.Background(), "all", "x"); err == nil || !strings.Contains(err.Error(), "control-only") {
		t.Errorf("Send(all) without mesh = %v, want control-only error", err)
	}
	res, err := a.Peers()
	if err != nil {
		t.Fatalf("Peers without mesh: %v", err)
	}
	if res.Data["count"].(int) != 0 {
		t.Errorf("peers count without mesh = %v, want 0", res.Data["count"])
	}
}
