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

// bobAddr is a syntactically valid lowercase address a (semi-trusted) local
// session might try to CLAIM in order to intercept relay-bound sends.
const bobAddr = "0x00000000000000000000000000000000000000b0"

// TestSendLocalPrecedenceBypassesRelay proves Layer-A routing is NAME-only and
// relay-bypassed: a send to a local session by NAME routes local (proven by
// (a) the local deliverer receiving the frame, (b) Send reporting local:true,
// (c) NO outbox entry — the relay-offline path would have queued or errored).
// The relay is intentionally not ready (invalid URL), so a local send that
// SUCCEEDS could only have bypassed the relay.
func TestSendLocalPrecedenceBypassesRelay(t *testing.T) {
	a := newDLAgent(t, "wss://relay.invalid/ws")
	reg := mesh.New()
	a.SetMesh(reg, "main")
	cap := &captureDeliverer{}
	reg.Register(&mesh.Entry{Name: "bob", Transport: mesh.TransportWS}, cap)

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

	// (2) NO relay frame was emitted by the name send: outbox is empty.
	outbox, err := a.st.GetOutbox()
	if err != nil {
		t.Fatalf("GetOutbox: %v", err)
	}
	if len(outbox) != 0 {
		t.Errorf("outbox has %d entries — a relay send leaked (must be 0, fully bypassed)", len(outbox))
	}

	// (3) Explicit ".attn" suffix forces RELAY resolution (never local), even
	// though a like-named local session exists.
	if _, err := a.Send(ctx, "bob.attn", "via-relay"); err == nil {
		t.Error("Send(bob.attn) succeeded, want relay-resolution failure (no relay) — .attn must bypass local")
	}
	if got := cap.count(); got != 1 {
		t.Errorf("local deliverer got %d frames, want 1 (bob.attn must NOT route local)", got)
	}

	// (4) A valid 0x address goes to the RELAY path (and errors, since the relay
	// is offline + no cached key) — local routing is NAME-only.
	_, err = a.Send(ctx, "0x000000000000000000000000000000000000dead", "to-relay")
	if err == nil {
		t.Error("Send(non-local addr) succeeded, want relay-offline error")
	}
	if !strings.Contains(err.Error(), "relay offline") {
		t.Errorf("non-local send error = %v, want a relay-offline error", err)
	}
}

// TestSendAddressShadowingDoesNotInterceptRelay is the M3-audit-M2 regression:
// a local session must NOT be able to intercept a 0x-addressed (relay-bound)
// send by claiming that address. Two structural guarantees are asserted:
//
//  1. mesh.Entry no longer carries a self-asserted address at all — the registry
//     is name-keyed only, so there is nothing to match a 0x target against.
//  2. agent.Send(<0x address>) ALWAYS takes the relay path even when a local
//     session named exactly like that address string is registered — the local
//     deliverer receives NOTHING, and the send fails relay-offline (no plaintext
//     leaked to a local session).
func TestSendAddressShadowingDoesNotInterceptRelay(t *testing.T) {
	a := newDLAgent(t, "wss://relay.invalid/ws")
	reg := mesh.New()
	a.SetMesh(reg, "main")

	// A malicious/buggy local session registers under a NAME that is literally a
	// real remote peer's 0x address, the closest it can now get to "claiming" it.
	shadow := &captureDeliverer{}
	reg.Register(&mesh.Entry{Name: bobAddr, Transport: mesh.TransportWS}, shadow)

	// Sending to that 0x address must hit the relay (offline → error), NOT the
	// local shadow deliverer — a valid address is never name-resolved locally.
	_, err := a.Send(context.Background(), bobAddr, "plaintext-for-remote-bob")
	if err == nil {
		t.Fatal("Send(0xbob) succeeded, want relay-offline error (must not route to a local shadow)")
	}
	if !strings.Contains(err.Error(), "relay offline") {
		t.Errorf("Send(0xbob) error = %v, want relay-offline (relay path, not local)", err)
	}
	if shadow.count() != 0 {
		t.Errorf("address-shadow local session intercepted %d frame(s) addressed to a remote 0x peer — MUST be 0", shadow.count())
	}

	// And the outbox stays empty (no cached key → cannot even queue), proving the
	// plaintext was never handed to the local mesh.
	if outbox, _ := a.st.GetOutbox(); len(outbox) != 0 {
		t.Errorf("outbox has %d entries, want 0", len(outbox))
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
