package mesh

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
)

// capturing deliverer records every frame it receives (concurrency-safe).
type capture struct {
	mu     sync.Mutex
	frames []Frame
	fail   bool
}

func (c *capture) Deliver(f Frame) error {
	if c.fail {
		return fmt.Errorf("deliver failed")
	}
	c.mu.Lock()
	c.frames = append(c.frames, f)
	c.mu.Unlock()
	return nil
}

func (c *capture) texts() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, len(c.frames))
	for i, f := range c.frames {
		out[i] = f.Text
	}
	return out
}

func (c *capture) count() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.frames)
}

func TestRegisterLookupDeregister(t *testing.T) {
	r := New()
	cap := &capture{}
	rel := r.Register(&Entry{Name: "bob", Harness: "pi", Transport: TransportWS}, cap)

	e, ok := r.Lookup("bob")
	if !ok {
		t.Fatal("Lookup(bob) not found after Register")
	}
	if e.Harness != "pi" || e.Transport != TransportWS {
		t.Errorf("entry metadata wrong: %+v", e)
	}
	if e.RegisteredAt.IsZero() {
		t.Error("RegisteredAt not stamped")
	}
	// Resolve is NAME-only (M3 audit M2): a bare name hits, a 0x address does NOT
	// (it must take the relay path, never shadow it via the local registry).
	if _, ok := r.Resolve("bob"); !ok {
		t.Error("Resolve(bob) not found (name routing must work)")
	}
	if _, ok := r.Resolve("0xBOB"); ok {
		t.Error("Resolve(0xBOB) matched — the mesh must NOT route by self-asserted address")
	}
	if r.Count() != 1 {
		t.Errorf("Count = %d, want 1", r.Count())
	}

	rel()
	if _, ok := r.Lookup("bob"); ok {
		t.Error("Lookup(bob) still found after release")
	}
	if r.Count() != 0 {
		t.Errorf("Count = %d after release, want 0", r.Count())
	}
}

// TestLastRegistrationWins + stale-release-is-noop: a fresh registration replaces
// an old one, and the OLD registration's release must NOT evict the fresh entry.
func TestLastRegistrationWinsStaleReleaseNoop(t *testing.T) {
	r := New()
	old := &capture{}
	fresh := &capture{}

	relOld := r.Register(&Entry{Name: "w1", Transport: TransportWS}, old)
	relFresh := r.Register(&Entry{Name: "w1", Transport: TransportWS}, fresh)

	// Route should hit the fresh deliverer.
	if _, err := r.Route("w1", Frame{Text: "to-fresh"}); err != nil {
		t.Fatalf("Route: %v", err)
	}
	if old.count() != 0 {
		t.Errorf("old deliverer got %d frames, want 0", old.count())
	}
	if got := fresh.texts(); len(got) != 1 || got[0] != "to-fresh" {
		t.Errorf("fresh deliverer texts = %v, want [to-fresh]", got)
	}

	// The STALE release (old) must be a no-op — entry stays.
	relOld()
	if _, ok := r.Lookup("w1"); !ok {
		t.Fatal("stale release evicted the fresh entry — race-unsafe")
	}

	// The fresh release removes it.
	relFresh()
	if _, ok := r.Lookup("w1"); ok {
		t.Error("fresh release did not remove entry")
	}
}

// TestRouteCorrectTargetingNoLeak is the registry-level no-leak proof: Route(B)
// reaches ONLY B; A and C get nothing.
func TestRouteCorrectTargetingNoLeak(t *testing.T) {
	r := New()
	a, b, c := &capture{}, &capture{}, &capture{}
	r.Register(&Entry{Name: "A", Transport: TransportWS}, a)
	r.Register(&Entry{Name: "B", Transport: TransportWS}, b)
	r.Register(&Entry{Name: "C", Transport: TransportWS}, c)

	if _, err := r.Route("B", Frame{ID: "m1", From: "A", Text: "hi-B"}); err != nil {
		t.Fatalf("Route(B): %v", err)
	}
	if got := b.texts(); len(got) != 1 || got[0] != "hi-B" {
		t.Errorf("B texts = %v, want [hi-B]", got)
	}
	if a.count() != 0 {
		t.Errorf("A leaked %d frames, want 0", a.count())
	}
	if c.count() != 0 {
		t.Errorf("C leaked %d frames (NO-LEAK violation), want 0", c.count())
	}

	// Unknown target → ErrNoSession, nothing delivered.
	if _, err := r.Route("Z", Frame{Text: "x"}); err != ErrNoSession {
		t.Errorf("Route(Z) err = %v, want ErrNoSession", err)
	}
}

// TestBroadcastExcludesSender: send("all") from A reaches B+C, never A.
func TestBroadcastExcludesSender(t *testing.T) {
	r := New()
	a, b, c := &capture{}, &capture{}, &capture{}
	r.Register(&Entry{Name: "A", Transport: TransportWS}, a)
	r.Register(&Entry{Name: "B", Transport: TransportWS}, b)
	r.Register(&Entry{Name: "C", Transport: TransportWS}, c)

	sent := r.Broadcast(Frame{From: "A", Text: "all-msg", Broadcast: true}, "A")
	if len(sent) != 2 || sent[0] != "B" || sent[1] != "C" {
		t.Errorf("Broadcast sent = %v, want [B C]", sent)
	}
	if a.count() != 0 {
		t.Errorf("A received its own broadcast (%d frames), want 0", a.count())
	}
	if b.count() != 1 || c.count() != 1 {
		t.Errorf("B/C counts = %d/%d, want 1/1", b.count(), c.count())
	}
}

// TestBroadcastBestEffort: a failing deliverer is omitted from the sent list but
// does not abort the fan-out to others.
func TestBroadcastBestEffort(t *testing.T) {
	r := New()
	ok1, bad, ok2 := &capture{}, &capture{fail: true}, &capture{}
	r.Register(&Entry{Name: "ok1", Transport: TransportWS}, ok1)
	r.Register(&Entry{Name: "bad", Transport: TransportHTTP}, bad)
	r.Register(&Entry{Name: "ok2", Transport: TransportWS}, ok2)

	sent := r.Broadcast(Frame{Text: "x"}, "")
	if len(sent) != 2 || sent[0] != "ok1" || sent[1] != "ok2" {
		t.Errorf("Broadcast sent = %v, want [ok1 ok2] (bad omitted)", sent)
	}
}

func TestListAndNames(t *testing.T) {
	r := New()
	r.Register(&Entry{Name: "zeta", Harness: "opencode", Transport: TransportHTTP}, &capture{})
	r.Register(&Entry{Name: "alpha", Harness: "pi", Transport: TransportWS}, &capture{})

	names := r.Names()
	if len(names) != 2 || names[0] != "alpha" || names[1] != "zeta" {
		t.Errorf("Names = %v, want sorted [alpha zeta]", names)
	}
	views := r.List()
	if len(views) != 2 || views[0].Name != "alpha" || views[1].Name != "zeta" {
		t.Errorf("List names = %v, want sorted [alpha zeta]", views)
	}
	if views[1].Harness != "opencode" || views[1].Transport != TransportHTTP {
		t.Errorf("zeta view wrong: %+v", views[1])
	}
}

func TestDeregisterByName(t *testing.T) {
	r := New()
	r.Register(&Entry{Name: "x", Transport: TransportHTTP}, &capture{})
	if !r.DeregisterByName("x") {
		t.Error("DeregisterByName(x) = false, want true")
	}
	if r.DeregisterByName("x") {
		t.Error("DeregisterByName(x) twice = true, want false")
	}
}

// TestConcurrentRegistryRace hammers the registry from many goroutines to prove
// -race cleanliness: concurrent register/deregister/route/broadcast/list.
func TestConcurrentRegistryRace(t *testing.T) {
	r := New()
	const workers = 16
	var wg sync.WaitGroup
	var delivered int64

	// A long-lived target everyone routes to.
	sink := &capture{}
	r.Register(&Entry{Name: "sink", Transport: TransportWS}, sink)

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			name := fmt.Sprintf("s%d", n)
			for j := 0; j < 200; j++ {
				rel := r.Register(&Entry{Name: name, Transport: TransportWS}, &capture{})
				if _, err := r.Route("sink", Frame{Text: "ping"}); err == nil {
					atomic.AddInt64(&delivered, 1)
				}
				_ = r.Broadcast(Frame{Text: "b"}, name)
				_ = r.List()
				_ = r.Names()
				_, _ = r.Resolve("nobody")
				rel()
			}
		}(i)
	}
	wg.Wait()

	if delivered == 0 {
		t.Error("no routes delivered under concurrency")
	}
	if _, ok := r.Lookup("sink"); !ok {
		t.Error("sink evicted by concurrent churn")
	}
}
