package relay

import (
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/identity"
)

func TestHTTPBaseFromWS(t *testing.T) {
	cases := map[string]string{
		"wss://attn.s0nderlabs.xyz/ws": "https://attn.s0nderlabs.xyz",
		"ws://localhost:8787/ws":       "http://localhost:8787",
		"wss://relay.example.com":      "https://relay.example.com",
	}
	for in, want := range cases {
		if got := httpBaseFromWS(in); got != want {
			t.Errorf("httpBaseFromWS(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalize(t *testing.T) {
	if normalize("  0xABC123  ") != "0xabc123" {
		t.Errorf("normalize trim+lower failed: %q", normalize("  0xABC123  "))
	}
}

func TestLowerAll(t *testing.T) {
	got := lowerAll([]string{"0xAA", " 0xBb "})
	if len(got) != 2 || got[0] != "0xaa" || got[1] != "0xbb" {
		t.Errorf("lowerAll = %v", got)
	}
}

func newTestSession(t *testing.T, h Handlers) *Session {
	t.Helper()
	id, err := identity.Generate()
	if err != nil {
		t.Fatal(err)
	}
	return NewSession(id, "wss://relay.example.com/ws", h)
}

// TestDrainWaitersUnblocks forces many pending key/resolve/presence waiters (as
// a disconnect would orphan) and asserts drainWaiters unblocks every one with a
// nil result, clears the maps, and is idempotent — no leak, no hang. Run with
// -race: drain sends on the waiter channels while reader goroutines receive,
// and the maps are only touched under s.mu.
func TestDrainWaitersUnblocks(t *testing.T) {
	s := newTestSession(t, Handlers{})

	const n = 64
	var wg sync.WaitGroup
	keyResults := make(chan *string, n)

	for i := 0; i < n; i++ {
		ch := make(chan *string, 1)
		key := fmt.Sprintf("0x%040x", i)
		s.mu.Lock()
		s.keyWaits[key] = append(s.keyWaits[key], ch)
		s.mu.Unlock()
		wg.Add(1)
		go func() {
			defer wg.Done()
			select {
			case v := <-ch:
				keyResults <- v
			case <-time.After(3 * time.Second):
				t.Error("key waiter hung past drain")
			}
		}()
	}

	resCh := make(chan *resolveResult, 1)
	s.mu.Lock()
	s.resWaits["alice"] = append(s.resWaits["alice"], resCh)
	s.mu.Unlock()
	presCh := make(chan *presenceResult, 1)
	s.mu.Lock()
	s.presWaits["0xbeef"] = append(s.presWaits["0xbeef"], presCh)
	s.mu.Unlock()

	resGot := make(chan *resolveResult, 1)
	presGot := make(chan *presenceResult, 1)
	wg.Add(2)
	go func() { defer wg.Done(); resGot <- waitRes(t, resCh) }()
	go func() { defer wg.Done(); presGot <- waitPres(t, presCh) }()

	time.Sleep(20 * time.Millisecond) // let the readers block
	s.drainWaiters()
	wg.Wait()

	close(keyResults)
	for v := range keyResults {
		if v != nil {
			t.Fatalf("key waiter got %v, want nil", *v)
		}
	}
	if r := <-resGot; r != nil {
		t.Fatalf("resolve waiter got %+v, want nil", r)
	}
	if p := <-presGot; p != nil {
		t.Fatalf("presence waiter got %+v, want nil", p)
	}

	s.mu.Lock()
	leak := len(s.keyWaits) + len(s.resWaits) + len(s.presWaits)
	s.mu.Unlock()
	if leak != 0 {
		t.Fatalf("waiter maps not cleared: %d entries remain", leak)
	}

	s.drainWaiters() // idempotent — must not panic on empty maps
}

func waitRes(t *testing.T, ch chan *resolveResult) *resolveResult {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Error("resolve waiter hung past drain")
		return nil
	}
}

func waitPres(t *testing.T, ch chan *presenceResult) *presenceResult {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(3 * time.Second):
		t.Error("presence waiter hung past drain")
		return nil
	}
}

// TestRemoveWaiter checks a per-call timeout removes only its own channel.
func TestRemoveWaiter(t *testing.T) {
	m := map[string][]chan *string{}
	a, b := make(chan *string, 1), make(chan *string, 1)
	m["k"] = []chan *string{a, b}
	removeWaiter(m, "k", a)
	if len(m["k"]) != 1 || m["k"][0] != b {
		t.Fatalf("removeWaiter left %v", m["k"])
	}
	removeWaiter(m, "k", b)
	if _, ok := m["k"]; ok {
		t.Fatal("empty key should be deleted")
	}
	removeWaiter(m, "missing", a) // no-op, must not panic
}

// TestOnReadySingleFlight asserts launchOnReady never runs two OnReady callbacks
// at once (no overlapping outbox flushes on a reconnect storm) and that Wait
// joins the in-flight one. Run with -race.
func TestOnReadySingleFlight(t *testing.T) {
	var concurrent, maxConcurrent int32
	release := make(chan struct{})
	s := newTestSession(t, Handlers{
		OnReady: func() {
			c := atomic.AddInt32(&concurrent, 1)
			for {
				old := atomic.LoadInt32(&maxConcurrent)
				if c <= old || atomic.CompareAndSwapInt32(&maxConcurrent, old, c) {
					break
				}
			}
			<-release
			atomic.AddInt32(&concurrent, -1)
		},
	})

	// Two rapid (re)auths: the second must be single-flighted away while the
	// first is still running.
	s.launchOnReady()
	s.launchOnReady()
	time.Sleep(30 * time.Millisecond)
	if got := atomic.LoadInt32(&maxConcurrent); got != 1 {
		t.Fatalf("max concurrent OnReady = %d, want 1", got)
	}

	close(release)
	s.Wait() // must join the in-flight goroutine
	if atomic.LoadInt32(&concurrent) != 0 {
		t.Fatal("OnReady goroutine not joined by Wait")
	}
	if s.onReadyActive.Load() {
		t.Fatal("onReadyActive should be reset after completion")
	}
}
