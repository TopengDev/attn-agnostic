// Package mesh is attn-agnostic's Layer-A local mesh: a harness-agnostic registry
// of named local sessions plus relay-bypassed routing between them. It is the Go
// equivalent of the upstream Claude Code plugin's local.ts + server.ts handleSend
// precedence, but adapted to our single-daemon topology: instead of a per-process
// peers-dir of Unix sockets, ONE daemon holds the registry over the transports it
// already owns (M2's WS subscriber hub + daemon-driven HTTP). See
// docs/ideation/02c-prototype-localmesh-findings.md.
//
// The mesh is TRUSTED-LOCAL, SAME-HOST, UNENCRYPTED by design (loopback only):
// local messages between sessions on the same machine never touch the relay and
// are not end-to-end encrypted. Registration + routing MUST NOT be exposed
// off-host (the daemon's HTTP/WS interface binds loopback only and validates
// http-target endpoints accordingly).
//
// This package is a leaf: it imports only the standard library, so both the
// agent layer (Send precedence, peers, send-all) and the httpapi layer (WS
// self-registration, http-target registration, local-frame routing) can depend
// on it without an import cycle.
package mesh

import (
	"errors"
	"sort"
	"strings"
	"sync"
	"time"
)

// Transport identifies how a registered session receives a routed local frame.
type Transport string

const (
	// TransportWS is a WebSocket subscriber that self-registered by connecting
	// ws://127.0.0.1:<port>/?session=<name> (pi-style). Delivery pushes a frame
	// onto its WS send channel.
	TransportWS Transport = "ws-subscriber"
	// TransportHTTP is a session whose adapter registered an HTTP inject endpoint
	// via POST /local/register (opencode/hermes-style). Delivery POSTs a frame to
	// that loopback endpoint.
	TransportHTTP Transport = "http-target"
)

// ErrNoSession is returned when a routing target is not a registered local session.
var ErrNoSession = errors.New("no such local session")

// Frame is a local-mesh message handed to a Deliverer. It is transport-neutral;
// each Deliverer renders it into its own wire shape (the WS deliverer maps it to
// pi-setup's inbound frame; the HTTP deliverer POSTs a JSON body). A local frame
// is always trust="local" / local=true on the wire — that is added by the
// deliverer, not stored here.
type Frame struct {
	ID          string // unique message id (caller-minted)
	From        string // sender's LOCAL SESSION NAME (e.g. "main", "worker-1") — not an address
	FromAddress string // sender's relay identity, if any (informational)
	Text        string // message body
	Ts          int64  // unix millis
	Broadcast   bool   // true when this frame is part of a send("all") fan-out
	ReactionFor string // message id this frame reacts to (empty for normal messages)
}

// Deliverer delivers a Frame to one registered session's live transport. The
// implementation lives in the transport layer (httpapi): a WS deliverer wraps a
// subscriber connection; an HTTP deliverer wraps a registered inject endpoint.
// Deliver MUST be safe to call without holding the registry lock and MUST NOT
// block indefinitely (the registry calls it outside its own lock, but a hung
// deliverer would still stall a routing call).
type Deliverer interface {
	Deliver(Frame) error
}

// Entry is a registered local session. The deliver handle is unexported so it is
// never copied out by List/snapshots (callers route through the Registry).
type Entry struct {
	Name         string
	Harness      string // best-effort harness label ("pi", "opencode", "hermes", "" = unknown)
	Transport    Transport
	Address      string // optional relay identity (lowercased) — enables address-match routing
	PID          int    // optional process id (0 = unknown)
	RegisteredAt time.Time

	deliver Deliverer
}

// View is a read-only snapshot of an Entry (no deliver handle) for enumeration.
type View struct {
	Name         string    `json:"name"`
	Harness      string    `json:"harness,omitempty"`
	Transport    Transport `json:"transport"`
	Address      string    `json:"address,omitempty"`
	PID          int       `json:"pid,omitempty"`
	RegisteredAt time.Time `json:"registered_at"`
}

// Registry is the concurrency-safe table of live local sessions, keyed by name.
type Registry struct {
	mu      sync.RWMutex
	entries map[string]*Entry
}

// New builds an empty registry.
func New() *Registry {
	return &Registry{entries: make(map[string]*Entry)}
}

// Register inserts (or replaces, last-registration-wins) the entry for e.Name and
// returns a release func that deregisters THIS EXACT registration. The release is
// a no-op if a newer registration has since replaced e (identity compare on the
// *Entry pointer) — this is what makes a stale WS disconnect safe: an old
// connection's release must never evict the fresh entry that replaced it.
//
// Register sets e.deliver and stamps RegisteredAt if unset. e.Name is required.
func (r *Registry) Register(e *Entry, d Deliverer) (release func()) {
	if e == nil || e.Name == "" || d == nil {
		return func() {}
	}
	e.deliver = d
	if e.RegisteredAt.IsZero() {
		e.RegisteredAt = time.Now()
	}
	if e.Address != "" {
		e.Address = strings.ToLower(strings.TrimSpace(e.Address))
	}
	r.mu.Lock()
	r.entries[e.Name] = e
	r.mu.Unlock()
	return func() {
		r.mu.Lock()
		if cur, ok := r.entries[e.Name]; ok && cur == e {
			delete(r.entries, e.Name)
		}
		r.mu.Unlock()
	}
}

// DeregisterByName removes the entry for name regardless of which registration
// owns it (used by the explicit POST /local/deregister control path). Returns
// true if an entry was removed.
func (r *Registry) DeregisterByName(name string) bool {
	r.mu.Lock()
	defer r.mu.Unlock()
	if _, ok := r.entries[name]; ok {
		delete(r.entries, name)
		return true
	}
	return false
}

// Lookup returns the entry registered under an exact session name.
func (r *Registry) Lookup(name string) (*Entry, bool) {
	r.mu.RLock()
	defer r.mu.RUnlock()
	e, ok := r.entries[name]
	return e, ok
}

// LookupByAddress returns the (first) entry whose relay identity matches addr
// (case-insensitive). Used for CC-style address-match routing precedence.
func (r *Registry) LookupByAddress(addr string) (*Entry, bool) {
	addr = strings.ToLower(strings.TrimSpace(addr))
	if addr == "" {
		return nil, false
	}
	r.mu.RLock()
	defer r.mu.RUnlock()
	for _, e := range r.entries {
		if e.Address != "" && e.Address == addr {
			return e, true
		}
	}
	return nil, false
}

// Resolve looks up a routing target by either session name or relay address,
// mirroring CC handleSend: a bare name resolves by name; a 0x... target resolves
// by address.
func (r *Registry) Resolve(to string) (*Entry, bool) {
	if strings.HasPrefix(strings.ToLower(strings.TrimSpace(to)), "0x") {
		return r.LookupByAddress(to)
	}
	return r.Lookup(to)
}

// RouteTo delivers f to a specific entry (caller already resolved it).
func (r *Registry) RouteTo(e *Entry, f Frame) error {
	if e == nil || e.deliver == nil {
		return ErrNoSession
	}
	return e.deliver.Deliver(f)
}

// Route resolves to (by name or address) and delivers f to exactly that one
// session. Returns the resolved session name. Delivery happens OUTSIDE the
// registry lock so a slow deliverer cannot block registration/deregistration.
func (r *Registry) Route(to string, f Frame) (string, error) {
	e, ok := r.Resolve(to)
	if !ok {
		return "", ErrNoSession
	}
	if err := e.deliver.Deliver(f); err != nil {
		return e.Name, err
	}
	return e.Name, nil
}

// Broadcast delivers f to every registered session except exclude (by name) and
// returns the names that accepted delivery. Targets are snapshotted under the
// lock; delivery happens outside it. A deliverer that errors is simply omitted
// from the returned slice (best-effort fan-out, matching CC handleSend('all')).
func (r *Registry) Broadcast(f Frame, exclude string) []string {
	r.mu.RLock()
	targets := make([]*Entry, 0, len(r.entries))
	for _, e := range r.entries {
		if e.Name == exclude {
			continue
		}
		targets = append(targets, e)
	}
	r.mu.RUnlock()

	sent := make([]string, 0, len(targets))
	for _, e := range targets {
		if err := e.deliver.Deliver(f); err == nil {
			sent = append(sent, e.Name)
		}
	}
	sort.Strings(sent)
	return sent
}

// List returns a name-sorted read-only snapshot of every registered session.
func (r *Registry) List() []View {
	r.mu.RLock()
	views := make([]View, 0, len(r.entries))
	for _, e := range r.entries {
		views = append(views, View{
			Name: e.Name, Harness: e.Harness, Transport: e.Transport,
			Address: e.Address, PID: e.PID, RegisteredAt: e.RegisteredAt,
		})
	}
	r.mu.RUnlock()
	sort.Slice(views, func(i, j int) bool { return views[i].Name < views[j].Name })
	return views
}

// Names returns a name-sorted list of registered session names (the pi-compat
// /local-peers shape).
func (r *Registry) Names() []string {
	r.mu.RLock()
	names := make([]string, 0, len(r.entries))
	for name := range r.entries {
		names = append(names, name)
	}
	r.mu.RUnlock()
	sort.Strings(names)
	return names
}

// Count returns the number of registered sessions.
func (r *Registry) Count() int {
	r.mu.RLock()
	defer r.mu.RUnlock()
	return len(r.entries)
}
