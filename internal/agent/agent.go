// Package agent is attnd's orchestrator: it owns the SQLite store, the
// persistent relay Session, and the Base names client, and implements the 29
// attn operations at the semantic level (the daemon equivalent of the upstream
// plugin's server.ts tool handlers). It applies block/contact/mute policy and
// persistence on inbound, and tracks last-inbound state for reply/react.
package agent

import (
	"context"
	"fmt"
	"log"
	"regexp"
	"strings"
	"sync"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/config"
	"github.com/TopengDev/attn-agnostic/internal/names"
	"github.com/TopengDev/attn-agnostic/internal/relay"
	"github.com/TopengDev/attn-agnostic/internal/store"
)

// Result is an operation's outcome: a human-readable line plus optional
// machine-readable data (echoed to the control client for evidence).
type Result struct {
	Text string         `json:"text"`
	Data map[string]any `json:"data,omitempty"`
}

func text(format string, a ...any) Result { return Result{Text: fmt.Sprintf(format, a...)} }

// Agent ties the store, relay session, and names client together.
type Agent struct {
	cfg   *config.Config
	st    *store.Store
	sess  *relay.Session
	names *names.Client
	log   *log.Logger

	mu                   sync.Mutex
	lastInboundFrom      string
	lastInboundGroup     string
	lastInboundMessageID string
	presenceState        string
	presenceMessage      string
}

var addrRe = regexp.MustCompile(`^0x[0-9a-fA-F]{40}$`)

func isValidAddress(a string) bool { return addrRe.MatchString(a) }

func normalize(a string) string { return strings.ToLower(strings.TrimSpace(a)) }

// New builds an Agent and wires the relay Session's handlers to it. Call Start
// to dial Base + begin the relay loop.
func New(cfg *config.Config, st *store.Store, logger *log.Logger) *Agent {
	a := &Agent{cfg: cfg, st: st, log: logger, presenceState: "online"}
	a.sess = relay.NewSession(cfg.ID, cfg.RelayURL, relay.Handlers{
		OnInbound:    a.handleInbound,
		OnKeyLearned: a.onKeyLearned,
		OnReady:      a.onReady,
		Logf:         func(f string, args ...any) { a.log.Printf("[relay] "+f, args...) },
	})
	return a
}

// Session exposes the relay session (for the daemon's control hooks).
func (a *Agent) Session() *relay.Session { return a.sess }

// Address returns the agent's identity address.
func (a *Agent) Address() string { return a.cfg.ID.Address() }

// Start dials Base, hydrates presence + key cache from the store, and launches
// the relay connect/reconnect loop. It returns immediately; Run lives in goroutines.
func (a *Agent) Start(ctx context.Context) error {
	// Hydrate persisted presence.
	if v, ok, _ := a.st.MetaGet("presence_state"); ok && (v == "online" || v == "away") {
		a.presenceState = v
	}
	if v, ok, _ := a.st.MetaGet("presence_message"); ok {
		a.presenceMessage = v
	}

	// Dial Base (cheap — no connection until first call). Non-fatal: name ops
	// surface an error if it's unavailable, but messaging works without it.
	nc, err := names.New(ctx, a.cfg.BaseRPC)
	if err != nil {
		a.log.Printf("[names] Base dial failed (name ops degraded): %v", err)
	} else {
		a.names = nc
	}

	go a.sess.Run(ctx)
	return nil
}

// Close releases the names client (store + session are owned by the daemon).
func (a *Agent) Close() {
	if a.names != nil {
		a.names.Close()
	}
}

// onKeyLearned persists a resolved pubkey to the DB key cache.
func (a *Agent) onKeyLearned(address, pubHex string) {
	if err := a.st.SaveKeyCache(address, pubHex); err != nil {
		a.log.Printf("[store] save key cache %s: %v", address, err)
	}
}

// onReady runs after each successful (re)auth: prime the session key cache from
// the DB, re-assert presence, and flush the outbox.
func (a *Agent) onReady() {
	a.mu.Lock()
	pState, pMsg := a.presenceState, a.presenceMessage
	a.mu.Unlock()
	if err := a.sess.SetPresence(pState, pMsg); err != nil {
		a.log.Printf("[relay] presence re-assert failed: %v", err)
	}
	a.flushOutbox()
}

func (a *Agent) flushOutbox() {
	items, err := a.st.GetOutbox()
	if err != nil {
		a.log.Printf("[store] read outbox: %v", err)
		return
	}
	if len(items) == 0 {
		return
	}
	a.log.Printf("[relay] flushing %d queued outbound message(s)", len(items))
	for _, it := range items {
		if it.Attempts >= 10 {
			a.log.Printf("[relay] outbox %s exceeded 10 attempts, discarding", it.ID)
			_ = a.st.DeleteOutbox(it.ID)
			continue
		}
		if err := a.sess.SendQueued(it.ID, it.To, it.Encrypted, it.Signature); err != nil {
			_ = a.st.IncrementOutboxAttempts(it.ID)
			continue
		}
		_ = a.st.DeleteOutbox(it.ID)
	}
}

// cachedKey returns a recipient pubkey from the session cache or the DB.
func (a *Agent) cachedKey(addr string) string {
	if k := a.sess.CachedKey(addr); k != "" {
		return k
	}
	if k, _ := a.st.GetKeyCache(addr); k != "" {
		a.sess.PrimeKey(addr, k)
		return k
	}
	return ""
}

// ── inbound pipeline (policy + persistence) ──────────────────────────────

func (a *Agent) handleInbound(ev relay.InboundEvent) {
	blocked, _ := a.st.IsBlocked(ev.From)
	if blocked {
		return // silently drop (already acked by the session)
	}

	switch ev.Kind {
	case "dm":
		a.handleInboundDM(ev)
	case "group":
		a.handleInboundGroup(ev)
	case "group_invite":
		if err := a.st.SaveGroupInvite(store.GroupInvite{
			GroupID: ev.GroupID, GroupName: ev.GroupName, From: ev.From, Members: ev.Members, Ts: ev.Ts,
		}); err != nil {
			a.log.Printf("[store] save group invite: %v", err)
		}
		a.log.Printf("[inbound] group invite to %q (%d members) from %s", ev.GroupName, len(ev.Members), ev.From)
	case "group_member_update":
		switch ev.Action {
		case "joined":
			_ = a.st.AddGroupMember(ev.GroupID, ev.From, "")
		case "left":
			_ = a.st.RemoveGroupMember(ev.GroupID, ev.From)
		}
		a.log.Printf("[inbound] group %q member update: %s %s", ev.GroupName, ev.From, ev.Action)
	case "reaction":
		a.handleInboundReaction(ev)
	}
}

func (a *Agent) handleInboundDM(ev relay.InboundEvent) {
	if !ev.Verified {
		a.log.Printf("[inbound] DM from %s has INVALID signature — dropping", ev.From)
		return
	}
	a.syncContactName(ev.From, ev.FromName)

	isContact, _ := a.st.IsContact(ev.From)
	if !isContact {
		_ = a.st.SavePending(ev.ID, ev.From, ev.Plaintext, ev.Ts)
		if notified, _ := a.st.HasPendingNotified(ev.From); !notified {
			_ = a.st.MarkPendingNotified(ev.From)
			a.log.Printf("[inbound] PENDING message from unknown agent %s (approve with add_contact)", ev.From)
		}
		return
	}

	_ = a.st.SaveMessage(ev.ID, ev.From, "inbound", ev.Plaintext, tsISO(ev.Ts))
	if a.muted(ev.From, "") {
		a.log.Printf("[inbound] DM from %s saved (muted — skipped surfacing)", ev.From)
		return
	}
	a.setLastInbound(ev.From, "", ev.ID)
	a.log.Printf("[inbound] DM from %s: %s", ev.From, preview(ev.Plaintext))
}

func (a *Agent) handleInboundGroup(ev relay.InboundEvent) {
	a.syncContactName(ev.From, ev.FromName)
	_ = a.st.SaveMessage(ev.ID, ev.GroupID, "inbound", ev.Plaintext, tsISO(ev.Ts))
	if a.muted(ev.From, ev.GroupID) {
		a.log.Printf("[inbound] group %s msg from %s saved (muted)", ev.GroupID, ev.From)
		return
	}
	a.setLastInbound(ev.From, ev.GroupID, ev.ID)
	a.log.Printf("[inbound] group %q from %s: %s", ev.GroupName, ev.From, preview(ev.Plaintext))
}

func (a *Agent) handleInboundReaction(ev relay.InboundEvent) {
	if ev.GroupID == "" {
		// DM reaction: require a verified signature from a known contact.
		if !ev.Verified {
			return
		}
		if ok, _ := a.st.IsContact(ev.From); !ok {
			return
		}
	}
	_ = a.st.SaveReaction(ev.ReactionFor, ev.From, ev.Plaintext, tsISO(ev.Ts))
	a.log.Printf("[inbound] reaction %s from %s on %s", ev.Plaintext, ev.From, ev.ReactionFor)
}

// muted reports whether a peer/group is currently silenced (global, per-agent,
// or per-group). Pending and invites bypass this (handled by not calling it).
func (a *Agent) muted(from, groupID string) bool {
	if all, _ := a.st.IsAllMuted(); all {
		return true
	}
	if groupID != "" {
		if m, _ := a.st.IsMuted(groupID, store.MuteGroup); m {
			return true
		}
	}
	if m, _ := a.st.IsMuted(from, store.MuteAgent); m {
		return true
	}
	return false
}

func (a *Agent) syncContactName(address, relayName string) {
	if relayName == "" {
		return
	}
	local, _ := a.st.GetContactName(address)
	if relayName != local {
		_ = a.st.UpdateContactName(address, relayName)
	}
}

func (a *Agent) setLastInbound(from, group, msgID string) {
	a.mu.Lock()
	a.lastInboundFrom = from
	a.lastInboundGroup = group
	a.lastInboundMessageID = msgID
	a.mu.Unlock()
	// Persist so reply/react survive a restart.
	_ = a.st.MetaSet("last_inbound_from", from)
	_ = a.st.MetaSet("last_inbound_group", group)
	_ = a.st.MetaSet("last_inbound_msgid", msgID)
}

func (a *Agent) getLastInbound() (from, group, msgID string) {
	a.mu.Lock()
	from, group, msgID = a.lastInboundFrom, a.lastInboundGroup, a.lastInboundMessageID
	a.mu.Unlock()
	if from == "" && msgID == "" {
		// Fall back to persisted state (e.g. after restart).
		from, _, _ = a.st.MetaGet("last_inbound_from")
		group, _, _ = a.st.MetaGet("last_inbound_group")
		msgID, _, _ = a.st.MetaGet("last_inbound_msgid")
	}
	return
}

// ── small helpers ────────────────────────────────────────────────────────

func tsISO(ms int64) string {
	if ms <= 0 {
		ms = time.Now().UnixMilli()
	}
	return time.UnixMilli(ms).UTC().Format("2006-01-02T15:04:05.000Z")
}

func preview(s string) string {
	s = strings.ReplaceAll(s, "\n", " ")
	if len(s) > 80 {
		return s[:80] + "…"
	}
	return s
}
