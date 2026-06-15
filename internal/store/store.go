// Package store is attnd's persistent state: a SQLite database (pure-Go
// modernc.org/sqlite, no CGO so M4 cross-compile stays clean) holding contacts,
// blocked, pending, outbox, key cache, groups, group members, group invites,
// message history, reactions, mutes, and a small key/value meta table (into
// which presence is folded — single source of truth that survives restart,
// replacing the upstream plugin's separate presence.json).
//
// The schema mirrors the upstream s0nderlabs/attn plugin (history.ts) so the
// semantics match the network's expectations exactly. All migrations are
// idempotent (CREATE TABLE IF NOT EXISTS).
package store

import (
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	_ "modernc.org/sqlite"
)

// Store wraps the SQLite connection. The connection pool is pinned to a single
// connection (SetMaxOpenConns(1)) so writes from the inbound pipeline and the
// control handler serialize instead of racing for the write lock.
type Store struct {
	db *sql.DB
}

// nowISO formats a timestamp the way the upstream plugin does
// (new Date().toISOString()): UTC, millisecond precision, trailing Z.
func nowISO() string { return time.Now().UTC().Format("2006-01-02T15:04:05.000Z") }

func nowMs() int64 { return time.Now().UnixMilli() }

// Open opens (creating if needed) the SQLite database at path and runs the
// idempotent migrations.
func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=busy_timeout(5000)&_pragma=journal_mode(WAL)&_pragma=foreign_keys(off)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, fmt.Errorf("open sqlite %s: %w", path, err)
	}
	db.SetMaxOpenConns(1)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("ping sqlite: %w", err)
	}
	s := &Store{db: db}
	if err := s.migrate(); err != nil {
		db.Close()
		return nil, err
	}
	return s, nil
}

// Close closes the database.
func (s *Store) Close() error { return s.db.Close() }

func (s *Store) migrate() error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS messages (
			id TEXT PRIMARY KEY,
			peer TEXT NOT NULL,
			direction TEXT NOT NULL CHECK(direction IN ('inbound','outbound')),
			content TEXT NOT NULL,
			ts TEXT NOT NULL
		)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_peer_ts ON messages(peer, ts DESC)`,
		`CREATE INDEX IF NOT EXISTS idx_messages_dir_ts ON messages(direction, ts)`,
		`CREATE TABLE IF NOT EXISTS contacts (
			address TEXT PRIMARY KEY,
			name TEXT,
			added_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS blocked (
			address TEXT PRIMARY KEY,
			blocked_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS pending (
			id TEXT PRIMARY KEY,
			from_address TEXT NOT NULL,
			plaintext TEXT NOT NULL,
			ts INTEGER NOT NULL,
			notified INTEGER DEFAULT 0
		)`,
		`CREATE INDEX IF NOT EXISTS idx_pending_from ON pending(from_address)`,
		`CREATE TABLE IF NOT EXISTS outbox (
			id TEXT PRIMARY KEY,
			to_address TEXT NOT NULL,
			encrypted TEXT NOT NULL,
			signature TEXT NOT NULL,
			ts INTEGER NOT NULL,
			attempts INTEGER DEFAULT 0
		)`,
		`CREATE TABLE IF NOT EXISTS key_cache (
			address TEXT PRIMARY KEY,
			public_key TEXT NOT NULL,
			updated_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS groups (
			id TEXT PRIMARY KEY,
			name TEXT NOT NULL,
			created_at TEXT NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS group_members (
			group_id TEXT NOT NULL,
			address TEXT NOT NULL,
			name TEXT,
			PRIMARY KEY (group_id, address)
		)`,
		`CREATE TABLE IF NOT EXISTS group_invites (
			group_id TEXT PRIMARY KEY,
			group_name TEXT NOT NULL,
			from_address TEXT NOT NULL,
			members_json TEXT NOT NULL,
			ts INTEGER NOT NULL
		)`,
		`CREATE TABLE IF NOT EXISTS reactions (
			message_id TEXT NOT NULL,
			from_address TEXT NOT NULL,
			emoji TEXT NOT NULL,
			ts TEXT NOT NULL,
			PRIMARY KEY (message_id, from_address)
		)`,
		`CREATE TABLE IF NOT EXISTS mutes (
			target TEXT NOT NULL,
			kind TEXT NOT NULL CHECK(kind IN ('agent','group','all')),
			until INTEGER,
			created_at INTEGER NOT NULL,
			PRIMARY KEY (target, kind)
		)`,
		`CREATE TABLE IF NOT EXISTS meta (
			key TEXT PRIMARY KEY,
			value TEXT NOT NULL
		)`,
	}
	for _, q := range stmts {
		if _, err := s.db.Exec(q); err != nil {
			return fmt.Errorf("migrate: %w (stmt: %.60s)", err, q)
		}
	}
	return nil
}

func lc(s string) string { return strings.ToLower(strings.TrimSpace(s)) }

// ── Messages ────────────────────────────────────────────────────────────

// Message is a stored history row.
type Message struct {
	ID        string
	Peer      string
	Direction string // inbound | outbound
	Content   string
	Ts        string
}

// SaveMessage inserts a message (ignored if the id already exists). ts may be
// empty to stamp now.
func (s *Store) SaveMessage(id, peer, direction, content, ts string) error {
	if ts == "" {
		ts = nowISO()
	}
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO messages (id, peer, direction, content, ts) VALUES (?,?,?,?,?)`,
		id, lc(peer), direction, content, ts)
	return err
}

// GetHistory returns the most recent `limit` messages with peer, oldest-first.
func (s *Store) GetHistory(peer string, limit int) ([]Message, error) {
	if limit <= 0 {
		limit = 20
	}
	rows, err := s.db.Query(
		`SELECT id, peer, direction, content, ts FROM messages WHERE peer = ? ORDER BY ts DESC LIMIT ?`,
		lc(peer), limit)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Message
	for rows.Next() {
		var m Message
		if err := rows.Scan(&m.ID, &m.Peer, &m.Direction, &m.Content, &m.Ts); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	// Reverse to oldest-first (matches plugin's .reverse()).
	for i, j := 0, len(out)-1; i < j; i, j = i+1, j-1 {
		out[i], out[j] = out[j], out[i]
	}
	return out, rows.Err()
}

// GetMessageByID returns a single message or (nil, nil) if absent.
func (s *Store) GetMessageByID(id string) (*Message, error) {
	var m Message
	err := s.db.QueryRow(
		`SELECT id, peer, direction, content, ts FROM messages WHERE id = ?`, id).
		Scan(&m.ID, &m.Peer, &m.Direction, &m.Content, &m.Ts)
	if errors.Is(err, sql.ErrNoRows) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return &m, nil
}

// ── Contacts ────────────────────────────────────────────────────────────

// Contact is a stored contact row.
type Contact struct {
	Address string
	Name    string // may be empty
	AddedAt string
}

// IsContact reports whether address is a known contact.
func (s *Store) IsContact(address string) (bool, error) {
	var a string
	err := s.db.QueryRow(`SELECT address FROM contacts WHERE address = ?`, lc(address)).Scan(&a)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// AddContact upserts a contact. A non-empty name overwrites; an empty name
// preserves any existing name (COALESCE), matching the upstream behaviour.
func (s *Store) AddContact(address, name string) error {
	var namePtr any
	if name == "" {
		namePtr = nil
	} else {
		namePtr = name
	}
	_, err := s.db.Exec(
		`INSERT INTO contacts (address, name, added_at) VALUES (?,?,?)
		 ON CONFLICT(address) DO UPDATE SET name = COALESCE(excluded.name, contacts.name)`,
		lc(address), namePtr, nowISO())
	return err
}

// UpdateContactName sets (or clears, with empty) a contact's name.
func (s *Store) UpdateContactName(address, name string) error {
	var namePtr any
	if name == "" {
		namePtr = nil
	} else {
		namePtr = name
	}
	_, err := s.db.Exec(`UPDATE contacts SET name = ? WHERE address = ?`, namePtr, lc(address))
	return err
}

// GetContactName returns the contact's name, or "" if none/absent.
func (s *Store) GetContactName(address string) (string, error) {
	var name sql.NullString
	err := s.db.QueryRow(`SELECT name FROM contacts WHERE address = ?`, lc(address)).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	if err != nil {
		return "", err
	}
	return name.String, nil
}

// GetContactByName resolves a (case-insensitive) name to an address, or "".
func (s *Store) GetContactByName(name string) (string, error) {
	var addr string
	err := s.db.QueryRow(`SELECT address FROM contacts WHERE lower(name) = ? LIMIT 1`, lc(name)).Scan(&addr)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return addr, err
}

// GetContacts lists contacts, newest-first.
func (s *Store) GetContacts() ([]Contact, error) {
	rows, err := s.db.Query(`SELECT address, name, added_at FROM contacts ORDER BY added_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Contact
	for rows.Next() {
		var c Contact
		var name sql.NullString
		if err := rows.Scan(&c.Address, &name, &c.AddedAt); err != nil {
			return nil, err
		}
		c.Name = name.String
		out = append(out, c)
	}
	return out, rows.Err()
}

// RemoveContact deletes a contact.
func (s *Store) RemoveContact(address string) error {
	_, err := s.db.Exec(`DELETE FROM contacts WHERE address = ?`, lc(address))
	return err
}

// ── Blocked ─────────────────────────────────────────────────────────────

// BlockContact blocks an address (and removes it from contacts + pending).
func (s *Store) BlockContact(address string) error {
	a := lc(address)
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO blocked (address, blocked_at) VALUES (?,?)`, a, nowISO()); err != nil {
		return err
	}
	if _, err := s.db.Exec(`DELETE FROM contacts WHERE address = ?`, a); err != nil {
		return err
	}
	_, err := s.db.Exec(`DELETE FROM pending WHERE from_address = ?`, a)
	return err
}

// UnblockContact removes a block.
func (s *Store) UnblockContact(address string) error {
	_, err := s.db.Exec(`DELETE FROM blocked WHERE address = ?`, lc(address))
	return err
}

// IsBlocked reports whether address is blocked.
func (s *Store) IsBlocked(address string) (bool, error) {
	var a string
	err := s.db.QueryRow(`SELECT address FROM blocked WHERE address = ?`, lc(address)).Scan(&a)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// Blocked is a blocked row.
type Blocked struct {
	Address   string
	BlockedAt string
}

// GetBlocked lists blocked addresses, newest-first.
func (s *Store) GetBlocked() ([]Blocked, error) {
	rows, err := s.db.Query(`SELECT address, blocked_at FROM blocked ORDER BY blocked_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Blocked
	for rows.Next() {
		var b Blocked
		if err := rows.Scan(&b.Address, &b.BlockedAt); err != nil {
			return nil, err
		}
		out = append(out, b)
	}
	return out, rows.Err()
}

// ── Pending ─────────────────────────────────────────────────────────────

// PendingMsg is a queued message from an unknown sender.
type PendingMsg struct {
	ID        string
	From      string
	Plaintext string
	Ts        int64
}

// SavePending stores a pending message (ignored on duplicate id).
func (s *Store) SavePending(id, from, plaintext string, ts int64) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO pending (id, from_address, plaintext, ts) VALUES (?,?,?,?)`,
		id, lc(from), plaintext, ts)
	return err
}

// HasPendingNotified reports whether a pending sender was already surfaced.
func (s *Store) HasPendingNotified(from string) (bool, error) {
	var n int
	err := s.db.QueryRow(`SELECT notified FROM pending WHERE from_address = ? AND notified = 1 LIMIT 1`, lc(from)).Scan(&n)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	return err == nil, err
}

// MarkPendingNotified marks all pending rows from a sender as notified.
func (s *Store) MarkPendingNotified(from string) error {
	_, err := s.db.Exec(`UPDATE pending SET notified = 1 WHERE from_address = ?`, lc(from))
	return err
}

// FlushPending returns and deletes all pending messages from a sender, oldest-first.
func (s *Store) FlushPending(from string) ([]PendingMsg, error) {
	a := lc(from)
	rows, err := s.db.Query(`SELECT id, plaintext, ts FROM pending WHERE from_address = ? ORDER BY ts ASC`, a)
	if err != nil {
		return nil, err
	}
	var out []PendingMsg
	for rows.Next() {
		var m PendingMsg
		m.From = a
		if err := rows.Scan(&m.ID, &m.Plaintext, &m.Ts); err != nil {
			rows.Close()
			return nil, err
		}
		out = append(out, m)
	}
	rows.Close()
	if err := rows.Err(); err != nil {
		return nil, err
	}
	if len(out) > 0 {
		if _, err := s.db.Exec(`DELETE FROM pending WHERE from_address = ?`, a); err != nil {
			return nil, err
		}
	}
	return out, nil
}

// PendingSender summarises a pending sender and their message count.
type PendingSender struct {
	From  string
	Count int
}

// GetPendingSenders lists pending senders with counts, busiest-first.
func (s *Store) GetPendingSenders() ([]PendingSender, error) {
	rows, err := s.db.Query(`SELECT from_address, COUNT(*) AS c FROM pending GROUP BY from_address ORDER BY c DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []PendingSender
	for rows.Next() {
		var p PendingSender
		if err := rows.Scan(&p.From, &p.Count); err != nil {
			return nil, err
		}
		out = append(out, p)
	}
	return out, rows.Err()
}

// ── Outbox ──────────────────────────────────────────────────────────────

// OutboxItem is a queued outbound message awaiting a live relay.
type OutboxItem struct {
	ID        string
	To        string
	Encrypted string
	Signature string
	Ts        int64
	Attempts  int
}

// SaveOutbox queues an outbound message (ignored on duplicate id).
func (s *Store) SaveOutbox(id, to, encrypted, signature string, ts int64) error {
	_, err := s.db.Exec(
		`INSERT OR IGNORE INTO outbox (id, to_address, encrypted, signature, ts) VALUES (?,?,?,?,?)`,
		id, lc(to), encrypted, signature, ts)
	return err
}

// GetOutbox lists queued outbound messages, oldest-first.
func (s *Store) GetOutbox() ([]OutboxItem, error) {
	rows, err := s.db.Query(`SELECT id, to_address, encrypted, signature, ts, attempts FROM outbox ORDER BY ts ASC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []OutboxItem
	for rows.Next() {
		var o OutboxItem
		if err := rows.Scan(&o.ID, &o.To, &o.Encrypted, &o.Signature, &o.Ts, &o.Attempts); err != nil {
			return nil, err
		}
		out = append(out, o)
	}
	return out, rows.Err()
}

// DeleteOutbox removes a queued message.
func (s *Store) DeleteOutbox(id string) error {
	_, err := s.db.Exec(`DELETE FROM outbox WHERE id = ?`, id)
	return err
}

// IncrementOutboxAttempts bumps the retry counter for a queued message.
func (s *Store) IncrementOutboxAttempts(id string) error {
	_, err := s.db.Exec(`UPDATE outbox SET attempts = attempts + 1 WHERE id = ?`, id)
	return err
}

// ── Key cache ───────────────────────────────────────────────────────────

// SaveKeyCache upserts an address→pubkey mapping.
func (s *Store) SaveKeyCache(address, publicKey string) error {
	_, err := s.db.Exec(
		`INSERT INTO key_cache (address, public_key, updated_at) VALUES (?,?,?)
		 ON CONFLICT(address) DO UPDATE SET public_key = excluded.public_key, updated_at = excluded.updated_at`,
		lc(address), publicKey, nowISO())
	return err
}

// GetKeyCache returns the cached pubkey for address, or "".
func (s *Store) GetKeyCache(address string) (string, error) {
	var pk string
	err := s.db.QueryRow(`SELECT public_key FROM key_cache WHERE address = ?`, lc(address)).Scan(&pk)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return pk, err
}

// ── Groups ──────────────────────────────────────────────────────────────

// GroupMember is a member row.
type GroupMember struct {
	Address string
	Name    string
}

// Group is a group summary row.
type Group struct {
	ID          string
	Name        string
	CreatedAt   string
	MemberCount int
}

// CreateGroup creates a group and inserts its members (all idempotent).
func (s *Store) CreateGroup(id, name string, members []GroupMember) error {
	if _, err := s.db.Exec(`INSERT OR IGNORE INTO groups (id, name, created_at) VALUES (?,?,?)`, id, name, nowISO()); err != nil {
		return err
	}
	for _, m := range members {
		if err := s.AddGroupMember(id, m.Address, m.Name); err != nil {
			return err
		}
	}
	return nil
}

// GetGroups lists groups with member counts, newest-first.
func (s *Store) GetGroups() ([]Group, error) {
	rows, err := s.db.Query(
		`SELECT g.id, g.name, g.created_at, COUNT(gm.address) AS member_count
		 FROM groups g LEFT JOIN group_members gm ON g.id = gm.group_id
		 GROUP BY g.id ORDER BY g.created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Group
	for rows.Next() {
		var g Group
		if err := rows.Scan(&g.ID, &g.Name, &g.CreatedAt, &g.MemberCount); err != nil {
			return nil, err
		}
		out = append(out, g)
	}
	return out, rows.Err()
}

// GetGroupMembers lists a group's members.
func (s *Store) GetGroupMembers(groupID string) ([]GroupMember, error) {
	rows, err := s.db.Query(`SELECT address, name FROM group_members WHERE group_id = ?`, groupID)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GroupMember
	for rows.Next() {
		var m GroupMember
		var name sql.NullString
		if err := rows.Scan(&m.Address, &name); err != nil {
			return nil, err
		}
		m.Name = name.String
		out = append(out, m)
	}
	return out, rows.Err()
}

// GetGroupName returns a group's name, or "".
func (s *Store) GetGroupName(groupID string) (string, error) {
	var name string
	err := s.db.QueryRow(`SELECT name FROM groups WHERE id = ?`, groupID).Scan(&name)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return name, err
}

// AddGroupMember adds a member to a group (idempotent).
func (s *Store) AddGroupMember(groupID, address, name string) error {
	var namePtr any
	if name == "" {
		namePtr = nil
	} else {
		namePtr = name
	}
	_, err := s.db.Exec(`INSERT OR IGNORE INTO group_members (group_id, address, name) VALUES (?,?,?)`, groupID, lc(address), namePtr)
	return err
}

// RemoveGroupMember removes a member from a group.
func (s *Store) RemoveGroupMember(groupID, address string) error {
	_, err := s.db.Exec(`DELETE FROM group_members WHERE group_id = ? AND address = ?`, groupID, lc(address))
	return err
}

// DeleteGroup removes a group and all its members.
func (s *Store) DeleteGroup(groupID string) error {
	if _, err := s.db.Exec(`DELETE FROM group_members WHERE group_id = ?`, groupID); err != nil {
		return err
	}
	_, err := s.db.Exec(`DELETE FROM groups WHERE id = ?`, groupID)
	return err
}

// ── Group invites ───────────────────────────────────────────────────────

// GroupInvite is a pending invite row.
type GroupInvite struct {
	GroupID   string
	GroupName string
	From      string
	Members   []string
	Ts        int64
}

// SaveGroupInvite stores (replacing) a pending invite.
func (s *Store) SaveGroupInvite(inv GroupInvite) error {
	members := inv.Members
	if members == nil {
		members = []string{}
	}
	membersJSON, err := json.Marshal(members)
	if err != nil {
		return err
	}
	_, err = s.db.Exec(
		`INSERT OR REPLACE INTO group_invites (group_id, group_name, from_address, members_json, ts) VALUES (?,?,?,?,?)`,
		inv.GroupID, inv.GroupName, lc(inv.From), string(membersJSON), inv.Ts)
	return err
}

// GetGroupInvites lists pending invites, newest-first.
func (s *Store) GetGroupInvites() ([]GroupInvite, error) {
	rows, err := s.db.Query(`SELECT group_id, group_name, from_address, members_json, ts FROM group_invites ORDER BY ts DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []GroupInvite
	for rows.Next() {
		var iv GroupInvite
		var membersJSON string
		if err := rows.Scan(&iv.GroupID, &iv.GroupName, &iv.From, &membersJSON, &iv.Ts); err != nil {
			return nil, err
		}
		_ = json.Unmarshal([]byte(membersJSON), &iv.Members)
		out = append(out, iv)
	}
	return out, rows.Err()
}

// DeleteGroupInvite removes a pending invite.
func (s *Store) DeleteGroupInvite(groupID string) error {
	_, err := s.db.Exec(`DELETE FROM group_invites WHERE group_id = ?`, groupID)
	return err
}

// ── Reactions ───────────────────────────────────────────────────────────

// Reaction is a stored reaction row.
type Reaction struct {
	MessageID string
	From      string
	Emoji     string
	Ts        string
}

// SaveReaction stores (replacing) a reaction.
func (s *Store) SaveReaction(messageID, from, emoji, ts string) error {
	if ts == "" {
		ts = nowISO()
	}
	_, err := s.db.Exec(
		`INSERT OR REPLACE INTO reactions (message_id, from_address, emoji, ts) VALUES (?,?,?,?)`,
		messageID, lc(from), emoji, ts)
	return err
}

// GetReactionsForMessages returns reactions for the given message ids.
func (s *Store) GetReactionsForMessages(ids []string) ([]Reaction, error) {
	if len(ids) == 0 {
		return nil, nil
	}
	ph := strings.TrimRight(strings.Repeat("?,", len(ids)), ",")
	args := make([]any, len(ids))
	for i, id := range ids {
		args[i] = id
	}
	rows, err := s.db.Query(
		`SELECT message_id, from_address, emoji, ts FROM reactions WHERE message_id IN (`+ph+`)`, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Reaction
	for rows.Next() {
		var r Reaction
		if err := rows.Scan(&r.MessageID, &r.From, &r.Emoji, &r.Ts); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	return out, rows.Err()
}

// ── Mutes ───────────────────────────────────────────────────────────────

// MuteKind is the target class of a mute.
type MuteKind = string

const (
	// MuteAgent mutes a single agent address.
	MuteAgent MuteKind = "agent"
	// MuteGroup mutes a group id.
	MuteGroup MuteKind = "group"
	// MuteAll is the global mute (target sentinel "*").
	MuteAll MuteKind = "all"
)

// AllMuteTarget is the sentinel row key for the global mute.
const AllMuteTarget = "*"

// Mute is a mute row.
type Mute struct {
	Target    string
	Kind      MuteKind
	Until     sql.NullInt64 // ms epoch, null = indefinite
	CreatedAt int64
}

// AddMute upserts a mute. untilMs <= 0 means indefinite.
func (s *Store) AddMute(target string, kind MuteKind, untilMs int64) error {
	var until any
	if untilMs > 0 {
		until = untilMs
	} else {
		until = nil
	}
	_, err := s.db.Exec(
		`INSERT INTO mutes (target, kind, until, created_at) VALUES (?,?,?,?)
		 ON CONFLICT(target, kind) DO UPDATE SET until = excluded.until, created_at = excluded.created_at`,
		lc(target), kind, until, nowMs())
	return err
}

// RemoveMute deletes a mute; returns whether a row was removed.
func (s *Store) RemoveMute(target string, kind MuteKind) (bool, error) {
	res, err := s.db.Exec(`DELETE FROM mutes WHERE target = ? AND kind = ?`, lc(target), kind)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

// IsMuted reports whether (target,kind) is muted, lazily expiring timed mutes.
func (s *Store) IsMuted(target string, kind MuteKind) (bool, error) {
	var until sql.NullInt64
	err := s.db.QueryRow(`SELECT until FROM mutes WHERE target = ? AND kind = ?`, lc(target), kind).Scan(&until)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	if until.Valid && until.Int64 <= nowMs() {
		_, _ = s.db.Exec(`DELETE FROM mutes WHERE target = ? AND kind = ?`, lc(target), kind)
		return false, nil
	}
	return true, nil
}

// IsAllMuted reports whether the global mute is active.
func (s *Store) IsAllMuted() (bool, error) { return s.IsMuted(AllMuteTarget, MuteAll) }

// GetMutes lists active mutes (expiring timed ones first), newest-first.
func (s *Store) GetMutes() ([]Mute, error) {
	if _, err := s.db.Exec(`DELETE FROM mutes WHERE until IS NOT NULL AND until <= ?`, nowMs()); err != nil {
		return nil, err
	}
	rows, err := s.db.Query(`SELECT target, kind, until, created_at FROM mutes ORDER BY created_at DESC`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Mute
	for rows.Next() {
		var m Mute
		if err := rows.Scan(&m.Target, &m.Kind, &m.Until, &m.CreatedAt); err != nil {
			return nil, err
		}
		out = append(out, m)
	}
	return out, rows.Err()
}

// GetMuteCreatedAt returns when (target,kind) was muted, or (0,false).
func (s *Store) GetMuteCreatedAt(target string, kind MuteKind) (int64, bool, error) {
	var c int64
	err := s.db.QueryRow(`SELECT created_at FROM mutes WHERE target = ? AND kind = ?`, lc(target), kind).Scan(&c)
	if errors.Is(err, sql.ErrNoRows) {
		return 0, false, nil
	}
	if err != nil {
		return 0, false, err
	}
	return c, true, nil
}

// CountInboundSince counts inbound messages from peer at/after sinceMs.
func (s *Store) CountInboundSince(peer string, sinceMs int64) (int, error) {
	sinceISO := time.UnixMilli(sinceMs).UTC().Format("2006-01-02T15:04:05.000Z")
	var n int
	err := s.db.QueryRow(
		`SELECT COUNT(*) FROM messages WHERE peer = ? AND direction = 'inbound' AND ts >= ?`,
		lc(peer), sinceISO).Scan(&n)
	return n, err
}

// CountAllInboundSince counts all inbound messages at/after sinceMs.
func (s *Store) CountAllInboundSince(sinceMs int64) (int, error) {
	sinceISO := time.UnixMilli(sinceMs).UTC().Format("2006-01-02T15:04:05.000Z")
	var n int
	err := s.db.QueryRow(`SELECT COUNT(*) FROM messages WHERE direction = 'inbound' AND ts >= ?`, sinceISO).Scan(&n)
	return n, err
}

// ── Meta (key/value; presence folded here) ──────────────────────────────

// MetaGet returns the meta value for key, or ("", false).
func (s *Store) MetaGet(key string) (string, bool, error) {
	var v string
	err := s.db.QueryRow(`SELECT value FROM meta WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", false, nil
	}
	if err != nil {
		return "", false, err
	}
	return v, true, nil
}

// MetaSet upserts a meta key/value.
func (s *Store) MetaSet(key, value string) error {
	_, err := s.db.Exec(
		`INSERT INTO meta (key, value) VALUES (?,?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`,
		key, value)
	return err
}
