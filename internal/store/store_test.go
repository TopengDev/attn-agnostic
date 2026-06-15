package store

import (
	"path/filepath"
	"testing"
	"time"
)

func openTemp(t *testing.T) (*Store, string) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "attnd.db")
	s, err := Open(path)
	if err != nil {
		t.Fatalf("Open: %v", err)
	}
	t.Cleanup(func() { s.Close() })
	return s, path
}

func TestMigrateIdempotent(t *testing.T) {
	s, _ := openTemp(t)
	// Re-running migrate must not error (idempotent CREATE IF NOT EXISTS).
	if err := s.migrate(); err != nil {
		t.Fatalf("second migrate: %v", err)
	}
	// Spot-check a couple of tables exist.
	for _, tbl := range []string{"messages", "contacts", "mutes", "group_invites", "meta"} {
		var name string
		err := s.db.QueryRow(`SELECT name FROM sqlite_master WHERE type='table' AND name=?`, tbl).Scan(&name)
		if err != nil || name != tbl {
			t.Fatalf("table %s missing: %v", tbl, err)
		}
	}
}

func TestMessageRoundTrip(t *testing.T) {
	s, _ := openTemp(t)
	peer := "0xAABBccddeeff00112233445566778899aabbccdd"
	if err := s.SaveMessage("m1", peer, "outbound", "hello", ""); err != nil {
		t.Fatal(err)
	}
	if err := s.SaveMessage("m2", peer, "inbound", "world", ""); err != nil {
		t.Fatal(err)
	}
	// Duplicate id ignored.
	if err := s.SaveMessage("m1", peer, "outbound", "DUP", ""); err != nil {
		t.Fatal(err)
	}
	msgs, err := s.GetHistory(peer, 20)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 {
		t.Fatalf("want 2 messages, got %d", len(msgs))
	}
	// Peer must be normalized to lowercase.
	if msgs[0].Peer != "0xaabbccddeeff00112233445566778899aabbccdd" {
		t.Fatalf("peer not lowercased: %s", msgs[0].Peer)
	}
	got, err := s.GetMessageByID("m1")
	if err != nil || got == nil || got.Content != "hello" {
		t.Fatalf("GetMessageByID m1: %v %+v", err, got)
	}
}

func TestContactUpsertCoalesce(t *testing.T) {
	s, _ := openTemp(t)
	a := "0x1111111111111111111111111111111111111111"
	if err := s.AddContact(a, "alice"); err != nil {
		t.Fatal(err)
	}
	// Empty name must NOT clobber existing name (COALESCE).
	if err := s.AddContact(a, ""); err != nil {
		t.Fatal(err)
	}
	name, err := s.GetContactName(a)
	if err != nil || name != "alice" {
		t.Fatalf("want alice, got %q (%v)", name, err)
	}
	// Non-empty name overwrites.
	if err := s.AddContact(a, "alice.attn"); err != nil {
		t.Fatal(err)
	}
	name, _ = s.GetContactName(a)
	if name != "alice.attn" {
		t.Fatalf("want alice.attn, got %q", name)
	}
	addr, err := s.GetContactByName("ALICE.ATTN")
	if err != nil || addr != a {
		t.Fatalf("GetContactByName: %q %v", addr, err)
	}
	ok, _ := s.IsContact(a)
	if !ok {
		t.Fatal("IsContact should be true")
	}
}

func TestBlockRemovesContactAndPending(t *testing.T) {
	s, _ := openTemp(t)
	a := "0x2222222222222222222222222222222222222222"
	_ = s.AddContact(a, "bob")
	_ = s.SavePending("p1", a, "hi", time.Now().UnixMilli())
	if err := s.BlockContact(a); err != nil {
		t.Fatal(err)
	}
	if ok, _ := s.IsBlocked(a); !ok {
		t.Fatal("should be blocked")
	}
	if ok, _ := s.IsContact(a); ok {
		t.Fatal("block must remove from contacts")
	}
	senders, _ := s.GetPendingSenders()
	if len(senders) != 0 {
		t.Fatalf("block must clear pending, got %d", len(senders))
	}
	_ = s.UnblockContact(a)
	if ok, _ := s.IsBlocked(a); ok {
		t.Fatal("unblock failed")
	}
}

func TestPendingFlush(t *testing.T) {
	s, _ := openTemp(t)
	a := "0x3333333333333333333333333333333333333333"
	_ = s.SavePending("p1", a, "first", 100)
	_ = s.SavePending("p2", a, "second", 200)
	if has, _ := s.HasPendingNotified(a); has {
		t.Fatal("should not be notified yet")
	}
	_ = s.MarkPendingNotified(a)
	if has, _ := s.HasPendingNotified(a); !has {
		t.Fatal("should be notified")
	}
	msgs, err := s.FlushPending(a)
	if err != nil {
		t.Fatal(err)
	}
	if len(msgs) != 2 || msgs[0].Plaintext != "first" || msgs[1].Plaintext != "second" {
		t.Fatalf("flush order wrong: %+v", msgs)
	}
	again, _ := s.FlushPending(a)
	if len(again) != 0 {
		t.Fatal("flush must delete")
	}
}

func TestOutbox(t *testing.T) {
	s, _ := openTemp(t)
	_ = s.SaveOutbox("o1", "0x4444444444444444444444444444444444444444", "ct", "sig", 1)
	items, _ := s.GetOutbox()
	if len(items) != 1 || items[0].Attempts != 0 {
		t.Fatalf("outbox: %+v", items)
	}
	_ = s.IncrementOutboxAttempts("o1")
	items, _ = s.GetOutbox()
	if items[0].Attempts != 1 {
		t.Fatalf("attempts not incremented: %d", items[0].Attempts)
	}
	_ = s.DeleteOutbox("o1")
	items, _ = s.GetOutbox()
	if len(items) != 0 {
		t.Fatal("outbox delete failed")
	}
}

func TestKeyCache(t *testing.T) {
	s, _ := openTemp(t)
	a := "0x5555555555555555555555555555555555555555"
	_ = s.SaveKeyCache(a, "0x04aaa")
	pk, _ := s.GetKeyCache(a)
	if pk != "0x04aaa" {
		t.Fatalf("keycache: %q", pk)
	}
	_ = s.SaveKeyCache(a, "0x04bbb")
	pk, _ = s.GetKeyCache(a)
	if pk != "0x04bbb" {
		t.Fatalf("keycache update: %q", pk)
	}
}

func TestGroupsAndInvites(t *testing.T) {
	s, _ := openTemp(t)
	gid := "g-123"
	m1 := "0x6666666666666666666666666666666666666666"
	m2 := "0x7777777777777777777777777777777777777777"
	if err := s.CreateGroup(gid, "devs", []GroupMember{{Address: m1, Name: "me"}, {Address: m2}}); err != nil {
		t.Fatal(err)
	}
	groups, _ := s.GetGroups()
	if len(groups) != 1 || groups[0].MemberCount != 2 {
		t.Fatalf("groups: %+v", groups)
	}
	if n, _ := s.GetGroupName(gid); n != "devs" {
		t.Fatalf("group name: %q", n)
	}
	_ = s.RemoveGroupMember(gid, m2)
	mem, _ := s.GetGroupMembers(gid)
	if len(mem) != 1 {
		t.Fatalf("members after remove: %+v", mem)
	}
	// Invite round-trips members_json.
	inv := GroupInvite{GroupID: "g-999", GroupName: "ops", From: m1, Members: []string{m1, m2}, Ts: 42}
	if err := s.SaveGroupInvite(inv); err != nil {
		t.Fatal(err)
	}
	invs, _ := s.GetGroupInvites()
	if len(invs) != 1 || len(invs[0].Members) != 2 || invs[0].Members[1] != m2 {
		t.Fatalf("invite round-trip: %+v", invs)
	}
	_ = s.DeleteGroupInvite("g-999")
	invs, _ = s.GetGroupInvites()
	if len(invs) != 0 {
		t.Fatal("delete invite failed")
	}
	_ = s.DeleteGroup(gid)
	groups, _ = s.GetGroups()
	if len(groups) != 0 {
		t.Fatal("delete group failed")
	}
}

func TestReactions(t *testing.T) {
	s, _ := openTemp(t)
	_ = s.SaveReaction("m1", "0x8888888888888888888888888888888888888888", "🔥", "")
	_ = s.SaveReaction("m1", "0x8888888888888888888888888888888888888888", "👍", "") // replace
	rs, _ := s.GetReactionsForMessages([]string{"m1", "m2"})
	if len(rs) != 1 || rs[0].Emoji != "👍" {
		t.Fatalf("reactions: %+v", rs)
	}
}

func TestMutes(t *testing.T) {
	s, _ := openTemp(t)
	a := "0x9999999999999999999999999999999999999999"
	_ = s.AddMute(a, MuteAgent, 0) // indefinite
	if ok, _ := s.IsMuted(a, MuteAgent); !ok {
		t.Fatal("should be muted")
	}
	// Timed mute already in the past expires lazily.
	_ = s.AddMute("g-1", MuteGroup, time.Now().UnixMilli()-1000)
	if ok, _ := s.IsMuted("g-1", MuteGroup); ok {
		t.Fatal("expired mute should read false")
	}
	// Global mute.
	_ = s.AddMute(AllMuteTarget, MuteAll, 0)
	if ok, _ := s.IsAllMuted(); !ok {
		t.Fatal("global mute not active")
	}
	mutes, _ := s.GetMutes()
	// agent + all remain (group expired and was purged).
	if len(mutes) != 2 {
		t.Fatalf("active mutes: %+v", mutes)
	}
	if _, ok, _ := s.GetMuteCreatedAt(a, MuteAgent); !ok {
		t.Fatal("created_at missing")
	}
	removed, _ := s.RemoveMute(a, MuteAgent)
	if !removed {
		t.Fatal("remove mute failed")
	}
}

func TestMeta(t *testing.T) {
	s, _ := openTemp(t)
	if _, ok, _ := s.MetaGet("presence_state"); ok {
		t.Fatal("meta should be empty")
	}
	_ = s.MetaSet("presence_state", "away")
	_ = s.MetaSet("presence_state", "online") // upsert
	v, ok, _ := s.MetaGet("presence_state")
	if !ok || v != "online" {
		t.Fatalf("meta: %q %v", v, ok)
	}
}

func TestRestartSurvival(t *testing.T) {
	path := filepath.Join(t.TempDir(), "attnd.db")
	s, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	peer := "0xabcabcabcabcabcabcabcabcabcabcabcabcabca"
	_ = s.SaveMessage("persist1", peer, "inbound", "survive me", "")
	_ = s.AddContact(peer, "persisted")
	_ = s.MetaSet("presence_state", "away")
	if err := s.Close(); err != nil {
		t.Fatal(err)
	}

	// Reopen the SAME file → state must still be there.
	s2, err := Open(path)
	if err != nil {
		t.Fatal(err)
	}
	defer s2.Close()
	got, err := s2.GetMessageByID("persist1")
	if err != nil || got == nil || got.Content != "survive me" {
		t.Fatalf("message did not survive restart: %v %+v", err, got)
	}
	name, _ := s2.GetContactName(peer)
	if name != "persisted" {
		t.Fatalf("contact did not survive restart: %q", name)
	}
	v, ok, _ := s2.MetaGet("presence_state")
	if !ok || v != "away" {
		t.Fatalf("meta did not survive restart: %q", v)
	}
}
