package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ethereum/go-ethereum/common"

	icrypto "github.com/TopengDev/attn-agnostic/internal/crypto"
	"github.com/TopengDev/attn-agnostic/internal/mesh"
	"github.com/TopengDev/attn-agnostic/internal/names"
	"github.com/TopengDev/attn-agnostic/internal/store"
)

// Dispatch routes a named operation to its handler. This is the daemon-internal
// invocation path the control socket drives (the M2 HTTP/CLI/MCP wrappers will
// front the same dispatch).
func (a *Agent) Dispatch(ctx context.Context, op string, args map[string]any) (Result, error) {
	s := func(k string) string {
		if v, ok := args[k]; ok {
			if str, ok := v.(string); ok {
				return str
			}
		}
		return ""
	}
	switch op {
	case "send":
		return a.Send(ctx, s("to"), s("message"))
	case "reply":
		return a.Reply(ctx, s("message"))
	case "send_file":
		return a.SendFile(ctx, s("to"), s("path"))
	case "history":
		return a.History(s("with"), intArg(args, "limit", 20))
	case "add_contact":
		return a.AddContact(ctx, s("address"), s("name"))
	case "remove_contact":
		return a.RemoveContact(s("address"))
	case "block":
		return a.Block(s("address"), boolArg(args, "unblock"))
	case "contacts":
		return a.Contacts(ctx)
	case "create_group":
		return a.CreateGroup(ctx, s("name"), strSlice(args["members"]))
	case "send_group":
		return a.SendGroup(ctx, s("group_id"), s("message"))
	case "add_to_group":
		return a.AddToGroup(ctx, s("group_id"), s("address"), s("name"))
	case "leave_group":
		return a.LeaveGroup(ctx, s("group_id"))
	case "accept_group":
		return a.AcceptGroup(ctx, s("group_id"))
	case "decline_group":
		return a.DeclineGroup(ctx, s("group_id"))
	case "kick_from_group":
		return a.KickFromGroup(ctx, s("group_id"), s("address"))
	case "transfer_group_admin":
		return a.TransferGroupAdmin(ctx, s("group_id"), s("address"))
	case "groups":
		return a.Groups()
	case "peers":
		return a.Peers()
	case "react":
		return a.React(ctx, s("emoji"), s("message_id"))
	case "register_name":
		return a.RegisterName(ctx, firstNonEmpty(s("label"), s("name")))
	case "lookup":
		return a.Lookup(ctx, s("query"))
	case "names":
		return a.Names(ctx, s("address"))
	case "transfer_name":
		return a.TransferName(ctx, firstNonEmpty(s("label"), s("name")), s("to"))
	case "set_primary_name":
		return a.SetPrimaryName(ctx, firstNonEmpty(s("label"), s("name")))
	case "mute":
		return a.Mute(ctx, s("target"), s("duration"))
	case "unmute":
		return a.Unmute(ctx, s("target"))
	case "mutes":
		return a.Mutes()
	case "status":
		return a.Status(s("state"), s("message"))
	case "status_of":
		return a.StatusOf(ctx, s("target"))
	default:
		return Result{}, fmt.Errorf("unknown op: %s", op)
	}
}

// ── messaging ────────────────────────────────────────────────────────────

// Send delivers a message to a local session (Layer-A mesh, relay-bypassed), an
// address, or a .attn name. Routing precedence: (1) "all" → local broadcast over
// the registry minus this session; (2) a recipient that is a bare local session
// NAME is delivered locally, bypassing the relay; (3) everything else (a 0x
// address, a .attn name, or an unknown name) goes to the relay unchanged.
//
// The local mesh is routed by NAME ONLY — a 0x-address target ALWAYS goes to the
// (encrypted) relay, never to a local session that merely claimed that address.
// Honoring a self-asserted address for local routing would let a buggy/semi-
// trusted local session intercept plaintext addressed to a real (possibly
// REMOTE) peer (M3 audit M2 — address-shadowing). An explicit ".attn" suffix
// likewise always forces relay name-resolution.
func (a *Agent) Send(ctx context.Context, to, message string) (Result, error) {
	if to == "" {
		return Result{}, fmt.Errorf("recipient is required")
	}

	if reg, self := a.meshAndSelf(); reg != nil {
		if to == "all" {
			return a.sendLocalBroadcast(reg, self, message)
		}
		// Local mesh = NAME-routed only. A 0x address or a ".attn" name is NEVER
		// resolved against the local registry; it always takes the relay path.
		if !isValidAddress(to) && !strings.HasSuffix(strings.ToLower(to), ".attn") {
			if entry, ok := reg.Resolve(to); ok {
				return a.sendLocalTo(reg, entry, self, message)
			}
		}
	} else if to == "all" {
		return Result{}, fmt.Errorf(`local broadcast ("all") requires the local mesh (the HTTP/WS interface) — unavailable in control-only mode`)
	}

	addr := to
	display := to
	if !isValidAddress(to) {
		resolved, err := a.resolveName(ctx, to)
		if err != nil || resolved == "" {
			return Result{}, fmt.Errorf("could not resolve %q (not a local session or registered .attn name)", to)
		}
		addr = resolved
		display = fmt.Sprintf("%s.attn (%s)", strings.TrimSuffix(strings.ToLower(to), ".attn"), addr)
	}
	addr = normalize(addr)
	pub := a.cachedKey(addr)

	if a.sess.IsReady() {
		res, encrypted, err := a.sess.Send(ctx, addr, message, pub)
		if err != nil {
			// Transmitted but unconfirmed (delivery_status timeout) still counts
			// as sent; a true failure before transmit returns the error.
			if encrypted != "" && strings.Contains(err.Error(), "delivery_status") {
				_ = a.st.SaveMessage(res.ID, addr, "outbound", message, "")
				a.addContactAndFlush(ctx, addr, "")
				return Result{Text: fmt.Sprintf("Message sent to %s (no delivery confirmation within 15s)", display),
					Data: map[string]any{"id": res.ID, "status": "unconfirmed"}}, nil
			}
			return Result{}, err
		}
		_ = a.st.SaveMessage(res.ID, addr, "outbound", message, "")
		a.addContactAndFlush(ctx, addr, "")
		return Result{
			Text: fmt.Sprintf("Message sent to %s", display),
			Data: map[string]any{"id": res.ID, "status": res.Status, "recipient_state": res.RecipientState, "ciphertext_len": len(encrypted)},
		}, nil
	}

	// Relay offline → queue if we have a cached key to encrypt with.
	if pub == "" {
		return Result{}, fmt.Errorf("relay offline and recipient key not cached — cannot queue")
	}
	id := a.sess.NewMessageID()
	encrypted, err := icrypto.EncryptBase64(pub, []byte(message))
	if err != nil {
		return Result{}, fmt.Errorf("encrypt: %w", err)
	}
	sig, err := a.sess.SignEnvelope(id, addr, encrypted)
	if err != nil {
		return Result{}, err
	}
	if err := a.st.SaveOutbox(id, addr, encrypted, sig, time.Now().UnixMilli()); err != nil {
		return Result{}, err
	}
	_ = a.st.SaveMessage(id, addr, "outbound", message, "")
	a.addContactAndFlush(ctx, addr, "")
	return Result{Text: fmt.Sprintf("Message queued for %s (relay offline). Will send on reconnect.", display),
		Data: map[string]any{"id": id, "status": "queued"}}, nil
}

// meshAndSelf returns the wired local-mesh registry + this daemon's session name
// under the agent lock (both are set once at startup via SetMesh).
func (a *Agent) meshAndSelf() (*mesh.Registry, string) {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.mesh, a.selfName
}

// sendLocalTo delivers message to exactly one local session, bypassing the relay
// entirely (no encryption, no outbox, same-host). It persists to history under a
// `local:<name>` peer key so a local thread is distinguishable from a relay one.
func (a *Agent) sendLocalTo(reg *mesh.Registry, entry *mesh.Entry, self, message string) (Result, error) {
	id := a.sess.NewMessageID()
	frame := mesh.Frame{
		ID: id, From: self, FromAddress: a.Address(),
		Text: message, Ts: time.Now().UnixMilli(),
	}
	if err := reg.RouteTo(entry, frame); err != nil {
		return Result{}, fmt.Errorf("local delivery to %q failed: %w", entry.Name, err)
	}
	_ = a.st.SaveMessage(id, "local:"+entry.Name, "outbound", message, "")
	return Result{
		Text: fmt.Sprintf("Message sent to local session %q (relay bypassed)", entry.Name),
		Data: map[string]any{"id": id, "status": "delivered", "local": true, "to": entry.Name},
	}, nil
}

// sendLocalBroadcast fans message out to every local session except this one.
func (a *Agent) sendLocalBroadcast(reg *mesh.Registry, self, message string) (Result, error) {
	id := a.sess.NewMessageID()
	frame := mesh.Frame{
		ID: id, From: self, FromAddress: a.Address(),
		Text: message, Ts: time.Now().UnixMilli(), Broadcast: true,
	}
	sent := reg.Broadcast(frame, self)
	if len(sent) == 0 {
		return Result{}, fmt.Errorf("no local peers are running")
	}
	_ = a.st.SaveMessage(id, "local:all", "outbound", message, "")
	return Result{
		Text: fmt.Sprintf("Message broadcast to %d local session(s): %s", len(sent), strings.Join(sent, ", ")),
		Data: map[string]any{"id": id, "status": "delivered", "local": true, "recipients": len(sent), "sessions": sent},
	}, nil
}

// Reply sends to the most recent inbound sender (or group).
func (a *Agent) Reply(ctx context.Context, message string) (Result, error) {
	from, group, _ := a.getLastInbound()
	target := group
	if target == "" {
		target = from
	}
	if target == "" {
		return Result{}, fmt.Errorf("no recent inbound message to reply to")
	}
	if group != "" {
		return a.SendGroup(ctx, group, message)
	}
	return a.Send(ctx, target, message)
}

// React adds an emoji reaction to a message (defaults to the last inbound).
func (a *Agent) React(ctx context.Context, emoji, messageID string) (Result, error) {
	if emoji == "" {
		return Result{}, fmt.Errorf("emoji is required")
	}
	emoji = emojiToUnicode(emoji)

	from, group, lastMsgID := a.getLastInbound()
	resolvedMsgID := messageID
	var recipient, groupID string

	if messageID == "" {
		resolvedMsgID = lastMsgID
		if resolvedMsgID == "" {
			return Result{}, fmt.Errorf("no message to react to — provide message_id or receive a message first")
		}
		recipient = from
		groupID = group
		if recipient == "" && groupID == "" {
			return Result{}, fmt.Errorf("no recent inbound message to react to")
		}
	} else {
		msg, err := a.st.GetMessageByID(messageID)
		if err != nil || msg == nil {
			return Result{}, fmt.Errorf("message not found: %s", messageID)
		}
		if msg.Direction == "outbound" {
			return Result{}, fmt.Errorf("cannot react to your own outbound message")
		}
		if isValidAddress(msg.Peer) {
			recipient = msg.Peer
		} else {
			groupID = msg.Peer
		}
	}

	if groupID != "" {
		return a.reactGroup(ctx, groupID, resolvedMsgID, emoji)
	}
	if err := a.sess.SendReaction(ctx, recipient, resolvedMsgID, emoji, a.cachedKey(recipient)); err != nil {
		return Result{}, fmt.Errorf("send reaction: %w", err)
	}
	_ = a.st.SaveReaction(resolvedMsgID, a.Address(), emoji, "")
	return text("Reacted %s to message from %s", emoji, recipient), nil
}

func (a *Agent) reactGroup(ctx context.Context, groupID, messageID, emoji string) (Result, error) {
	members, _ := a.st.GetGroupMembers(groupID)
	if len(members) == 0 {
		return Result{}, fmt.Errorf("group not found or has no members")
	}
	groupName, _ := a.st.GetGroupName(groupID)
	blobs := a.encryptForMembers(ctx, members, emoji)
	if len(blobs) == 0 {
		return Result{}, fmt.Errorf("could not encrypt for any group member")
	}
	body := map[string]any{
		"id": a.sess.NewMessageID(), "from": a.Address(), "group_id": groupID,
		"group_name": groupName, "message_id": messageID, "blobs": blobs,
	}
	if _, err := a.signedJSON(ctx, http.MethodPost, "/groups/"+groupID+"/react", body); err != nil {
		return Result{}, err
	}
	_ = a.st.SaveReaction(messageID, a.Address(), emoji, "")
	return text("Reacted %s in group %q", emoji, groupName), nil
}

// SendFile encrypts a file, uploads it to the relay, and sends a file reference.
func (a *Agent) SendFile(ctx context.Context, to, path string) (Result, error) {
	if to == "" || path == "" {
		return Result{}, fmt.Errorf("recipient and path are required")
	}
	addr := to
	if !isValidAddress(to) {
		resolved, err := a.resolveName(ctx, to)
		if err != nil || resolved == "" {
			return Result{}, fmt.Errorf("could not resolve %q", to)
		}
		addr = resolved
	}
	addr = normalize(addr)

	info, err := os.Stat(path)
	if err != nil {
		return Result{}, fmt.Errorf("file not found: %s", path)
	}
	if info.Size() > 10*1024*1024 {
		return Result{}, fmt.Errorf("file too large (max 10 MB)")
	}
	data, err := os.ReadFile(path)
	if err != nil {
		return Result{}, err
	}
	pub := a.cachedKey(addr)
	if pub == "" {
		pub, err = a.sess.GetKey(ctx, addr)
		if err != nil || pub == "" {
			return Result{}, fmt.Errorf("could not find public key for %s", addr)
		}
	}
	blob, err := icrypto.EncryptBinary(pub, data)
	if err != nil {
		return Result{}, fmt.Errorf("encrypt file: %w", err)
	}
	fileKey := a.sess.NewMessageID()
	resp, err := a.sess.SignedRequest(ctx, http.MethodPost, "/upload", strings.NewReader(string(blob)), map[string]string{"X-File-Key": fileKey})
	if err != nil {
		return Result{}, fmt.Errorf("upload: %w", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		b, _ := io.ReadAll(resp.Body)
		return Result{}, fmt.Errorf("upload failed (%d): %s", resp.StatusCode, strings.TrimSpace(string(b)))
	}
	var up struct {
		URL string `json:"url"`
	}
	_ = json.NewDecoder(resp.Body).Decode(&up)
	fileRef, _ := json.Marshal(map[string]any{
		"type": "file", "url": up.URL, "key": fileKey,
		"filename": filepath.Base(path), "size": info.Size(), "mime": "application/octet-stream",
	})
	return a.Send(ctx, addr, string(fileRef))
}

// ── contacts ─────────────────────────────────────────────────────────────

// AddContact approves/pre-approves an agent and delivers any pending messages.
func (a *Agent) AddContact(ctx context.Context, address, name string) (Result, error) {
	if !isValidAddress(address) {
		return Result{}, fmt.Errorf("invalid Ethereum address")
	}
	// A relay-provided .attn primary name overrides the manual name.
	if pn := a.fetchPrimaryName(ctx, address); pn != "" {
		name = pn
	}
	flushed := a.addContactAndFlush(ctx, address, name)
	label := address
	if name != "" {
		label = fmt.Sprintf("%s (%s)", name, address)
	}
	if flushed > 0 {
		return text("Added %s as contact. Delivered %d pending message(s).", label, flushed), nil
	}
	return text("Added %s as contact.", label), nil
}

// addContactAndFlush adds a contact and drains any pending messages into history.
func (a *Agent) addContactAndFlush(ctx context.Context, address, name string) int {
	_ = a.st.AddContact(address, name)
	pending, err := a.st.FlushPending(address)
	if err != nil || len(pending) == 0 {
		return 0
	}
	for _, pm := range pending {
		_ = a.st.SaveMessage(pm.ID, address, "inbound", pm.Plaintext, tsISO(pm.Ts))
	}
	a.log.Printf("[inbound] delivered %d pending message(s) from %s on add_contact", len(pending), address)
	return len(pending)
}

// RemoveContact removes a contact.
func (a *Agent) RemoveContact(address string) (Result, error) {
	if !isValidAddress(address) {
		return Result{}, fmt.Errorf("invalid Ethereum address")
	}
	if err := a.st.RemoveContact(address); err != nil {
		return Result{}, err
	}
	return text("Removed %s from contacts.", address), nil
}

// Block blocks (or unblocks) an agent.
func (a *Agent) Block(address string, unblock bool) (Result, error) {
	if !isValidAddress(address) {
		return Result{}, fmt.Errorf("invalid Ethereum address")
	}
	if unblock {
		if err := a.st.UnblockContact(address); err != nil {
			return Result{}, err
		}
		return text("Unblocked %s.", address), nil
	}
	if err := a.st.BlockContact(address); err != nil {
		return Result{}, err
	}
	return text("Blocked %s. All messages from them will be silently dropped.", address), nil
}

// Contacts lists contacts, pending requests, and blocked agents.
func (a *Agent) Contacts(ctx context.Context) (Result, error) {
	contacts, _ := a.st.GetContacts()
	pending, _ := a.st.GetPendingSenders()
	blocked, _ := a.st.GetBlocked()

	var b strings.Builder
	fmt.Fprintf(&b, "Your address: %s\n\n", a.Address())
	fmt.Fprintf(&b, "Contacts (%d):\n", len(contacts))
	if len(contacts) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, c := range contacts {
		label := c.Address
		if c.Name != "" {
			label = fmt.Sprintf("%s — %s", c.Name, c.Address)
		}
		fmt.Fprintf(&b, "  %s (added %s)\n", label, datePart(c.AddedAt))
	}
	fmt.Fprintf(&b, "\nPending requests (%d):\n", len(pending))
	if len(pending) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, p := range pending {
		fmt.Fprintf(&b, "  %s (%d message(s))\n", p.From, p.Count)
	}
	fmt.Fprintf(&b, "\nBlocked (%d):\n", len(blocked))
	if len(blocked) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, bl := range blocked {
		fmt.Fprintf(&b, "  %s (blocked %s)\n", bl.Address, datePart(bl.BlockedAt))
	}
	return Result{Text: b.String(), Data: map[string]any{"contacts": len(contacts), "pending": len(pending), "blocked": len(blocked)}}, nil
}

// History returns recent messages with a peer/group.
func (a *Agent) History(with string, limit int) (Result, error) {
	if with == "" {
		return Result{}, fmt.Errorf("`with` is required")
	}
	msgs, err := a.st.GetHistory(with, limit)
	if err != nil {
		return Result{}, err
	}
	if len(msgs) == 0 {
		return text("No messages found with %s", with), nil
	}
	ids := make([]string, len(msgs))
	for i, m := range msgs {
		ids[i] = m.ID
	}
	reactions, _ := a.st.GetReactionsForMessages(ids)
	byMsg := map[string][]string{}
	for _, r := range reactions {
		byMsg[r.MessageID] = append(byMsg[r.MessageID], r.Emoji)
	}
	name, _ := a.st.GetContactName(with)
	if name == "" {
		name, _ = a.st.GetGroupName(with)
	}
	header := fmt.Sprintf("Messages with %s", with)
	if name != "" {
		header = fmt.Sprintf("Messages with %s (%s)", name, with)
	}
	var b strings.Builder
	b.WriteString(header + ":\n")
	for _, m := range msgs {
		arrow := "→"
		if m.Direction == "inbound" {
			arrow = "←"
		}
		t := strings.Replace(m.Ts, "T", " ", 1)
		t = strings.TrimSuffix(t, "Z")
		line := fmt.Sprintf("[%s] %s %s", t, arrow, m.Content)
		if rs := byMsg[m.ID]; len(rs) > 0 {
			line += fmt.Sprintf(" [reactions: %s]", strings.Join(rs, ", "))
		}
		b.WriteString(line + "\n")
	}
	return Result{Text: b.String(), Data: map[string]any{"count": len(msgs)}}, nil
}

// ── groups ───────────────────────────────────────────────────────────────

// CreateGroup creates a group locally and registers it on the relay.
func (a *Agent) CreateGroup(ctx context.Context, name string, members []string) (Result, error) {
	if name == "" {
		return Result{}, fmt.Errorf("group name is required")
	}
	if len(members) == 0 {
		return Result{}, fmt.Errorf("at least one member is required")
	}
	uniq := map[string]bool{a.Address(): true}
	all := []string{a.Address()}
	for _, m := range members {
		if !isValidAddress(m) {
			return Result{}, fmt.Errorf("invalid address: %s", m)
		}
		lm := normalize(m)
		if !uniq[lm] {
			uniq[lm] = true
			all = append(all, lm)
		}
	}
	id := a.sess.NewMessageID()
	gm := make([]store.GroupMember, len(all))
	for i, m := range all {
		gm[i] = store.GroupMember{Address: m}
	}
	if err := a.st.CreateGroup(id, name, gm); err != nil {
		return Result{}, err
	}
	body := map[string]any{"id": id, "name": name, "members": all, "admin": a.Address()}
	if _, err := a.signedJSON(ctx, http.MethodPost, "/groups", body); err != nil {
		return Result{}, fmt.Errorf("create group on relay: %w", err)
	}
	return Result{Text: fmt.Sprintf("Created group %q (%d members). ID: %s", name, len(all), id),
		Data: map[string]any{"group_id": id, "members": len(all)}}, nil
}

// SendGroup sends an encrypted message to every group member.
func (a *Agent) SendGroup(ctx context.Context, groupID, message string) (Result, error) {
	members, _ := a.st.GetGroupMembers(groupID)
	if len(members) == 0 {
		return Result{}, fmt.Errorf("group not found or has no members")
	}
	groupName, _ := a.st.GetGroupName(groupID)
	blobs := a.encryptForMembers(ctx, members, message)
	if len(blobs) == 0 {
		return Result{}, fmt.Errorf("could not encrypt for any group member")
	}
	id := a.sess.NewMessageID()
	body := map[string]any{"id": id, "from": a.Address(), "group_id": groupID, "group_name": groupName, "blobs": blobs}
	if _, err := a.signedJSON(ctx, http.MethodPost, "/groups/"+groupID+"/send", body); err != nil {
		return Result{}, err
	}
	_ = a.st.SaveMessage(id, groupID, "outbound", message, "")
	return Result{Text: fmt.Sprintf("Message sent to group %q (%d members)", groupName, len(members)),
		Data: map[string]any{"id": id, "recipients": len(blobs)}}, nil
}

// AddToGroup adds a member to a group.
func (a *Agent) AddToGroup(ctx context.Context, groupID, address, name string) (Result, error) {
	if !isValidAddress(address) {
		return Result{}, fmt.Errorf("invalid Ethereum address")
	}
	if err := a.st.AddGroupMember(groupID, address, name); err != nil {
		return Result{}, err
	}
	if _, err := a.signedJSON(ctx, http.MethodPost, "/groups/"+groupID+"/members", map[string]any{"address": normalize(address)}); err != nil {
		return Result{}, err
	}
	label := address
	if name != "" {
		label = fmt.Sprintf("%s (%s)", name, address)
	}
	return text("Added %s to group.", label), nil
}

// LeaveGroup leaves a group.
func (a *Agent) LeaveGroup(ctx context.Context, groupID string) (Result, error) {
	groupName, _ := a.st.GetGroupName(groupID)
	if groupName == "" {
		return Result{}, fmt.Errorf("group not found")
	}
	if _, err := a.signedRaw(ctx, http.MethodDelete, "/groups/"+groupID+"/members/"+a.Address(), nil); err != nil {
		return Result{}, err
	}
	_ = a.st.DeleteGroup(groupID)
	return text("Left group %q.", groupName), nil
}

// AcceptGroup accepts a pending group invite.
func (a *Agent) AcceptGroup(ctx context.Context, groupID string) (Result, error) {
	invites, _ := a.st.GetGroupInvites()
	var inv *store.GroupInvite
	for i := range invites {
		if invites[i].GroupID == groupID {
			inv = &invites[i]
			break
		}
	}
	if inv == nil {
		return Result{}, fmt.Errorf("no pending invite for this group ID")
	}
	gm := make([]store.GroupMember, len(inv.Members))
	for i, m := range inv.Members {
		gm[i] = store.GroupMember{Address: m}
	}
	if err := a.st.CreateGroup(groupID, inv.GroupName, gm); err != nil {
		return Result{}, err
	}
	if _, err := a.signedJSON(ctx, http.MethodPost, "/groups/"+groupID+"/accept", map[string]any{"address": a.Address()}); err != nil {
		return Result{}, err
	}
	_ = a.st.DeleteGroupInvite(groupID)
	return text("Joined group %q (%d members).", inv.GroupName, len(inv.Members)), nil
}

// DeclineGroup declines a pending group invite.
func (a *Agent) DeclineGroup(ctx context.Context, groupID string) (Result, error) {
	invites, _ := a.st.GetGroupInvites()
	var inv *store.GroupInvite
	for i := range invites {
		if invites[i].GroupID == groupID {
			inv = &invites[i]
			break
		}
	}
	if inv == nil {
		return Result{}, fmt.Errorf("no pending invite for this group ID")
	}
	_, _ = a.signedRaw(ctx, http.MethodDelete, "/groups/"+groupID+"/members/"+a.Address(), nil)
	_ = a.st.DeleteGroupInvite(groupID)
	return text("Declined invite for group %q.", inv.GroupName), nil
}

// KickFromGroup removes a member from a group (admin only on the relay).
func (a *Agent) KickFromGroup(ctx context.Context, groupID, address string) (Result, error) {
	if !isValidAddress(address) {
		return Result{}, fmt.Errorf("invalid Ethereum address")
	}
	if _, err := a.signedRaw(ctx, http.MethodDelete, "/groups/"+groupID+"/members/"+normalize(address), nil); err != nil {
		return Result{}, err
	}
	_ = a.st.RemoveGroupMember(groupID, address)
	return text("Kicked %s from the group.", address), nil
}

// TransferGroupAdmin transfers the group admin role.
func (a *Agent) TransferGroupAdmin(ctx context.Context, groupID, address string) (Result, error) {
	if !isValidAddress(address) {
		return Result{}, fmt.Errorf("invalid Ethereum address")
	}
	body := map[string]any{"from": a.Address(), "to": normalize(address)}
	if _, err := a.signedJSON(ctx, http.MethodPost, "/groups/"+groupID+"/transfer", body); err != nil {
		return Result{}, err
	}
	return text("Transferred admin to %s.", address), nil
}

// Groups lists groups, pending invites, and members.
func (a *Agent) Groups() (Result, error) {
	groups, _ := a.st.GetGroups()
	invites, _ := a.st.GetGroupInvites()
	var b strings.Builder
	if len(invites) > 0 {
		fmt.Fprintf(&b, "Pending Invites (%d):\n", len(invites))
		for _, iv := range invites {
			fmt.Fprintf(&b, "  %q from %s (%d members)\n  ID: %s\n", iv.GroupName, iv.From, len(iv.Members), iv.GroupID)
		}
		b.WriteString("\n")
	}
	fmt.Fprintf(&b, "Groups (%d):\n", len(groups))
	if len(groups) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, g := range groups {
		fmt.Fprintf(&b, "\n  %s (%d members)\n  ID: %s\n", g.Name, g.MemberCount, g.ID)
		members, _ := a.st.GetGroupMembers(g.ID)
		for _, m := range members {
			label := m.Address
			if m.Name != "" {
				label = fmt.Sprintf("%s — %s", m.Name, m.Address)
			}
			fmt.Fprintf(&b, "    %s\n", label)
		}
	}
	return Result{Text: b.String(), Data: map[string]any{"groups": len(groups), "invites": len(invites)}}, nil
}

// Peers is the Layer-A local-mesh discovery op: it enumerates the live registry
// of named local sessions (name, harness, transport, relay address, online).
func (a *Agent) Peers() (Result, error) {
	reg, self := a.meshAndSelf()
	if reg == nil {
		return Result{Text: "Local mesh is not enabled (control-only mode — no HTTP/WS interface).",
			Data: map[string]any{"peers": []any{}, "count": 0}}, nil
	}
	views := reg.List()
	peers := make([]map[string]any, 0, len(views))
	var b strings.Builder
	fmt.Fprintf(&b, "Local sessions (%d):\n", len(views))
	if len(views) == 0 {
		b.WriteString("  (none)\n")
	}
	for _, v := range views {
		marker := "  "
		if v.Name == self {
			marker = "→ "
		}
		line := v.Name
		if v.Harness != "" {
			line += " [" + v.Harness + "]"
		}
		fmt.Fprintf(&b, "  %s%s\n", marker, line)
		peers = append(peers, map[string]any{
			"name": v.Name, "harness": v.Harness, "transport": string(v.Transport),
			"online": true,
		})
	}
	return Result{Text: b.String(), Data: map[string]any{"peers": peers, "count": len(views)}}, nil
}

// ── names (Base) ─────────────────────────────────────────────────────────

// Lookup resolves a name→address or address→primary name.
func (a *Agent) Lookup(ctx context.Context, query string) (Result, error) {
	if query == "" {
		return Result{}, fmt.Errorf("query is required")
	}
	// Reverse: address → primary name.
	if strings.HasPrefix(query, "0x") {
		if a.names == nil {
			return Result{}, fmt.Errorf("Base RPC unavailable")
		}
		name, err := a.names.PrimaryNameOf(ctx, common.HexToAddress(query))
		if err != nil {
			return Result{}, err
		}
		if name == "" {
			return text("No primary .attn name set for %s", query), nil
		}
		return Result{Text: fmt.Sprintf("%s → %s.attn", query, name), Data: map[string]any{"name": name + ".attn"}}, nil
	}
	// Forward: name → address. Prefer relay resolve (also yields pubkey), fall
	// back to on-chain.
	label := strings.ToLower(strings.TrimSuffix(query, ".attn"))
	if a.sess.IsReady() {
		if addr, pub, err := a.sess.Resolve(ctx, label); err == nil && addr != "" {
			connected := " (never connected)"
			if pub != "" {
				connected = " (connected)"
			}
			return Result{Text: fmt.Sprintf("%s.attn → %s%s", label, addr, connected),
				Data: map[string]any{"address": addr, "connected": pub != ""}}, nil
		}
	}
	if a.names == nil {
		return Result{}, fmt.Errorf("Base RPC unavailable and relay resolve failed")
	}
	owner, _, err := a.names.Resolve(ctx, label)
	if err != nil {
		return Result{}, err
	}
	if owner == (common.Address{}) {
		return text("%q is not registered.", label+".attn"), nil
	}
	return Result{Text: fmt.Sprintf("%s.attn → %s", label, strings.ToLower(owner.Hex())),
		Data: map[string]any{"address": strings.ToLower(owner.Hex())}}, nil
}

// Names lists .attn names owned by an address (defaults to self).
func (a *Agent) Names(ctx context.Context, address string) (Result, error) {
	target := normalize(address)
	if target == "" {
		target = a.Address()
	}
	// Relay endpoint gives the full list.
	if resp, err := a.sess.PlainGet(ctx, "/names?address="+target); err == nil {
		defer resp.Body.Close()
		if resp.StatusCode/100 == 2 {
			var out struct {
				Names []string `json:"names"`
			}
			if json.NewDecoder(resp.Body).Decode(&out) == nil {
				if len(out.Names) == 0 {
					return text("No .attn names found for %s", target), nil
				}
				var b strings.Builder
				fmt.Fprintf(&b, "Names owned by %s:\n", target)
				for _, n := range out.Names {
					fmt.Fprintf(&b, "  %s.attn\n", n)
				}
				return Result{Text: b.String(), Data: map[string]any{"names": out.Names}}, nil
			}
		}
	}
	// Fallback: on-chain count.
	if a.names == nil {
		return Result{}, fmt.Errorf("Base RPC unavailable")
	}
	count, err := a.names.BalanceOf(ctx, common.HexToAddress(target))
	if err != nil {
		return Result{}, err
	}
	return Result{Text: fmt.Sprintf("%s owns %s .attn name(s). Connect to relay for the full listing.", target, count.String()),
		Data: map[string]any{"count": count.String()}}, nil
}

// RegisterName is GATED: it validates the path (availability, fee, eth_call
// simulation) but REFUSES the paid on-chain broadcast (0.001 ETH, irreversible).
func (a *Agent) RegisterName(ctx context.Context, label string) (Result, error) {
	if label == "" {
		return Result{}, fmt.Errorf("label is required")
	}
	if a.names == nil {
		return Result{}, fmt.Errorf("Base RPC unavailable")
	}
	label = strings.ToLower(strings.TrimSuffix(label, ".attn"))
	if len(label) < 3 || len(label) > 32 {
		return Result{}, fmt.Errorf("label must be 3-32 characters")
	}
	avail, err := a.names.Available(ctx, label)
	if err != nil {
		return Result{}, err
	}
	if !avail {
		return Result{}, fmt.Errorf("%q is already taken", label+".attn")
	}
	fee, err := a.names.RegistrationFee(ctx)
	if err != nil {
		return Result{}, err
	}
	simErr := a.names.SimulateRegister(ctx, common.HexToAddress(a.Address()), label, fee)
	calldata, gateErr := a.names.SendRegister(ctx, label, false) // allow=false → always gated
	return Result{
		Text: fmt.Sprintf("GATED — register %q would cost %s ETH (irreversible on Base mainnet). Path validated but NOT broadcast: %v",
			label+".attn", weiToEth(fee), gateErr),
		Data: map[string]any{
			"gated": true, "label": label, "fee_wei": fee.String(),
			"calldata":   "0x" + common.Bytes2Hex(calldata),
			"simulation": simErrString(simErr),
		},
	}, nil
}

// TransferName is GATED (on-chain ERC-721 transfer, costs gas).
func (a *Agent) TransferName(ctx context.Context, label, to string) (Result, error) {
	if label == "" {
		return Result{}, fmt.Errorf("label is required")
	}
	if !isValidAddress(to) {
		return Result{}, fmt.Errorf("invalid recipient address")
	}
	if a.names == nil {
		return Result{}, fmt.Errorf("Base RPC unavailable")
	}
	label = strings.ToLower(strings.TrimSuffix(label, ".attn"))
	node, err := a.names.Namehash(ctx, label)
	if err != nil {
		return Result{}, err
	}
	tokenID := names.TokenIDFromNode(node)
	calldata, gateErr := a.names.SendTransferName(ctx, common.HexToAddress(a.Address()), common.HexToAddress(to), tokenID, false)
	return Result{
		Text: fmt.Sprintf("GATED — transfer %q to %s would mutate on-chain state (gas, irreversible). NOT broadcast: %v", label+".attn", to, gateErr),
		Data: map[string]any{"gated": true, "label": label, "token_id": tokenID.String(), "calldata": "0x" + common.Bytes2Hex(calldata)},
	}, nil
}

// SetPrimaryName is GATED (on-chain state change, costs gas).
func (a *Agent) SetPrimaryName(ctx context.Context, label string) (Result, error) {
	if label == "" {
		return Result{}, fmt.Errorf("label is required")
	}
	if a.names == nil {
		return Result{}, fmt.Errorf("Base RPC unavailable")
	}
	label = strings.ToLower(strings.TrimSuffix(label, ".attn"))
	calldata, gateErr := a.names.SendSetPrimaryName(ctx, label, false)
	return Result{
		Text: fmt.Sprintf("GATED — set primary name to %q would mutate on-chain state (gas). NOT broadcast: %v", label+".attn", gateErr),
		Data: map[string]any{"gated": true, "label": label, "calldata": "0x" + common.Bytes2Hex(calldata)},
	}, nil
}

// ── mutes ────────────────────────────────────────────────────────────────

// Mute silences a target (agent, group, or "all"), optionally for a duration.
func (a *Agent) Mute(ctx context.Context, target, duration string) (Result, error) {
	if target == "" {
		return Result{}, fmt.Errorf("target is required")
	}
	var untilMs int64
	if duration != "" {
		d, ok := parseDuration(duration)
		if !ok {
			return Result{}, fmt.Errorf("invalid duration %q — use e.g. 30m, 1h, 1d, 7d", duration)
		}
		untilMs = time.Now().UnixMilli() + d.Milliseconds()
	}
	if isGlobalMuteTarget(target) {
		if err := a.st.AddMute(store.AllMuteTarget, store.MuteAll, untilMs); err != nil {
			return Result{}, err
		}
		return text("Muted all inbound%s. Messages still save to history but skip surfacing.", durSuffix(untilMs)), nil
	}
	tgt, kind, label, err := a.resolveMuteTarget(ctx, target)
	if err != nil {
		return Result{}, err
	}
	if err := a.st.AddMute(tgt, kind, untilMs); err != nil {
		return Result{}, err
	}
	return text("Muted %s %s%s.", kind, label, durSuffix(untilMs)), nil
}

// Unmute removes a mute and reports how many messages arrived while muted.
func (a *Agent) Unmute(ctx context.Context, target string) (Result, error) {
	if target == "" {
		return Result{}, fmt.Errorf("target is required")
	}
	if isGlobalMuteTarget(target) {
		since, ok, _ := a.st.GetMuteCreatedAt(store.AllMuteTarget, store.MuteAll)
		if !ok {
			return text("Global mute was not active"), nil
		}
		count, _ := a.st.CountAllInboundSince(since)
		_, _ = a.st.RemoveMute(store.AllMuteTarget, store.MuteAll)
		return text("Unmuted all%s", arrivedSuffix(count)), nil
	}
	tgt, kind, label, err := a.resolveMuteTarget(ctx, target)
	if err != nil {
		return Result{}, err
	}
	since, ok, _ := a.st.GetMuteCreatedAt(tgt, kind)
	if !ok {
		return text("%s was not muted", label), nil
	}
	count, _ := a.st.CountInboundSince(tgt, since)
	removed, _ := a.st.RemoveMute(tgt, kind)
	if !removed {
		return text("%s was not muted", label), nil
	}
	return text("Unmuted %s%s", label, arrivedSuffix(count)), nil
}

// Mutes lists active mutes.
func (a *Agent) Mutes() (Result, error) {
	mutes, _ := a.st.GetMutes()
	if len(mutes) == 0 {
		return text("No active mutes"), nil
	}
	now := time.Now().UnixMilli()
	var b strings.Builder
	fmt.Fprintf(&b, "Active mutes (%d):\n", len(mutes))
	for _, m := range mutes {
		remaining := "indefinite"
		if m.Until.Valid {
			remaining = formatRemaining(m.Until.Int64 - now)
		}
		if m.Kind == store.MuteAll {
			fmt.Fprintf(&b, "- all: global mute — %s\n", remaining)
			continue
		}
		name := m.Target
		if m.Kind == store.MuteAgent {
			if n, _ := a.st.GetContactName(m.Target); n != "" {
				name = n
			}
		} else {
			if n, _ := a.st.GetGroupName(m.Target); n != "" {
				name = n
			}
		}
		extra := ""
		if name != m.Target {
			extra = fmt.Sprintf(" (%s)", m.Target)
		}
		fmt.Fprintf(&b, "- %s: %s%s — %s\n", m.Kind, name, extra, remaining)
	}
	return Result{Text: b.String(), Data: map[string]any{"count": len(mutes)}}, nil
}

// ── status ───────────────────────────────────────────────────────────────

// Status sets this agent's availability (online/away) + optional message.
func (a *Agent) Status(state, message string) (Result, error) {
	if state != "online" && state != "away" {
		return Result{}, fmt.Errorf(`state must be "online" or "away"`)
	}
	message = strings.TrimSpace(message)
	a.mu.Lock()
	a.presenceState, a.presenceMessage = state, message
	a.mu.Unlock()
	_ = a.st.MetaSet("presence_state", state)
	_ = a.st.MetaSet("presence_message", message)
	relayErr := a.sess.SetPresence(state, message)
	suffix := ". Messages deliver immediately."
	if state == "away" {
		if message != "" {
			suffix = fmt.Sprintf(`. Senders will see: "away: %s". Messages queue at relay until you return.`, message)
		} else {
			suffix = ". Senders will see you as away. Messages queue at relay until you return."
		}
	}
	note := ""
	if relayErr != nil {
		note = " (relay offline — applied locally, will re-assert on reconnect)"
	}
	return Result{Text: fmt.Sprintf("Status set to %s%s%s", state, suffix, note),
		Data: map[string]any{"state": state, "message": message, "relay_applied": relayErr == nil}}, nil
}

// StatusOf queries another agent's availability.
func (a *Agent) StatusOf(ctx context.Context, target string) (Result, error) {
	if target == "" {
		return Result{}, fmt.Errorf("target is required")
	}
	addr := target
	label := target
	if !isValidAddress(target) {
		resolved, err := a.resolveName(ctx, target)
		if err != nil || resolved == "" {
			return Result{}, fmt.Errorf("could not resolve %q", target)
		}
		addr = resolved
		label = fmt.Sprintf("%s.attn (%s)", strings.TrimSuffix(strings.ToLower(target), ".attn"), addr)
	}
	state, msg, ok, err := a.sess.QueryPresence(ctx, addr)
	if err != nil {
		return Result{}, err
	}
	if !ok {
		return text("%s: unknown (no response from relay)", label), nil
	}
	suffix := ""
	if msg != "" {
		suffix = fmt.Sprintf(`: "%s"`, msg)
	}
	return Result{Text: fmt.Sprintf("%s is %s%s", label, state, suffix), Data: map[string]any{"state": state, "message": msg}}, nil
}

// ── shared helpers ───────────────────────────────────────────────────────

// resolveName cascades relay → HTTP → on-chain → contacts DB, mirroring the
// upstream resolveAttnName, and pre-approves the resolved address as a contact.
func (a *Agent) resolveName(ctx context.Context, input string) (string, error) {
	label := strings.ToLower(strings.TrimSuffix(strings.TrimSpace(input), ".attn"))
	if len(label) < 3 {
		return "", fmt.Errorf("invalid name")
	}
	var addr string
	if a.sess.IsReady() {
		if r, pub, err := a.sess.Resolve(ctx, label); err == nil && r != "" {
			addr = r
			if pub != "" {
				_ = a.st.SaveKeyCache(addr, pub)
			}
		}
	}
	if addr == "" {
		if resp, err := a.sess.PlainGet(ctx, "/resolve?name="+label); err == nil {
			func() {
				defer resp.Body.Close()
				if resp.StatusCode/100 == 2 {
					var out struct {
						Address string `json:"address"`
					}
					if json.NewDecoder(resp.Body).Decode(&out) == nil && out.Address != "" && out.Address != "0x0000000000000000000000000000000000000000" {
						addr = normalize(out.Address)
					}
				}
			}()
		}
	}
	if addr == "" && a.names != nil {
		if owner, _, err := a.names.Resolve(ctx, label); err == nil && owner != (common.Address{}) {
			addr = strings.ToLower(owner.Hex())
		}
	}
	if addr == "" {
		if c, _ := a.st.GetContactByName(label + ".attn"); c != "" {
			addr = c
		} else if c, _ := a.st.GetContactByName(label); c != "" {
			addr = c
		}
	}
	if addr != "" {
		a.addContactAndFlush(ctx, addr, label+".attn")
	}
	return addr, nil
}

func (a *Agent) resolveMuteTarget(ctx context.Context, input string) (target string, kind store.MuteKind, label string, err error) {
	t := strings.TrimSpace(input)
	if isValidAddress(t) {
		return normalize(t), store.MuteAgent, normalize(t), nil
	}
	groups, _ := a.st.GetGroups()
	for _, g := range groups {
		if g.ID == t {
			return g.ID, store.MuteGroup, fmt.Sprintf("%s (%s)", g.Name, g.ID), nil
		}
	}
	addr, _ := a.resolveName(ctx, t)
	if addr != "" {
		return addr, store.MuteAgent, fmt.Sprintf("%s.attn (%s)", strings.TrimSuffix(strings.ToLower(t), ".attn"), addr), nil
	}
	return "", "", "", fmt.Errorf("could not resolve %q — expected address, .attn name, group ID, or \"all\"", input)
}

// encryptForMembers builds the per-recipient ciphertext map for a group send.
func (a *Agent) encryptForMembers(ctx context.Context, members []store.GroupMember, plaintext string) map[string]string {
	blobs := map[string]string{}
	for _, m := range members {
		if normalize(m.Address) == a.Address() {
			continue
		}
		pub := a.cachedKey(m.Address)
		if pub == "" {
			pub, _ = a.sess.GetKey(ctx, m.Address)
		}
		if pub == "" {
			continue
		}
		ct, err := icrypto.EncryptBase64(pub, []byte(plaintext))
		if err == nil {
			blobs[normalize(m.Address)] = ct
		}
	}
	return blobs
}

func (a *Agent) fetchPrimaryName(ctx context.Context, address string) string {
	resp, err := a.sess.PlainGet(ctx, "/primary?address="+normalize(address))
	if err != nil {
		return ""
	}
	defer resp.Body.Close()
	if resp.StatusCode/100 != 2 {
		return ""
	}
	var out struct {
		Name string `json:"name"`
	}
	if json.NewDecoder(resp.Body).Decode(&out) == nil {
		return out.Name
	}
	return ""
}

// signedJSON performs a signed relay request with a JSON body and returns the
// response body, erroring on non-2xx.
func (a *Agent) signedJSON(ctx context.Context, method, path string, body any) ([]byte, error) {
	raw, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	return a.signedRaw(ctx, method, path, raw)
}

func (a *Agent) signedRaw(ctx context.Context, method, path string, body []byte) ([]byte, error) {
	var rdr io.Reader
	headers := map[string]string{}
	if body != nil {
		rdr = strings.NewReader(string(body))
		headers["Content-Type"] = "application/json"
	}
	resp, err := a.sess.SignedRequest(ctx, method, path, rdr, headers)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	respBody, _ := io.ReadAll(resp.Body)
	if resp.StatusCode/100 != 2 {
		return nil, fmt.Errorf("relay %s %s failed (%d): %s", method, path, resp.StatusCode, strings.TrimSpace(string(respBody)))
	}
	return respBody, nil
}
