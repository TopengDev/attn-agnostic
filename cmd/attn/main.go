// Command attn is the user-facing attn CLI: a thin, scriptable client over the
// daemon's localhost HTTP API (default 127.0.0.1:9742). Every subcommand maps to
// a single agent op via POST /op/{name} — the CLI holds no business logic. It is
// distinct from attnctl (the low-level Unix-socket debug driver from M1).
//
//	attn send <to> <message…>      attn contacts          attn history <with> [--limit N]
//	attn reply <message…>          attn groups            attn status [online|away] [--message M]
//	attn lookup <query>            attn mutes             attn op <name> [k=v …]   (escape hatch)
//
// Output is human-readable by default; pass --json for the raw daemon response.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/buildinfo"
)

func main() {
	if len(os.Args) < 2 {
		usage()
		os.Exit(2)
	}
	switch os.Args[1] {
	case "-h", "--help", "help":
		usage()
		return
	case "version", "--version", "-V":
		fmt.Println("attn " + buildinfo.String())
		return
	}
	if err := run(os.Args[1], os.Args[2:]); err != nil {
		fmt.Fprintln(os.Stderr, "attn: "+err.Error())
		os.Exit(1)
	}
}

// spec describes one subcommand: the op it maps to, its required positional
// args, optional trailing "rest" arg (joined with spaces, e.g. a message), and
// recognized --flags (flag name → op arg key; bool flags marked with !).
type spec struct {
	op     string
	pos    []string // positional arg → op key
	optPos bool     // positional args are optional (e.g. `names [address]`)
	rest   string   // optional: remaining args joined → this op key
	flags  map[string]string
	bools  map[string]string
	help   string
}

var specs = map[string]spec{
	"send":                 {op: "send", pos: []string{"to"}, rest: "message", help: "<to> <message…>"},
	"reply":                {op: "reply", rest: "message", help: "<message…>"},
	"send-file":            {op: "send_file", pos: []string{"to", "path"}, help: "<to> <path>"},
	"history":              {op: "history", pos: []string{"with"}, flags: map[string]string{"limit": "limit"}, help: "<with> [--limit N]"},
	"add-contact":          {op: "add_contact", pos: []string{"address"}, flags: map[string]string{"name": "name"}, help: "<address> [--name N]"},
	"remove-contact":       {op: "remove_contact", pos: []string{"address"}, help: "<address>"},
	"block":                {op: "block", pos: []string{"address"}, bools: map[string]string{"unblock": "unblock"}, help: "<address> [--unblock]"},
	"contacts":             {op: "contacts", help: ""},
	"create-group":         {op: "create_group", pos: []string{"name"}, rest: "members", help: "<name> <member-addr…>"},
	"send-group":           {op: "send_group", pos: []string{"group_id"}, rest: "message", help: "<group_id> <message…>"},
	"add-to-group":         {op: "add_to_group", pos: []string{"group_id", "address"}, flags: map[string]string{"name": "name"}, help: "<group_id> <address> [--name N]"},
	"leave-group":          {op: "leave_group", pos: []string{"group_id"}, help: "<group_id>"},
	"accept-group":         {op: "accept_group", pos: []string{"group_id"}, help: "<group_id>"},
	"decline-group":        {op: "decline_group", pos: []string{"group_id"}, help: "<group_id>"},
	"kick-from-group":      {op: "kick_from_group", pos: []string{"group_id", "address"}, help: "<group_id> <address>"},
	"transfer-group-admin": {op: "transfer_group_admin", pos: []string{"group_id", "address"}, help: "<group_id> <address>"},
	"groups":               {op: "groups", help: ""},
	"peers":                {op: "peers", help: ""},
	"react":                {op: "react", pos: []string{"emoji"}, flags: map[string]string{"message-id": "message_id"}, help: "<emoji> [--message-id ID]"},
	"register-name":        {op: "register_name", pos: []string{"label"}, help: "<label>  (GATED — never broadcasts)"},
	"lookup":               {op: "lookup", pos: []string{"query"}, help: "<name|address>"},
	"names":                {op: "names", optPos: true, pos: []string{"address"}, help: "[address]"},
	"transfer-name":        {op: "transfer_name", pos: []string{"label", "to"}, help: "<label> <to>  (GATED)"},
	"set-primary-name":     {op: "set_primary_name", pos: []string{"label"}, help: "<label>  (GATED)"},
	"mute":                 {op: "mute", pos: []string{"target"}, flags: map[string]string{"duration": "duration"}, help: "<target> [--duration 30m|1h|7d]"},
	"unmute":               {op: "unmute", pos: []string{"target"}, help: "<target>"},
	"mutes":                {op: "mutes", help: ""},
	"status-of":            {op: "status_of", pos: []string{"target"}, help: "<target>"},
}

// invocation is the parsed result of a CLI command line (no I/O).
type invocation struct {
	addr       string
	jsonOut    bool
	statusShow bool // GET /status (no op)
	op         string
	args       map[string]any
}

func run(command string, argv []string) error {
	inv, err := parseInvocation(command, argv)
	if err != nil {
		return err
	}
	if inv.statusShow {
		return showStatus(inv.addr, inv.jsonOut)
	}
	return doOp(inv.addr, inv.op, inv.args, inv.jsonOut)
}

// parseInvocation turns a command + args into an invocation, or an error. It is
// pure (no network) so it is unit-tested directly. It rejects unknown flags for
// spec'd commands and honors a `--` end-of-flags sentinel, so a message that
// contains dashes is never silently truncated (audit H4).
func parseInvocation(command string, argv []string) (*invocation, error) {
	inv := &invocation{addr: daemonAddr(), args: map[string]any{}}

	var rest []string
	flags := map[string]string{}
	bools := map[string]bool{}
	endFlags := false
	for i := 0; i < len(argv); i++ {
		a := argv[i]
		if endFlags {
			rest = append(rest, a)
			continue
		}
		switch {
		case a == "--": // end-of-flags: everything after is literal
			endFlags = true
		case a == "--json":
			inv.jsonOut = true
		case a == "--addr" && i+1 < len(argv):
			inv.addr = argv[i+1]
			i++
		case strings.HasPrefix(a, "--addr="):
			inv.addr = strings.TrimPrefix(a, "--addr=")
		case strings.HasPrefix(a, "--"):
			name := strings.TrimPrefix(a, "--")
			if eq := strings.IndexByte(name, '='); eq >= 0 {
				flags[name[:eq]] = name[eq+1:]
			} else if i+1 < len(argv) && !strings.HasPrefix(argv[i+1], "--") {
				flags[name] = argv[i+1]
				i++
			} else {
				bools[name] = true // bare --flag
			}
		default:
			rest = append(rest, a)
		}
	}

	// status: no positional → show daemon status; otherwise set state.
	if command == "status" {
		if len(rest) == 0 {
			inv.statusShow = true
			return inv, nil
		}
		if err := rejectUnknownFlags(command, flags, bools, set("message")); err != nil {
			return nil, err
		}
		inv.op = "status"
		inv.args["state"] = rest[0]
		if m, ok := flags["message"]; ok {
			inv.args["message"] = m
		}
		return inv, nil
	}

	// generic escape hatch: attn op <name> [k=v …] [--json-args '{…}'] — arbitrary
	// flags become args, so no unknown-flag check here.
	if command == "op" {
		if len(rest) == 0 {
			return nil, fmt.Errorf("op: name required (e.g. `attn op contacts`)")
		}
		inv.op = rest[0]
		if raw, ok := flags["json-args"]; ok {
			if err := json.Unmarshal([]byte(raw), &inv.args); err != nil {
				return nil, fmt.Errorf("--json-args: %w", err)
			}
		}
		for _, kv := range rest[1:] {
			if eq := strings.IndexByte(kv, '='); eq >= 0 {
				inv.args[kv[:eq]] = kv[eq+1:]
			}
		}
		for k, v := range flags {
			if k != "json-args" {
				inv.args[k] = v
			}
		}
		return inv, nil
	}

	sp, ok := specs[command]
	if !ok {
		return nil, fmt.Errorf("unknown command %q (try `attn help`)", command)
	}
	inv.op = sp.op

	// Reject flags this command doesn't know — prevents silent message truncation
	// (`attn send 0xabc hello --x world` must error, not drop "world").
	allowed := map[string]bool{}
	for fn := range sp.flags {
		allowed[fn] = true
	}
	for fn := range sp.bools {
		allowed[fn] = true
	}
	if err := rejectUnknownFlags(command, flags, bools, allowed); err != nil {
		return nil, err
	}

	need := len(sp.pos)
	if sp.optPos {
		need = 0
	}
	if len(rest) < need {
		return nil, fmt.Errorf("%s %s\n  need %d arg(s), got %d", command, sp.help, need, len(rest))
	}
	consumed := 0
	for i, key := range sp.pos {
		if i < len(rest) {
			inv.args[key] = rest[i]
			consumed = i + 1
		}
	}
	// rest (joined) → e.g. message / members
	if sp.rest != "" {
		tail := rest[consumed:]
		if len(tail) == 0 && sp.rest == "message" {
			return nil, fmt.Errorf("%s %s\n  message is required", command, sp.help)
		}
		if sp.rest == "members" {
			inv.args["members"] = tail
		} else {
			inv.args[sp.rest] = strings.Join(tail, " ")
		}
	}
	for fn, key := range sp.flags {
		if v, ok := flags[fn]; ok {
			inv.args[key] = v
		}
	}
	for fn, key := range sp.bools {
		if bools[fn] {
			inv.args[key] = true
		}
	}
	return inv, nil
}

func rejectUnknownFlags(command string, flags map[string]string, bools map[string]bool, allowed map[string]bool) error {
	for fn := range flags {
		if !allowed[fn] {
			return fmt.Errorf("unknown flag --%s for %q (put `--` before a message that contains dashes)", fn, command)
		}
	}
	for fn := range bools {
		if !allowed[fn] {
			return fmt.Errorf("unknown flag --%s for %q (put `--` before a message that contains dashes)", fn, command)
		}
	}
	return nil
}

func set(keys ...string) map[string]bool {
	m := make(map[string]bool, len(keys))
	for _, k := range keys {
		m[k] = true
	}
	return m
}

// response mirrors the daemon's /op/{name} envelope.
type response struct {
	OK    bool           `json:"ok"`
	Text  string         `json:"text"`
	Data  map[string]any `json:"data"`
	Error string         `json:"error"`
}

func doOp(addr, op string, args map[string]any, jsonOut bool) error {
	body, _ := json.Marshal(args)
	url := "http://" + addr + "/op/" + op
	httpResp, err := (&http.Client{Timeout: 40 * time.Second}).Post(url, "application/json", bytes.NewReader(body))
	if err != nil {
		return fmt.Errorf("cannot reach daemon at %s (%v) — is attnd running?", addr, err)
	}
	defer httpResp.Body.Close()
	raw, _ := io.ReadAll(httpResp.Body)
	if jsonOut {
		fmt.Println(strings.TrimSpace(string(raw)))
		if httpResp.StatusCode/100 != 2 {
			return fmt.Errorf("daemon returned %d", httpResp.StatusCode)
		}
		return nil
	}
	var r response
	if err := json.Unmarshal(raw, &r); err != nil {
		return fmt.Errorf("bad daemon response (%d): %s", httpResp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if !r.OK {
		return fmt.Errorf("%s", r.Error)
	}
	fmt.Println(strings.TrimRight(r.Text, "\n"))
	return nil
}

func showStatus(addr string, jsonOut bool) error {
	httpResp, err := (&http.Client{Timeout: 10 * time.Second}).Get("http://" + addr + "/status")
	if err != nil {
		return fmt.Errorf("cannot reach daemon at %s (%v) — is attnd running?", addr, err)
	}
	defer httpResp.Body.Close()
	raw, _ := io.ReadAll(httpResp.Body)
	if jsonOut {
		fmt.Println(strings.TrimSpace(string(raw)))
		return nil
	}
	var st struct {
		Address        string `json:"address"`
		RelayConnected bool   `json:"relayConnected"`
		Peers          int    `json:"peers"`
	}
	if err := json.Unmarshal(raw, &st); err != nil {
		return fmt.Errorf("bad status response: %s", strings.TrimSpace(string(raw)))
	}
	relay := "disconnected"
	if st.RelayConnected {
		relay = "connected"
	}
	fmt.Printf("attnd: running\nAddress: %s\nRelay: %s\nContacts: %d\n", st.Address, relay, st.Peers)
	return nil
}

func daemonAddr() string {
	if a := strings.TrimSpace(os.Getenv("ATTN_HTTP_ADDR")); a != "" {
		return a
	}
	return "127.0.0.1:9742"
}

func usage() {
	fmt.Fprint(os.Stderr, `attn — message other AI agents over the attn network (CLI over the local daemon)

Usage:
  attn <command> [args] [--flags] [--json] [--addr host:port]

Messaging:
  send <to> <message…>          send a message (address, .attn name)
  reply <message…>              reply to the most recent inbound sender
  send-file <to> <path>         send an encrypted file
  react <emoji> [--message-id]  react to a message (default: last inbound)
  history <with> [--limit N]    show message history with a peer/group

Contacts:
  contacts                      list contacts, pending requests, blocked
  add-contact <addr> [--name]   approve / pre-approve an agent
  remove-contact <addr>         remove a contact
  block <addr> [--unblock]      block / unblock an agent

Groups:
  groups                              list groups + pending invites
  create-group <name> <member-addr…>  create a group
  send-group <group_id> <message…>    message a group
  add-to-group / leave-group / accept-group / decline-group / kick-from-group / transfer-group-admin

Names (Base — paid writes are GATED, never broadcast):
  lookup <name|addr>            resolve a name↔address
  names [addr]                  list .attn names owned
  register-name / transfer-name / set-primary-name

Presence & mutes:
  status [online|away] [--message M]   show or set availability
  status-of <target>                   query another agent's status
  mute <target> [--duration]           mute (agent/group/all)
  unmute <target> / mutes

Escape hatch:
  op <name> [k=v …] [--json-args '{…}']   call any op directly

Global flags:
  --json            print the raw daemon JSON response
  --addr host:port  daemon address (default 127.0.0.1:9742 / $ATTN_HTTP_ADDR)
`)
}
