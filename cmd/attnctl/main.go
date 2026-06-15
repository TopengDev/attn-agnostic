// Command attnctl drives a running attnd daemon over its Unix control socket.
// It is the thin invocation path used to exercise + verify every outbound op
// against the live network through the daemon's single connection.
//
// Usage:
//
//	attnctl [--sock PATH] [--json] <op> [key=value ...]
//	attnctl send to=0x.. message="hello"
//	attnctl send to=chilldawg.attn message="hi"
//	attnctl create_group name=devs members=0xaaa..,0xbbb..
//	attnctl history with=0x.. limit=10
//	attnctl _info        # daemon address + relay readiness
//	attnctl _drop_conn   # force a reconnect (test)
//
// Numeric values (limit) and comma-lists (members) are parsed automatically;
// pass --json to send a raw JSON args object as the final argument instead.
package main

import (
	"encoding/json"
	"fmt"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/TopengDev/attn-agnostic/internal/config"
	"github.com/TopengDev/attn-agnostic/internal/control"
)

func main() {
	args := os.Args[1:]
	sock := ""
	rawJSON := false

	// Parse leading flags.
	for len(args) > 0 && strings.HasPrefix(args[0], "--") {
		switch {
		case args[0] == "--sock" && len(args) >= 2:
			sock, args = args[1], args[2:]
		case args[0] == "--json":
			rawJSON, args = true, args[1:]
		default:
			fmt.Fprintf(os.Stderr, "unknown flag: %s\n", args[0])
			os.Exit(2)
		}
	}
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: attnctl [--sock PATH] [--json] <op> [key=value ...]")
		os.Exit(2)
	}

	op := args[0]
	rest := args[1:]

	opArgs := map[string]any{}
	if rawJSON {
		if len(rest) != 1 {
			fmt.Fprintln(os.Stderr, "--json expects exactly one JSON object argument")
			os.Exit(2)
		}
		if err := json.Unmarshal([]byte(rest[0]), &opArgs); err != nil {
			fmt.Fprintf(os.Stderr, "invalid --json: %v\n", err)
			os.Exit(2)
		}
	} else {
		for _, kv := range rest {
			i := strings.IndexByte(kv, '=')
			if i < 0 {
				fmt.Fprintf(os.Stderr, "expected key=value, got %q\n", kv)
				os.Exit(2)
			}
			opArgs[kv[:i]] = coerce(kv[:i], kv[i+1:])
		}
	}

	if sock == "" {
		if home, err := config.Home(); err == nil {
			sock = home + "/attnd.sock"
		} else {
			fmt.Fprintf(os.Stderr, "resolve home: %v\n", err)
			os.Exit(1)
		}
	}

	resp, err := control.Call(sock, control.Request{Op: op, Args: opArgs}, 35*time.Second)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if resp.Text != "" {
		fmt.Println(resp.Text)
	}
	if len(resp.Data) > 0 {
		b, _ := json.MarshalIndent(resp.Data, "", "  ")
		fmt.Println("data:", string(b))
	}
	if !resp.OK {
		if resp.Error != "" {
			fmt.Fprintf(os.Stderr, "error: %s\n", resp.Error)
		}
		os.Exit(1)
	}
}

// coerce turns string values into the right type: `limit` → int, `members` →
// comma-separated list, everything else stays a string.
func coerce(key, val string) any {
	switch key {
	case "limit":
		if n, err := strconv.Atoi(val); err == nil {
			return n
		}
	case "members":
		parts := strings.Split(val, ",")
		out := make([]string, 0, len(parts))
		for _, p := range parts {
			if p = strings.TrimSpace(p); p != "" {
				out = append(out, p)
			}
		}
		return out
	case "unblock":
		if b, err := strconv.ParseBool(val); err == nil {
			return b
		}
	}
	return val
}
