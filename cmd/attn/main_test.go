package main

import "testing"

// TestParseInvocationNoSilentTruncation is the H3 regression test: an unknown
// mid-message flag must be rejected, not silently consume a message word.
func TestParseInvocationNoSilentTruncation(t *testing.T) {
	// Pre-fix: `--x` consumed "world" as its value and the daemon sent only
	// "hello". Now it must error.
	if _, err := parseInvocation("send", []string{"0xabc", "hello", "--x", "world"}); err == nil {
		t.Fatal("expected an error for an unknown flag, got nil (silent truncation)")
	}
}

func TestParseInvocationEndOfFlagsSentinel(t *testing.T) {
	inv, err := parseInvocation("send", []string{"0xabc", "--", "hello", "--x", "world"})
	if err != nil {
		t.Fatalf("`--` sentinel should allow literal dashes: %v", err)
	}
	if inv.op != "send" || inv.args["to"] != "0xabc" {
		t.Fatalf("bad parse: %+v", inv.args)
	}
	if got := inv.args["message"]; got != "hello --x world" {
		t.Fatalf("message = %q, want the full literal %q", got, "hello --x world")
	}
}

func TestParseInvocationHappyPaths(t *testing.T) {
	// Plain send.
	inv, err := parseInvocation("send", []string{"0xabc", "hello there"})
	if err != nil {
		t.Fatal(err)
	}
	if inv.args["to"] != "0xabc" || inv.args["message"] != "hello there" {
		t.Fatalf("bad send args: %+v", inv.args)
	}

	// Known flag is accepted.
	inv, err = parseInvocation("history", []string{"0xabc", "--limit", "5"})
	if err != nil {
		t.Fatalf("history --limit should be accepted: %v", err)
	}
	if inv.args["limit"] != "5" {
		t.Fatalf("limit = %v, want 5", inv.args["limit"])
	}

	// Known bool flag.
	inv, err = parseInvocation("block", []string{"0xabc", "--unblock"})
	if err != nil {
		t.Fatalf("block --unblock should be accepted: %v", err)
	}
	if inv.args["unblock"] != true {
		t.Fatalf("unblock = %v, want true", inv.args["unblock"])
	}

	// status with no positional → show.
	inv, err = parseInvocation("status", nil)
	if err != nil {
		t.Fatal(err)
	}
	if !inv.statusShow {
		t.Fatal("status with no arg should be statusShow")
	}

	// op escape hatch accepts arbitrary flags as args.
	inv, err = parseInvocation("op", []string{"send", "--to", "0xabc", "--message", "hi"})
	if err != nil {
		t.Fatalf("op escape hatch should accept arbitrary flags: %v", err)
	}
	if inv.op != "send" || inv.args["to"] != "0xabc" || inv.args["message"] != "hi" {
		t.Fatalf("bad op args: %+v", inv.args)
	}
}
