package opencode

import (
	"strings"
	"testing"
)

func TestRenderRelayMessage(t *testing.T) {
	out := Render(InboundFrame{Type: "message", From: "0xabc", Message: "hello there"})
	if !strings.Contains(out, "from 0xabc") {
		t.Errorf("missing provenance from: %q", out)
	}
	if !strings.Contains(out, "hello there") {
		t.Errorf("missing body: %q", out)
	}
	if !strings.HasPrefix(out, "📨 attn inbound · message ·") {
		t.Errorf("unexpected header: %q", out)
	}
}

func TestRenderLocalMessageUsesAgentName(t *testing.T) {
	out := Render(InboundFrame{Type: "message", From: "main", Message: "ping", Local: true, AgentName: "main"})
	if !strings.Contains(out, "local message") {
		t.Errorf("local frame should be labelled local message: %q", out)
	}
	// AgentName == From → no duplicated "(addr)"
	if strings.Contains(out, "main (main)") {
		t.Errorf("should not duplicate identical name/from: %q", out)
	}

	out2 := Render(InboundFrame{Type: "message", From: "0xdead", Message: "hi", AgentName: "alice"})
	if !strings.Contains(out2, "alice (0xdead)") {
		t.Errorf("want resolved-name (addr) form: %q", out2)
	}
}

func TestRenderFile(t *testing.T) {
	out := Render(InboundFrame{Type: "file", From: "bob", Filename: "report.pdf", Size: 2048, Path: "/inbox/report.pdf"})
	if !strings.Contains(out, "received file: report.pdf") || !strings.Contains(out, "2048 bytes") {
		t.Errorf("file render wrong: %q", out)
	}
}

func TestRenderGroupScope(t *testing.T) {
	out := Render(InboundFrame{Type: "message", From: "x", Message: "m", GroupID: "g1", GroupName: "builders"})
	if !strings.Contains(out, "group builders") {
		t.Errorf("missing group scope: %q", out)
	}
}

func TestRenderTruncatesHostileBody(t *testing.T) {
	huge := strings.Repeat("A", maxRenderBody+5000)
	out := Render(InboundFrame{Type: "message", From: "x", Message: huge})
	if len(out) > maxRenderBody+200 {
		t.Errorf("body not truncated: len=%d", len(out))
	}
	if !strings.Contains(out, "[truncated]") {
		t.Errorf("missing truncation marker: %q", out[len(out)-50:])
	}
}

func TestRenderUnknownFromFallsBack(t *testing.T) {
	out := Render(InboundFrame{Type: "message", Message: "m"})
	if !strings.Contains(out, "from unknown") {
		t.Errorf("empty from should fall back to unknown: %q", out)
	}
}
