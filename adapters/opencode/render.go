package opencode

import (
	"fmt"
	"strings"
)

// maxRenderBody caps the injected body so a hostile inbound can't blow up an
// opencode turn (the daemon already read-limits frames; this is defense in depth).
const maxRenderBody = 16 << 10 // 16 KiB

// Render turns an inbound attn frame into the text injected into the opencode
// session. Inbound content is UNTRUSTED: it is rendered as clearly-delimited,
// labelled DATA with an explicit provenance header, never as instructions. The
// model sees an "attn inbound" envelope it can choose to act on, the same way
// Claude Code surfaces a <channel> message.
//
// The header is greppable (carries from + a stable marker) so a session's
// transcript can be asserted in tests / the live mesh no-leak proof.
func Render(f InboundFrame) string {
	from := f.From
	if f.AgentName != "" && f.AgentName != f.From {
		from = fmt.Sprintf("%s (%s)", f.AgentName, f.From)
	}
	if from == "" {
		from = "unknown"
	}

	kind := "message"
	switch {
	case f.Type == "file":
		kind = "file"
	case f.Local:
		kind = "local message"
	}

	scope := ""
	if f.GroupID != "" {
		name := f.GroupName
		if name == "" {
			name = f.GroupID
		}
		scope = fmt.Sprintf(" · group %s", name)
	}

	var body string
	if f.Type == "file" {
		name := f.Filename
		if name == "" {
			name = f.Path
		}
		body = fmt.Sprintf("[received file: %s (%d bytes) at %s]", name, f.Size, f.Path)
	} else {
		body = f.Message
	}
	if len(body) > maxRenderBody {
		body = body[:maxRenderBody] + "\n…[truncated]"
	}

	// Provenance header + fenced body. The header labels the source as untrusted
	// relayed/local data so the model treats the body as content, not commands.
	header := fmt.Sprintf("📨 attn inbound · %s · from %s%s", kind, from, scope)
	return strings.Join([]string{header, body}, "\n")
}
