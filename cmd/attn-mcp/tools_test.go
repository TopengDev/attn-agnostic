package main

import "testing"

// canonical is the exact 29-tool set from the upstream attn plugin's ListTools
// handler. The MCP server MUST expose this set with matching names + required
// fields so MCP-native harnesses are drop-in.
var canonical = map[string][]string{
	"send":                 {"to", "message"},
	"reply":                {"message"},
	"send_file":            {"to", "path"},
	"history":              {"with"},
	"add_contact":          {"address"},
	"remove_contact":       {"address"},
	"block":                {"address"},
	"contacts":             {},
	"create_group":         {"name", "members"},
	"send_group":           {"group_id", "message"},
	"add_to_group":         {"group_id", "address"},
	"leave_group":          {"group_id"},
	"accept_group":         {"group_id"},
	"decline_group":        {"group_id"},
	"kick_from_group":      {"group_id", "address"},
	"transfer_group_admin": {"group_id", "address"},
	"groups":               {},
	"peers":                {},
	"react":                {"emoji"},
	"register_name":        {"label"},
	"lookup":               {"query"},
	"names":                {},
	"transfer_name":        {"label", "to"},
	"set_primary_name":     {"label"},
	"mute":                 {"target"},
	"unmute":               {"target"},
	"mutes":                {},
	"status":               {"state"},
	"status_of":            {"target"},
}

func TestToolSurfaceParity(t *testing.T) {
	if len(toolDefs) != 29 {
		t.Fatalf("toolDefs has %d entries, want 29", len(toolDefs))
	}
	seen := map[string]bool{}
	for _, td := range toolDefs {
		seen[td.name] = true
		wantReq, ok := canonical[td.name]
		if !ok {
			t.Errorf("tool %q is not in the canonical upstream set", td.name)
			continue
		}
		if len(td.required) != len(wantReq) {
			t.Errorf("tool %q required = %v, want %v", td.name, td.required, wantReq)
			continue
		}
		want := map[string]bool{}
		for _, r := range wantReq {
			want[r] = true
		}
		for _, r := range td.required {
			if !want[r] {
				t.Errorf("tool %q has unexpected required field %q", td.name, r)
			}
		}
		// Every required field must be present in properties.
		for _, r := range td.required {
			if td.props == nil || td.props[r] == nil {
				t.Errorf("tool %q requires %q but has no such property", td.name, r)
			}
		}
		// Schema must build with type=object.
		if sc := td.schema(); sc.Type != "object" {
			t.Errorf("tool %q schema type = %q, want object", td.name, sc.Type)
		}
	}
	for name := range canonical {
		if !seen[name] {
			t.Errorf("canonical tool %q is missing from toolDefs", name)
		}
	}
}
