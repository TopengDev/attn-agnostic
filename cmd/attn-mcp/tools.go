package main

import "github.com/google/jsonschema-go/jsonschema"

// toolDef is one MCP tool: name + description + input schema. The names,
// descriptions, and schemas mirror the upstream attn plugin's ListTools handler
// (s0nderlabs/attn packages/plugin/src/server.ts) so MCP-native harnesses see an
// identical 29-tool surface. Each tool forwards to the daemon's POST /op/{name}.
type toolDef struct {
	name     string
	desc     string
	props    map[string]*jsonschema.Schema
	required []string
}

func str(desc string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "string", Description: desc}
}
func num(desc string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "number", Description: desc}
}
func boolp(desc string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "boolean", Description: desc}
}
func strArr(desc string) *jsonschema.Schema {
	return &jsonschema.Schema{Type: "array", Description: desc, Items: &jsonschema.Schema{Type: "string"}}
}
func enumStr(desc string, vals ...string) *jsonschema.Schema {
	e := make([]any, len(vals))
	for i, v := range vals {
		e[i] = v
	}
	return &jsonschema.Schema{Type: "string", Description: desc, Enum: e}
}

// schema builds the object input schema for a tool.
func (t toolDef) schema() *jsonschema.Schema {
	props := t.props
	if props == nil {
		props = map[string]*jsonschema.Schema{}
	}
	req := t.required
	if req == nil {
		req = []string{}
	}
	return &jsonschema.Schema{Type: "object", Properties: props, Required: req}
}

var toolDefs = []toolDef{
	{
		name: "send",
		desc: `Send a message to another agent by address, .attn name, or local session name. Plain names (e.g. "chilldawg") try local peers first, then .attn name resolution.`,
		props: map[string]*jsonschema.Schema{
			"to":      str(`Local session name (e.g., "bob"), "all" for local broadcast, or Ethereum address (0x...)`),
			"message": str("Message text to send"),
		},
		required: []string{"to", "message"},
	},
	{
		name:     "reply",
		desc:     "Reply to the agent who most recently sent a message.",
		props:    map[string]*jsonschema.Schema{"message": str("Message text to send")},
		required: []string{"message"},
	},
	{
		name: "send_file",
		desc: "Send an encrypted file to another agent. The file is encrypted, uploaded to the relay, and a reference is sent as a message.",
		props: map[string]*jsonschema.Schema{
			"to":   str("Recipient Ethereum address (0x...)"),
			"path": str("Absolute path to the file to send"),
		},
		required: []string{"to", "path"},
	},
	{
		name: "history",
		desc: "Fetch recent message history with a specific agent or group from the local database.",
		props: map[string]*jsonschema.Schema{
			"with":  str("Agent address, group ID, or local session name to fetch history with"),
			"limit": num("Number of recent messages to return (default: 20)"),
		},
		required: []string{"with"},
	},
	{
		name: "add_contact",
		desc: "Add an agent to your contacts. Messages from contacts are delivered immediately; unknown agents go to pending.",
		props: map[string]*jsonschema.Schema{
			"address": str("Agent Ethereum address to add (0x...)"),
			"name":    str("Optional display name for this agent"),
		},
		required: []string{"address"},
	},
	{
		name:     "remove_contact",
		desc:     "Remove an agent from your contacts. Messages from them will go to the pending queue again.",
		props:    map[string]*jsonschema.Schema{"address": str("Agent Ethereum address to remove (0x...)")},
		required: []string{"address"},
	},
	{
		name: "block",
		desc: "Block an agent. All messages from them will be silently dropped. Also removes from contacts.",
		props: map[string]*jsonschema.Schema{
			"address": str("Agent Ethereum address to block (0x...)"),
			"unblock": boolp("Set to true to unblock instead of block"),
		},
		required: []string{"address"},
	},
	{
		name: "contacts",
		desc: "List your contacts, pending message requests, and blocked agents.",
	},
	{
		name: "create_group",
		desc: "Create a group for multi-agent messaging. All members receive every message.",
		props: map[string]*jsonschema.Schema{
			"name":    str("Group name"),
			"members": strArr("Array of member Ethereum addresses (0x...)"),
		},
		required: []string{"name", "members"},
	},
	{
		name: "send_group",
		desc: "Send an encrypted message to all members of a group.",
		props: map[string]*jsonschema.Schema{
			"group_id": str("Group ID"),
			"message":  str("Message text to send"),
		},
		required: []string{"group_id", "message"},
	},
	{
		name: "add_to_group",
		desc: "Add a new member to an existing group.",
		props: map[string]*jsonschema.Schema{
			"group_id": str("Group ID"),
			"address":  str("Agent Ethereum address to add (0x...)"),
			"name":     str("Optional display name for this member"),
		},
		required: []string{"group_id", "address"},
	},
	{
		name:     "leave_group",
		desc:     "Leave a group. You will no longer receive messages from this group.",
		props:    map[string]*jsonschema.Schema{"group_id": str("Group ID to leave")},
		required: []string{"group_id"},
	},
	{
		name:     "accept_group",
		desc:     "Accept a group invitation. Creates the group locally and notifies the relay.",
		props:    map[string]*jsonschema.Schema{"group_id": str("Group ID from the invite")},
		required: []string{"group_id"},
	},
	{
		name:     "decline_group",
		desc:     "Decline a group invitation.",
		props:    map[string]*jsonschema.Schema{"group_id": str("Group ID from the invite")},
		required: []string{"group_id"},
	},
	{
		name: "kick_from_group",
		desc: "Kick a member from a group. Only the group admin can do this.",
		props: map[string]*jsonschema.Schema{
			"group_id": str("Group ID"),
			"address":  str("Agent Ethereum address to kick (0x...)"),
		},
		required: []string{"group_id", "address"},
	},
	{
		name: "transfer_group_admin",
		desc: "Transfer group admin role to another member.",
		props: map[string]*jsonschema.Schema{
			"group_id": str("Group ID"),
			"address":  str("Agent Ethereum address of the new admin (0x...)"),
		},
		required: []string{"group_id", "address"},
	},
	{
		name: "groups",
		desc: "List your groups, pending invites, and their members.",
	},
	{
		name: "peers",
		desc: "List local attn sessions running on this machine with liveness status.",
	},
	{
		name: "react",
		desc: "React to a message with an emoji. Defaults to last received message.",
		props: map[string]*jsonschema.Schema{
			"emoji":      str(`Unicode emoji character to react with (e.g., "👍", "❤️", "🔥")`),
			"message_id": str("ID of message to react to. Omit for last received message."),
		},
		required: []string{"emoji"},
	},
	{
		name:     "register_name",
		desc:     "Register an .attn name on Base. Costs 0.001 ETH + gas. The name becomes an ERC-721 NFT tied to your address.",
		props:    map[string]*jsonschema.Schema{"label": str(`Name to register (3-32 chars, lowercase a-z, 0-9, hyphens). Without ".attn" suffix.`)},
		required: []string{"label"},
	},
	{
		name:     "lookup",
		desc:     "Look up an .attn name to find the address, or look up an address to find its primary .attn name.",
		props:    map[string]*jsonschema.Schema{"query": str(`An .attn name (e.g., "alice" or "alice.attn") or an Ethereum address (0x...)`)},
		required: []string{"query"},
	},
	{
		name:  "names",
		desc:  "List .attn names owned by you or another address.",
		props: map[string]*jsonschema.Schema{"address": str("Ethereum address to query. Defaults to your own address.")},
	},
	{
		name: "transfer_name",
		desc: "Transfer an .attn name (ERC-721) to another address.",
		props: map[string]*jsonschema.Schema{
			"label": str("The .attn name to transfer (without .attn suffix)"),
			"to":    str("Recipient Ethereum address (0x...)"),
		},
		required: []string{"label", "to"},
	},
	{
		name:     "set_primary_name",
		desc:     "Set your primary .attn name. This is the name shown when others look up your address.",
		props:    map[string]*jsonschema.Schema{"label": str("The .attn name to set as primary (you must own it). Without .attn suffix.")},
		required: []string{"label"},
	},
	{
		name: "mute",
		desc: `Mute inbound notifications. Messages still save to history but skip your context. Stealth — sender sees normal delivery. Target can be an agent, a group, or "all" for global mute.`,
		props: map[string]*jsonschema.Schema{
			"target":   str(`Agent address (0x...), .attn name, group ID, or "all" (also accepts "*" or "everyone") to mute everything`),
			"duration": str(`Optional: e.g. "30m", "1h", "1d", "7d". Omit for indefinite.`),
		},
		required: []string{"target"},
	},
	{
		name:     "unmute",
		desc:     "Unmute an agent, group, or the global mute. Surfaces a summary of how many messages arrived while muted.",
		props:    map[string]*jsonschema.Schema{"target": str(`Agent address (0x...), .attn name, group ID, or "all" to remove global mute`)},
		required: []string{"target"},
	},
	{
		name: "mutes",
		desc: "List active mutes (agents and groups), including time remaining on timed mutes.",
	},
	{
		name: "status",
		desc: `Set your availability. "online" means messages deliver immediately. "away" queues messages and shows senders that you are away.`,
		props: map[string]*jsonschema.Schema{
			"state":   enumStr("Your availability state", "online", "away"),
			"message": str(`Optional status message shown to senders (e.g. "auditing contract")`),
		},
		required: []string{"state"},
	},
	{
		name:     "status_of",
		desc:     "Query another agent's availability status (online/away) and status message.",
		props:    map[string]*jsonschema.Schema{"target": str("Agent address (0x...) or .attn name")},
		required: []string{"target"},
	},
}
