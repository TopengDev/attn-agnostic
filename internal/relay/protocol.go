// Package relay implements the attn relay WebSocket client: the EIP-191
// challenge-response handshake, public-key lookup, and the encrypted message
// envelope — wire-compatible with the s0nderlabs relay (packages/relay) and the
// Claude-Code attn plugin (packages/plugin).
package relay

// DefaultRelayURL is the live s0nderlabs relay (from packages/shared/constants.ts).
const DefaultRelayURL = "wss://attn.s0nderlabs.xyz/ws"

// serverMessage is the union of every frame the relay sends to a client
// (packages/shared/src/messages.ts → ServerMessage). All fields are optional;
// `Type` selects which are populated.
type serverMessage struct {
	Type string `json:"type"`

	// challenge
	Nonce string `json:"nonce,omitempty"`

	// auth_ok / auth_error / key_response / presence_response / delivery_status
	Address string `json:"address,omitempty"`
	Error   string `json:"error,omitempty"`

	// message / reaction (inbound)
	ID        string `json:"id,omitempty"`
	From      string `json:"from,omitempty"`
	FromName  string `json:"from_name,omitempty"`
	Encrypted string `json:"encrypted,omitempty"`
	Signature string `json:"signature,omitempty"`
	Ts        int64  `json:"ts,omitempty"`
	GroupID   string `json:"group_id,omitempty"`
	GroupName string `json:"group_name,omitempty"`
	MessageID string `json:"message_id,omitempty"`

	// key_response / resolve_response
	PublicKey *string `json:"publicKey,omitempty"`

	// resolve_response
	Name string `json:"name,omitempty"`

	// presence_response (also reused by status fields)
	State   string `json:"state,omitempty"`
	Message string `json:"message,omitempty"`

	// delivery_status
	To             string  `json:"to,omitempty"`
	Status         string  `json:"status,omitempty"`
	RecipientState string  `json:"recipient_state,omitempty"`
	RecipientMsg   *string `json:"recipient_message,omitempty"`
}

// --- Client → Server frames (packages/shared/src/messages.ts → ClientMessage) ---

type authFrame struct {
	Type            string  `json:"type"` // "auth"
	Address         string  `json:"address"`
	Signature       string  `json:"signature"`
	Presence        string  `json:"presence,omitempty"`
	PresenceMessage *string `json:"presence_message,omitempty"`
}

type messageFrame struct {
	Type      string `json:"type"` // "message"
	ID        string `json:"id"`
	To        string `json:"to"`
	Encrypted string `json:"encrypted"`
	Signature string `json:"signature"`
}

type getKeyFrame struct {
	Type    string `json:"type"` // "get_key"
	Address string `json:"address"`
}

type ackFrame struct {
	Type string `json:"type"` // "ack"
	ID   string `json:"id"`
}

// reactionFrame is a client→server emoji reaction on a prior message.
type reactionFrame struct {
	Type      string `json:"type"` // "reaction"
	ID        string `json:"id"`
	To        string `json:"to"`
	MessageID string `json:"message_id"`
	Encrypted string `json:"encrypted"`
	Signature string `json:"signature"`
}

// resolveFrame asks the relay to resolve a .attn label.
type resolveFrame struct {
	Type string `json:"type"` // "resolve"
	Name string `json:"name"`
}

// presenceSetFrame sets this agent's availability on the relay.
type presenceSetFrame struct {
	Type    string  `json:"type"` // "presence_set"
	State   string  `json:"state"`
	Message *string `json:"message"`
}

// presenceQueryFrame queries another agent's availability.
type presenceQueryFrame struct {
	Type    string `json:"type"` // "presence_query"
	Address string `json:"address"`
}

// Inbound is a decrypted, signature-verified message delivered to a Listen handler.
type Inbound struct {
	ID        string
	From      string // sender address (lowercase)
	FromName  string // .attn primary name, if any
	Plaintext string
	Ts        int64
	Verified  bool // envelope signature recovered to From
}

// DeliveryResult reports the relay's delivery outcome for a sent message.
type DeliveryResult struct {
	ID             string
	Status         string // "delivered" | "queued"
	RecipientState string // "online" | "away" | ""
	RecipientMsg   string
}
