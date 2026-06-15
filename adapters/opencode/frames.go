package opencode

import "encoding/json"

// InboundFrame is a frame the daemon pushes to a WS subscriber. It mirrors
// attnd's surfaceToFrame contract (internal/httpapi/ws.go): a relay OR local
// message arrives as type:"message" (file as type:"file"); local-mesh sends carry
// local:true + trust:"local"; the daemon also echoes a type:"local-ack" to the
// sender after a {type:"local"} send. We only INJECT message/file frames.
//
// `From` is the sender identity — for a local frame it is the sender's SESSION
// NAME (not an address); for a relay frame it is the sender's address (with
// AgentName the resolved .attn name, if any).
type InboundFrame struct {
	Type         string `json:"type"`
	From         string `json:"from"`
	Message      string `json:"message"`
	ID           string `json:"id"`
	Ts           int64  `json:"ts"`
	Local        bool   `json:"local"`
	Trust        string `json:"trust"`
	DeliveryMode string `json:"deliveryMode"`
	AgentName    string `json:"agentName"`
	GroupID      string `json:"groupId"`
	GroupName    string `json:"groupName"`

	// file frames
	Filename string `json:"filename"`
	Path     string `json:"path"`
	Size     int64  `json:"size"`

	// reaction frames
	ReactionFor string `json:"reactionMessageId"`

	// local-ack frames (echoed to a {type:"local"} sender; never injected)
	To        string `json:"to"`
	Delivered bool   `json:"delivered"`
	Detail    string `json:"detail"`
}

// localSendFrame is the client→daemon mesh-send frame. The daemon attributes the
// sender from the WS connection's ?session= name (no client-supplied "from"), so
// a session cannot spoof another. to="all" broadcasts to every other session.
type localSendFrame struct {
	Type        string `json:"type"` // always "local"
	To          string `json:"to"`
	Message     string `json:"message"`
	ReactionFor string `json:"reaction_for,omitempty"`
}

func newLocalSend(to, message string) []byte {
	b, _ := json.Marshal(localSendFrame{Type: "local", To: to, Message: message})
	return b
}

// injectable reports whether a frame should be injected into the opencode
// session. local-ack, reactions, and bodyless frames are not injected.
func (f InboundFrame) injectable() bool {
	switch f.Type {
	case "message":
		return f.Message != ""
	case "file":
		return f.Filename != "" || f.Path != ""
	default:
		// local-ack, reaction, and any unknown control frame: not injected.
		return false
	}
}
