package relay

import (
	"strings"

	"github.com/google/uuid"
)

// normalize lowercases an Ethereum address, matching the relay/plugin which key
// everything on address.toLowerCase().
func normalize(addr string) string {
	return strings.ToLower(strings.TrimSpace(addr))
}

// newID mints a message id. The upstream plugin uses crypto.randomUUID(); the
// relay only requires a unique string of <=100 chars, so a UUIDv4 matches.
func newID() string {
	return uuid.NewString()
}
