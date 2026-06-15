// Package config resolves attnd's platform paths and loads the agent identity.
//
// Path resolution is deliberately portable (no hardcoded /home): the base dir
// comes from os.UserConfigDir() — XDG_CONFIG_HOME / ~/.config on Linux,
// ~/Library/Application Support on macOS, %AppData% on Windows — with an
// ATTN_HOME override for tests and custom installs. Full cross-platform install
// (service files, key migration) is M4; M1 only needs the base resolution to be
// correct and overridable.
package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"

	"github.com/TopengDev/attn-agnostic/internal/identity"
)

// Config holds the resolved runtime paths and the loaded identity.
type Config struct {
	Home     string // base dir holding key + db (+ inbox, control socket)
	KeyPath  string // <Home>/key.hex
	DBPath   string // <Home>/attnd.db
	InboxDir string // <Home>/inbox
	SockPath string // <Home>/attnd.sock (control socket)
	HTTPAddr string // localhost bind for the REST + WS product interface
	RelayURL string
	BaseRPC  string
	ID       *identity.Identity
}

const (
	// DefaultRelayURL is the live s0nderlabs relay (shared/constants.ts).
	DefaultRelayURL = "wss://attn.s0nderlabs.xyz/ws"
	// DefaultBaseRPC is Base mainnet (shared/constants.ts: BASE_RPC_DEFAULT).
	DefaultBaseRPC = "https://mainnet.base.org"
	// DefaultHTTPAddr is the localhost REST+WS bind, matching pi-setup's existing
	// `127.0.0.1:9742` daemon contract (extensions/attn/index.ts).
	DefaultHTTPAddr = "127.0.0.1:9742"
)

// Home resolves attnd's base directory without loading anything. Precedence:
// ATTN_HOME env > os.UserConfigDir()/attn.
func Home() (string, error) {
	if h := strings.TrimSpace(os.Getenv("ATTN_HOME")); h != "" {
		return h, nil
	}
	base, err := os.UserConfigDir()
	if err != nil {
		return "", fmt.Errorf("resolve user config dir: %w", err)
	}
	return filepath.Join(base, "attn"), nil
}

// Load resolves all paths, ensures the home dir exists, and loads (or, if
// allowGenerate, generates) the agent identity.
//
// Identity precedence: explicit keyHex arg > ATTN_PRIVATE_KEY env > <Home>/key.hex.
// A generated key is persisted to <Home>/key.hex with 0600 perms.
func Load(keyHex string, allowGenerate bool) (*Config, error) {
	home, err := Home()
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(home, 0o700); err != nil {
		return nil, fmt.Errorf("mkdir home %s: %w", home, err)
	}

	cfg := &Config{
		Home:     home,
		KeyPath:  filepath.Join(home, "key.hex"),
		DBPath:   filepath.Join(home, "attnd.db"),
		InboxDir: filepath.Join(home, "inbox"),
		SockPath: filepath.Join(home, "attnd.sock"),
		HTTPAddr: envOr("ATTN_HTTP_ADDR", DefaultHTTPAddr),
		RelayURL: envOr("ATTN_RELAY_URL", DefaultRelayURL),
		BaseRPC:  envOr("ATTN_BASE_RPC", DefaultBaseRPC),
	}

	id, err := loadIdentity(cfg, keyHex, allowGenerate)
	if err != nil {
		return nil, err
	}
	cfg.ID = id
	return cfg, nil
}

func loadIdentity(cfg *Config, keyHex string, allowGenerate bool) (*identity.Identity, error) {
	// 1. Explicit arg (e.g. --key for live tests).
	if s := strings.TrimSpace(keyHex); s != "" {
		return identity.FromHex(s)
	}
	// 2. Environment.
	if s := strings.TrimSpace(os.Getenv("ATTN_PRIVATE_KEY")); s != "" {
		return identity.FromHex(s)
	}
	// 3. Key file.
	if b, err := os.ReadFile(cfg.KeyPath); err == nil {
		s := strings.TrimSpace(string(b))
		if s != "" {
			return identity.FromHex(s)
		}
	}
	// 4. Generate + persist (only when explicitly allowed).
	if !allowGenerate {
		return nil, fmt.Errorf("no identity: set ATTN_PRIVATE_KEY, pass --key, or place a hex key at %s", cfg.KeyPath)
	}
	id, err := identity.Generate()
	if err != nil {
		return nil, err
	}
	if err := os.WriteFile(cfg.KeyPath, []byte(id.PrivateKeyHex()+"\n"), 0o600); err != nil {
		return nil, fmt.Errorf("persist generated key: %w", err)
	}
	return id, nil
}

func envOr(key, def string) string {
	if v := strings.TrimSpace(os.Getenv(key)); v != "" {
		return v
	}
	return def
}
