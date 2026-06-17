package config

import (
	"path/filepath"
	"testing"
)

func TestInboxDirEnvOverride(t *testing.T) {
	home := t.TempDir()
	t.Setenv("ATTN_HOME", home)
	t.Setenv("ATTN_PRIVATE_KEY", "") // prevent real env from interfering

	// Unset — falls back to <home>/inbox.
	t.Setenv("ATTN_INBOX_DIR", "")
	cfg, err := Load("", true)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if want := filepath.Join(home, "inbox"); cfg.InboxDir != want {
		t.Errorf("InboxDir without env = %q, want %q", cfg.InboxDir, want)
	}

	// Set — uses the env value verbatim.
	custom := filepath.Join(t.TempDir(), "custom-inbox")
	t.Setenv("ATTN_INBOX_DIR", custom)
	cfg2, err := Load("", false) // key already exists from first Load
	if err != nil {
		t.Fatalf("Load with ATTN_INBOX_DIR: %v", err)
	}
	if cfg2.InboxDir != custom {
		t.Errorf("InboxDir with env = %q, want %q", cfg2.InboxDir, custom)
	}
}
