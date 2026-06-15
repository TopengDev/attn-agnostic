package agent

import (
	"math/big"
	"testing"
	"time"
)

func TestParseDuration(t *testing.T) {
	cases := []struct {
		in   string
		want time.Duration
		ok   bool
	}{
		{"30m", 30 * time.Minute, true},
		{"1h", time.Hour, true},
		{"1d", 24 * time.Hour, true},
		{"7d", 7 * 24 * time.Hour, true},
		{"2w", 14 * 24 * time.Hour, true},
		{"5s", 0, false},
		{"abc", 0, false},
		{"", 0, false},
	}
	for _, c := range cases {
		got, ok := parseDuration(c.in)
		if ok != c.ok || (ok && got != c.want) {
			t.Errorf("parseDuration(%q) = %v,%v want %v,%v", c.in, got, ok, c.want, c.ok)
		}
	}
}

func TestIsGlobalMuteTarget(t *testing.T) {
	for _, s := range []string{"all", "ALL", "*", "everyone", " all "} {
		if !isGlobalMuteTarget(s) {
			t.Errorf("isGlobalMuteTarget(%q) should be true", s)
		}
	}
	for _, s := range []string{"0xabc", "alice.attn", "group-1"} {
		if isGlobalMuteTarget(s) {
			t.Errorf("isGlobalMuteTarget(%q) should be false", s)
		}
	}
}

func TestEmojiToUnicode(t *testing.T) {
	if emojiToUnicode("fire") != "🔥" {
		t.Error("fire shortcode")
	}
	if emojiToUnicode("🔥") != "🔥" {
		t.Error("raw emoji passthrough")
	}
	if emojiToUnicode("unknown") != "unknown" {
		t.Error("unknown passthrough")
	}
}

func TestWeiToEth(t *testing.T) {
	if got := weiToEth(big.NewInt(1_000_000_000_000_000)); got != "0.001" {
		t.Errorf("0.001 ETH: got %q", got)
	}
	if got := weiToEth(big.NewInt(0)); got != "0" {
		t.Errorf("zero: got %q", got)
	}
}

func TestIsValidAddress(t *testing.T) {
	if !isValidAddress("0xc2c3bb724a9c85cd2a252c1778fec42be7639df1") {
		t.Error("valid address rejected")
	}
	for _, bad := range []string{"c2c3bb724a9c85cd2a252c1778fec42be7639df1", "0xZZ", "chilldawg.attn", ""} {
		if isValidAddress(bad) {
			t.Errorf("invalid address accepted: %q", bad)
		}
	}
}

func TestFormatRemaining(t *testing.T) {
	if formatRemaining(45_000) != "45s" {
		t.Errorf("45s: %s", formatRemaining(45_000))
	}
	if formatRemaining(90_000) != "2m" {
		t.Errorf("90s→ceil 2m: %s", formatRemaining(90_000))
	}
	if formatRemaining(3_600_000) != "1h" {
		t.Errorf("1h: %s", formatRemaining(3_600_000))
	}
}
