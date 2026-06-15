package relay

import "testing"

func TestHTTPBaseFromWS(t *testing.T) {
	cases := map[string]string{
		"wss://attn.s0nderlabs.xyz/ws": "https://attn.s0nderlabs.xyz",
		"ws://localhost:8787/ws":       "http://localhost:8787",
		"wss://relay.example.com":      "https://relay.example.com",
	}
	for in, want := range cases {
		if got := httpBaseFromWS(in); got != want {
			t.Errorf("httpBaseFromWS(%q) = %q, want %q", in, got, want)
		}
	}
}

func TestNormalize(t *testing.T) {
	if normalize("  0xABC123  ") != "0xabc123" {
		t.Errorf("normalize trim+lower failed: %q", normalize("  0xABC123  "))
	}
}

func TestLowerAll(t *testing.T) {
	got := lowerAll([]string{"0xAA", " 0xBb "})
	if len(got) != 2 || got[0] != "0xaa" || got[1] != "0xbb" {
		t.Errorf("lowerAll = %v", got)
	}
}
