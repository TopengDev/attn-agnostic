package names

import (
	"bytes"
	"context"
	"math/big"
	"os"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/crypto"
)

// TestPackSelectors asserts the encoded calldata begins with the correct 4-byte
// function selector (keccak256 of the canonical signature). This proves the
// gated write paths encode the exact transaction the registrar expects —
// without ever broadcasting.
func TestPackSelectors(t *testing.T) {
	parsed, err := ParsedABI()
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{abi: parsed}

	cases := []struct {
		name string
		sig  string
		data []byte
	}{
		{"register", "register(string)", mustPack(t, func() ([]byte, error) { return c.PackRegister("alice") })},
		{"setPrimaryName", "setPrimaryName(string)", mustPack(t, func() ([]byte, error) { return c.PackSetPrimaryName("alice") })},
		{"transferFrom", "transferFrom(address,address,uint256)", mustPack(t, func() ([]byte, error) {
			return c.PackTransferFrom(common.HexToAddress("0x1"), common.HexToAddress("0x2"), big.NewInt(7))
		})},
	}
	for _, tc := range cases {
		want := crypto.Keccak256([]byte(tc.sig))[:4]
		if len(tc.data) < 4 || !bytes.Equal(tc.data[:4], want) {
			t.Fatalf("%s: selector mismatch — want %x, got % x", tc.name, want, tc.data)
		}
	}
}

// TestPackRegisterRoundTrip proves the label survives encode→decode (the
// argument is packed correctly, not just the selector).
func TestPackRegisterRoundTrip(t *testing.T) {
	parsed, err := ParsedABI()
	if err != nil {
		t.Fatal(err)
	}
	c := &Client{abi: parsed}
	data, err := c.PackRegister("CHILLDAWG.attn") // normalized to "chilldawg"
	if err != nil {
		t.Fatal(err)
	}
	args, err := parsed.Methods["register"].Inputs.Unpack(data[4:])
	if err != nil {
		t.Fatal(err)
	}
	if len(args) != 1 || args[0].(string) != "chilldawg" {
		t.Fatalf("label round-trip failed: %+v", args)
	}
}

func TestTokenIDFromNode(t *testing.T) {
	var node [32]byte
	node[31] = 0x01
	if got := TokenIDFromNode(node); got.Cmp(big.NewInt(1)) != 0 {
		t.Fatalf("tokenId: want 1, got %s", got)
	}
}

// TestGate proves the broadcast paths refuse without explicit approval (which
// M1 never grants), while still returning inspectable calldata.
func TestGate(t *testing.T) {
	parsed, _ := ParsedABI()
	c := &Client{abi: parsed}
	data, err := c.SendRegister(context.Background(), "alice", false)
	if err != ErrPaidWriteGated {
		t.Fatalf("expected ErrPaidWriteGated, got %v", err)
	}
	if len(data) < 4 {
		t.Fatal("gated path should still return calldata for inspection")
	}
}

// TestLiveResolve hits Base mainnet to resolve a known name and reverse it.
// Gated behind ATTN_LIVE_BASE=1 so `go test ./...` stays hermetic/offline-safe.
func TestLiveResolve(t *testing.T) {
	if os.Getenv("ATTN_LIVE_BASE") != "1" {
		t.Skip("set ATTN_LIVE_BASE=1 to run the live Base mainnet resolve")
	}
	rpc := os.Getenv("ATTN_BASE_RPC")
	if rpc == "" {
		rpc = "https://mainnet.base.org"
	}
	ctx := context.Background()
	c, err := New(ctx, rpc)
	if err != nil {
		t.Fatal(err)
	}
	defer c.Close()

	owner, _, err := c.Resolve(ctx, "chilldawg")
	if err != nil {
		t.Fatalf("resolve chilldawg.attn: %v", err)
	}
	t.Logf("chilldawg.attn → %s", owner.Hex())
	if owner == (common.Address{}) {
		t.Fatal("chilldawg.attn resolved to zero address")
	}
	name, err := c.PrimaryNameOf(ctx, owner)
	if err != nil {
		t.Fatalf("primaryNameOf: %v", err)
	}
	t.Logf("%s → %s.attn (reverse)", owner.Hex(), name)
}

func mustPack(t *testing.T, f func() ([]byte, error)) []byte {
	t.Helper()
	b, err := f()
	if err != nil {
		t.Fatal(err)
	}
	return b
}
