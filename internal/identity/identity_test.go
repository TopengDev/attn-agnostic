package identity

import (
	"encoding/json"
	"os"
	"path/filepath"
	"testing"
)

type vectors struct {
	PrivateKey string `json:"privateKey"`
	Address    string `json:"address"`
	PublicKey  string `json:"publicKey"`
	Message    string `json:"message"`
	Signature  string `json:"signature"`
}

func loadVectors(t *testing.T) vectors {
	t.Helper()
	path := filepath.Join("..", "..", "testdata", "vectors.json")
	b, err := os.ReadFile(path)
	if err != nil {
		t.Skipf("test vectors not generated (run: cd testdata/interop && bun install && bun gen-vectors.ts): %v", err)
	}
	var v vectors
	if err := json.Unmarshal(b, &v); err != nil {
		t.Fatalf("parse vectors: %v", err)
	}
	return v
}

// TestIdentity_AgainstViem proves our secp256k1 -> address + uncompressed pubkey
// derivation matches viem's privateKeyToAccount (the upstream plugin's scheme).
func TestIdentity_AgainstViem(t *testing.T) {
	v := loadVectors(t)
	id, err := FromHex(v.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	if id.Address() != v.Address {
		t.Errorf("address mismatch:\n got  %s\n want %s", id.Address(), v.Address)
	}
	if id.PublicKeyHex() != v.PublicKey {
		t.Errorf("pubkey mismatch:\n got  %s\n want %s", id.PublicKeyHex(), v.PublicKey)
	}
}

// TestSignPersonal_AgainstViem proves our EIP-191 personal_sign is byte-identical
// to viem's account.signMessage (deterministic RFC-6979 ECDSA, v in {27,28}).
func TestSignPersonal_AgainstViem(t *testing.T) {
	v := loadVectors(t)
	id, err := FromHex(v.PrivateKey)
	if err != nil {
		t.Fatal(err)
	}
	sig, err := id.SignPersonal(v.Message)
	if err != nil {
		t.Fatal(err)
	}
	if sig != v.Signature {
		t.Errorf("signature mismatch:\n got  %s\n want %s", sig, v.Signature)
	}
}

// TestRecoverPersonal proves we recover the signer from a viem-produced signature.
func TestRecoverPersonal_FromViemSig(t *testing.T) {
	v := loadVectors(t)
	recovered, err := RecoverPersonal(v.Message, v.Signature)
	if err != nil {
		t.Fatal(err)
	}
	if recovered != v.Address {
		t.Errorf("recovered %s, want %s", recovered, v.Address)
	}
}

// TestEnvelopeRoundTrip proves the envelope sign/verify scheme (the per-message
// authentication the relay relays and the recipient checks).
func TestEnvelopeRoundTrip(t *testing.T) {
	id, err := Generate()
	if err != nil {
		t.Fatal(err)
	}
	msgID := "550e8400-e29b-41d4-a716-446655440000"
	to := "0x4b4f476aa75665fa25e4b27de036971f1aa21eb7"
	encrypted := "BKzCINxDissPnlow+VRyNHJ/abc=" // arbitrary base64-ish payload

	sig, err := id.SignEnvelope(msgID, to, encrypted)
	if err != nil {
		t.Fatal(err)
	}
	ok, err := VerifyEnvelope(id.Address(), msgID, to, encrypted, sig)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("envelope signature did not verify to signer")
	}
	// Tampering must fail verification.
	ok, _ = VerifyEnvelope(id.Address(), msgID, to, encrypted+"x", sig)
	if ok {
		t.Fatal("verification passed on tampered envelope")
	}
}

// TestSerializeEnvelope_FieldOrder locks the exact JSON shape the upstream signs:
// {"id":..,"to":..,"encrypted":..} with no spaces and no HTML escaping.
func TestSerializeEnvelope_FieldOrder(t *testing.T) {
	got, err := serializeEnvelope("ID1", "0xABC", "a+b/c=")
	if err != nil {
		t.Fatal(err)
	}
	want := `{"id":"ID1","to":"0xABC","encrypted":"a+b/c="}`
	if got != want {
		t.Errorf("serializeEnvelope:\n got  %s\n want %s", got, want)
	}
}
