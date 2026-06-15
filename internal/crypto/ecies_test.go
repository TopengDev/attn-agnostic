package crypto

import (
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

type vectors struct {
	PrivateKey         string `json:"privateKey"`
	PublicKey          string `json:"publicKey"`
	Plaintext          string `json:"plaintext"`
	EciesCiphertextB64 string `json:"eciesCiphertextB64"`
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

func privBytes(t *testing.T, hexKey string) []byte {
	t.Helper()
	b, err := hex.DecodeString(strings.TrimPrefix(hexKey, "0x"))
	if err != nil {
		t.Fatalf("decode priv: %v", err)
	}
	return b
}

// TestDecrypt_EciesjsCiphertext is the core JS->Go interop proof: ciphertext
// produced by the real eciesjs@0.4.18 (the plugin's exact version) MUST decrypt
// under our ecies/go core to the original plaintext.
func TestDecrypt_EciesjsCiphertext(t *testing.T) {
	v := loadVectors(t)
	pt, err := DecryptBase64(privBytes(t, v.PrivateKey), v.EciesCiphertextB64)
	if err != nil {
		t.Fatalf("decrypt eciesjs ciphertext: %v", err)
	}
	if string(pt) != v.Plaintext {
		t.Errorf("plaintext mismatch:\n got  %q\n want %q", string(pt), v.Plaintext)
	}
}

// TestEncryptDecrypt_RoundTrip proves our own encrypt/decrypt are consistent.
func TestEncryptDecrypt_RoundTrip(t *testing.T) {
	v := loadVectors(t)
	msg := []byte("round-trip 🔐 unicode + symbols +/=")
	ct, err := EncryptBase64(v.PublicKey, msg)
	if err != nil {
		t.Fatalf("encrypt: %v", err)
	}
	pt, err := DecryptBase64(privBytes(t, v.PrivateKey), ct)
	if err != nil {
		t.Fatalf("decrypt: %v", err)
	}
	if string(pt) != string(msg) {
		t.Errorf("round-trip mismatch:\n got  %q\n want %q", string(pt), string(msg))
	}
}
