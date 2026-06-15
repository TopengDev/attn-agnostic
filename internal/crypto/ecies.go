// Package crypto implements the attn end-to-end message encryption: ECIES over
// secp256k1, wire-compatible with the upstream s0nderlabs/attn plugin.
//
// The upstream plugin (packages/plugin/src/crypto.ts) uses the `eciesjs` npm
// package (v0.4.18) with its default configuration:
//   - curve secp256k1
//   - AES-256-GCM, 16-byte nonce
//   - uncompressed (65-byte) ephemeral public key
//   - HKDF-SHA256 over (ephemeralPub || sharedPoint), both uncompressed
//   - wire layout: [ephPub:65][nonce:16][gcmTag:16][ciphertext], then base64
//
// We reimplement against github.com/ecies/go/v2 (eciesgo), authored by the same
// maintainer as eciesjs and kept cross-compatible under the same default config.
// Cross-compatibility is proven empirically by the test vectors in ecies_test.go
// (real eciesjs@0.4.18 ciphertext, generated via bun, must decrypt here — and
// our ciphertext must decrypt in eciesjs).
package crypto

import (
	"encoding/base64"
	"fmt"
	"strings"

	eciesgo "github.com/ecies/go/v2"
)

// EncryptBinary encrypts plaintext to the recipient's secp256k1 public key (hex,
// with or without 0x prefix; compressed or uncompressed) using ECIES, returning
// the RAW ciphertext bytes ([ephPub][nonce][tag][ct]). This is the form the
// upstream `encryptBinary` produces for file uploads (uploaded raw, not base64).
func EncryptBinary(recipientPubHex string, plaintext []byte) ([]byte, error) {
	pubHex := strings.TrimPrefix(strings.TrimSpace(recipientPubHex), "0x")
	pub, err := eciesgo.NewPublicKeyFromHex(pubHex)
	if err != nil {
		return nil, fmt.Errorf("parse recipient pubkey: %w", err)
	}
	ct, err := eciesgo.Encrypt(pub, plaintext)
	if err != nil {
		return nil, fmt.Errorf("ecies encrypt: %w", err)
	}
	return ct, nil
}

// DecryptBinary decrypts RAW ECIES ciphertext bytes with the 32-byte secp256k1
// private key (the inverse of EncryptBinary; used for received files).
func DecryptBinary(privKey []byte, ct []byte) ([]byte, error) {
	priv := eciesgo.NewPrivateKeyFromBytes(privKey)
	pt, err := eciesgo.Decrypt(priv, ct)
	if err != nil {
		return nil, fmt.Errorf("ecies decrypt: %w", err)
	}
	return pt, nil
}

// EncryptBase64 encrypts plaintext to the recipient's secp256k1 public key using
// ECIES and returns standard-base64 ciphertext — the exact form the relay
// envelope carries in its `encrypted` field.
func EncryptBase64(recipientPubHex string, plaintext []byte) (string, error) {
	ct, err := EncryptBinary(recipientPubHex, plaintext)
	if err != nil {
		return "", err
	}
	return base64.StdEncoding.EncodeToString(ct), nil
}

// DecryptBase64 decrypts a standard-base64 ECIES ciphertext with the 32-byte
// secp256k1 private key, returning the recovered plaintext.
func DecryptBase64(privKey []byte, b64 string) ([]byte, error) {
	ct, err := base64.StdEncoding.DecodeString(strings.TrimSpace(b64))
	if err != nil {
		return nil, fmt.Errorf("base64 decode: %w", err)
	}
	return DecryptBinary(privKey, ct)
}
