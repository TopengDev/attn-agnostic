// Package identity implements the attn agent identity: a secp256k1 keypair,
// its derived Ethereum address, EIP-191 (personal_sign) challenge signing, and
// the message-envelope sign/verify scheme.
//
// Clean-room reimplementation of the scheme in s0nderlabs/attn
// (packages/plugin/src/crypto.ts + env.ts). The upstream plugin derives identity
// via viem's privateKeyToAccount and signs via account.signMessage (EIP-191).
// go-ethereum's crypto package produces byte-identical results.
package identity

import (
	"bytes"
	"crypto/ecdsa"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ethereum/go-ethereum/accounts"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"github.com/ethereum/go-ethereum/crypto"
)

// Identity is an attn agent identity backed by a secp256k1 private key.
type Identity struct {
	priv *ecdsa.PrivateKey
	addr string // lowercase 0x-prefixed ETH address
}

// Generate creates a fresh random secp256k1 identity.
func Generate() (*Identity, error) {
	priv, err := crypto.GenerateKey()
	if err != nil {
		return nil, fmt.Errorf("generate key: %w", err)
	}
	return fromKey(priv), nil
}

// FromHex loads an identity from a hex private key (with or without 0x prefix).
func FromHex(privHex string) (*Identity, error) {
	privHex = strings.TrimPrefix(strings.TrimSpace(privHex), "0x")
	priv, err := crypto.HexToECDSA(privHex)
	if err != nil {
		return nil, fmt.Errorf("parse private key: %w", err)
	}
	return fromKey(priv), nil
}

func fromKey(priv *ecdsa.PrivateKey) *Identity {
	// Standard Ethereum address derivation: keccak256(uncompressedPub[1:])[12:].
	// Lowercased to match the upstream plugin (deriveIdentity → .toLowerCase()).
	addr := strings.ToLower(crypto.PubkeyToAddress(priv.PublicKey).Hex())
	return &Identity{priv: priv, addr: addr}
}

// Address returns the lowercase 0x-prefixed Ethereum address.
func (id *Identity) Address() string { return id.addr }

// PrivateKeyHex returns the 0x-prefixed 32-byte private key hex.
func (id *Identity) PrivateKeyHex() string {
	return "0x" + hexutil.Encode(crypto.FromECDSA(id.priv))[2:]
}

// PublicKeyHex returns the 0x04-prefixed uncompressed (65-byte) public key hex.
// This is the form the relay derives (via recoverPublicKey) and serves to peers
// for ECIES encryption.
func (id *Identity) PublicKeyHex() string {
	return hexutil.Encode(crypto.FromECDSAPub(&id.priv.PublicKey))
}

// PrivateKeyBytes returns the raw 32-byte private key (for the ECIES layer).
func (id *Identity) PrivateKeyBytes() []byte { return crypto.FromECDSA(id.priv) }

// SignPersonal signs a message with EIP-191 (personal_sign), matching viem's
// account.signMessage({message}). The returned signature is 0x-prefixed, 65
// bytes, with the recovery id normalized to {27,28} (Ethereum convention).
func (id *Identity) SignPersonal(message string) (string, error) {
	hash := accounts.TextHash([]byte(message))
	sig, err := crypto.Sign(hash, id.priv)
	if err != nil {
		return "", fmt.Errorf("sign: %w", err)
	}
	// go-ethereum yields v in {0,1}; EIP-191/viem expect {27,28}.
	sig[64] += 27
	return hexutil.Encode(sig), nil
}

// serializeEnvelope mirrors the upstream serializeEnvelope: JSON.stringify of
// {id, to, encrypted} in that exact field order, with no HTML escaping (to match
// JavaScript JSON.stringify, which does not escape <, >, &).
func serializeEnvelope(idStr, to, encrypted string) (string, error) {
	env := struct {
		ID        string `json:"id"`
		To        string `json:"to"`
		Encrypted string `json:"encrypted"`
	}{ID: idStr, To: to, Encrypted: encrypted}

	var buf bytes.Buffer
	enc := json.NewEncoder(&buf)
	enc.SetEscapeHTML(false)
	if err := enc.Encode(env); err != nil {
		return "", err
	}
	// json.Encoder appends a trailing newline; JSON.stringify does not.
	return strings.TrimRight(buf.String(), "\n"), nil
}

// SignEnvelope signs the message envelope exactly as the upstream signEnvelope:
// personal_sign over JSON.stringify({id, to, encrypted}).
func (id *Identity) SignEnvelope(idStr, to, encrypted string) (string, error) {
	payload, err := serializeEnvelope(idStr, to, encrypted)
	if err != nil {
		return "", err
	}
	return id.SignPersonal(payload)
}

// VerifyEnvelope verifies that signature over the envelope {id, to, encrypted}
// recovers to the claimed `from` address (case-insensitive), mirroring the
// upstream verifyEnvelope.
func VerifyEnvelope(from, idStr, to, encrypted, signature string) (bool, error) {
	payload, err := serializeEnvelope(idStr, to, encrypted)
	if err != nil {
		return false, err
	}
	recovered, err := RecoverPersonal(payload, signature)
	if err != nil {
		return false, err
	}
	return strings.EqualFold(recovered, from), nil
}

// RecoverPersonal recovers the signer address from an EIP-191 personal_sign
// signature over the given message. Returns a lowercase 0x address.
func RecoverPersonal(message, signature string) (string, error) {
	sig, err := hexutil.Decode(signature)
	if err != nil {
		return "", fmt.Errorf("decode signature: %w", err)
	}
	if len(sig) != 65 {
		return "", fmt.Errorf("signature must be 65 bytes, got %d", len(sig))
	}
	// Normalize recovery id back to {0,1} for SigToPub.
	s := make([]byte, 65)
	copy(s, sig)
	if s[64] >= 27 {
		s[64] -= 27
	}
	hash := accounts.TextHash([]byte(message))
	pub, err := crypto.SigToPub(hash, s)
	if err != nil {
		return "", fmt.Errorf("recover pubkey: %w", err)
	}
	return strings.ToLower(crypto.PubkeyToAddress(*pub).Hex()), nil
}
