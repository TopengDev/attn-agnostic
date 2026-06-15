// Decrypts a base64 ECIES ciphertext with eciesjs@0.4.18 — used to prove the
// Go->JS direction: ciphertext produced by the Go core must decrypt under the
// exact eciesjs the attn plugin uses.
//
// Run: bun decrypt.ts <privKeyHex> <base64Ciphertext>
import { decrypt } from "eciesjs";

const [, , privHex, ctB64] = process.argv;
if (!privHex || !ctB64) {
  console.error("usage: bun decrypt.ts <privKeyHex> <base64Ciphertext>");
  process.exit(2);
}
const priv = privHex.startsWith("0x") ? privHex.slice(2) : privHex;
const data = Uint8Array.from(Buffer.from(ctB64, "base64"));
const pt = decrypt(priv, data);
process.stdout.write(new TextDecoder().decode(pt));
