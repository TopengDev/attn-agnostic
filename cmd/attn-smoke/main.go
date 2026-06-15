// Command attn-smoke is the M0 smoke/interop binary for the attn-agnostic Go
// core. It exercises the identity, crypto, and relay layers against the LIVE
// s0nderlabs relay and doubles as a CLI for cross-client interop testing with
// the Claude-Code attn plugin.
//
// Usage:
//
//	attn-smoke [-key HEX | -keyfile PATH] [-relay URL] [-v] <command> [args]
//
// Commands:
//
//	gen-key            generate a new identity, save to keyfile, print address
//	addr               print the identity address
//	pubkey             print the uncompressed (0x04) public key
//	sign     <msg>     EIP-191 personal_sign a message, print 0x signature
//	encrypt  <pubhex> <plaintext>   ECIES-encrypt, print base64 ciphertext
//	decrypt  <b64>     ECIES-decrypt base64 ciphertext with the identity key
//	send     <to> <msg>   connect+auth+send an E2E message via the relay
//	listen             connect+auth and print inbound messages (Ctrl-C to stop)
//	interop  <to> <msg>   send, then listen for a reply (full round-trip)
package main

import (
	"context"
	"flag"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
	"time"

	icrypto "github.com/TopengDev/attn-agnostic/internal/crypto"
	"github.com/TopengDev/attn-agnostic/internal/identity"
	"github.com/TopengDev/attn-agnostic/internal/relay"
)

func main() {
	fs := flag.NewFlagSet("attn-smoke", flag.ExitOnError)
	keyHex := fs.String("key", "", "private key hex (overrides keyfile/env)")
	keyFile := fs.String("keyfile", defaultKeyFile(), "path to private key hex file")
	relayURL := fs.String("relay", relay.DefaultRelayURL, "relay WebSocket URL")
	verbose := fs.Bool("v", false, "verbose relay logging")
	fs.Parse(os.Args[1:])

	args := fs.Args()
	if len(args) == 0 {
		fs.Usage()
		os.Exit(2)
	}
	cmd, rest := args[0], args[1:]

	switch cmd {
	case "gen-key":
		runGenKey(*keyFile)
	case "addr":
		id := mustLoad(*keyHex, *keyFile)
		fmt.Println(id.Address())
	case "pubkey":
		id := mustLoad(*keyHex, *keyFile)
		fmt.Println(id.PublicKeyHex())
	case "sign":
		need(rest, 1, "sign <message>")
		id := mustLoad(*keyHex, *keyFile)
		sig, err := id.SignPersonal(rest[0])
		check(err)
		fmt.Println(sig)
	case "encrypt":
		need(rest, 2, "encrypt <recipientPubHex> <plaintext>")
		ct, err := icrypto.EncryptBase64(rest[0], []byte(rest[1]))
		check(err)
		fmt.Println(ct)
	case "decrypt":
		need(rest, 1, "decrypt <base64>")
		id := mustLoad(*keyHex, *keyFile)
		pt, err := icrypto.DecryptBase64(id.PrivateKeyBytes(), rest[0])
		check(err)
		fmt.Println(string(pt))
	case "send":
		need(rest, 2, "send <toAddress> <message>")
		runSend(*keyHex, *keyFile, *relayURL, *verbose, rest[0], rest[1])
	case "listen":
		runListen(*keyHex, *keyFile, *relayURL, *verbose)
	case "interop":
		need(rest, 2, "interop <toAddress> <message>")
		runInterop(*keyHex, *keyFile, *relayURL, *verbose, rest[0], rest[1])
	default:
		fmt.Fprintf(os.Stderr, "unknown command %q\n", cmd)
		fs.Usage()
		os.Exit(2)
	}
}

func defaultKeyFile() string {
	if v := os.Getenv("ATTN_KEYFILE"); v != "" {
		return v
	}
	return "attn-key.hex"
}

// loadOrEnv resolves the identity from (in order): -key flag, ATTN_PRIVATE_KEY
// env, keyfile.
func loadOrEnv(keyHex, keyFile string) (*identity.Identity, error) {
	if keyHex != "" {
		return identity.FromHex(keyHex)
	}
	if env := os.Getenv("ATTN_PRIVATE_KEY"); env != "" {
		return identity.FromHex(env)
	}
	if keyFile != "" {
		if b, err := os.ReadFile(keyFile); err == nil {
			return identity.FromHex(strings.TrimSpace(string(b)))
		}
	}
	return nil, fmt.Errorf("no key: pass -key, set ATTN_PRIVATE_KEY, or run gen-key first (keyfile %q not found)", keyFile)
}

func mustLoad(keyHex, keyFile string) *identity.Identity {
	id, err := loadOrEnv(keyHex, keyFile)
	check(err)
	return id
}

func runGenKey(keyFile string) {
	id, err := identity.Generate()
	check(err)
	if keyFile != "" {
		if dir := filepath.Dir(keyFile); dir != "." {
			_ = os.MkdirAll(dir, 0o700)
		}
		check(os.WriteFile(keyFile, []byte(id.PrivateKeyHex()+"\n"), 0o600))
		fmt.Fprintf(os.Stderr, "saved key to %s (chmod 600)\n", keyFile)
	}
	fmt.Printf("address: %s\n", id.Address())
	fmt.Printf("pubkey:  %s\n", id.PublicKeyHex())
}

func runSend(keyHex, keyFile, relayURL string, verbose bool, to, msg string) {
	id := mustLoad(keyHex, keyFile)
	ctx, cancel := context.WithTimeout(context.Background(), 45*time.Second)
	defer cancel()

	fmt.Fprintf(os.Stderr, "connecting to %s as %s\n", relayURL, id.Address())
	c, err := relay.Dial(ctx, relayURL, id, verbose)
	check(err)
	defer c.Close()
	fmt.Fprintln(os.Stderr, "authenticated ✓")

	res, ct, err := c.Send(ctx, to, msg, "")
	check(err)
	fmt.Printf("sent to %s\n", strings.ToLower(to))
	fmt.Printf("  status:    %s\n", res.Status)
	if res.RecipientState != "" {
		fmt.Printf("  recipient: %s\n", res.RecipientState)
	}
	fmt.Printf("  ciphertext (b64, %d bytes): %s\n", len(ct), preview(ct, 80))
}

func runListen(keyHex, keyFile, relayURL string, verbose bool) {
	id := mustLoad(keyHex, keyFile)
	ctx, cancel := signalContext()
	defer cancel()

	fmt.Fprintf(os.Stderr, "connecting to %s as %s\n", relayURL, id.Address())
	c, err := relay.Dial(ctx, relayURL, id, verbose)
	check(err)
	defer c.Close()
	fmt.Fprintf(os.Stderr, "authenticated ✓ — listening as %s (Ctrl-C to stop)\n", id.Address())

	err = c.Listen(ctx, func(in relay.Inbound) {
		name := in.FromName
		if name == "" {
			name = in.From
		}
		fmt.Printf("\n📨 inbound id=%s from=%s verified=%v\n", in.ID, name, in.Verified)
		fmt.Printf("   %s\n", in.Plaintext)
	})
	if err != nil && ctx.Err() == nil {
		check(err)
	}
}

func runInterop(keyHex, keyFile, relayURL string, verbose bool, to, msg string) {
	id := mustLoad(keyHex, keyFile)
	ctx, cancel := signalContext()
	defer cancel()

	fmt.Fprintf(os.Stderr, "connecting to %s as %s\n", relayURL, id.Address())
	c, err := relay.Dial(ctx, relayURL, id, verbose)
	check(err)
	defer c.Close()
	fmt.Fprintln(os.Stderr, "authenticated ✓")

	gotReply := make(chan relay.Inbound, 4)
	go func() {
		_ = c.Listen(ctx, func(in relay.Inbound) {
			fmt.Printf("\n📨 inbound id=%s from=%s verified=%v\n   %s\n", in.ID, in.From, in.Verified, in.Plaintext)
			select {
			case gotReply <- in:
			default:
			}
		})
	}()

	res, ct, err := c.Send(ctx, to, msg, "")
	check(err)
	fmt.Printf("→ sent to %s status=%s recipient=%s\n", strings.ToLower(to), res.Status, res.RecipientState)
	fmt.Printf("  ciphertext (b64): %s\n", preview(ct, 80))
	fmt.Fprintln(os.Stderr, "waiting up to 180s for a reply (Ctrl-C to stop)…")

	select {
	case <-gotReply:
		fmt.Println("✓ round-trip reply received + decrypted")
	case <-time.After(180 * time.Second):
		fmt.Fprintln(os.Stderr, "no reply within 180s (send leg still proven by delivery_status)")
	case <-ctx.Done():
	}
}

func signalContext() (context.Context, context.CancelFunc) {
	ctx, cancel := context.WithCancel(context.Background())
	ch := make(chan os.Signal, 1)
	signal.Notify(ch, os.Interrupt, syscall.SIGTERM)
	go func() {
		<-ch
		cancel()
	}()
	return ctx, cancel
}

func preview(s string, n int) string {
	if len(s) <= n {
		return s
	}
	return s[:n] + "…"
}

func need(args []string, n int, usage string) {
	if len(args) < n {
		fmt.Fprintf(os.Stderr, "usage: attn-smoke %s\n", usage)
		os.Exit(2)
	}
}

func check(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
