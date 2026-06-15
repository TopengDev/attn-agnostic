// Package names is attnd's client for the AttnNames registrar on Base mainnet
// (the .attn name system). It binds the registrar ABI via go-ethereum and talks
// to a Base RPC endpoint.
//
// READ paths (resolve, primaryNameOf, available, registrationFee, namehash,
// balanceOf, ownerOf) are free eth_calls and used live. WRITE paths (register,
// transferFrom, setPrimaryName) cost real ETH and are IRREVERSIBLE on mainnet —
// they are fully encoded + unit-tested + eth_call-simulatable here, but the
// actual broadcast is GATED behind Send* methods that refuse unless an explicit
// allow flag is set. M1 never sets it; a paid registration requires the
// supervisor's go.
package names

import (
	"context"
	"fmt"
	"math/big"
	"strings"

	"github.com/ethereum/go-ethereum"
	"github.com/ethereum/go-ethereum/accounts/abi"
	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/ethclient"
)

// ContractAddress is the AttnNames registrar on Base (shared/constants.ts).
const ContractAddress = "0x5caDD2F7d8fC6B35bb220cC3DB8DBc187E02dC7A"

// registrarABI is the AttnNames ABI (mirrors shared/attn-names-abi.ts).
const registrarABI = `[
 {"type":"function","name":"resolve","inputs":[{"name":"label","type":"string"}],"outputs":[{"name":"owner_","type":"address"},{"name":"node","type":"bytes32"}],"stateMutability":"view"},
 {"type":"function","name":"primaryNameOf","inputs":[{"name":"addr","type":"address"}],"outputs":[{"name":"","type":"string"}],"stateMutability":"view"},
 {"type":"function","name":"available","inputs":[{"name":"label","type":"string"}],"outputs":[{"name":"","type":"bool"}],"stateMutability":"view"},
 {"type":"function","name":"register","inputs":[{"name":"label","type":"string"}],"outputs":[],"stateMutability":"payable"},
 {"type":"function","name":"setPrimaryName","inputs":[{"name":"label","type":"string"}],"outputs":[],"stateMutability":"nonpayable"},
 {"type":"function","name":"registrationFee","inputs":[],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view"},
 {"type":"function","name":"namehash","inputs":[{"name":"label","type":"string"}],"outputs":[{"name":"","type":"bytes32"}],"stateMutability":"pure"},
 {"type":"function","name":"balanceOf","inputs":[{"name":"owner","type":"address"}],"outputs":[{"name":"","type":"uint256"}],"stateMutability":"view"},
 {"type":"function","name":"ownerOf","inputs":[{"name":"tokenId","type":"uint256"}],"outputs":[{"name":"","type":"address"}],"stateMutability":"view"},
 {"type":"function","name":"transferFrom","inputs":[{"name":"from","type":"address"},{"name":"to","type":"address"},{"name":"tokenId","type":"uint256"}],"outputs":[],"stateMutability":"nonpayable"}
]`

// Client talks to the AttnNames registrar on Base.
type Client struct {
	addr common.Address
	abi  abi.ABI
	eth  *ethclient.Client
}

// ParsedABI returns the registrar ABI without dialing — useful for offline
// encoding unit tests.
func ParsedABI() (abi.ABI, error) {
	return abi.JSON(strings.NewReader(registrarABI))
}

// New dials the Base RPC and binds the registrar ABI.
func New(ctx context.Context, rpcURL string) (*Client, error) {
	parsed, err := ParsedABI()
	if err != nil {
		return nil, fmt.Errorf("parse registrar abi: %w", err)
	}
	eth, err := ethclient.DialContext(ctx, rpcURL)
	if err != nil {
		return nil, fmt.Errorf("dial base rpc %s: %w", rpcURL, err)
	}
	return &Client{addr: common.HexToAddress(ContractAddress), abi: parsed, eth: eth}, nil
}

// Close releases the RPC connection.
func (c *Client) Close() {
	if c.eth != nil {
		c.eth.Close()
	}
}

func normalizeLabel(label string) string {
	return strings.TrimSuffix(strings.ToLower(strings.TrimSpace(label)), ".attn")
}

// call packs a read method, eth_calls it, and unpacks the result tuple.
func (c *Client) call(ctx context.Context, method string, args ...interface{}) ([]interface{}, error) {
	data, err := c.abi.Pack(method, args...)
	if err != nil {
		return nil, fmt.Errorf("pack %s: %w", method, err)
	}
	out, err := c.eth.CallContract(ctx, ethereum.CallMsg{To: &c.addr, Data: data}, nil)
	if err != nil {
		return nil, fmt.Errorf("eth_call %s: %w", method, err)
	}
	vals, err := c.abi.Unpack(method, out)
	if err != nil {
		return nil, fmt.Errorf("unpack %s: %w", method, err)
	}
	return vals, nil
}

// ── READ paths (live, free) ──────────────────────────────────────────────

// Resolve returns the owner address and namehash node for a label. owner is the
// zero address if the name is not registered.
func (c *Client) Resolve(ctx context.Context, label string) (common.Address, [32]byte, error) {
	vals, err := c.call(ctx, "resolve", normalizeLabel(label))
	if err != nil {
		return common.Address{}, [32]byte{}, err
	}
	if len(vals) != 2 {
		return common.Address{}, [32]byte{}, fmt.Errorf("resolve: expected 2 outputs, got %d", len(vals))
	}
	owner, ok := vals[0].(common.Address)
	if !ok {
		return common.Address{}, [32]byte{}, fmt.Errorf("resolve: owner not an address")
	}
	node, ok := vals[1].([32]byte)
	if !ok {
		return common.Address{}, [32]byte{}, fmt.Errorf("resolve: node not bytes32")
	}
	return owner, node, nil
}

// PrimaryNameOf returns the primary .attn label set for an address ("" if none).
func (c *Client) PrimaryNameOf(ctx context.Context, addr common.Address) (string, error) {
	vals, err := c.call(ctx, "primaryNameOf", addr)
	if err != nil {
		return "", err
	}
	name, ok := vals[0].(string)
	if !ok {
		return "", fmt.Errorf("primaryNameOf: not a string")
	}
	return name, nil
}

// Available reports whether a label is unregistered.
func (c *Client) Available(ctx context.Context, label string) (bool, error) {
	vals, err := c.call(ctx, "available", normalizeLabel(label))
	if err != nil {
		return false, err
	}
	b, ok := vals[0].(bool)
	if !ok {
		return false, fmt.Errorf("available: not a bool")
	}
	return b, nil
}

// RegistrationFee returns the current registration fee in wei.
func (c *Client) RegistrationFee(ctx context.Context) (*big.Int, error) {
	vals, err := c.call(ctx, "registrationFee")
	if err != nil {
		return nil, err
	}
	fee, ok := vals[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("registrationFee: not a uint256")
	}
	return fee, nil
}

// Namehash returns the contract's namehash for a label.
func (c *Client) Namehash(ctx context.Context, label string) ([32]byte, error) {
	vals, err := c.call(ctx, "namehash", normalizeLabel(label))
	if err != nil {
		return [32]byte{}, err
	}
	node, ok := vals[0].([32]byte)
	if !ok {
		return [32]byte{}, fmt.Errorf("namehash: not bytes32")
	}
	return node, nil
}

// BalanceOf returns how many .attn names an address owns.
func (c *Client) BalanceOf(ctx context.Context, addr common.Address) (*big.Int, error) {
	vals, err := c.call(ctx, "balanceOf", addr)
	if err != nil {
		return nil, err
	}
	bal, ok := vals[0].(*big.Int)
	if !ok {
		return nil, fmt.Errorf("balanceOf: not a uint256")
	}
	return bal, nil
}

// OwnerOf returns the owner of a name's tokenId.
func (c *Client) OwnerOf(ctx context.Context, tokenID *big.Int) (common.Address, error) {
	vals, err := c.call(ctx, "ownerOf", tokenID)
	if err != nil {
		return common.Address{}, err
	}
	owner, ok := vals[0].(common.Address)
	if !ok {
		return common.Address{}, fmt.Errorf("ownerOf: not an address")
	}
	return owner, nil
}

// TokenIDFromNode converts a namehash node to its ERC-721 tokenId (BigInt(node)).
func TokenIDFromNode(node [32]byte) *big.Int {
	return new(big.Int).SetBytes(node[:])
}

// ── WRITE encoders (pure, offline, unit-testable) ────────────────────────
// These produce the exact calldata the registrar expects. They DO NOT touch
// the network and DO NOT cost anything. Broadcasting them is the gated part.

// PackRegister encodes register(label).
func (c *Client) PackRegister(label string) ([]byte, error) {
	return c.abi.Pack("register", normalizeLabel(label))
}

// PackTransferFrom encodes transferFrom(from, to, tokenId).
func (c *Client) PackTransferFrom(from, to common.Address, tokenID *big.Int) ([]byte, error) {
	return c.abi.Pack("transferFrom", from, to, tokenID)
}

// PackSetPrimaryName encodes setPrimaryName(label).
func (c *Client) PackSetPrimaryName(label string) ([]byte, error) {
	return c.abi.Pack("setPrimaryName", normalizeLabel(label))
}

// ── WRITE simulation (live, free) + broadcast GATE ───────────────────────

// SimulateRegister eth_calls register(label) with the fee attached as if `from`
// were sending it. This validates the write path (the call would not revert)
// WITHOUT spending anything — no transaction is broadcast.
func (c *Client) SimulateRegister(ctx context.Context, from common.Address, label string, fee *big.Int) error {
	data, err := c.PackRegister(label)
	if err != nil {
		return err
	}
	_, err = c.eth.CallContract(ctx, ethereum.CallMsg{
		From:  from,
		To:    &c.addr,
		Value: fee,
		Data:  data,
	}, nil)
	return err
}

// ErrPaidWriteGated is returned by every broadcast path while the money gate is
// closed (always, in M1).
var ErrPaidWriteGated = fmt.Errorf("paid on-chain write is GATED (0.001 ETH, irreversible on Base mainnet) — requires explicit operator approval, refused in M1")

// SendRegister is the broadcast path for register(label). It is permanently
// gated in M1: it returns ErrPaidWriteGated unless `allow` is true, and M1
// never passes true. The encoded calldata is returned for inspection.
func (c *Client) SendRegister(_ context.Context, label string, allow bool) ([]byte, error) {
	data, err := c.PackRegister(label)
	if err != nil {
		return nil, err
	}
	if !allow {
		return data, ErrPaidWriteGated
	}
	return data, fmt.Errorf("broadcast not implemented in M1 (gate is a hard stop, not a TODO)")
}

// SendTransferName is the gated broadcast path for transferFrom.
func (c *Client) SendTransferName(_ context.Context, from, to common.Address, tokenID *big.Int, allow bool) ([]byte, error) {
	data, err := c.PackTransferFrom(from, to, tokenID)
	if err != nil {
		return nil, err
	}
	if !allow {
		return data, ErrPaidWriteGated
	}
	return data, fmt.Errorf("broadcast not implemented in M1 (gate is a hard stop, not a TODO)")
}

// SendSetPrimaryName is the gated broadcast path for setPrimaryName. Although it
// is not itself payable, it still mutates on-chain state + costs gas, so it
// shares the same gate.
func (c *Client) SendSetPrimaryName(_ context.Context, label string, allow bool) ([]byte, error) {
	data, err := c.PackSetPrimaryName(label)
	if err != nil {
		return nil, err
	}
	if !allow {
		return data, ErrPaidWriteGated
	}
	return data, fmt.Errorf("broadcast not implemented in M1 (gate is a hard stop, not a TODO)")
}
