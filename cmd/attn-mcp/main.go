// Command attn-mcp exposes attn's 29-tool surface to MCP-native harnesses
// (Claude Code / opencode / hermes) over stdio or HTTP (streamable + legacy SSE).
// It is OUTBOUND / management ONLY: every tool call is forwarded to the running
// attnd daemon's localhost REST API (POST /op/{name}), so the MCP server holds no
// business logic and never opens a second relay connection. Inbound push is NOT
// done over MCP (server-initiated notifications never reach the model — proven in
// the prototype); inbound is the daemon's WS stream (see internal/httpapi).
package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const version = "0.1.0"

func main() {
	var (
		transport = flag.String("transport", "stdio", "transport: stdio | http")
		httpAddr  = flag.String("addr", "127.0.0.1:9743", "HTTP transport bind (localhost only)")
		daemon    = flag.String("daemon", "", "attnd REST address (default 127.0.0.1:9742 / ATTN_HTTP_ADDR)")
	)
	flag.Parse()

	daemonAddr := *daemon
	if daemonAddr == "" {
		if e := strings.TrimSpace(os.Getenv("ATTN_HTTP_ADDR")); e != "" {
			daemonAddr = e
		} else {
			daemonAddr = "127.0.0.1:9742"
		}
	}

	logger := log.New(os.Stderr, "attn-mcp ", log.LstdFlags|log.Lmsgprefix)
	server := buildServer(daemonAddr)

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	switch *transport {
	case "stdio":
		logger.Printf("serving 29 tools over stdio → daemon %s", daemonAddr)
		if err := server.Run(ctx, &mcp.StdioTransport{}); err != nil && ctx.Err() == nil {
			logger.Fatalf("stdio transport: %v", err)
		}
	case "http":
		if err := serveHTTP(ctx, server, *httpAddr, daemonAddr, logger); err != nil {
			logger.Fatalf("http transport: %v", err)
		}
	default:
		logger.Fatalf("unknown transport %q (want stdio|http)", *transport)
	}
}

// buildServer registers all 29 tools, each forwarding to the daemon by op-name.
func buildServer(daemonAddr string) *mcp.Server {
	server := mcp.NewServer(&mcp.Implementation{Name: "attn", Version: version}, nil)
	handler := func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		op := req.Params.Name
		args := map[string]any{}
		if len(req.Params.Arguments) > 0 {
			if err := json.Unmarshal(req.Params.Arguments, &args); err != nil {
				return errResult("invalid arguments: " + err.Error()), nil
			}
		}
		text, daemonErr, transportErr := callDaemon(ctx, daemonAddr, op, args)
		if transportErr != nil {
			return errResult(transportErr.Error()), nil
		}
		if daemonErr != "" {
			return errResult(daemonErr), nil
		}
		return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: text}}}, nil
	}
	for _, td := range toolDefs {
		server.AddTool(&mcp.Tool{Name: td.name, Description: td.desc, InputSchema: td.schema()}, handler)
	}
	return server
}

// serveHTTP mounts the streamable-HTTP transport at /mcp and the legacy SSE
// transport at /sse, on a loopback-only listener.
func serveHTTP(ctx context.Context, server *mcp.Server, addr, daemonAddr string, logger *log.Logger) error {
	if err := loopbackOnly(addr); err != nil {
		return err
	}
	getServer := func(*http.Request) *mcp.Server { return server }
	mux := http.NewServeMux()
	mux.Handle("/mcp", mcp.NewStreamableHTTPHandler(getServer, nil))
	mux.Handle("/sse", mcp.NewSSEHandler(getServer, nil))
	// ReadHeaderTimeout + ReadTimeout + IdleTimeout guard against slow-loris;
	// WriteTimeout is intentionally left unset because the streamable-HTTP / SSE
	// transports hold long-lived server→client response streams.
	srv := &http.Server{
		Handler:           mux,
		ReadHeaderTimeout: 10 * time.Second,
		ReadTimeout:       30 * time.Second,
		IdleTimeout:       120 * time.Second,
	}
	ln, err := net.Listen("tcp", addr)
	if err != nil {
		return fmt.Errorf("listen %s: %w", addr, err)
	}
	go func() {
		<-ctx.Done()
		sctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		_ = srv.Shutdown(sctx)
	}()
	logger.Printf("serving 29 tools over HTTP on http://%s (/mcp streamable, /sse legacy) → daemon %s", addr, daemonAddr)
	if err := srv.Serve(ln); err != nil && err != http.ErrServerClosed {
		return err
	}
	return nil
}

func errResult(msg string) *mcp.CallToolResult {
	return &mcp.CallToolResult{IsError: true, Content: []mcp.Content{&mcp.TextContent{Text: msg}}}
}

// callDaemon POSTs args to the daemon's /op/{name} and returns (text, daemonError,
// transportError). daemonError is a business-level failure (the daemon replied
// ok:false); transportError is "daemon unreachable / bad response".
func callDaemon(ctx context.Context, addr, op string, args map[string]any) (string, string, error) {
	body, _ := json.Marshal(args)
	url := "http://" + addr + "/op/" + op
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, url, strings.NewReader(string(body)))
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := (&http.Client{Timeout: 45 * time.Second}).Do(req)
	if err != nil {
		return "", "", fmt.Errorf("attnd unreachable at %s (%v) — is the daemon running?", addr, err)
	}
	defer resp.Body.Close()
	raw, _ := io.ReadAll(io.LimitReader(resp.Body, 4<<20))
	var r struct {
		OK    bool   `json:"ok"`
		Text  string `json:"text"`
		Error string `json:"error"`
	}
	if err := json.Unmarshal(raw, &r); err != nil {
		return "", "", fmt.Errorf("bad daemon response (%d): %s", resp.StatusCode, strings.TrimSpace(string(raw)))
	}
	if !r.OK {
		return "", r.Error, nil
	}
	return r.Text, "", nil
}

func loopbackOnly(addr string) error {
	host, _, err := net.SplitHostPort(addr)
	if err != nil {
		return fmt.Errorf("invalid addr %q: %w", addr, err)
	}
	host = strings.TrimSpace(host)
	if host == "localhost" {
		return nil
	}
	ip := net.ParseIP(host)
	if ip == nil || !ip.IsLoopback() {
		return fmt.Errorf("refusing non-loopback bind %q (localhost only)", addr)
	}
	return nil
}
