// Package control is attnd's local control plane: a newline-delimited JSON
// request/response protocol over a Unix domain socket. attnctl drives the
// daemon's single live relay connection through it (one connection, one store
// writer — avoids opening a second relay connection on the same address).
//
// This is a deliberately minimal precursor to M2's HTTP/CLI/MCP interfaces; it
// exists only so M1's outbound ops can be driven and verified through the actual
// daemon. The Unix socket is Linux/macOS; M4's portable surface is HTTP.
package control

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"net"
	"os"
	"time"
)

// Request is a single control command.
type Request struct {
	Op   string         `json:"op"`
	Args map[string]any `json:"args,omitempty"`
}

// Response is the result of a control command.
type Response struct {
	OK    bool           `json:"ok"`
	Text  string         `json:"text,omitempty"`
	Data  map[string]any `json:"data,omitempty"`
	Error string         `json:"error,omitempty"`
}

// Handler executes a request and returns a response.
type Handler func(ctx context.Context, req Request) Response

// Serve listens on a Unix socket at sockPath and serves one request per
// connection until ctx is cancelled. Any stale socket file is removed first.
func Serve(ctx context.Context, sockPath string, h Handler) error {
	_ = os.Remove(sockPath)
	ln, err := net.Listen("unix", sockPath)
	if err != nil {
		return fmt.Errorf("listen unix %s: %w", sockPath, err)
	}
	_ = os.Chmod(sockPath, 0o600)

	go func() {
		<-ctx.Done()
		_ = ln.Close()
		_ = os.Remove(sockPath)
	}()

	for {
		conn, err := ln.Accept()
		if err != nil {
			if ctx.Err() != nil {
				return nil // graceful shutdown
			}
			return err
		}
		go serveConn(ctx, conn, h)
	}
}

func serveConn(ctx context.Context, conn net.Conn, h Handler) {
	defer conn.Close()
	_ = conn.SetReadDeadline(time.Now().Add(5 * time.Minute))
	br := bufio.NewReader(conn)
	line, err := br.ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return
	}
	var req Request
	if err := json.Unmarshal(line, &req); err != nil {
		writeResp(conn, Response{OK: false, Error: "bad request: " + err.Error()})
		return
	}
	resp := h(ctx, req)
	writeResp(conn, resp)
}

func writeResp(conn net.Conn, resp Response) {
	b, _ := json.Marshal(resp)
	b = append(b, '\n')
	_, _ = conn.Write(b)
}

// Call connects to the daemon socket, sends one request, and returns the response.
func Call(sockPath string, req Request, timeout time.Duration) (Response, error) {
	conn, err := net.DialTimeout("unix", sockPath, timeout)
	if err != nil {
		return Response{}, fmt.Errorf("connect to attnd at %s: %w (is the daemon running?)", sockPath, err)
	}
	defer conn.Close()
	_ = conn.SetDeadline(time.Now().Add(timeout))

	b, _ := json.Marshal(req)
	b = append(b, '\n')
	if _, err := conn.Write(b); err != nil {
		return Response{}, err
	}
	br := bufio.NewReader(conn)
	line, err := br.ReadBytes('\n')
	if err != nil && len(line) == 0 {
		return Response{}, err
	}
	var resp Response
	if err := json.Unmarshal(line, &resp); err != nil {
		return Response{}, fmt.Errorf("decode response: %w", err)
	}
	return resp, nil
}
