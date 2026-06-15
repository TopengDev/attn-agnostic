// Package buildinfo carries the binary's version metadata, injected at link
// time by scripts/build.sh via -ldflags "-X". The defaults make a plain
// `go build` (no ldflags) still produce a sensible, non-empty version, so the
// binaries are self-describing whether built by the matrix script or directly.
package buildinfo

// These are overridden at link time, e.g.:
//
//	go build -ldflags "\
//	  -X github.com/TopengDev/attn-agnostic/internal/buildinfo.Version=v0.1.0 \
//	  -X github.com/TopengDev/attn-agnostic/internal/buildinfo.Commit=abc1234 \
//	  -X github.com/TopengDev/attn-agnostic/internal/buildinfo.Date=2026-06-16T00:00:00Z" ./cmd/attnd
var (
	// Version is a semantic version or `git describe` output.
	Version = "dev"
	// Commit is the short git commit the build was produced from.
	Commit = "none"
	// Date is the build timestamp (UTC, RFC3339), set by the build script.
	Date = "unknown"
)

// String renders a one-line version banner: "v0.1.0 (commit abc1234, built …)".
func String() string {
	return Version + " (commit " + Commit + ", built " + Date + ")"
}
