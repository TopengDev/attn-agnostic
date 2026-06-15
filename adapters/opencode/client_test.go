package opencode

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

// fakeOpencode stands up an httptest server that mimics the opencode v1.3.13
// routes the client uses (verified live in the prototype). It records the last
// prompt_async body + Authorization header for assertions.
type fakeOpencode struct {
	srv          *httptest.Server
	version      string
	sessionRoute int // status for GET /session (drift-guard probe + list)
	promptStatus int // status for prompt_async
	lastPrompt   string
	lastAuth     string
}

func newFakeOpencode(t *testing.T) *fakeOpencode {
	t.Helper()
	f := &fakeOpencode{version: "1.3.13", sessionRoute: http.StatusOK, promptStatus: http.StatusNoContent}
	mux := http.NewServeMux()
	mux.HandleFunc("GET /global/health", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"healthy": true, "version": f.version})
	})
	mux.HandleFunc("GET /session", func(w http.ResponseWriter, r *http.Request) {
		if f.sessionRoute != http.StatusOK {
			w.WriteHeader(f.sessionRoute)
			return
		}
		json.NewEncoder(w).Encode([]map[string]any{
			{"id": "ses_old", "title": "old", "time": map[string]any{"created": 100, "updated": 100}},
			{"id": "ses_new", "title": "new", "time": map[string]any{"created": 200, "updated": 300}},
		})
	})
	mux.HandleFunc("POST /session", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]any{"id": "ses_created", "directory": r.URL.Query().Get("directory")})
	})
	mux.HandleFunc("POST /session/{id}/prompt_async", func(w http.ResponseWriter, r *http.Request) {
		f.lastAuth = r.Header.Get("Authorization")
		var body struct {
			Parts []struct {
				Type string `json:"type"`
				Text string `json:"text"`
			} `json:"parts"`
		}
		json.NewDecoder(r.Body).Decode(&body)
		if len(body.Parts) > 0 {
			f.lastPrompt = body.Parts[0].Text
		}
		w.WriteHeader(f.promptStatus)
	})
	mux.HandleFunc("GET /session/{id}/message", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode([]map[string]any{
			{"info": map[string]any{"id": "m1", "role": "user"}, "parts": []map[string]any{{"type": "text", "text": "q"}}},
			{"info": map[string]any{"id": "m2", "role": "assistant"}, "parts": []map[string]any{{"type": "reasoning", "text": "think"}, {"type": "text", "text": "42"}}},
		})
	})
	f.srv = httptest.NewServer(mux)
	t.Cleanup(f.srv.Close)
	return f
}

func TestClientHealthAndVerifyRoutes(t *testing.T) {
	f := newFakeOpencode(t)
	c := NewClient(f.srv.URL, "")

	ver, err := c.Health(context.Background())
	if err != nil || ver != "1.3.13" {
		t.Fatalf("Health = %q, %v; want 1.3.13", ver, err)
	}

	if _, err := c.VerifyRoutes(context.Background(), "1.3"); err != nil {
		t.Fatalf("VerifyRoutes(1.3) unexpected error: %v", err)
	}
	if _, err := c.VerifyRoutes(context.Background(), "9.9"); err == nil {
		t.Fatal("VerifyRoutes(9.9) should fail on pin mismatch")
	}

	// Drift guard: if the session route is not live, VerifyRoutes must fail even
	// though /global/health is fine.
	f.sessionRoute = http.StatusNotFound
	if _, err := c.VerifyRoutes(context.Background(), "1.3"); err == nil {
		t.Fatal("VerifyRoutes should fail when GET /session is not live")
	}
}

func TestClientCreateAndListSessions(t *testing.T) {
	f := newFakeOpencode(t)
	c := NewClient(f.srv.URL, "")

	sessions, err := c.ListSessions(context.Background(), "/tmp/x")
	if err != nil || len(sessions) != 2 {
		t.Fatalf("ListSessions = %v, %v; want 2", sessions, err)
	}

	s, err := c.CreateSession(context.Background(), "/tmp/x", "t")
	if err != nil || s.ID != "ses_created" {
		t.Fatalf("CreateSession = %v, %v; want ses_created", s, err)
	}
	if s.Directory != "/tmp/x" {
		t.Errorf("CreateSession directory = %q; want /tmp/x (query not forwarded)", s.Directory)
	}
}

func TestClientPromptAsync(t *testing.T) {
	f := newFakeOpencode(t)
	c := NewClient(f.srv.URL, "")

	if err := c.PromptAsync(context.Background(), "ses_1", "hello world"); err != nil {
		t.Fatalf("PromptAsync error: %v", err)
	}
	if f.lastPrompt != "hello world" {
		t.Errorf("prompt body text = %q; want %q", f.lastPrompt, "hello world")
	}

	// empty sessionID is a client-side error (no request)
	if err := c.PromptAsync(context.Background(), "", "x"); err == nil {
		t.Error("PromptAsync with empty sessionID should error")
	}

	// non-2xx surfaces as an error
	f.promptStatus = http.StatusInternalServerError
	if err := c.PromptAsync(context.Background(), "ses_1", "x"); err == nil {
		t.Error("PromptAsync should error on HTTP 500")
	}
}

func TestClientPasswordHeader(t *testing.T) {
	f := newFakeOpencode(t)
	c := NewClient(f.srv.URL, "s3cret")
	if err := c.PromptAsync(context.Background(), "ses_1", "x"); err != nil {
		t.Fatalf("PromptAsync error: %v", err)
	}
	if f.lastAuth != "Bearer s3cret" {
		t.Errorf("Authorization = %q; want %q", f.lastAuth, "Bearer s3cret")
	}
}

func TestClientMessages(t *testing.T) {
	f := newFakeOpencode(t)
	c := NewClient(f.srv.URL, "")
	msgs, err := c.Messages(context.Background(), "ses_1")
	if err != nil || len(msgs) != 2 {
		t.Fatalf("Messages = %v, %v; want 2", msgs, err)
	}
	if msgs[0].Info.Role != "user" || msgs[0].Text() != "q" {
		t.Errorf("msg0 = %+v; want user/q", msgs[0])
	}
	// Text() concatenates only text parts (skips reasoning).
	if got := msgs[1].Text(); got != "42" {
		t.Errorf("msg1.Text() = %q; want 42 (reasoning part skipped)", got)
	}
	if !strings.Contains(msgs[1].Info.Role, "assistant") {
		t.Errorf("msg1 role = %q; want assistant", msgs[1].Info.Role)
	}
}
