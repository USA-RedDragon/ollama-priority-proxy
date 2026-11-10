package server_test

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/USA-RedDragon/ollama-priority-proxy/internal/config"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/server"
	"github.com/prometheus/client_golang/prometheus"
)

var errDown = errors.New("down")

type fakeUpstream struct {
	ready error
	paths []string
}

func (f *fakeUpstream) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	f.paths = append(f.paths, r.URL.Path)
	w.WriteHeader(http.StatusTeapot)
}

func (f *fakeUpstream) Ready(context.Context) error { return f.ready }

func get(t *testing.T, h http.Handler, path string) (int, string) {
	t.Helper()
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodGet, path, nil))
	b, _ := io.ReadAll(rec.Body)
	return rec.Code, string(b)
}

func TestHealthDoesNotHitUpstream(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{ready: errDown}
	if code, body := get(t, server.Handler(up), "/health"); code != http.StatusOK || body != "OK" {
		t.Errorf("/health = %d %q", code, body)
	}
	if len(up.paths) != 0 {
		t.Errorf("upstream got %v", up.paths)
	}
}

func TestReady(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{}
	if code, _ := get(t, server.Handler(up), "/ready"); code != http.StatusOK {
		t.Errorf("/ready = %d, want 200", code)
	}
	up.ready = errDown
	if code, _ := get(t, server.Handler(up), "/ready"); code != http.StatusServiceUnavailable {
		t.Errorf("/ready = %d, want 503", code)
	}
}

func TestOtherPathsGoUpstream(t *testing.T) {
	t.Parallel()
	up := &fakeUpstream{}
	h := server.Handler(up)
	for _, p := range []string{"/", "/api/tags", "/low/health", "//api/chat"} {
		if code, _ := get(t, h, p); code != http.StatusTeapot {
			t.Errorf("%s = %d, want the upstream's 418", p, code)
		}
	}
	if len(up.paths) != 4 || up.paths[3] != "//api/chat" {
		t.Errorf("upstream got %v", up.paths)
	}
}

func TestStartStop(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{
		HTTP:    config.HTTP{Bind: "127.0.0.1"},
		Metrics: config.Metrics{Enabled: true, Bind: "[::1]"},
	}
	s := server.New(cfg, &fakeUpstream{}, prometheus.NewRegistry())
	if err := s.Start(); err != nil {
		t.Fatal(err)
	}
	if err := s.Stop(); err != nil {
		t.Fatal(err)
	}
}

func TestStartFailsOnBadAddress(t *testing.T) {
	t.Parallel()
	cfg := &config.Config{HTTP: config.HTTP{Bind: "256.0.0.1"}}
	if err := server.New(cfg, &fakeUpstream{}, prometheus.NewRegistry()).Start(); err == nil {
		t.Error("expected an error")
	}
}
