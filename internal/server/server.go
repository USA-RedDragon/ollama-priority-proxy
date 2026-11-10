// Package server runs the proxy and metrics HTTP listeners.
package server

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/USA-RedDragon/ollama-priority-proxy/internal/config"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"golang.org/x/sync/errgroup"
)

const (
	readHeaderTimeout = 5 * time.Second
	idleTimeout       = 2 * time.Minute
	healthPath        = "/health"
	readyPath         = "/ready"
)

// Upstream is the proxy handler plus its readiness check.
type Upstream interface {
	http.Handler
	Ready(ctx context.Context) error
}

// Server holds the proxy and metrics listeners.
type Server struct {
	servers         []*http.Server
	shutdownTimeout time.Duration
}

// Handler serves /health and /ready itself and sends every other request to
// upstream. It does not use http.ServeMux, so paths reach the upstream as
// the client sent them.
func Handler(upstream Upstream) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case healthPath:
			_, _ = w.Write([]byte("OK"))
		case readyPath:
			if err := upstream.Ready(r.Context()); err != nil {
				slog.Warn("Upstream not ready", "error", err.Error())
				http.Error(w, "upstream not ready", http.StatusServiceUnavailable)
				return
			}
			_, _ = w.Write([]byte("OK"))
		default:
			upstream.ServeHTTP(w, r)
		}
	})
}

func addr(bind string, port uint16) string {
	return net.JoinHostPort(strings.Trim(bind, "[]"), strconv.Itoa(int(port)))
}

// New returns a Server. The proxy listener has no WriteTimeout because
// generation responses stream for minutes.
func New(cfg *config.Config, upstream Upstream, gatherer prometheus.Gatherer) *Server {
	s := &Server{shutdownTimeout: cfg.HTTP.ShutdownTimeout, servers: []*http.Server{{
		Addr:              addr(cfg.HTTP.Bind, cfg.HTTP.Port),
		Handler:           Handler(upstream),
		ReadHeaderTimeout: readHeaderTimeout,
		IdleTimeout:       idleTimeout,
	}}}
	if cfg.Metrics.Enabled {
		mux := http.NewServeMux()
		mux.Handle("/metrics", promhttp.HandlerFor(gatherer, promhttp.HandlerOpts{}))
		s.servers = append(s.servers, &http.Server{
			Addr:              addr(cfg.Metrics.Bind, cfg.Metrics.Port),
			Handler:           mux,
			ReadHeaderTimeout: readHeaderTimeout,
			WriteTimeout:      readHeaderTimeout,
		})
	}
	return s
}

// Start opens every listener, closing the ones already open if one fails,
// then serves in the background.
func (s *Server) Start() error {
	var lc net.ListenConfig
	listeners := make([]net.Listener, 0, len(s.servers))
	for _, srv := range s.servers {
		l, err := lc.Listen(context.Background(), "tcp", srv.Addr)
		if err != nil {
			for _, opened := range listeners {
				_ = opened.Close()
			}
			return fmt.Errorf("listen on %s: %w", srv.Addr, err)
		}
		listeners = append(listeners, l)
	}
	for i, srv := range s.servers {
		l := listeners[i]
		go func() {
			if err := srv.Serve(l); err != nil && !errors.Is(err, http.ErrServerClosed) {
				slog.Error("HTTP server failed", "address", srv.Addr, "error", err)
			}
		}()
		slog.Info("HTTP server started", "address", l.Addr().String())
	}
	return nil
}

// Stop shuts every listener down, waiting up to http.shutdown-timeout for
// requests in flight.
func (s *Server) Stop() error {
	ctx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
	defer cancel()
	var g errgroup.Group
	for _, srv := range s.servers {
		g.Go(func() error {
			return srv.Shutdown(ctx)
		})
	}
	return g.Wait()
}
