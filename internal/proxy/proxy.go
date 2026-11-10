// Package proxy forwards requests to Ollama and schedules generation
// requests by priority class.
package proxy

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"math"
	"net/http"
	"net/http/httputil"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/USA-RedDragon/ollama-priority-proxy/internal/metrics"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/scheduler"
)

// LowPrefix is the path prefix that marks a request as low priority. It is
// removed before the request is forwarded.
const LowPrefix = "/low"

// PriorityHeader marks a request as low priority when set to "low". It is
// removed before the request is forwarded.
const PriorityHeader = "X-Priority"

const (
	routeOther       = "other"
	readyTimeout     = 3 * time.Second
	statusCanceled   = 499
	reasonUpstream   = "upstream"
	reasonMaxWait    = "max_wait"
	reasonCanceled   = "canceled"
	reasonBadRequest = "bad_request"
	idleConnsToHost  = 32
)

// MaxLowBodyBytes is the largest low priority request body. Low priority
// bodies are held in memory while the request waits in the queue.
const MaxLowBodyBytes = 64 << 20

var ErrUpstreamNotReady = errors.New("upstream is not ready")

// Options configures a Proxy.
type Options struct {
	Upstream        *url.URL
	GenerationPaths []string
	Scheduler       *scheduler.Scheduler
	Metrics         *metrics.Metrics
	// MaxWait is the low priority queue limit, used for Retry-After.
	MaxWait time.Duration
	// Transport defaults to a clone of http.DefaultTransport.
	Transport http.RoundTripper
}

// Proxy is an http.Handler that forwards every request to the upstream.
type Proxy struct {
	upstream   *url.URL
	generation map[string]bool
	sched      *scheduler.Scheduler
	metrics    *metrics.Metrics
	maxWait    time.Duration
	transport  http.RoundTripper
	rp         *httputil.ReverseProxy
}

type ctxKey struct{}

type requestInfo struct {
	err error
}

// New returns a Proxy.
func New(opts Options) *Proxy {
	transport := opts.Transport
	if transport == nil {
		if t, ok := http.DefaultTransport.(*http.Transport); ok {
			t = t.Clone()
			t.MaxIdleConnsPerHost = idleConnsToHost
			transport = t
		} else {
			transport = http.DefaultTransport
		}
	}
	p := &Proxy{
		upstream:   opts.Upstream,
		generation: make(map[string]bool, len(opts.GenerationPaths)),
		sched:      opts.Scheduler,
		metrics:    opts.Metrics,
		maxWait:    opts.MaxWait,
		transport:  transport,
	}
	for _, path := range opts.GenerationPaths {
		p.generation[normalize(path)] = true
	}
	p.rp = &httputil.ReverseProxy{
		Rewrite: func(pr *httputil.ProxyRequest) {
			pr.SetURL(p.upstream)
			pr.SetXForwarded()
			pr.Out.Header.Del(PriorityHeader)
		},
		Transport:     transport,
		FlushInterval: -1,
		ErrorLog:      slog.NewLogLogger(slog.Default().Handler(), slog.LevelWarn),
		ErrorHandler: func(w http.ResponseWriter, r *http.Request, err error) {
			if info, ok := r.Context().Value(ctxKey{}).(*requestInfo); ok {
				info.err = err
			}
			w.WriteHeader(http.StatusBadGateway)
		},
	}
	return p
}

func normalize(path string) string {
	if len(path) > 1 {
		return strings.TrimRight(path, "/")
	}
	return path
}

func stripLow(path string) (string, bool) {
	if path == LowPrefix {
		return "/", true
	}
	if rest, ok := strings.CutPrefix(path, LowPrefix+"/"); ok {
		return "/" + rest, true
	}
	return path, false
}

// ServeHTTP classifies, schedules and forwards a request.
func (p *Proxy) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	start := time.Now()
	class := metrics.ClassHigh
	u := *r.URL
	if stripped, ok := stripLow(u.Path); ok {
		class = metrics.ClassLow
		u.Path = stripped
		if u.RawPath != "" {
			u.RawPath, _ = stripLow(u.RawPath)
		}
	} else if strings.EqualFold(r.Header.Get(PriorityHeader), metrics.ClassLow) {
		class = metrics.ClassLow
	}

	route := routeOther
	generation := r.Method == http.MethodPost && p.generation[normalize(u.Path)]
	if generation {
		route = normalize(u.Path)
	}

	log := slog.With("method", r.Method, "path", u.Path, "class", class, "route", route)

	var wait time.Duration
	if generation {
		if class == metrics.ClassHigh {
			defer p.sched.AcquireHigh()()
		} else {
			if err := bufferBody(w, r); err != nil {
				p.rejectBody(w, r, err, route, log)
				return
			}
			release, err := p.sched.AcquireLow(r.Context())
			wait = time.Since(start)
			if err != nil {
				p.rejectLow(w, err, route, wait, log)
				return
			}
			defer release()
		}
		wait = time.Since(start)
		p.metrics.QueueWait.WithLabelValues(class).Observe(wait.Seconds())
	}

	info := &requestInfo{}
	ctx := context.WithValue(r.Context(), ctxKey{}, info)
	out := r.WithContext(ctx)
	out.URL = &u
	rec := &statusRecorder{ResponseWriter: w, code: http.StatusOK}
	upstreamStart := time.Now()

	defer func(ctx context.Context) {
		aborted := recover()
		elapsed := time.Since(upstreamStart)
		p.metrics.UpstreamLatency.WithLabelValues(class, route).Observe(elapsed.Seconds())
		code := rec.code
		err := info.err
		if aborted != nil && err == nil {
			err = fmt.Errorf("response copy aborted: %v", aborted)
		}
		if err != nil {
			reason := reasonUpstream
			if ctx.Err() != nil {
				reason = reasonCanceled
				code = statusCanceled
			}
			p.metrics.Errors.WithLabelValues(class, route, reason).Inc()
			log.Warn("Request failed", "reason", reason, "status", code, "wait", wait, "duration", elapsed, "error", err.Error())
		} else {
			level := slog.LevelDebug
			if generation {
				level = slog.LevelInfo
			}
			log.Log(ctx, level, "Request done", "status", code, "wait", wait, "duration", elapsed)
		}
		p.metrics.Requests.WithLabelValues(class, route, strconv.Itoa(code)).Inc()
		if aborted != nil {
			panic(aborted)
		}
	}(r.Context())

	p.rp.ServeHTTP(rec, out)
}

// bufferBody reads the request body into memory. The server only notices a
// client disconnect once the body has been read to EOF, so a queued request
// whose body is still unread would never see its context canceled.
func bufferBody(w http.ResponseWriter, r *http.Request) error {
	if r.Body == nil || r.Body == http.NoBody {
		return nil
	}
	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, MaxLowBodyBytes))
	_ = r.Body.Close()
	if err != nil {
		return err
	}
	r.Body = io.NopCloser(bytes.NewReader(body))
	r.ContentLength = int64(len(body))
	return nil
}

func (p *Proxy) rejectBody(w http.ResponseWriter, r *http.Request, err error, route string, log *slog.Logger) {
	reason, code := reasonBadRequest, http.StatusBadRequest
	var tooLarge *http.MaxBytesError
	switch {
	case r.Context().Err() != nil:
		reason, code = reasonCanceled, statusCanceled
	case errors.As(err, &tooLarge):
		code = http.StatusRequestEntityTooLarge
	}
	p.metrics.Errors.WithLabelValues(metrics.ClassLow, route, reason).Inc()
	p.metrics.Requests.WithLabelValues(metrics.ClassLow, route, strconv.Itoa(code)).Inc()
	log.Warn("Failed to read low priority request body", "status", code, "error", err.Error())
	if code != statusCanceled {
		http.Error(w, "failed to read request body", code)
	}
}

func (p *Proxy) rejectLow(w http.ResponseWriter, err error, route string, wait time.Duration, log *slog.Logger) {
	if errors.Is(err, scheduler.ErrMaxWait) {
		p.metrics.Errors.WithLabelValues(metrics.ClassLow, route, reasonMaxWait).Inc()
		p.metrics.Requests.WithLabelValues(metrics.ClassLow, route, strconv.Itoa(http.StatusServiceUnavailable)).Inc()
		log.Warn("Low priority request waited too long", "wait", wait)
		retry := max(1, int(math.Ceil(p.maxWait.Seconds())))
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("Retry-After", strconv.Itoa(retry))
		w.WriteHeader(http.StatusServiceUnavailable)
		_, _ = fmt.Fprintf(w, "{\"error\":%q}\n", "low priority queue wait exceeded "+p.maxWait.String())
		return
	}
	p.metrics.Errors.WithLabelValues(metrics.ClassLow, route, reasonCanceled).Inc()
	p.metrics.Requests.WithLabelValues(metrics.ClassLow, route, strconv.Itoa(statusCanceled)).Inc()
	log.Info("Low priority request canceled while queued", "wait", wait)
}

// Ready checks that the upstream answers GET /api/version.
func (p *Proxy) Ready(ctx context.Context) error {
	ctx, cancel := context.WithTimeout(ctx, readyTimeout)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, p.upstream.JoinPath("/api/version").String(), nil)
	if err != nil {
		return err
	}
	resp, err := p.transport.RoundTrip(req)
	if err != nil {
		return err
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return fmt.Errorf("%w: status %d", ErrUpstreamNotReady, resp.StatusCode)
	}
	return nil
}

type statusRecorder struct {
	http.ResponseWriter
	code        int
	wroteHeader bool
}

func (s *statusRecorder) WriteHeader(code int) {
	if !s.wroteHeader && (code >= http.StatusOK || code == http.StatusSwitchingProtocols) {
		s.code = code
		s.wroteHeader = true
	}
	s.ResponseWriter.WriteHeader(code)
}

func (s *statusRecorder) Write(b []byte) (int, error) {
	s.wroteHeader = true
	return s.ResponseWriter.Write(b)
}

func (s *statusRecorder) Unwrap() http.ResponseWriter {
	return s.ResponseWriter
}
