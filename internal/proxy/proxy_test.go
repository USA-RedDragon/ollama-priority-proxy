package proxy_test

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/USA-RedDragon/ollama-priority-proxy/internal/metrics"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/proxy"
	"github.com/USA-RedDragon/ollama-priority-proxy/internal/scheduler"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/testutil"
)

const (
	idLow        = "low"
	idHeader     = "X-Test-Id"
	streamHeader = "X-Test-Stream"
	apiChat      = "/api/chat"
	chatPath     = "/v1/chat/completions"
	lowChatPath  = "/low/v1/chat/completions"
	quiet        = 200 * time.Millisecond
	eventually   = 5 * time.Second
)

func generationPaths() []string {
	return []string{chatPath, "/v1/completions", "/v1/embeddings", "/v1/responses", apiChat, "/api/generate", "/api/embed", "/api/embeddings"}
}

type arrival struct {
	id       string
	method   string
	path     string
	query    string
	body     string
	priority string
	custom   string
}

type fakeUpstream struct {
	t        *testing.T
	arrivals chan arrival
	canceled chan string
	done     chan struct{}

	mu       sync.Mutex
	releases map[string]chan struct{}
}

func newFakeUpstream(t *testing.T) (*fakeUpstream, *httptest.Server) {
	t.Helper()
	f := &fakeUpstream{
		t:        t,
		arrivals: make(chan arrival, 100),
		canceled: make(chan string, 100),
		done:     make(chan struct{}),
		releases: map[string]chan struct{}{},
	}
	srv := httptest.NewServer(http.HandlerFunc(f.serve))
	t.Cleanup(srv.Close)
	t.Cleanup(func() { close(f.done) })
	return f, srv
}

func (f *fakeUpstream) release(id string) chan struct{} {
	f.mu.Lock()
	defer f.mu.Unlock()
	ch, ok := f.releases[id]
	if !ok {
		ch = make(chan struct{})
		f.releases[id] = ch
	}
	return ch
}

func (f *fakeUpstream) finish(id string) {
	close(f.release(id))
}

func (f *fakeUpstream) serve(w http.ResponseWriter, r *http.Request) {
	body, _ := io.ReadAll(r.Body)
	id := r.Header.Get(idHeader)
	f.arrivals <- arrival{
		id:       id,
		method:   r.Method,
		path:     r.URL.Path,
		query:    r.URL.RawQuery,
		body:     string(body),
		priority: r.Header.Get(proxy.PriorityHeader),
		custom:   r.Header.Get("X-Custom"),
	}
	if id == "" {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"ok":true}`))
		return
	}
	if r.Header.Get(streamHeader) != "" {
		w.Header().Set("Content-Type", "application/x-ndjson")
		_, _ = w.Write([]byte("{\"chunk\":1}\n"))
		_ = http.NewResponseController(w).Flush()
	}
	select {
	case <-f.release(id):
		_, _ = w.Write([]byte("{\"done\":true}\n"))
	case <-r.Context().Done():
		f.canceled <- id
	case <-f.done:
	}
}

func (f *fakeUpstream) next(t *testing.T) arrival {
	t.Helper()
	select {
	case a := <-f.arrivals:
		return a
	case <-time.After(eventually):
		t.Fatal("timed out waiting for an upstream request")
		return arrival{}
	}
}

func (f *fakeUpstream) none(t *testing.T) {
	t.Helper()
	select {
	case a := <-f.arrivals:
		t.Fatalf("unexpected upstream request %+v", a)
	case <-time.After(quiet):
	}
}

type harness struct {
	up      *fakeUpstream
	sched   *scheduler.Scheduler
	reg     *prometheus.Registry
	metrics *metrics.Metrics
	url     string
}

func newHarness(t *testing.T, opts scheduler.Options) *harness {
	t.Helper()
	up, upSrv := newFakeUpstream(t)
	target, err := url.Parse(upSrv.URL)
	if err != nil {
		t.Fatal(err)
	}
	sched := scheduler.New(opts)
	reg := prometheus.NewRegistry()
	m, err := metrics.New(reg, sched)
	if err != nil {
		t.Fatal(err)
	}
	p := proxy.New(proxy.Options{
		Upstream:        target,
		GenerationPaths: generationPaths(),
		Scheduler:       sched,
		Metrics:         m,
		MaxWait:         opts.MaxWait,
	})
	srv := httptest.NewServer(p)
	t.Cleanup(srv.Close)
	return &harness{up: up, sched: sched, reg: reg, metrics: m, url: srv.URL}
}

type result struct {
	resp *http.Response
	body string
	err  error
}

func (h *harness) send(ctx context.Context, method, path, id string, header http.Header) <-chan result {
	out := make(chan result, 1)
	go func() {
		req, err := http.NewRequestWithContext(ctx, method, h.url+path, strings.NewReader(`{"model":"m"}`))
		if err != nil {
			out <- result{err: err}
			return
		}
		for k, v := range header {
			req.Header[k] = v
		}
		if id != "" {
			req.Header.Set(idHeader, id)
		}
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			out <- result{err: err}
			return
		}
		b, err := io.ReadAll(resp.Body)
		_ = resp.Body.Close()
		out <- result{resp: resp, body: string(b), err: err}
	}()
	return out
}

func (h *harness) post(path, id string) <-chan result {
	return h.send(context.Background(), http.MethodPost, path, id, nil)
}

func wait(t *testing.T, ch <-chan result) result {
	t.Helper()
	select {
	case r := <-ch:
		if r.err != nil {
			t.Fatalf("request failed: %v", r.err)
		}
		return r
	case <-time.After(eventually):
		t.Fatal("timed out waiting for a response")
		return result{}
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(eventually)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func (h *harness) queued(n int) func() bool {
	return func() bool { return h.sched.Stats().LowQueued == n }
}

func TestHighPassthroughStreaming(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})

	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, h.url+chatPath+"?a=1&b=2", strings.NewReader(`{"model":"m","stream":true}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set(idHeader, "s")
	req.Header.Set(streamHeader, "1")
	req.Header.Set("X-Custom", "kept")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	a := h.up.next(t)
	if a.method != http.MethodPost || a.path != chatPath || a.query != "a=1&b=2" || a.body != `{"model":"m","stream":true}` || a.custom != "kept" {
		t.Errorf("upstream got %+v", a)
	}
	if got := resp.Header.Get("Content-Type"); got != "application/x-ndjson" {
		t.Errorf("Content-Type = %q", got)
	}

	r := bufio.NewReader(resp.Body)
	line, err := r.ReadString('\n')
	if err != nil || line != "{\"chunk\":1}\n" {
		t.Fatalf("first chunk = %q, %v; want it before the upstream finishes", line, err)
	}
	if got := h.sched.Stats().HighInFlight; got != 1 {
		t.Errorf("high in flight = %d, want 1 while streaming", got)
	}
	h.up.finish("s")
	rest, err := io.ReadAll(r)
	if err != nil || string(rest) != "{\"done\":true}\n" {
		t.Fatalf("rest = %q, %v", rest, err)
	}
	waitFor(t, "high in flight to reach 0", func() bool { return h.sched.Stats().HighInFlight == 0 })
	if got := testutil.ToFloat64(h.metrics.Requests.WithLabelValues(metrics.ClassHigh, chatPath, "200")); got != 1 {
		t.Errorf("requests_total = %v, want 1", got)
	}
}

func TestHighNotLimited(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})
	r1 := h.post(chatPath, "h1")
	r2 := h.post(apiChat, "h2")
	h.up.next(t)
	h.up.next(t)
	h.up.finish("h1")
	h.up.finish("h2")
	wait(t, r1)
	wait(t, r2)
}

func TestLowHeldWhileHighInFlight(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{LowMaxInFlight: 1})

	high := h.post(chatPath, "high")
	h.up.next(t)
	low := h.post(lowChatPath, idLow)
	waitFor(t, "low to queue", h.queued(1))
	h.up.none(t)

	h.up.finish("high")
	wait(t, high)
	if a := h.up.next(t); a.id != idLow || a.path != chatPath {
		t.Fatalf("got %+v, want the low request on %s", a, chatPath)
	}
	h.up.finish(idLow)
	if r := wait(t, low); r.resp.StatusCode != http.StatusOK {
		t.Errorf("low status = %d", r.resp.StatusCode)
	}
}

func TestHighWhileLowWaiting(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})

	first := h.post(chatPath, "h1")
	h.up.next(t)
	low := h.post(lowChatPath, idLow)
	waitFor(t, "low to queue", h.queued(1))
	second := h.post("/api/generate", "h2")
	if a := h.up.next(t); a.id != "h2" {
		t.Fatalf("got %s, want the second high request first", a.id)
	}
	h.up.finish("h1")
	wait(t, first)
	h.up.none(t)
	h.up.finish("h2")
	wait(t, second)
	if a := h.up.next(t); a.id != idLow {
		t.Fatalf("got %s, want low", a.id)
	}
	h.up.finish(idLow)
	wait(t, low)
}

func TestLowInFlightNotPreempted(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})
	low := h.post(lowChatPath, idLow)
	h.up.next(t)
	high := h.post(chatPath, "high")
	if a := h.up.next(t); a.id != "high" {
		t.Fatalf("got %s, want high", a.id)
	}
	h.up.finish("high")
	wait(t, high)
	h.up.finish(idLow)
	if r := wait(t, low); r.resp.StatusCode != http.StatusOK {
		t.Errorf("low status = %d", r.resp.StatusCode)
	}
}

func TestLowMaxInFlight(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{LowMaxInFlight: 2})

	results := map[string]<-chan result{}
	for _, id := range []string{"a", "b", "c"} {
		results[id] = h.post(lowChatPath, id)
	}
	got := map[string]bool{h.up.next(t).id: true, h.up.next(t).id: true}
	h.up.none(t)
	if s := h.sched.Stats(); s.LowInFlight != 2 || s.LowQueued != 1 {
		t.Fatalf("stats = %+v, want 2 in flight and 1 queued", s)
	}
	var first string
	for id := range got {
		first = id
		break
	}
	h.up.finish(first)
	wait(t, results[first])
	third := h.up.next(t)
	if got[third.id] {
		t.Fatalf("got %s again", third.id)
	}
	for id := range results {
		if id != first {
			h.up.finish(id)
			wait(t, results[id])
		}
	}
}

func TestLowFIFO(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})

	ids := []string{"1", "2", "3", "4"}
	results := make([]<-chan result, 0, 1+len(ids))
	results = append(results, h.post(lowChatPath, "0"))
	h.up.next(t)
	for i, id := range ids {
		results = append(results, h.post(lowChatPath, id))
		waitFor(t, "low "+id+" to queue", h.queued(i+1))
	}
	h.up.finish("0")
	wait(t, results[0])
	for i, id := range ids {
		if a := h.up.next(t); a.id != id {
			t.Fatalf("dispatch %d = %s, want %s", i, a.id, id)
		}
		h.up.finish(id)
		wait(t, results[i+1])
	}
}

func TestIdleGrace(t *testing.T) {
	t.Parallel()
	const grace = 400 * time.Millisecond
	h := newHarness(t, scheduler.Options{IdleGrace: grace})

	first := h.post(lowChatPath, "first")
	if a := h.up.next(t); a.id != "first" {
		t.Fatalf("got %s; a low request with no earlier high request should go at once", a.id)
	}
	h.up.finish("first")
	wait(t, first)

	high := h.post(chatPath, "high")
	h.up.next(t)
	low := h.post(lowChatPath, idLow)
	waitFor(t, "low to queue", h.queued(1))
	h.up.finish("high")
	wait(t, high)
	released := time.Now()

	time.Sleep(grace / 2)
	turn := h.post(chatPath, "turn")
	if a := h.up.next(t); a.id != "turn" {
		t.Fatalf("got %s, want the next high request of the turn", a.id)
	}
	h.up.finish("turn")
	wait(t, turn)
	turnDone := time.Now()

	a := h.up.next(t)
	if a.id != idLow {
		t.Fatalf("got %s, want low", a.id)
	}
	if since := time.Since(released); since < grace {
		t.Errorf("low sent %v after the first high finished, want at least %v", since, grace)
	}
	if since := time.Since(turnDone); since < grace-50*time.Millisecond {
		t.Errorf("low sent %v after the last high finished, want about %v", since, grace)
	}
	h.up.finish(idLow)
	wait(t, low)
}

func TestCancelQueuedLow(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})

	high := h.post(chatPath, "high")
	h.up.next(t)
	ctx, cancel := context.WithCancel(t.Context())
	low := h.send(ctx, http.MethodPost, lowChatPath, idLow, nil)
	waitFor(t, "low to queue", h.queued(1))
	cancel()
	select {
	case r := <-low:
		if !errors.Is(r.err, context.Canceled) {
			t.Errorf("err = %v, want context.Canceled", r.err)
		}
	case <-time.After(eventually):
		t.Fatal("canceled request did not return")
	}
	waitFor(t, "queue to empty", h.queued(0))
	waitFor(t, "canceled metric", func() bool {
		return testutil.ToFloat64(h.metrics.Errors.WithLabelValues(metrics.ClassLow, chatPath, "canceled")) == 1
	})

	h.up.finish("high")
	wait(t, high)
	h.up.none(t)
	if s := h.sched.Stats(); s.LowInFlight != 0 {
		t.Errorf("stats = %+v", s)
	}
}

func TestCancelInFlightCancelsUpstream(t *testing.T) {
	t.Parallel()
	for _, path := range []string{chatPath, lowChatPath} {
		h := newHarness(t, scheduler.Options{})
		ctx, cancel := context.WithCancel(t.Context())
		res := h.send(ctx, http.MethodPost, path, "x", nil)
		h.up.next(t)
		cancel()
		select {
		case id := <-h.up.canceled:
			if id != "x" {
				t.Errorf("canceled %s", id)
			}
		case <-time.After(eventually):
			t.Fatalf("%s: upstream request was not canceled", path)
		}
		<-res
		waitFor(t, "in flight to reach 0", func() bool {
			s := h.sched.Stats()
			return s.HighInFlight == 0 && s.LowInFlight == 0
		})
	}
}

func TestNonGenerationBypass(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})

	high := h.post(chatPath, "high")
	h.up.next(t)
	for _, c := range []struct{ method, path, want string }{
		{http.MethodGet, "/low/api/tags", "/api/tags"},
		{http.MethodPost, "/low/api/show", "/api/show"},
		{http.MethodGet, "/low/v1/models", "/v1/models"},
		{http.MethodGet, "/low/v1/chat/completions", chatPath},
		{http.MethodGet, "/api/ps", "/api/ps"},
	} {
		r := wait(t, h.send(t.Context(), c.method, c.path, "", nil))
		if r.resp.StatusCode != http.StatusOK || r.body != `{"ok":true}` {
			t.Errorf("%s %s: %d %q", c.method, c.path, r.resp.StatusCode, r.body)
		}
		if a := h.up.next(t); a.path != c.want {
			t.Errorf("%s %s reached upstream as %s, want %s", c.method, c.path, a.path, c.want)
		}
	}
	h.up.finish("high")
	wait(t, high)
}

func TestLowPrefix(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})

	for _, c := range []struct{ path, want, query string }{
		{"/low/api/chat?x=1", apiChat, "x=1"},
		{"/low", "/", ""},
		{"/lowfoo/api/chat", "/lowfoo/api/chat", ""},
	} {
		r := wait(t, h.send(t.Context(), http.MethodGet, c.path, "", nil))
		if r.resp.StatusCode != http.StatusOK {
			t.Errorf("%s: status %d", c.path, r.resp.StatusCode)
		}
		if a := h.up.next(t); a.path != c.want || a.query != c.query {
			t.Errorf("%s reached upstream as %s?%s, want %s?%s", c.path, a.path, a.query, c.want, c.query)
		}
	}
}

func TestPriorityHeader(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})

	high := h.post(chatPath, "high")
	h.up.next(t)
	low := h.send(t.Context(), http.MethodPost, chatPath, idLow, http.Header{proxy.PriorityHeader: {"low"}})
	waitFor(t, "header-marked request to queue", h.queued(1))
	h.up.finish("high")
	wait(t, high)
	if a := h.up.next(t); a.id != idLow || a.priority != "" {
		t.Errorf("got %+v, want low without the priority header", a)
	}
	h.up.finish(idLow)
	wait(t, low)
}

func TestMaxWait(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{MaxWait: 300 * time.Millisecond})

	high := h.post(chatPath, "high")
	h.up.next(t)
	start := time.Now()
	r := wait(t, h.post(lowChatPath, idLow))
	if r.resp.StatusCode != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503", r.resp.StatusCode)
	}
	if elapsed := time.Since(start); elapsed < 300*time.Millisecond {
		t.Errorf("gave up after %v, want at least 300ms", elapsed)
	}
	if got := r.resp.Header.Get("Retry-After"); got != "1" {
		t.Errorf("Retry-After = %q, want 1", got)
	}
	if !strings.Contains(r.body, `"error"`) {
		t.Errorf("body = %q", r.body)
	}
	if s := h.sched.Stats(); s.LowQueued != 0 {
		t.Errorf("stats = %+v, want an empty queue", s)
	}
	if got := testutil.ToFloat64(h.metrics.Errors.WithLabelValues(metrics.ClassLow, chatPath, "max_wait")); got != 1 {
		t.Errorf("max_wait errors = %v", got)
	}
	h.up.finish("high")
	wait(t, high)
	h.up.none(t)
}

func TestUpstreamDown(t *testing.T) {
	t.Parallel()
	srv := httptest.NewServer(http.NotFoundHandler())
	target, _ := url.Parse(srv.URL)
	srv.Close()
	sched := scheduler.New(scheduler.Options{})
	m, err := metrics.New(prometheus.NewRegistry(), sched)
	if err != nil {
		t.Fatal(err)
	}
	p := proxy.New(proxy.Options{Upstream: target, GenerationPaths: generationPaths(), Scheduler: sched, Metrics: m})
	rec := httptest.NewRecorder()
	p.ServeHTTP(rec, httptest.NewRequestWithContext(t.Context(), http.MethodPost, chatPath, strings.NewReader("{}")))
	if rec.Code != http.StatusBadGateway {
		t.Errorf("status = %d, want 502", rec.Code)
	}
	if got := testutil.ToFloat64(m.Errors.WithLabelValues(metrics.ClassHigh, chatPath, "upstream")); got != 1 {
		t.Errorf("upstream errors = %v", got)
	}
	if err := p.Ready(t.Context()); err == nil {
		t.Error("Ready should fail when the upstream is down")
	}
}

func TestReady(t *testing.T) {
	t.Parallel()
	up, upSrv := newFakeUpstream(t)
	target, _ := url.Parse(upSrv.URL)
	sched := scheduler.New(scheduler.Options{})
	m, err := metrics.New(prometheus.NewRegistry(), sched)
	if err != nil {
		t.Fatal(err)
	}
	p := proxy.New(proxy.Options{Upstream: target, Scheduler: sched, Metrics: m})
	if err := p.Ready(t.Context()); err != nil {
		t.Fatal(err)
	}
	if a := up.next(t); a.path != "/api/version" || a.method != http.MethodGet {
		t.Errorf("ready check sent %+v", a)
	}
}

func TestExpectContinueRecordsFinalStatus(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})
	for _, path := range []string{apiChat, "/low/api/chat"} {
		r := wait(t, h.send(t.Context(), http.MethodPost, path, "", http.Header{"Expect": {"100-continue"}}))
		if r.resp.StatusCode != http.StatusOK {
			t.Errorf("%s: status %d", path, r.resp.StatusCode)
		}
		h.up.next(t)
	}
	for _, class := range []string{metrics.ClassHigh, metrics.ClassLow} {
		waitFor(t, class+" requests_total{code=200}", func() bool {
			return testutil.ToFloat64(h.metrics.Requests.WithLabelValues(class, apiChat, "200")) == 1
		})
		if got := testutil.ToFloat64(h.metrics.Requests.WithLabelValues(class, apiChat, "100")); got != 0 {
			t.Errorf("%s requests_total{code=100} = %v, want 0", class, got)
		}
	}
}

func TestLowBodyTooLarge(t *testing.T) {
	t.Parallel()
	h := newHarness(t, scheduler.Options{})
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, h.url+lowChatPath, io.LimitReader(zeros{}, proxy.MaxLowBodyBytes+1))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusRequestEntityTooLarge {
		t.Errorf("status = %d, want 413", resp.StatusCode)
	}
	h.up.none(t)
}

type zeros struct{}

func (zeros) Read(b []byte) (int, error) {
	clear(b)
	return len(b), nil
}
