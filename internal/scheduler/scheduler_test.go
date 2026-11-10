package scheduler_test

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/USA-RedDragon/ollama-priority-proxy/internal/scheduler"
)

const quiet = 100 * time.Millisecond

func acquire(ctx context.Context, s *scheduler.Scheduler) <-chan error {
	out := make(chan error, 1)
	go func() {
		release, err := s.AcquireLow(ctx)
		if err == nil {
			release()
		}
		out <- err
	}()
	return out
}

func acquireHeld(t *testing.T, s *scheduler.Scheduler) func() {
	t.Helper()
	release, err := s.AcquireLow(t.Context())
	if err != nil {
		t.Fatal(err)
	}
	return release
}

func waitQueued(t *testing.T, s *scheduler.Scheduler) {
	const n = 1
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for s.Stats().LowQueued != n {
		if time.Now().After(deadline) {
			t.Fatalf("queue depth = %d, want %d", s.Stats().LowQueued, n)
		}
		time.Sleep(time.Millisecond)
	}
}

func TestLowRunsWhenIdle(t *testing.T) {
	t.Parallel()
	s := scheduler.New(scheduler.Options{IdleGrace: time.Hour})
	release := acquireHeld(t, s)
	if st := s.Stats(); st.LowInFlight != 1 || st.LowQueued != 0 {
		t.Errorf("stats = %+v", st)
	}
	release()
	release()
	if st := s.Stats(); st.LowInFlight != 0 {
		t.Errorf("a second release changed the count: %+v", st)
	}
}

func TestLowWaitsForHigh(t *testing.T) {
	t.Parallel()
	s := scheduler.New(scheduler.Options{})
	releaseHigh := s.AcquireHigh()
	done := acquire(t.Context(), s)
	waitQueued(t, s)
	select {
	case <-done:
		t.Fatal("low ran while high was in flight")
	case <-time.After(quiet):
	}
	releaseHigh()
	releaseHigh()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if st := s.Stats(); st.HighInFlight != 0 {
		t.Errorf("stats = %+v", st)
	}
}

func TestMaxInFlightDefaultsToOne(t *testing.T) {
	t.Parallel()
	s := scheduler.New(scheduler.Options{LowMaxInFlight: 0})
	release := acquireHeld(t, s)
	done := acquire(t.Context(), s)
	waitQueued(t, s)
	release()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
}

func TestCancelRemovesFromQueue(t *testing.T) {
	t.Parallel()
	s := scheduler.New(scheduler.Options{})
	releaseHigh := s.AcquireHigh()
	ctx, cancel := context.WithCancel(t.Context())
	done := acquire(ctx, s)
	waitQueued(t, s)
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("err = %v", err)
	}
	if st := s.Stats(); st.LowQueued != 0 || st.LowInFlight != 0 {
		t.Errorf("stats = %+v", st)
	}
	releaseHigh()
	acquireHeld(t, s)()
}

func TestMaxWait(t *testing.T) {
	t.Parallel()
	s := scheduler.New(scheduler.Options{MaxWait: 50 * time.Millisecond})
	defer s.AcquireHigh()()
	if err := <-acquire(t.Context(), s); !errors.Is(err, scheduler.ErrMaxWait) {
		t.Fatalf("err = %v, want ErrMaxWait", err)
	}
	if st := s.Stats(); st.LowQueued != 0 {
		t.Errorf("stats = %+v", st)
	}
}

func TestIdleGraceTimer(t *testing.T) {
	t.Parallel()
	const grace = 150 * time.Millisecond
	s := scheduler.New(scheduler.Options{IdleGrace: grace})
	releaseHigh := s.AcquireHigh()
	done := acquire(t.Context(), s)
	waitQueued(t, s)
	releaseHigh()
	start := time.Now()
	if err := <-done; err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(start); elapsed < grace-10*time.Millisecond {
		t.Errorf("low ran %v after high finished, want about %v", elapsed, grace)
	}
}
