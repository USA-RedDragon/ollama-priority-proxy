// Package scheduler decides when generation requests may be sent to the
// upstream. High priority requests always go at once. Low priority requests
// wait in a FIFO queue until no high priority request is in flight, the idle
// grace has passed since the last one finished, and fewer than the maximum
// number of low priority requests are in flight.
package scheduler

import (
	"container/list"
	"context"
	"errors"
	"sync"
	"time"
)

// ErrMaxWait is returned by AcquireLow when a request waited longer than the
// configured maximum.
var ErrMaxWait = errors.New("low priority request waited too long in the queue")

// Options configures a Scheduler.
type Options struct {
	// LowMaxInFlight is the most low priority requests in flight at once.
	// Values below 1 are treated as 1.
	LowMaxInFlight int
	// IdleGrace is how long no high priority request must be in flight
	// before a low priority request is dispatched.
	IdleGrace time.Duration
	// MaxWait is how long a low priority request may wait. 0 waits forever.
	MaxWait time.Duration
}

// Stats is a snapshot of the scheduler state.
type Stats struct {
	HighInFlight int
	LowInFlight  int
	LowQueued    int
}

// Scheduler holds the in-flight counts and the low priority queue.
type Scheduler struct {
	opts Options

	mu           sync.Mutex
	highInFlight int
	lowInFlight  int
	lastHighDone time.Time
	queue        *list.List
	timerArmed   bool
}

type waiter struct {
	ready   chan struct{}
	elem    *list.Element
	granted bool
}

// New returns a Scheduler.
func New(opts Options) *Scheduler {
	if opts.LowMaxInFlight < 1 {
		opts.LowMaxInFlight = 1
	}
	return &Scheduler{opts: opts, queue: list.New()}
}

// AcquireHigh marks a high priority request as in flight. Call the returned
// function once when the request is done.
func (s *Scheduler) AcquireHigh() func() {
	s.mu.Lock()
	s.highInFlight++
	s.mu.Unlock()

	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.highInFlight--
			s.lastHighDone = time.Now()
			s.dispatchLocked()
		})
	}
}

// AcquireLow waits until a low priority request may be sent. It returns
// ctx.Err() when ctx ends first and ErrMaxWait when the maximum wait passes.
// On success, call the returned function once when the request is done.
func (s *Scheduler) AcquireLow(ctx context.Context) (func(), error) {
	w := &waiter{ready: make(chan struct{})}
	s.mu.Lock()
	w.elem = s.queue.PushBack(w)
	s.dispatchLocked()
	s.mu.Unlock()

	var timeout <-chan time.Time
	if s.opts.MaxWait > 0 {
		t := time.NewTimer(s.opts.MaxWait)
		defer t.Stop()
		timeout = t.C
	}

	var err error
	select {
	case <-w.ready:
		return s.lowRelease(), nil
	case <-ctx.Done():
		err = ctx.Err()
	case <-timeout:
		err = ErrMaxWait
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if w.granted {
		s.lowInFlight--
		s.dispatchLocked()
	} else {
		s.queue.Remove(w.elem)
	}
	return nil, err
}

func (s *Scheduler) lowRelease() func() {
	var once sync.Once
	return func() {
		once.Do(func() {
			s.mu.Lock()
			defer s.mu.Unlock()
			s.lowInFlight--
			s.dispatchLocked()
		})
	}
}

// Stats returns the current in-flight counts and queue depth.
func (s *Scheduler) Stats() Stats {
	s.mu.Lock()
	defer s.mu.Unlock()
	return Stats{HighInFlight: s.highInFlight, LowInFlight: s.lowInFlight, LowQueued: s.queue.Len()}
}

func (s *Scheduler) dispatchLocked() {
	if s.highInFlight > 0 || s.queue.Len() == 0 || s.lowInFlight >= s.opts.LowMaxInFlight {
		return
	}
	if !s.lastHighDone.IsZero() && s.opts.IdleGrace > 0 {
		if remaining := s.opts.IdleGrace - time.Since(s.lastHighDone); remaining > 0 {
			if !s.timerArmed {
				s.timerArmed = true
				time.AfterFunc(remaining, func() {
					s.mu.Lock()
					defer s.mu.Unlock()
					s.timerArmed = false
					s.dispatchLocked()
				})
			}
			return
		}
	}
	for s.lowInFlight < s.opts.LowMaxInFlight && s.queue.Len() > 0 {
		front := s.queue.Front()
		w, _ := s.queue.Remove(front).(*waiter)
		w.granted = true
		s.lowInFlight++
		close(w.ready)
	}
}
