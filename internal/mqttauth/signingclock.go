// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"errors"
	"fmt"
	"log/slog"
	"math"
	"sync"
	"time"
)

// DurableMark is the persisted mark of a signing clock (gateway-config/v2
// gateway.state_directory). Found is the mark read at the start, if any.
// Persist durably writes exactly mark, unless a mark at or above it is already
// persisted, so the persisted mark never falls.
type DurableMark interface {
	Found() (int64, bool)
	Persist(mark int64) error
}

const (
	// SigningCeiling is the bound below which every mark and signing time
	// stays: 2^63 − 1 − 2,001, room for a first value and two reservations
	// above a mark without overflow.
	SigningCeiling = int64(math.MaxInt64 - 2001)
	// signingReservation is how far past the wall clock a mark is persisted,
	// so the disk is written about once a second rather than for every
	// message. A reservation is anchored to the wall clock, never to an
	// earlier mark, so restarts cannot accumulate a lead.
	signingReservation = int64(1000)
	// signingAheadWarning is how far ahead of the wall clock a signing time
	// may run before a warning: 300,000 ms, the default receiver skew.
	signingAheadWarning = int64(5 * 60 * 1000)
	// MarkWriteBound is the write bound of gateway-config/v2
	// gateway.state_directory rule 11, on the monotonic clock: a write of the
	// mark still outstanding past it opens an episode, and no value waits for
	// coverage longer than it in all. It bounds the disruption a stalled disk
	// causes, not when coverage runs out, which signing time decides.
	MarkWriteBound = time.Second
	// markRetryPacing is how soon after the previous write started a write is
	// retried while an episode is open: never sooner, and never per message.
	markRetryPacing = time.Second
)

var (
	// ErrSigningClockExhausted refuses a signing time at or above the ceiling:
	// the clock stops signing rather than reach it.
	ErrSigningClockExhausted = errors.New("signing clock exhausted")
	// ErrSigningClockClosed refuses a signing time after Close, which no
	// persisted mark could cover.
	ErrSigningClockClosed = errors.New("signing clock closed")
	// errWriteOutstanding is why an episode opens on a write still outstanding
	// past the write bound, or a value that waited for it.
	errWriteOutstanding = errors.New("the write has not completed within the write bound")
)

// afterFunc runs f once d has passed on the monotonic clock, and returns a
// function that stops it; a test stands in timers it fires itself.
var afterFunc = func(d time.Duration, f func()) func() bool { return time.AfterFunc(d, f).Stop }

// StartWriteError refuses a start whose first write of the mark has not
// completed within the write bound (rule 7). The write is still outstanding,
// and the state directory must stay held until it ends (rule 13).
type StartWriteError struct{ done <-chan struct{} }

func (e *StartWriteError) Error() string {
	return "persist signing clock mark: " + errWriteOutstanding.Error()
}

// Quiesce waits up to bound for the start's write to end, and reports whether
// it did.
func (e *StartWriteError) Quiesce(bound time.Duration) bool { return waitWithin(e.done, bound) }

func waitWithin(done <-chan struct{}, bound time.Duration) bool {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	select {
	case <-done:
		return true
	case <-timer.C:
		return false
	}
}

// SigningClock issues signed_at_ms for every envelope a process signs: the
// latest of the wall clock, one millisecond past the last value issued and,
// with a durable mark, one past the mark found at the start, so no two
// envelopes are signed at one value (gateway-api/v2 Envelope Freshness and
// Replay, Senders). One clock serves every signer it is given to. With a
// durable mark, every value issued is at or below a completed mark, so a
// restart never repeats one, except during an episode; without one, values
// never repeat within the process only.
//
// The mark is written as gateway-config/v2 gateway.state_directory rule 11
// states. At most one write is outstanding, and no write runs under the lock
// that issues values. A value the completed mark covers is issued without
// waiting, and the next reservation is written in the background once half of
// it is used. A value past it, after a clock jump or a burst faster than the
// extension, waits for a write to cover it, for no longer than MarkWriteBound
// in all and never past the moment an episode opens. An episode opens when a
// write fails, is still outstanding past MarkWriteBound, or a value has waited
// that long: it is warned about once, Degraded reports it, and until a write
// that covers every value issued completes, no value waits on the disk and the
// write is retried, paced from the previous write's start. Refusing to sign
// would silence the gateway heartbeat (gateway-config/v2 Limits). Every mark is
// at most the latest of the wall clock plus one reservation, read when the
// mark is chosen, the value being issued, the mark found and the highest mark
// written before, and never lower than a mark written before, whatever that
// write's outcome.
type SigningClock struct {
	now     func() time.Time
	durable DurableMark
	log     *slog.Logger
	after   func(time.Duration, func()) func() bool

	mu   sync.Mutex
	last int64
	// covered is the highest mark whose write completed in this run, or the
	// mark found.
	covered int64
	// written is the highest mark this run has written, whatever the outcome.
	written int64
	ahead   bool
	closed  bool
	episode bool
	// write is closed when the outstanding write ends; nil when none is.
	write        chan struct{}
	writeStarted time.Time
	// retry stops the paced retry pending, if any.
	retry func() bool
	// changed is closed and replaced whenever coverage, the episode or the
	// outstanding write changes, waking every value waiting for coverage.
	changed chan struct{}
}

// NewSigningClock returns an in-process clock over now: its values never
// repeat within the process.
func NewSigningClock(now func() time.Time, log *slog.Logger) *SigningClock {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	return &SigningClock{now: now, log: log, after: afterFunc, changed: make(chan struct{})}
}

// NewDurableSigningClock returns a clock over now whose values are covered by
// marks persisted through durable. Before returning it persists the first
// mark, the later of the mark found and the wall clock plus one reservation,
// which proves the directory writable. A write that fails, or a first value
// that would reach the ceiling, refuses the start, and so does a write that
// has not completed within MarkWriteBound, with a *StartWriteError: episodes
// never apply to the start's write.
func NewDurableSigningClock(now func() time.Time, durable DurableMark, log *slog.Logger) (*SigningClock, error) {
	c := NewSigningClock(now, log)
	c.durable = durable
	wall := c.now().UnixMilli()
	first := wall
	start := wall + signingReservation
	if found, ok := durable.Found(); ok {
		c.last = found
		c.covered = found
		first = max(first, found+1)
		start = max(start, found)
	}
	if first >= SigningCeiling {
		return nil, fmt.Errorf("%w: its first value would reach the ceiling", ErrSigningClockExhausted)
	}
	start = min(start, SigningCeiling-1)
	c.written = start
	c.writeStarted = time.Now()
	done := make(chan struct{})
	var err error
	go func() {
		defer close(done)
		err = durable.Persist(start)
	}()
	expired := make(chan struct{})
	stop := c.after(MarkWriteBound, func() { close(expired) })
	defer stop()
	select {
	case <-done:
	case <-expired:
		select {
		case <-done:
		default:
			return nil, &StartWriteError{done: done}
		}
	}
	if err != nil {
		return nil, fmt.Errorf("persist signing clock mark: %w", err)
	}
	c.covered = max(c.covered, start)
	return c, nil
}

// reservationFor is the mark that covers at: one reservation past the wall
// clock, or at itself when the clock runs ahead, below the ceiling.
func reservationFor(wall, at int64) int64 {
	return min(max(wall+signingReservation, at), SigningCeiling-1)
}

// valueWait is one value waiting for coverage; its timer opens an episode
// once it has waited MarkWriteBound.
type valueWait struct {
	done bool
	stop func() bool
}

// Next returns the next signing time. It refuses one at or above the ceiling,
// and, for a durable clock, one after Close.
func (c *SigningClock) Next() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	var wait *valueWait
	defer func() {
		if wait != nil {
			wait.done = true
			wait.stop()
		}
	}()
	for {
		wall := c.now().UnixMilli()
		at := max(wall, c.last+1)
		if at >= SigningCeiling {
			return 0, ErrSigningClockExhausted
		}
		if c.durable != nil {
			switch {
			case c.closed:
				// Once the stop is decided no value is issued and no write
				// starts, so the state directory can reach quiescence.
				c.log.Info("signing clock is stopped; refusing to sign")
				return 0, ErrSigningClockClosed
			case at > c.covered && !c.episode:
				// Past the coverage: wait for a write to cover it, for no
				// longer than the write bound in all.
				if c.write == nil {
					c.startWriteLocked(reservationFor(wall, at))
				}
				if wait == nil {
					w := &valueWait{}
					w.stop = c.after(MarkWriteBound, func() {
						c.mu.Lock()
						defer c.mu.Unlock()
						if !w.done {
							c.openEpisodeLocked(errWriteOutstanding)
						}
					})
					wait = w
				}
				changed := c.changed
				c.mu.Unlock()
				<-changed
				c.mu.Lock()
				continue
			case !c.episode && c.write == nil && c.covered-at < signingReservation/2 && reservationFor(wall, at) > c.covered:
				// Only an extension that would move the mark forward is
				// started: after a step back, the coverage already persisted
				// is used up first.
				c.startWriteLocked(reservationFor(wall, at))
			}
		}
		switch ahead := at-wall > signingAheadWarning; {
		case ahead && !c.ahead:
			c.log.Warn("signing clock is ahead of the wall clock by more than receivers' default skew; they refuse what is signed until the wall clock catches up",
				"signed_at_ms", at, "wall_ms", wall)
			c.ahead = true
		case !ahead:
			c.ahead = false
		}
		c.last = at
		return at, nil
	}
}

// startWriteLocked starts the one outstanding write of the mark, never lower
// than a mark written before.
func (c *SigningClock) startWriteLocked(mark int64) {
	mark = max(mark, c.written)
	c.written = mark
	done := make(chan struct{})
	c.write = done
	c.writeStarted = time.Now()
	stop := c.after(MarkWriteBound, func() {
		c.mu.Lock()
		defer c.mu.Unlock()
		if c.write == done {
			c.openEpisodeLocked(errWriteOutstanding)
		}
	})
	go c.persist(mark, done, stop)
}

// persist writes mark off every lock that issues values, and takes its
// outcome once.
func (c *SigningClock) persist(mark int64, done chan struct{}, stopBound func() bool) {
	err := c.durable.Persist(mark)
	stopBound()
	c.mu.Lock()
	defer c.mu.Unlock()
	c.write = nil
	close(done)
	if err != nil {
		c.openEpisodeLocked(err)
	} else {
		c.covered = max(c.covered, mark)
		// Judged under the lock that issues values, so no value is issued
		// between the comparison and the episode's end.
		if c.episode && c.covered >= c.last {
			c.episode = false
			c.log.Info("signing clock mark persisted again; every value issued is covered")
		}
	}
	if c.episode {
		c.scheduleRetryLocked()
	}
	c.broadcastLocked()
}

// openEpisodeLocked opens an episode, if none is open, with its cause.
func (c *SigningClock) openEpisodeLocked(cause error) {
	if c.episode {
		return
	}
	c.episode = true
	c.log.Warn("signing clock mark could not be persisted; signing goes on uncovered until a write completes", "error", cause)
	c.scheduleRetryLocked()
	c.broadcastLocked()
}

// scheduleRetryLocked schedules the next write of an episode, no sooner than
// markRetryPacing after the previous write started, unless a write is
// outstanding or one is already scheduled.
func (c *SigningClock) scheduleRetryLocked() {
	if c.retry != nil || c.closed || c.write != nil {
		return
	}
	c.retry = c.after(max(markRetryPacing-time.Since(c.writeStarted), 0), c.retryWrite)
}

func (c *SigningClock) retryWrite() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.retry = nil
	if c.closed || !c.episode || c.write != nil {
		return
	}
	c.startWriteLocked(reservationFor(c.now().UnixMilli(), c.last))
}

func (c *SigningClock) broadcastLocked() {
	close(c.changed)
	c.changed = make(chan struct{})
}

// Degraded reports whether an episode is open: values may be issued that no
// completed mark covers. It never waits on a write.
func (c *SigningClock) Degraded() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.episode
}

// Close stops issuing values and starting writes, and waits for the
// outstanding write, if any, without a bound.
func (c *SigningClock) Close() { <-c.Stop() }

// CloseWithin stops issuing values and starting writes, waits up to bound for
// the outstanding write, if any, and reports whether none is outstanding.
func (c *SigningClock) CloseWithin(bound time.Duration) bool { return waitWithin(c.Stop(), bound) }

// Stop stops issuing values and starting writes, the paced retry included,
// and returns a channel closed once no write of the mark is outstanding
// (gateway-config/v2 gateway.state_directory rule 13). Until it is closed,
// the state directory must stay held.
func (c *SigningClock) Stop() <-chan struct{} {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.retry != nil {
		c.retry()
		c.retry = nil
	}
	c.broadcastLocked()
	if c.write != nil {
		return c.write
	}
	idle := make(chan struct{})
	close(idle)
	return idle
}
