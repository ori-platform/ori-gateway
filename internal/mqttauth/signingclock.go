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
)

var (
	// ErrSigningClockExhausted refuses a signing time at or above the ceiling:
	// the clock stops signing rather than reach it.
	ErrSigningClockExhausted = errors.New("signing clock exhausted")
	// ErrSigningClockClosed refuses a signing time after Close, which no
	// persisted mark could cover.
	ErrSigningClockClosed = errors.New("signing clock closed")
)

// SigningClock issues signed_at_ms for every envelope a process signs: the
// latest of the wall clock, one millisecond past the last value issued and,
// with a durable mark, one past the mark found at the start, so no two
// envelopes are signed at one value (gateway-api/v2 Envelope Freshness and
// Replay, Senders). One clock serves every signer it is given to. With a
// durable mark, every value issued is at or below a persisted mark, so a
// restart never repeats one, except while the mark cannot be written; without
// one, values never repeat within the process only.
//
// Signing never waits on the disk while a reservation lasts: the first is
// persisted when the clock is built, at the start, and each later one is
// extended in the background once half of it is used. Only a value past the
// reservation, after a clock jump or a burst faster than the extension,
// persists before it is issued. A failure to persist is logged and signing
// goes on, retrying the write for every value past the reservation until one
// succeeds, since refusing to sign would silence the gateway heartbeat
// (gateway-config/v2 Limits). Every mark is at most the later of the wall
// clock plus one reservation, read when the mark is chosen, and the value
// being issued, or the mark found: while the clock runs a reservation or more
// ahead, every value is persisted before it is issued.
// Close waits for an extension in flight.
type SigningClock struct {
	now     func() time.Time
	durable DurableMark
	log     *slog.Logger

	mu       sync.Mutex
	last     int64
	reserved int64
	warned   bool
	ahead    bool
	closed   bool
	// extension is closed when the extension in flight, if any, ends.
	extension chan struct{}
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
	return &SigningClock{now: now, log: log}
}

// NewDurableSigningClock returns a clock over now whose values are covered by
// marks persisted through durable. Before returning it persists the first
// mark, the later of the mark found and the wall clock plus one reservation,
// which proves the directory writable. A write that fails, or a first value
// that would reach the ceiling, refuses the start.
func NewDurableSigningClock(now func() time.Time, durable DurableMark, log *slog.Logger) (*SigningClock, error) {
	c := NewSigningClock(now, log)
	c.durable = durable
	wall := c.now().UnixMilli()
	first := wall
	start := wall + signingReservation
	if found, ok := durable.Found(); ok {
		c.last = found
		c.reserved = found
		first = max(first, found+1)
		start = max(start, found)
	}
	if first >= SigningCeiling {
		return nil, fmt.Errorf("%w: its first value would reach the ceiling", ErrSigningClockExhausted)
	}
	start = min(start, SigningCeiling-1)
	if err := durable.Persist(start); err != nil {
		return nil, fmt.Errorf("persist signing clock mark: %w", err)
	}
	c.reserved = max(c.reserved, start)
	return c, nil
}

// reservationFor is the mark that covers at: one reservation past the wall
// clock, or at itself when the clock runs ahead, below the ceiling.
func reservationFor(wall, at int64) int64 {
	return min(max(wall+signingReservation, at), SigningCeiling-1)
}

// warnPersistLocked warns once each time writes of the mark start failing.
func (c *SigningClock) warnPersistLocked(err error) {
	if !c.warned {
		c.warned = true
		c.log.Warn("signing clock could not be persisted; signing from the in-process clock", "error", err)
	}
}

// Next returns the next signing time. It refuses one at or above the ceiling,
// and, for a durable clock, one after Close.
func (c *SigningClock) Next() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	for {
		wall := c.now().UnixMilli()
		at := max(wall, c.last+1)
		if at >= SigningCeiling {
			return 0, ErrSigningClockExhausted
		}
		if c.durable != nil {
			switch {
			case c.closed:
				// No write outlives the clock's owner, and no value is issued
				// that no mark could cover.
				c.log.Error("signing clock used after it was closed; refusing to sign")
				return 0, ErrSigningClockClosed
			case at > c.reserved && c.extension != nil:
				// An extension is writing: wait for it, so its mark is never
				// written after a value it does not cover is issued.
				done := c.extension
				c.mu.Unlock()
				<-done
				c.mu.Lock()
				continue
			case at > c.reserved:
				// Past the reservation: persist before issuing.
				mark := reservationFor(wall, at)
				if err := c.durable.Persist(mark); err != nil {
					c.warnPersistLocked(err)
				} else {
					c.reserved = max(c.reserved, mark)
					c.warned = false
				}
			case c.reserved-at < signingReservation/2 && c.extension == nil:
				c.extension = make(chan struct{})
				go c.extend(c.extension)
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

// extend persists the next reservation off the signing path. Its mark is
// taken from the wall clock, never from the last reservation.
func (c *SigningClock) extend(done chan struct{}) {
	defer close(done)
	c.mu.Lock()
	mark := reservationFor(c.now().UnixMilli(), c.last)
	c.mu.Unlock()
	err := c.durable.Persist(mark)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.extension = nil
	if err != nil {
		c.warnPersistLocked(err)
		return
	}
	c.reserved = max(c.reserved, mark)
	c.warned = false
}

// Close stops every further durable write, background or not, and waits for
// an extension in flight, so no write to the durable mark outlives its owner.
func (c *SigningClock) Close() {
	c.mu.Lock()
	c.closed = true
	done := c.extension
	c.mu.Unlock()
	if done != nil {
		<-done
	}
}
