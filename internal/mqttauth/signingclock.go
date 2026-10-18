// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"log/slog"
	"sync"
	"time"
)

// DurableClock is a signing clock whose last value survives a restart.
// NextAtLeast issues and persists a value no lower than least, refusing one
// it could not persist; Last is the last value persisted.
type DurableClock interface {
	NextAtLeast(least int64) (int64, error)
	Last() int64
}

const (
	// signingReservation is how far ahead of an issued value the durable
	// mark is persisted, so the disk is written about once a second rather
	// than for every message. A restart resumes above the whole reservation,
	// at most this far ahead of the wall clock.
	signingReservation = int64(1000)
	// signingAheadWarning is how far ahead of the wall clock a signing time
	// may run before a warning: past the receivers' skew, they refuse what
	// the gateway signs as future_timestamp.
	signingAheadWarning = int64(5 * 60 * 1000)
)

// SigningClock issues signed_at_ms for every envelope a process signs: the
// wall clock, or one millisecond past the last value issued, whichever is
// later, so no two envelopes are signed at one value (gateway-api/v2 Envelope
// Freshness and Replay, Senders). One clock serves every signer it is given
// to. With a durable clock every issued value stays at or below a persisted
// reservation, so a restart under an unchanged or regressed clock never
// repeats one; without one, or while the reservation cannot be persisted,
// values never repeat within the process only.
//
// Signing never waits on the disk while a reservation lasts: the first is
// persisted when the clock is built, at startup, and each later one is
// extended in the background once half of it is used. Only a value past the
// reservation, after a clock jump or a burst faster than the extension,
// persists before it is issued. A failure to persist is logged and signing
// goes on, since a repeat can cost only the sender's own message and refusing
// to sign would silence it. Close waits for an extension in flight.
type SigningClock struct {
	now     func() time.Time
	durable DurableClock
	log     *slog.Logger

	mu        sync.Mutex
	last      int64
	reserved  int64
	warned    bool
	ahead     bool
	extending bool
	closed    bool
	extension sync.WaitGroup
}

// NewSigningClock returns a clock over now, persisted through durable when it
// is not nil. It resumes above the last reservation durable persisted, and
// persists its first reservation before returning.
func NewSigningClock(now func() time.Time, durable DurableClock, log *slog.Logger) *SigningClock {
	if now == nil {
		now = time.Now
	}
	if log == nil {
		log = slog.Default()
	}
	c := &SigningClock{now: now, durable: durable, log: log}
	if durable != nil {
		c.last = durable.Last()
		c.reserved = c.last
		c.mu.Lock()
		c.reserveLocked(max(now().UnixMilli(), c.last+1))
		c.mu.Unlock()
	}
	return c
}

// reserveLocked persists a reservation reaching signingReservation past at.
func (c *SigningClock) reserveLocked(at int64) {
	reserved, err := c.durable.NextAtLeast(at + signingReservation)
	if err != nil {
		c.warnPersistLocked()
		return
	}
	c.reserved = max(c.reserved, reserved)
	c.warned = false
}

func (c *SigningClock) warnPersistLocked() {
	if !c.warned {
		c.warned = true
		c.log.Warn("signing clock could not be persisted; signing from the in-process clock")
	}
}

// Next returns the next signing time.
func (c *SigningClock) Next() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	wall := c.now().UnixMilli()
	at := max(wall, c.last+1)
	if c.durable != nil {
		switch {
		case at > c.reserved:
			// Past the reservation: persist before issuing.
			c.reserveLocked(at)
		case c.reserved-at < signingReservation/2 && !c.extending && !c.closed:
			c.extending = true
			c.extension.Add(1)
			go c.extend(at)
		}
	}
	switch ahead := at-wall > signingAheadWarning; {
	case ahead && !c.ahead:
		c.log.Warn("signing clock is ahead of the wall clock by more than receivers' skew; they refuse what is signed until the wall clock catches up",
			"signed_at_ms", at, "wall_ms", wall)
		c.ahead = true
	case !ahead:
		c.ahead = false
	}
	c.last = at
	return at
}

// extend persists the next reservation off the signing path.
func (c *SigningClock) extend(at int64) {
	defer c.extension.Done()
	reserved, err := c.durable.NextAtLeast(at + signingReservation)
	c.mu.Lock()
	defer c.mu.Unlock()
	c.extending = false
	if err != nil {
		c.warnPersistLocked()
		return
	}
	c.reserved = max(c.reserved, reserved)
	c.warned = false
}

// Close starts no further background extension and waits for one in flight,
// so no write to the durable clock outlives its owner.
func (c *SigningClock) Close() {
	c.mu.Lock()
	c.closed = true
	c.mu.Unlock()
	c.extension.Wait()
}
