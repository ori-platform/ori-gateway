// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"bytes"
	"errors"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// flakyDurable is a durable clock whose persistence can fail; it records the
// least value it was asked for and the values it stored.
type flakyDurable struct {
	mu     sync.Mutex
	fail   bool
	stored int64
	now    func() time.Time
}

func (d *flakyDurable) setFail(fail bool) {
	d.mu.Lock()
	d.fail = fail
	d.mu.Unlock()
}

func (d *flakyDurable) storedMark() int64 {
	d.mu.Lock()
	defer d.mu.Unlock()
	return d.stored
}

func (d *flakyDurable) Last() int64 { return d.storedMark() }

func (d *flakyDurable) NextAtLeast(least int64) (int64, error) {
	d.mu.Lock()
	defer d.mu.Unlock()
	if d.fail {
		return 0, errors.New("disk full")
	}
	// The signing clock passes the least value it needs; the fake reads no
	// wall clock, so a background extension shares no state with the test.
	at := max(least, d.stored+1)
	d.stored = at
	return at, nil
}

// TestSignersNeverRepeatASignedAt issues signing times from one frozen
// clock: every value is new. When the durable clock cannot persist, signing
// goes on from the in-process value without repeating, one warning is
// logged, and the stored mark, once persistence returns, is never behind a
// value already issued.
func TestSignersNeverRepeatASignedAt(t *testing.T) {
	frozen := time.UnixMilli(1_787_000_000_000)
	now := func() time.Time { return frozen }
	durable := &flakyDurable{now: now}
	// The clock steps past the reservation, so a write is attempted.
	step := func() { frozen = frozen.Add(2 * time.Second) }
	logs := &bytes.Buffer{}
	clock := NewSigningClock(now, durable, slog.New(slog.NewTextHandler(logs, nil)))
	seen := map[int64]bool{}
	issue := func() int64 {
		at := clock.Next()
		if seen[at] {
			t.Fatalf("signing time %d issued twice", at)
		}
		seen[at] = true
		return at
	}
	for range 5 {
		issue()
	}
	durable.setFail(true)
	var during int64
	for range 5 {
		step()
		during = issue()
	}
	if strings.Count(logs.String(), "could not be persisted") != 1 {
		t.Fatalf("want one warning while persistence fails:\n%s", logs.String())
	}
	durable.setFail(false)
	step()
	after := issue()
	if durable.storedMark() < after || after <= during {
		t.Fatalf("stored %d after issuing %d, then %d; the stored mark fell behind", durable.storedMark(), during, after)
	}
	// A restart with the clock set back resumes above everything issued.
	frozen = frozen.Add(-time.Hour)
	restarted := NewSigningClock(now, durable, slog.New(slog.DiscardHandler))
	for range 5 {
		if at := restarted.Next(); seen[at] || at <= after {
			t.Fatalf("after a restart the clock issued %d, not above %d", at, after)
		}
	}
}

// TestSigningNeverWaitsOnTheDiskWithinAReservation builds the clock, which
// persists its first reservation at startup, then holds every later write
// until released: issuing values within the reservation never waits, and a
// background extension that is held finishes before Close returns.
func TestSigningNeverWaitsOnTheDiskWithinAReservation(t *testing.T) {
	frozen := time.UnixMilli(1_787_000_000_000)
	durable := &blockingDurable{flakyDurable: flakyDurable{now: func() time.Time { return frozen }}, release: make(chan struct{})}
	clock := NewSigningClock(func() time.Time { return frozen }, durable, slog.New(slog.DiscardHandler))
	durable.block()
	issued := make(chan struct{})
	go func() {
		for range signingReservation - 1 {
			clock.Next()
		}
		close(issued)
	}()
	select {
	case <-issued:
	case <-time.After(5 * time.Second):
		t.Fatal("signing within the reservation waited on the disk")
	}
	closed := make(chan struct{})
	go func() { clock.Close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("Close returned while an extension was still writing")
	case <-time.After(100 * time.Millisecond):
	}
	close(durable.release)
	<-closed
}

type blockingDurable struct {
	flakyDurable
	blocked bool
	release chan struct{}
}

func (d *blockingDurable) block() {
	d.mu.Lock()
	d.blocked = true
	d.mu.Unlock()
}

func (d *blockingDurable) NextAtLeast(least int64) (int64, error) {
	d.mu.Lock()
	blocked := d.blocked
	d.mu.Unlock()
	if blocked {
		<-d.release
	}
	return d.flakyDurable.NextAtLeast(least)
}

// TestASigningClockAheadOfTheWallIsReported resumes from a durable mark far
// ahead of the wall clock, as after a clock that ran fast: the first value is
// above the mark, and a warning names how far ahead it is, once.
func TestASigningClockAheadOfTheWallIsReported(t *testing.T) {
	wall := time.UnixMilli(1_787_000_000_000)
	durable := &flakyDurable{now: func() time.Time { return wall }, stored: wall.Add(time.Hour).UnixMilli()}
	ahead := durable.stored
	logs := &bytes.Buffer{}
	clock := NewSigningClock(func() time.Time { return wall }, durable, slog.New(slog.NewTextHandler(logs, nil)))
	if at := clock.Next(); at <= ahead {
		t.Fatalf("resumed at %d, not above the stored mark", at)
	}
	clock.Next()
	if strings.Count(logs.String(), "ahead of the wall clock") != 1 {
		t.Fatalf("want one warning for a clock running ahead:\n%s", logs.String())
	}
	// The wall clock catches up, then is stepped back again: a second
	// episode is warned about again.
	wall = wall.Add(2 * time.Hour)
	clock.Next()
	wall = wall.Add(-time.Hour)
	clock.Next()
	if strings.Count(logs.String(), "ahead of the wall clock") != 2 {
		t.Fatalf("want a second warning for a second episode:\n%s", logs.String())
	}
}

// TestARestartResumesAboveEveryReservation issues values that cross several
// reservation boundaries, then restarts with the clock set back: the restart
// resumes above every value issued.
func TestARestartResumesAboveEveryReservation(t *testing.T) {
	wall := time.UnixMilli(1_787_000_000_000)
	now := func() time.Time { return wall }
	durable := &flakyDurable{now: now}
	clock := NewSigningClock(now, durable, slog.New(slog.DiscardHandler))
	var issued int64
	for range 10 {
		wall = wall.Add(600 * time.Millisecond)
		issued = clock.Next()
	}
	wall = wall.Add(-time.Hour)
	if at := NewSigningClock(now, durable, slog.New(slog.DiscardHandler)).Next(); at <= issued {
		t.Fatalf("after a restart the clock issued %d, not above %d", at, issued)
	}
}

// TestTheValueOnePastAReservationIsPersisted issues, under a frozen clock,
// exactly one value past the first reservation, then restarts: the restart
// resumes above it, so that value was persisted before it was issued.
func TestTheValueOnePastAReservationIsPersisted(t *testing.T) {
	frozen := time.UnixMilli(1_787_000_000_000)
	now := func() time.Time { return frozen }
	durable := &flakyDurable{now: now}
	clock := NewSigningClock(now, durable, slog.New(slog.DiscardHandler))
	var issued int64
	for range signingReservation + 2 {
		issued = clock.Next()
	}
	if at := NewSigningClock(now, durable, slog.New(slog.DiscardHandler)).Next(); at <= issued {
		t.Fatalf("after a restart the clock issued %d, not above %d", at, issued)
	}
}

// TestAClosedClockNeverWritesDurably closes the clock and then signs past its
// reservation: nothing reaches the durable clock, and the misuse is logged.
func TestAClosedClockNeverWritesDurably(t *testing.T) {
	wall := time.UnixMilli(1_787_000_000_000)
	durable := &flakyDurable{}
	logs := &bytes.Buffer{}
	clock := NewSigningClock(func() time.Time { return wall }, durable, slog.New(slog.NewTextHandler(logs, nil)))
	clock.Close()
	stored := durable.storedMark()
	wall = wall.Add(time.Minute)
	clock.Next()
	if durable.storedMark() != stored {
		t.Fatal("a closed clock wrote its durable mark")
	}
	if !strings.Contains(logs.String(), "used after it was closed") {
		t.Fatalf("the misuse was not logged:\n%s", logs.String())
	}
}
