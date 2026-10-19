// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"bytes"
	"errors"
	"log/slog"
	"math/rand/v2"
	"strings"
	"sync"
	"testing"
	"time"
)

// testWall is a wall clock the test sets, safe to read from an extension.
type testWall struct {
	mu sync.Mutex
	at time.Time
}

func newTestWall() *testWall { return &testWall{at: time.UnixMilli(1_787_000_000_000)} }

func (w *testWall) now() time.Time {
	w.mu.Lock()
	defer w.mu.Unlock()
	return w.at
}

func (w *testWall) add(d time.Duration) {
	w.mu.Lock()
	w.at = w.at.Add(d)
	w.mu.Unlock()
}

// memMark is a durable mark in memory. Every mark it writes is held to
// gateway-config/v2 gateway.state_directory rule 9 against the values the
// test has seen issued: at least each of them, and at most the latest of the
// wall clock plus 1,000 ms, the mark found, and the value being issued, which
// rule 8 puts at most one past the last value issued or the mark found.
type memMark struct {
	t    *testing.T
	wall *testWall

	mu        sync.Mutex
	found     int64
	has       bool
	persisted int64
	writes    int
	fail      bool
	block     chan struct{}
	// blockOnce holds the next write only; held is closed once it is held.
	blockOnce chan struct{}
	held      chan struct{}
	issued    int64
}

func newMemMark(t *testing.T, wall *testWall) *memMark {
	return &memMark{t: t, wall: wall, persisted: -1}
}

// restarted is the mark a restart finds: the last one written.
func (m *memMark) restarted() *memMark {
	m.mu.Lock()
	defer m.mu.Unlock()
	next := newMemMark(m.t, m.wall)
	if m.persisted >= 0 {
		next.found, next.has = m.persisted, true
	}
	next.issued = m.issued
	return next
}

func (m *memMark) Found() (int64, bool) { return m.found, m.has }

func (m *memMark) Persist(mark int64) error {
	m.mu.Lock()
	block := m.block
	once, held := m.blockOnce, m.held
	m.blockOnce, m.held = nil, nil
	m.mu.Unlock()
	if held != nil {
		close(held)
	}
	if block != nil {
		<-block
	}
	// The write held once is exempt from fail: it is the write under test.
	if once != nil {
		<-once
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if mark >= SigningCeiling {
		m.t.Errorf("persisted %d at or above the ceiling", mark)
	}
	if m.fail && once == nil {
		return errors.New("disk full")
	}
	if mark <= m.persisted {
		return nil
	}
	wall := m.wall.now().UnixMilli()
	upper := max(wall+signingReservation, m.issued+1)
	if m.has {
		upper = max(upper, m.found+1)
	}
	if mark > upper {
		m.t.Errorf("persisted %d above rule 9's bound %d (wall %d, last issued %d)", mark, upper, wall, m.issued)
	}
	if mark < m.issued {
		m.t.Errorf("persisted %d below the issued value %d", mark, m.issued)
	}
	m.persisted = mark
	m.writes++
	return nil
}

func (m *memMark) setFail(fail bool) {
	m.mu.Lock()
	m.fail = fail
	m.mu.Unlock()
}

func (m *memMark) mark() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.persisted
}

func (m *memMark) writeCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.writes
}

// next issues one value from clock and records it as issued.
func (m *memMark) next(t *testing.T, clock *SigningClock) int64 {
	t.Helper()
	at, err := clock.Next()
	if err != nil {
		t.Fatalf("Next: %v", err)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if at <= m.issued {
		t.Fatalf("issued %d, not above the last issued %d", at, m.issued)
	}
	m.issued = at
	return at
}

func durableClock(t *testing.T, wall *testWall, mark *memMark, log *slog.Logger) *SigningClock {
	t.Helper()
	if log == nil {
		log = slog.New(slog.DiscardHandler)
	}
	clock, err := NewDurableSigningClock(wall.now, mark, log)
	if err != nil {
		t.Fatalf("NewDurableSigningClock: %v", err)
	}
	t.Cleanup(clock.Close)
	return clock
}

// TestSignersNeverRepeatASignedAt issues signing times while the mark cannot
// be written: signing goes on without repeating, one warning is logged, the
// write is retried for every value past the reservation, and the first that
// succeeds covers every value issued.
func TestSignersNeverRepeatASignedAt(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &bytes.Buffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	for range 5 {
		mark.next(t, clock)
	}
	mark.setFail(true)
	var during int64
	for range 5 {
		wall.add(2 * time.Second)
		during = mark.next(t, clock)
	}
	if strings.Count(logs.String(), "could not be persisted") != 1 {
		t.Fatalf("want one warning while persistence fails:\n%s", logs.String())
	}
	mark.setFail(false)
	wall.add(2 * time.Second)
	after := mark.next(t, clock)
	if mark.mark() < after || after <= during {
		t.Fatalf("mark %d after issuing %d, then %d; the mark fell behind", mark.mark(), during, after)
	}
	// A restart with the clock set back resumes above everything issued.
	wall.add(-time.Hour)
	restarted := mark.restarted()
	again := durableClock(t, wall, restarted, nil)
	for range 5 {
		restarted.next(t, again)
	}
}

// TestARestartLoopNeverAccumulatesALead starts the clock 400 times, 200 ms
// apart, as a gateway that crashes on every start would: each start issues
// above every earlier value, and the mark never runs more than one
// reservation ahead of the wall clock, so receivers never refuse the
// heartbeat as future_timestamp.
func TestARestartLoopNeverAccumulatesALead(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	for range 400 {
		clock, err := NewDurableSigningClock(wall.now, mark, slog.New(slog.DiscardHandler))
		if err != nil {
			t.Fatal(err)
		}
		mark.next(t, clock)
		clock.Close()
		if lead := mark.mark() - wall.now().UnixMilli(); lead > signingReservation {
			t.Fatalf("the mark is %d ms ahead of the wall clock after a restart", lead)
		}
		wall.add(200 * time.Millisecond)
		mark = mark.restarted()
	}
}

// TestTheStartWritesTheMarkItFound proves the directory writable when the
// mark found is already ahead of the wall clock plus a reservation: the start
// writes it unchanged rather than move it further ahead.
func TestTheStartWritesTheMarkItFound(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	mark.found, mark.has = wall.now().Add(time.Hour).UnixMilli(), true
	durableClock(t, wall, mark, nil)
	if mark.writeCount() != 1 || mark.mark() != mark.found {
		t.Fatalf("start wrote %d marks ending at %d, want the found mark %d written once", mark.writeCount(), mark.mark(), mark.found)
	}
	fresh := newMemMark(t, wall)
	durableClock(t, wall, fresh, nil)
	if want := wall.now().UnixMilli() + signingReservation; fresh.mark() != want {
		t.Fatalf("a fresh start wrote %d, want the wall clock plus a reservation, %d", fresh.mark(), want)
	}
}

// TestAStartThatCannotWriteIsRefused fails the start's write: no clock is
// returned.
func TestAStartThatCannotWriteIsRefused(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	mark.fail = true
	if _, err := NewDurableSigningClock(wall.now, mark, nil); err == nil {
		t.Fatal("a start whose mark could not be written was accepted")
	}
}

// TestTheClockNeverReachesTheCeiling starts one below the ceiling, where the
// first value would reach it, which is refused; and two below, where the
// first value is the last below the ceiling and the next is refused. No mark
// at or above the ceiling is written.
func TestTheClockNeverReachesTheCeiling(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	mark.found, mark.has = SigningCeiling-1, true
	if _, err := NewDurableSigningClock(wall.now, mark, nil); !errors.Is(err, ErrSigningClockExhausted) {
		t.Fatalf("a start whose first value reaches the ceiling: %v", err)
	}
	mark = newMemMark(t, wall)
	mark.found, mark.has = SigningCeiling-2, true
	clock := durableClock(t, wall, mark, nil)
	if at := mark.next(t, clock); at != SigningCeiling-1 {
		t.Fatalf("first value %d, want %d", at, SigningCeiling-1)
	}
	if _, err := clock.Next(); !errors.Is(err, ErrSigningClockExhausted) {
		t.Fatalf("a value at the ceiling: %v", err)
	}
	if _, err := NewSigningClock(func() time.Time { return time.UnixMilli(SigningCeiling) }, nil).Next(); !errors.Is(err, ErrSigningClockExhausted) {
		t.Fatalf("an in-process value at the ceiling: %v", err)
	}
}

// TestSigningNeverWaitsOnTheDiskWithinAReservation holds every write after
// the start's until released: issuing values within the reservation never
// waits, and a background extension that is held finishes before Close
// returns.
func TestSigningNeverWaitsOnTheDiskWithinAReservation(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	release := make(chan struct{})
	mark.mu.Lock()
	mark.block = release
	mark.mu.Unlock()
	issued := make(chan struct{})
	// The wall clock moves a millisecond per value, so an extension starts
	// half way through the reservation and would move the mark forward.
	go func() {
		for range signingReservation - 1 {
			wall.add(time.Millisecond)
			mark.next(t, clock)
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
	close(release)
	<-closed
}

// TestAWriteNeverLandsBelowAValueAlreadyIssued holds an extension's write
// while a value past the reservation is wanted, and fails every other write:
// the value waits for the extension, so the extension's mark is never written
// after a value it does not cover is issued.
func TestAWriteNeverLandsBelowAValueAlreadyIssued(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	release := make(chan struct{})
	releaseOnce := sync.OnceFunc(func() { close(release) })
	t.Cleanup(releaseOnce)
	held := make(chan struct{})
	mark.mu.Lock()
	mark.blockOnce, mark.held = release, held
	mark.fail = true
	mark.mu.Unlock()
	// Use more than half the reservation as the wall clock moves, which
	// starts an extension that is held.
	for range signingReservation/2 + 2 {
		wall.add(time.Millisecond)
		mark.next(t, clock)
	}
	clock.mu.Lock()
	extending := clock.extension != nil
	clock.mu.Unlock()
	if !extending {
		t.Fatal("no extension started")
	}
	select {
	case <-held:
	case <-time.After(5 * time.Second):
		t.Fatal("the extension never wrote")
	}
	wall.add(time.Hour)
	got := make(chan int64)
	go func() { got <- mark.next(t, clock) }()
	select {
	case at := <-got:
		t.Fatalf("issued %d past the reservation while an extension was writing", at)
	case <-time.After(100 * time.Millisecond):
	}
	releaseOnce()
	<-got
}

// TestEveryMarkStaysWithinItsBounds drives the clock through random steps,
// jumps and steps back of the wall clock, write failures and restarts: every
// mark written is held to rule 9 by memMark, and no value repeats.
func TestEveryMarkStaysWithinItsBounds(t *testing.T) {
	rng := rand.New(rand.NewPCG(1, 2))
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	for range 20000 {
		switch r := rng.IntN(100); {
		case r < 60:
		case r < 85:
			wall.add(time.Duration(rng.IntN(800)) * time.Millisecond)
		case r < 90:
			wall.add(time.Duration(rng.IntN(600)) * time.Second)
		case r < 93:
			// Rule 9's upper bound is judged against the wall clock read when
			// a mark is chosen, which memMark reads when the write lands, so
			// let an extension in flight land before the wall steps back.
			settle(clock)
			wall.add(-time.Duration(rng.IntN(600)) * time.Second)
		case r < 97:
			mark.setFail(rng.IntN(2) == 0)
		default:
			// End any failure episode with a write that covers every value
			// issued, then restart. A restart inside an episode may repeat
			// values no mark covers, the limit gateway-config/v2 accepts.
			mark.setFail(false)
			wall.add(2 * time.Second)
			mark.next(t, clock)
			clock.Close()
			mark = mark.restarted()
			clock = durableClock(t, wall, mark, nil)
		}
		mark.next(t, clock)
	}
}

// settle waits for an extension in flight, if any.
func settle(clock *SigningClock) {
	clock.mu.Lock()
	done := clock.extension
	clock.mu.Unlock()
	if done != nil {
		<-done
	}
}

// TestASigningClockAheadOfTheWallIsReported resumes from a mark far ahead of
// the wall clock, as after a clock that ran fast: the first value is above
// the mark, and a warning names how far ahead it is, once per episode.
func TestASigningClockAheadOfTheWallIsReported(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	ahead := wall.now().Add(time.Hour).UnixMilli()
	mark.found, mark.has = ahead, true
	logs := &bytes.Buffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	if at := mark.next(t, clock); at <= ahead {
		t.Fatalf("resumed at %d, not above the mark found", at)
	}
	mark.next(t, clock)
	if strings.Count(logs.String(), "ahead of the wall clock") != 1 {
		t.Fatalf("want one warning for a clock running ahead:\n%s", logs.String())
	}
	// The wall clock catches up, then is stepped back again: a second
	// episode is warned about again.
	wall.add(2 * time.Hour)
	mark.next(t, clock)
	wall.add(-time.Hour)
	mark.next(t, clock)
	if strings.Count(logs.String(), "ahead of the wall clock") != 2 {
		t.Fatalf("want a second warning for a second episode:\n%s", logs.String())
	}
}

// TestARestartResumesAboveEveryReservation issues values that cross several
// reservation boundaries, then restarts with the clock set back: the restart
// resumes above every value issued.
func TestARestartResumesAboveEveryReservation(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	for range 10 {
		wall.add(600 * time.Millisecond)
		mark.next(t, clock)
	}
	clock.Close()
	wall.add(-time.Hour)
	restarted := mark.restarted()
	restarted.next(t, durableClock(t, wall, restarted, nil))
}

// TestTheValueOnePastAReservationIsPersisted issues, under a frozen clock,
// exactly one value past the first reservation, then restarts: the restart
// resumes above it, so that value was persisted before it was issued.
func TestTheValueOnePastAReservationIsPersisted(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	for range signingReservation + 2 {
		mark.next(t, clock)
	}
	clock.Close()
	restarted := mark.restarted()
	restarted.next(t, durableClock(t, wall, restarted, nil))
}

// TestAClosedClockRefusesToSign closes the clock and then asks for a value
// past its reservation: it is refused, nothing reaches the mark, and the
// misuse is logged.
func TestAClosedClockRefusesToSign(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &bytes.Buffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	clock.Close()
	stored := mark.mark()
	wall.add(time.Minute)
	if _, err := clock.Next(); !errors.Is(err, ErrSigningClockClosed) {
		t.Fatalf("a closed clock signed: %v", err)
	}
	if mark.mark() != stored {
		t.Fatal("a closed clock wrote its mark")
	}
	if !strings.Contains(logs.String(), "used after it was closed") {
		t.Fatalf("the misuse was not logged:\n%s", logs.String())
	}
}

// TestAFailedWriteIsRetriedByTheNextValue fails one write past the
// reservation and then lets writes succeed: the very next value, within what
// the failed write would have reserved, retries the write, so no value goes
// uncovered once the disk recovers.
func TestAFailedWriteIsRetriedByTheNextValue(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	wall.add(2 * time.Second)
	mark.setFail(true)
	uncovered := mark.next(t, clock)
	mark.setFail(false)
	next := mark.next(t, clock)
	if got := mark.mark(); got < next || got < uncovered {
		t.Fatalf("mark %d after issuing %d then %d; the failed write was not retried", got, uncovered, next)
	}
}

// TestEachFailureEpisodeIsWarnedAboutAgain fails writes, lets one succeed,
// then fails them again: each episode is warned about once, with its cause.
func TestEachFailureEpisodeIsWarnedAboutAgain(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &bytes.Buffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	episode := func() {
		mark.setFail(true)
		for range 3 {
			wall.add(2 * time.Second)
			mark.next(t, clock)
		}
		mark.setFail(false)
		wall.add(2 * time.Second)
		mark.next(t, clock)
	}
	episode()
	if n := strings.Count(logs.String(), "could not be persisted"); n != 1 {
		t.Fatalf("%d warnings for one episode:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "disk full") {
		t.Fatalf("the warning does not carry its cause:\n%s", logs.String())
	}
	episode()
	if n := strings.Count(logs.String(), "could not be persisted"); n != 2 {
		t.Fatalf("%d warnings for two episodes:\n%s", n, logs.String())
	}
}

// TestTheAheadWarningIsAtThreeHundredThousandMilliseconds resumes from marks
// exactly 300,000 ms and one past it ahead of the wall clock: only the
// second is warned about, since the first value is one past the mark.
func TestTheAheadWarningIsAtThreeHundredThousandMilliseconds(t *testing.T) {
	for _, tc := range []struct {
		lead int64
		warn bool
	}{{signingAheadWarning - 1, false}, {signingAheadWarning, true}} {
		wall := newTestWall()
		mark := newMemMark(t, wall)
		mark.found, mark.has = wall.now().UnixMilli()+tc.lead, true
		logs := &bytes.Buffer{}
		clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
		at := mark.next(t, clock)
		if got := strings.Contains(logs.String(), "ahead of the wall clock"); got != tc.warn {
			t.Errorf("signed %d ms ahead: warned=%v, want %v", at-wall.now().UnixMilli(), got, tc.warn)
		}
	}
}

// TestNoMarkReachesTheCeilingNearIt runs the wall clock just below the
// ceiling: the start's mark and every later one stop one below it, and
// signing stops at it.
func TestNoMarkReachesTheCeilingNearIt(t *testing.T) {
	wall := &testWall{at: time.UnixMilli(SigningCeiling - 10)}
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	if got := mark.mark(); got != SigningCeiling-1 {
		t.Fatalf("start mark %d, want %d", got, SigningCeiling-1)
	}
	for {
		if _, err := clock.Next(); errors.Is(err, ErrSigningClockExhausted) {
			break
		} else if err != nil {
			t.Fatal(err)
		}
	}
	if got := mark.mark(); got != SigningCeiling-1 {
		t.Fatalf("mark %d at the ceiling, want %d", got, SigningCeiling-1)
	}
}

// TestExtensionsKeepALeadAsTheWallClockMoves moves the wall clock across many
// reservations, letting each extension land: the persisted mark stays at
// least half a reservation ahead of the wall clock, so signing never needs a
// write on its own path while the clock is not ahead.
func TestExtensionsKeepALeadAsTheWallClockMoves(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	for range 200 {
		wall.add(300 * time.Millisecond)
		mark.next(t, clock)
		settle(clock)
		if lead := mark.mark() - wall.now().UnixMilli(); lead < signingReservation/2 {
			t.Fatalf("the mark is %d ms ahead of the wall clock, under half a reservation", lead)
		}
	}
}

// TestAJumpPastTheReservationReservesAhead moves the wall clock an hour past
// the reservation: the value is persisted before it is issued under a mark one
// reservation past the wall clock, not at the value alone.
func TestAJumpPastTheReservationReservesAhead(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	wall.add(time.Hour)
	at := mark.next(t, clock)
	if got, want := mark.mark(), wall.now().UnixMilli()+signingReservation; got != want || at != wall.now().UnixMilli() {
		t.Fatalf("issued %d under mark %d, want the wall clock under %d", at, got, want)
	}
}

// TestAnExtensionThatSucceedsEndsAFailureEpisode fails an extension, lets the
// next one succeed, then fails another: each failing episode is warned about,
// so a success off the signing path ends an episode as one on it does.
func TestAnExtensionThatSucceedsEndsAFailureEpisode(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &bytes.Buffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	// Each step moves the wall clock 300 ms, a millisecond per value, so
	// every step past a reservation's half starts an extension.
	step := func(fail bool) {
		mark.setFail(fail)
		for range 300 {
			wall.add(time.Millisecond)
			mark.next(t, clock)
		}
		settle(clock)
	}
	step(false)
	step(true)
	if n := strings.Count(logs.String(), "could not be persisted"); n != 1 {
		t.Fatalf("%d warnings after a failed extension:\n%s", n, logs.String())
	}
	// No value passes the reservation, so only an extension can end the
	// episode, and only a later extension can start the next.
	step(false)
	step(true)
	if writes := mark.writeCount(); writes != 2 {
		t.Fatalf("%d writes, want the start's and one extension's", writes)
	}
	if n := strings.Count(logs.String(), "could not be persisted"); n != 2 {
		t.Fatalf("%d warnings, want a second after an extension succeeded:\n%s", n, logs.String())
	}
}

// TestARollbackUsesUpTheCoverageAlreadyPersistedFirst signs at a wall clock of
// 10,000 under a mark of 11,000, then rolls the wall clock back to 0: the
// values the mark already covers are issued with no write and no extension,
// and once that coverage is used up every value is persisted before it is
// issued.
func TestARollbackUsesUpTheCoverageAlreadyPersistedFirst(t *testing.T) {
	wall := &testWall{at: time.UnixMilli(10_000)}
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	if got := mark.mark(); got != 11_000 {
		t.Fatalf("start mark %d, want 11000", got)
	}
	if at := mark.next(t, clock); at != 10_000 {
		t.Fatalf("first value %d, want 10000", at)
	}
	wall.at = time.UnixMilli(0)
	for want := int64(10_001); want <= 11_000; want++ {
		if at := mark.next(t, clock); at != want {
			t.Fatalf("issued %d, want %d", at, want)
		}
		clock.mu.Lock()
		extending := clock.extension != nil
		clock.mu.Unlock()
		if extending {
			t.Fatalf("an extension started at %d while the persisted mark still covered it", want)
		}
	}
	if n := mark.writeCount(); n != 1 {
		t.Fatalf("%d writes while the persisted mark covered every value, want the start's only", n)
	}
	for i := int64(1); i <= 3; i++ {
		at := mark.next(t, clock)
		if at != 11_000+i || mark.mark() != at || mark.writeCount() != int(1+i) {
			t.Fatalf("issued %d under mark %d after %d writes; want each value persisted before it is issued", at, mark.mark(), mark.writeCount())
		}
	}
}
