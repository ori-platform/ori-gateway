// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"bytes"
	"context"
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
// test has seen issued: at most the latest of the wall clock plus 1,000 ms,
// the mark found, the value being issued, which rule 8 puts at most one past
// the last value issued or the mark found, and the highest mark written
// before; never below a mark written before; and, unless its clock is in an
// episode, at least every value issued.
type memMark struct {
	t    *testing.T
	wall *testWall
	// clock, once set, is asked whether an episode is open when a write lands.
	clock *SigningClock

	mu        sync.Mutex
	found     int64
	has       bool
	persisted int64
	attempted int64
	writes    int
	attempts  int
	fail      bool
	// failHeld fails the write held once, which fail exempts.
	failHeld bool
	// delay slows every write.
	delay time.Duration
	block chan struct{}
	// blockOnce holds the next write only; held is closed once it is held.
	blockOnce chan struct{}
	held      chan struct{}
	issued    int64
}

func newMemMark(t *testing.T, wall *testWall) *memMark {
	return &memMark{t: t, wall: wall, persisted: -1, attempted: -1}
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
	delay := m.delay
	m.mu.Unlock()
	time.Sleep(delay)
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
	if mark < m.attempted {
		m.t.Errorf("wrote %d below %d, a mark written before", mark, m.attempted)
	}
	wall := m.wall.now().UnixMilli()
	upper := max(wall+signingReservation, m.issued+1, m.attempted)
	if m.has {
		upper = max(upper, m.found+1)
	}
	if mark > upper {
		m.t.Errorf("persisted %d above rule 9's bound %d (wall %d, last issued %d)", mark, upper, wall, m.issued)
	}
	m.attempted = max(m.attempted, mark)
	m.attempts++
	if once != nil && m.failHeld {
		m.failHeld = false
		return errors.New("disk full")
	}
	if m.fail && once == nil {
		return errors.New("disk full")
	}
	if mark <= m.persisted {
		return nil
	}
	// The issued value is read before the episode: no episode closes while
	// this write is outstanding, so one closed now was closed throughout.
	if issued := m.issued; mark < issued && (m.clock == nil || !m.clock.Degraded()) {
		m.t.Errorf("persisted %d below the issued value %d outside an episode", mark, issued)
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

func (m *memMark) attemptCount() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attempts
}

func (m *memMark) highestAttempt() int64 {
	m.mu.Lock()
	defer m.mu.Unlock()
	return m.attempted
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
	mark.mu.Lock()
	mark.clock = clock
	mark.mu.Unlock()
	t.Cleanup(clock.Close)
	return clock
}

// fakeTimers stands in for the clock's timers: none expires until the test
// fires it.
type fakeTimers struct {
	mu      sync.Mutex
	pending []*fakeTimer
}

type fakeTimer struct {
	d       time.Duration
	f       func()
	stopped bool
}

// useFakeTimers gives every clock built from now on timers the test fires.
func useFakeTimers(t *testing.T) *fakeTimers {
	t.Helper()
	timers := &fakeTimers{}
	prev := afterFunc
	afterFunc = timers.after
	t.Cleanup(func() { afterFunc = prev })
	return timers
}

func (ft *fakeTimers) after(d time.Duration, f func()) func() bool {
	timer := &fakeTimer{d: d, f: f}
	ft.mu.Lock()
	ft.pending = append(ft.pending, timer)
	ft.mu.Unlock()
	return func() bool {
		ft.mu.Lock()
		defer ft.mu.Unlock()
		was := !timer.stopped
		timer.stopped = true
		return was
	}
}

// live is every timer neither fired nor stopped, oldest first.
func (ft *fakeTimers) live() []*fakeTimer {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	var live []*fakeTimer
	for _, timer := range ft.pending {
		if !timer.stopped {
			live = append(live, timer)
		}
	}
	ft.pending = live
	return append([]*fakeTimer(nil), live...)
}

// everyTimer is every timer armed, fired, stopped or not, since the last
// call to live.
func (ft *fakeTimers) everyTimer() []*fakeTimer {
	ft.mu.Lock()
	defer ft.mu.Unlock()
	return append([]*fakeTimer(nil), ft.pending...)
}

// fire expires timer, unless it was stopped.
func (ft *fakeTimers) fire(timer *fakeTimer) {
	ft.mu.Lock()
	if timer.stopped {
		ft.mu.Unlock()
		return
	}
	timer.stopped = true
	ft.mu.Unlock()
	timer.f()
}

// fireAll expires every live timer.
func (ft *fakeTimers) fireAll() {
	for _, timer := range ft.live() {
		ft.fire(timer)
	}
}

// recoverEpisode fires timers and settles writes until the clock's episode closes.
func recoverEpisode(t *testing.T, timers *fakeTimers, clock *SigningClock) {
	t.Helper()
	for range 10 {
		settle(clock)
		if !clock.Degraded() {
			return
		}
		timers.fireAll()
	}
	t.Fatal("the episode never closed")
}

// TestSignersNeverRepeatASignedAt issues signing times while the mark cannot
// be written: signing goes on without repeating, one warning is logged, and
// once the disk recovers the paced retry covers every value issued.
func TestSignersNeverRepeatASignedAt(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &syncBuffer{}
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
	if n := waitLogs(t, logs, "could not be persisted", 1); n != 1 {
		t.Fatalf("want one warning while persistence fails:\n%s", logs.String())
	}
	mark.setFail(false)
	wall.add(2 * time.Second)
	after := mark.next(t, clock)
	recoverEpisode(t, timers, clock)
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
	extending := clock.write != nil
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
	timers := useFakeTimers(t)
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
		case r < 95:
			mark.setFail(rng.IntN(2) == 0)
		case r < 97:
			// A paced retry, or a timer that opens an episode, expires.
			settle(clock)
			timers.fireAll()
		default:
			// End any failure episode with a write that covers every value
			// issued, then restart. A restart inside an episode may repeat
			// values no mark covers, the limit gateway-config/v2 accepts.
			mark.setFail(false)
			wall.add(2 * time.Second)
			mark.next(t, clock)
			recoverEpisode(t, timers, clock)
			clock.Close()
			mark = mark.restarted()
			clock = durableClock(t, wall, mark, nil)
		}
		mark.next(t, clock)
	}
}

// settle waits for the outstanding write, if any.
func settle(clock *SigningClock) {
	clock.mu.Lock()
	done := clock.write
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
	logs := &syncBuffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	if at := mark.next(t, clock); at <= ahead {
		t.Fatalf("resumed at %d, not above the mark found", at)
	}
	mark.next(t, clock)
	if waitLogs(t, logs, "ahead of the wall clock", 1) != 1 {
		t.Fatalf("want one warning for a clock running ahead:\n%s", logs.String())
	}
	// The wall clock catches up, then is stepped back again: a second
	// episode is warned about again.
	wall.add(2 * time.Hour)
	mark.next(t, clock)
	wall.add(-time.Hour)
	mark.next(t, clock)
	if waitLogs(t, logs, "ahead of the wall clock", 2) != 2 {
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

// TestAClosedClockRefusesToSign closes the clock and then asks for values
// past its reservation: each is refused, nothing reaches the mark, and the
// refusal is logged once.
func TestAClosedClockRefusesToSign(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &syncBuffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	clock.Close()
	stored := mark.mark()
	wall.add(time.Minute)
	for range 5 {
		if _, err := clock.Next(); !errors.Is(err, ErrSigningClockClosed) {
			t.Fatalf("a closed clock signed: %v", err)
		}
	}
	if mark.mark() != stored {
		t.Fatal("a closed clock wrote its mark")
	}
	if n := waitLogs(t, logs, "signing clock is stopped", 1); n != 1 {
		t.Fatalf("%d refusals logged for five calls, want one:\n%s", n, logs.String())
	}
}

// TestAFailedWriteIsRetriedOnItsPace fails one write past the reservation:
// the value is issued at once, no later value retries the write, and the
// write is retried once its paced timer expires, which covers every value.
func TestAFailedWriteIsRetriedOnItsPace(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	wall.add(2 * time.Second)
	mark.setFail(true)
	uncovered := mark.next(t, clock)
	settle(clock)
	mark.setFail(false)
	writes := mark.writeCount()
	var next int64
	for range 100 {
		next = mark.next(t, clock)
	}
	settle(clock)
	if mark.writeCount() != writes {
		t.Fatalf("%d writes while an episode was open with its retry pending, want none per message", mark.writeCount()-writes)
	}
	recoverEpisode(t, timers, clock)
	if got := mark.mark(); got < next || got < uncovered {
		t.Fatalf("mark %d after issuing %d then %d; the failed write was not retried", got, uncovered, next)
	}
}

// TestEachFailureEpisodeIsWarnedAboutAgain fails writes, lets one succeed,
// then fails them again: each episode is warned about once, with its cause.
func TestEachFailureEpisodeIsWarnedAboutAgain(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &syncBuffer{}
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
		recoverEpisode(t, timers, clock)
	}
	episode()
	if n := waitLogs(t, logs, "could not be persisted", 1); n != 1 {
		t.Fatalf("%d warnings for one episode:\n%s", n, logs.String())
	}
	if !strings.Contains(logs.String(), "disk full") {
		t.Fatalf("the warning does not carry its cause:\n%s", logs.String())
	}
	episode()
	if n := waitLogs(t, logs, "could not be persisted", 2); n != 2 {
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
		logs := &syncBuffer{}
		clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
		at := mark.next(t, clock)
		want := 0
		if tc.warn {
			want = 1
		}
		if got := waitLogs(t, logs, "ahead of the wall clock", want) == 1; got != tc.warn {
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

// TestAnExtensionThatFailsOpensAnEpisode fails an extension off the signing
// path: an episode opens and is warned about, though every value issued is
// still covered; the retry that succeeds ends it, and a second failing
// extension is warned about again.
func TestAnExtensionThatFailsOpensAnEpisode(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &syncBuffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	// Each step moves the wall clock a millisecond per value, so a step past
	// a reservation's half starts an extension.
	step := func(fail bool, values int) {
		mark.setFail(fail)
		for range values {
			wall.add(time.Millisecond)
			mark.next(t, clock)
		}
		settle(clock)
	}
	step(false, 300)
	step(true, 300)
	if !clock.Degraded() {
		t.Fatal("a failed extension opened no episode")
	}
	if n := waitLogs(t, logs, "could not be persisted", 1); n != 1 {
		t.Fatalf("%d warnings after a failed extension:\n%s", n, logs.String())
	}
	mark.setFail(false)
	recoverEpisode(t, timers, clock)
	step(true, 600)
	if n := waitLogs(t, logs, "could not be persisted", 2); n != 2 {
		t.Fatalf("%d warnings, want a second after the first episode closed:\n%s", n, logs.String())
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
		extending := clock.write != nil
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

// holdNext holds the mark's next write until the returned release is called,
// and returns a channel closed once that write is held.
func holdNext(t *testing.T, mark *memMark) (held <-chan struct{}, release func()) {
	t.Helper()
	gate, entered := make(chan struct{}), make(chan struct{})
	mark.mu.Lock()
	mark.blockOnce, mark.held = gate, entered
	mark.mu.Unlock()
	release = sync.OnceFunc(func() { close(gate) })
	t.Cleanup(release)
	return entered, release
}

// TestAStalledWriteOpensAnEpisodeAtTheWriteBound holds a write past the
// reservation on the real monotonic clock: the value waiting for it is issued
// once the write bound has passed, not before, an episode is open and warned
// about, and the next values wait on nothing although the write is still
// held. The write that lands late covers only its own mark, so the episode
// stays open until the paced retry covers every value issued.
func TestAStalledWriteOpensAnEpisodeAtTheWriteBound(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &syncBuffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	held, release := holdNext(t, mark)
	wall.add(time.Hour)
	began := time.Now()
	first := mark.next(t, clock)
	waited := time.Since(began)
	<-held
	if waited < MarkWriteBound-50*time.Millisecond || waited > MarkWriteBound+2*time.Second {
		t.Fatalf("a value past the coverage waited %v for a stalled write, want the write bound %v", waited, MarkWriteBound)
	}
	if !clock.Degraded() || waitLogs(t, logs, "could not be persisted", 1) != 1 {
		t.Fatalf("no episode opened at the write bound (degraded=%v):\n%s", clock.Degraded(), logs.String())
	}
	began = time.Now()
	// Past the held write's mark, one reservation past the wall clock.
	var last int64
	for range 1500 {
		wall.add(time.Millisecond)
		last = mark.next(t, clock)
	}
	if waited := time.Since(began); waited > 500*time.Millisecond {
		t.Fatalf("values waited %v during an episode", waited)
	}
	release()
	deadline := time.Now().Add(5 * time.Second)
	for clock.Degraded() {
		if time.Now().After(deadline) {
			t.Fatal("the paced retry never closed the episode")
		}
		time.Sleep(10 * time.Millisecond)
	}
	if got := mark.mark(); got < last || got < first {
		t.Fatalf("the episode closed under mark %d, below the last value %d", got, last)
	}
	if n := mark.attemptCount(); n < 3 {
		t.Fatalf("the episode closed after %d writes, want the start's, the late one and a retry", n)
	}
	if n := waitLogs(t, logs, "could not be persisted", 1); n != 1 {
		t.Fatalf("%d warnings for one episode:\n%s", n, logs.String())
	}
}

// waitLogs waits until logs holds want lines containing substr, which the
// clock writes once its lock is released, possibly from its writer, then
// returns how many it holds after a short settle.
func waitLogs(t *testing.T, logs *syncBuffer, substr string, want int) int {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for strings.Count(logs.String(), substr) < want && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	time.Sleep(20 * time.Millisecond)
	return strings.Count(logs.String(), substr)
}

// syncBuffer is a log buffer safe to write from timers.
type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// TestAValueWaitsOnlyTheRemainingDeadlineOfTheOutstandingWrite holds an
// extension, then asks for a value past the coverage: the extension's own
// write bound expiring, before the value's, opens the episode and releases
// the value, so no value waits past the moment an episode opens, and a value
// waiting alone is released by its own bound.
func TestAValueWaitsOnlyTheRemainingDeadlineOfTheOutstandingWrite(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	held, _ := holdNext(t, mark)
	for range signingReservation/2 + 2 {
		wall.add(time.Millisecond)
		mark.next(t, clock)
	}
	<-held
	writeBound := timers.live()
	if len(writeBound) != 1 || writeBound[0].d != MarkWriteBound {
		t.Fatalf("the outstanding write has %d timers, want one of the write bound", len(writeBound))
	}
	wall.add(time.Hour)
	got := make(chan int64, 1)
	go func() { got <- mark.next(t, clock) }()
	waitFor(t, "the value's own bound", func() bool { return len(timers.live()) == 2 })
	select {
	case <-got:
		t.Fatal("a value past the coverage was issued before any bound expired")
	case <-time.After(50 * time.Millisecond):
	}
	timers.fire(writeBound[0])
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("the value still waited after the outstanding write passed its bound")
	}
	if !clock.Degraded() {
		t.Fatal("no episode opened when the outstanding write passed its bound")
	}
}

// TestAValueThatWaitedTheWriteBoundOpensAnEpisode holds the write a value
// waits for, and expires the value's own bound: the value is issued, an
// episode is open, and the held write stays outstanding. The held write then
// lands below later values, which leaves the episode open.
func TestAValueThatWaitedTheWriteBoundOpensAnEpisode(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	held, release := holdNext(t, mark)
	wall.add(time.Hour)
	got := make(chan int64, 1)
	go func() { got <- mark.next(t, clock) }()
	<-held
	waitFor(t, "both bounds", func() bool { return len(timers.live()) == 2 })
	// The value's bound was armed after the write's.
	timers.fire(timers.live()[1])
	select {
	case <-got:
	case <-time.After(5 * time.Second):
		t.Fatal("a value waited past its own write bound")
	}
	clock.mu.Lock()
	outstanding := clock.write != nil
	clock.mu.Unlock()
	if !clock.Degraded() || !outstanding {
		t.Fatalf("degraded=%v, write outstanding=%v; want an episode with the write still outstanding", clock.Degraded(), outstanding)
	}
	// Values past the held write's mark, then the write lands late: it covers
	// only its own mark, so the episode stays open until a retry covers them.
	var last int64
	for range 1500 {
		wall.add(time.Millisecond)
		last = mark.next(t, clock)
	}
	release()
	settle(clock)
	if !clock.Degraded() {
		t.Fatalf("a write landing below the last value %d issued closed the episode", last)
	}
	recoverEpisode(t, timers, clock)
	if got := mark.mark(); got < last {
		t.Fatalf("the episode closed under mark %d, below the last value %d", got, last)
	}
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// TestRetriesArePacedFromThePreviousWritesStart fails every write: each
// failure arms one retry, no sooner than 1,000 ms after the write before it
// started, at most one write is ever outstanding, and only an expired retry
// writes again.
func TestRetriesArePacedFromThePreviousWritesStart(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	mark.setFail(true)
	wall.add(time.Hour)
	for range 50 {
		wall.add(time.Millisecond)
		mark.next(t, clock)
	}
	for round := range 5 {
		settle(clock)
		live := timers.live()
		if len(live) != 1 {
			t.Fatalf("round %d: %d timers armed, want one retry", round, len(live))
		}
		if d := live[0].d; d <= 0 || d > markRetryPacing {
			t.Fatalf("round %d: retry armed after %v, want within the pacing %v of the previous start", round, d, markRetryPacing)
		}
		writes := mark.attemptCount()
		for range 50 {
			mark.next(t, clock)
		}
		settle(clock)
		if mark.attemptCount() != writes {
			t.Fatalf("round %d: values started a write while a retry was pending", round)
		}
		timers.fire(live[0])
		settle(clock)
		if mark.attemptCount() != writes+1 {
			t.Fatalf("round %d: an expired retry wrote %d times, want once", round, mark.attemptCount()-writes)
		}
	}
}

// TestALateFailureThenRecovery fails a write that lands after its bound has
// opened the episode: the episode stays open, one warning stands for it, and
// a later write that covers every value closes it.
func TestALateFailureThenRecovery(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &syncBuffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, nil)))
	held, release := holdNext(t, mark)
	wall.add(time.Hour)
	got := make(chan int64, 1)
	go func() { got <- mark.next(t, clock) }()
	<-held
	timers.fireAll()
	<-got
	mark.mu.Lock()
	mark.fail = true
	mark.mu.Unlock()
	// The held write is exempt from fail; fail it explicitly on landing.
	mark.mu.Lock()
	mark.failHeld = true
	mark.mu.Unlock()
	release()
	settle(clock)
	if !clock.Degraded() {
		t.Fatal("a late failure closed the episode")
	}
	mark.setFail(false)
	recoverEpisode(t, timers, clock)
	if n := waitLogs(t, logs, "could not be persisted", 1); n != 1 {
		t.Fatalf("%d warnings for one episode:\n%s", n, logs.String())
	}
}

// TestAMarkNeverLowersOneAlreadyWritten fails a write after the wall clock
// moved ahead, then steps the wall clock back: the retry writes no lower than
// the failed mark, which may have landed (rule 9), and memMark holds it to
// the upper bound that allows rewriting it.
func TestAMarkNeverLowersOneAlreadyWritten(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	mark.setFail(true)
	wall.add(time.Hour)
	mark.next(t, clock)
	settle(clock)
	failed := mark.highestAttempt()
	wall.add(-2 * time.Hour)
	mark.setFail(false)
	recoverEpisode(t, timers, clock)
	if got := mark.mark(); got < failed {
		t.Fatalf("the retry wrote %d, below the failed mark %d", got, failed)
	}
}

// TestAStartWhoseFirstWriteStallsIsRefused holds the start's write past the
// write bound: the start is refused with a StartWriteError, which reports the
// write outstanding until it ends.
func TestAStartWhoseFirstWriteStallsIsRefused(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	held, release := holdNext(t, mark)
	refused := make(chan error, 1)
	go func() {
		_, err := NewDurableSigningClock(wall.now, mark, nil)
		refused <- err
	}()
	<-held
	waitFor(t, "the start's bound", func() bool { return len(timers.live()) == 1 })
	timers.fireAll()
	var startErr *StartWriteError
	select {
	case err := <-refused:
		if !errors.As(err, &startErr) {
			t.Fatalf("a start whose first write stalled: %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a start waited on its stalled write past the write bound")
	}
	if startErr.Quiesce(20 * time.Millisecond) {
		t.Fatal("the start's write reported ended while still held")
	}
	release()
	if !startErr.Quiesce(5 * time.Second) {
		t.Fatal("the start's write never reported ended")
	}
}

// TestCloseWithinReportsAWriteStillOutstanding holds a write and closes the
// clock within a bound: it reports the write outstanding, refuses to sign,
// cancels the paced retry and starts no write, and Close returns once the
// write ends.
func TestCloseWithinReportsAWriteStillOutstanding(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	held, release := holdNext(t, mark)
	wall.add(time.Hour)
	got := make(chan int64, 1)
	go func() { got <- mark.next(t, clock) }()
	<-held
	timers.fireAll()
	<-got
	if clock.CloseWithin(20 * time.Millisecond) {
		t.Fatal("CloseWithin reported quiescence with a write held")
	}
	if _, err := clock.Next(); !errors.Is(err, ErrSigningClockClosed) {
		t.Fatalf("a closing clock signed: %v", err)
	}
	release()
	clock.Close()
	attempts := mark.attemptCount()
	timers.fireAll()
	settle(clock)
	if mark.attemptCount() != attempts {
		t.Fatal("a closed clock started a write")
	}
}

// TestParallelSignersAcrossAStalledReservation signs from many goroutines
// while every write is slow, on real timers: no value repeats, no signer
// waits past the write bound plus scheduling slack, and the clock recovers
// once writes are fast again.
func TestParallelSignersAcrossAStalledReservation(t *testing.T) {
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	mark.mu.Lock()
	mark.delay = 1500 * time.Millisecond
	mark.mu.Unlock()
	var mu sync.Mutex
	seen := map[int64]bool{}
	var wg sync.WaitGroup
	for range 8 {
		wg.Go(func() {
			for range 200 {
				wall.add(time.Millisecond)
				began := time.Now()
				at, err := clock.Next()
				if err != nil {
					t.Error(err)
					return
				}
				if waited := time.Since(began); waited > MarkWriteBound+time.Second {
					t.Errorf("a signer waited %v", waited)
				}
				mu.Lock()
				if seen[at] {
					t.Errorf("value %d issued twice", at)
				}
				seen[at] = true
				mu.Unlock()
			}
		})
	}
	wg.Wait()
	mark.mu.Lock()
	mark.delay = 0
	mark.mu.Unlock()
	deadline := time.Now().Add(10 * time.Second)
	for clock.Degraded() {
		if time.Now().After(deadline) {
			t.Fatal("the clock never recovered")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// TestAnEpisodeOpenedBetweenWritesArmsARetry opens an episode while no write
// is outstanding, as a value's bound expiring just after a write landed short
// of it does: a retry is armed, so the episode never stays open with nothing
// left to close it.
func TestAnEpisodeOpenedBetweenWritesArmsARetry(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	wall.add(time.Hour)
	clock.mu.Lock()
	clock.last = wall.now().UnixMilli()
	clock.openEpisodeLocked(errWriteOutstanding)
	armed := clock.retry != nil
	clock.mu.Unlock()
	if !armed {
		t.Fatal("an episode opened between writes armed no retry")
	}
	recoverEpisode(t, timers, clock)
}

// TestAStopDuringAnEpisodeStartsNoWrite fails writes until an episode opens
// with its retry armed, then stops the clock: Stop reports no write
// outstanding, and no expiring timer starts a write after it.
func TestAStopDuringAnEpisodeStartsNoWrite(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	mark.setFail(true)
	wall.add(time.Hour)
	mark.next(t, clock)
	settle(clock)
	clock.mu.Lock()
	armed := clock.retry != nil
	clock.mu.Unlock()
	if !clock.Degraded() || !armed {
		t.Fatalf("degraded=%v, retry armed=%v; want an episode with its retry armed", clock.Degraded(), armed)
	}
	select {
	case <-clock.Stop():
	default:
		t.Fatal("Stop reported a write outstanding with none")
	}
	attempts := mark.attemptCount()
	for _, timer := range timers.everyTimer() {
		// Expire every timer, stopped or not, as a timer whose stop lost the
		// race with its expiry would.
		timer.f()
	}
	settle(clock)
	if mark.attemptCount() != attempts {
		t.Fatal("a write started after Stop reported none outstanding")
	}
}

// TestAWriteFailingAfterStopArmsNoRetry stops the clock while a write is
// held, then fails that write: no retry is armed, and no write follows.
func TestAWriteFailingAfterStopArmsNoRetry(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	clock := durableClock(t, wall, mark, nil)
	held, release := holdNext(t, mark)
	wall.add(time.Hour)
	got := make(chan int64, 1)
	go func() { got <- mark.next(t, clock) }()
	<-held
	timers.fireAll()
	<-got
	idle := clock.Stop()
	mark.mu.Lock()
	mark.failHeld = true
	mark.mu.Unlock()
	release()
	<-idle
	attempts := mark.attemptCount()
	clock.mu.Lock()
	armed := clock.retry != nil
	clock.mu.Unlock()
	if armed {
		t.Fatal("a write failing after Stop armed a retry")
	}
	for _, timer := range timers.everyTimer() {
		timer.f()
	}
	settle(clock)
	if mark.attemptCount() != attempts {
		t.Fatal("a write started after Stop")
	}
}

// blockingHandler holds every log record until released, as a full pipe or a
// stalled journal would.
type blockingHandler struct {
	entered chan struct{}
	once    sync.Once
	release chan struct{}
}

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *blockingHandler) Handle(context.Context, slog.Record) error {
	h.once.Do(func() { close(h.entered) })
	<-h.release
	return nil
}
func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler      { return h }

// TestALoggerThatBlocksNeverHoldsUpSigning blocks the logger as an episode
// opens: Degraded reports the episode, values are issued, and a call whose
// own line queues behind the blocked one (a closed clock's refusal) returns,
// since no caller writes a log line itself.
func TestALoggerThatBlocksNeverHoldsUpSigning(t *testing.T) {
	useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	handler := &blockingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	clock := durableClock(t, wall, mark, slog.New(handler))
	// Registered after the clock's Close, so it runs first.
	t.Cleanup(func() { close(handler.release) })
	mark.setFail(true)
	wall.add(time.Hour)
	go func() { _, _ = clock.Next() }()
	<-handler.entered
	done := make(chan bool, 1)
	go func() {
		degraded := clock.Degraded()
		for range 10 {
			if _, err := clock.Next(); err != nil {
				t.Error(err)
			}
		}
		clock.Stop()
		if _, err := clock.Next(); !errors.Is(err, ErrSigningClockClosed) {
			t.Errorf("a stopped clock signed: %v", err)
		}
		done <- degraded
	}()
	select {
	case degraded := <-done:
		if !degraded {
			t.Fatal("the episode being logged was not reported")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("a blocked logger held up Degraded, Next or a refusal")
	}
}

// TestLogLinesKeepTheOrderTheyWereDecidedIn blocks the logger on the warning
// of an episode opened by a write past its bound, then lets the write land and
// close the episode: the warning is still written before the recovery line.
func TestLogLinesKeepTheOrderTheyWereDecidedIn(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	handler := &orderHandler{release: make(chan struct{}), entered: make(chan struct{})}
	clock := durableClock(t, wall, mark, slog.New(handler))
	t.Cleanup(handler.unblock)
	held, release := holdNext(t, mark)
	wall.add(time.Hour)
	got := make(chan int64, 1)
	go func() { got <- mark.next(t, clock) }()
	<-held
	timers.fire(timers.live()[0])
	<-got
	<-handler.entered
	release()
	settle(clock)
	// A covering retry is not needed: the late write covers the value issued.
	recoverEpisode(t, timers, clock)
	handler.unblock()
	waitFor(t, "both lines", func() bool { return len(handler.lines()) >= 2 })
	lines := handler.lines()
	if !strings.Contains(lines[0], "could not be persisted") || !strings.Contains(lines[1], "persisted again") {
		t.Fatalf("log lines out of order: %q", lines)
	}
}

// orderHandler records messages in the order written, blocking the first
// it is given until unblocked; the others are not held behind it.
type orderHandler struct {
	mu       sync.Mutex
	messages []string
	records  []writtenRecord
	held     bool
	entered  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (h *orderHandler) unblock() { h.once.Do(func() { close(h.release) }) }

func (h *orderHandler) lines() []string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]string(nil), h.messages...)
}

func (h *orderHandler) Enabled(context.Context, slog.Level) bool { return true }
func (h *orderHandler) Handle(_ context.Context, r slog.Record) error {
	h.mu.Lock()
	first := !h.held
	h.held = true
	h.mu.Unlock()
	if first {
		close(h.entered)
		<-h.release
	}
	written := writtenRecord{message: r.Message, at: r.Time}
	r.Attrs(func(a slog.Attr) bool {
		switch a.Key {
		case "signed_at_ms":
			written.signedAt = a.Value.Int64()
		case "dropped":
			written.dropped = int(a.Value.Int64())
		}
		return true
	})
	h.mu.Lock()
	h.messages = append(h.messages, r.Message)
	h.records = append(h.records, written)
	h.mu.Unlock()
	return nil
}

type writtenRecord struct {
	message  string
	at       time.Time
	signedAt int64
	dropped  int
}

func (h *orderHandler) recorded() []writtenRecord {
	h.mu.Lock()
	defer h.mu.Unlock()
	return append([]writtenRecord(nil), h.records...)
}
func (h *orderHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *orderHandler) WithGroup(string) slog.Handler      { return h }

// queueAheadWarnings has an in-process clock decide n ahead-of-the-wall
// warnings, each with its own signed_at_ms, by stepping the wall clock back
// past the default skew and forward again.
func queueAheadWarnings(t *testing.T, wall *testWall, clock *SigningClock, n int) {
	t.Helper()
	for range n {
		wall.add(-time.Hour)
		if _, err := clock.Next(); err != nil {
			t.Fatal(err)
		}
		wall.add(2 * time.Hour)
		if _, err := clock.Next(); err != nil {
			t.Fatal(err)
		}
	}
}

// TestLogLinesBehindABlockedLoggerAreBounded blocks the logger on a first
// warning, then decides three queues' worth more: the queue holds at most its
// bound, the records are written in the order decided, each stamped when it
// was decided, and the dropped count comes after them.
func TestLogLinesBehindABlockedLoggerAreBounded(t *testing.T) {
	wall := newTestWall()
	handler := &orderHandler{release: make(chan struct{}), entered: make(chan struct{})}
	clock := NewSigningClock(wall.now, slog.New(handler))
	t.Cleanup(handler.unblock)
	// A first value, so stepping the wall clock back leaves signing ahead.
	if _, err := clock.Next(); err != nil {
		t.Fatal(err)
	}
	queueAheadWarnings(t, wall, clock, 1)
	select {
	case <-handler.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("no warning reached the logger")
	}
	decided := time.Now()
	queueAheadWarnings(t, wall, clock, 3*maxNotes)
	clock.mu.Lock()
	queued := len(clock.notes)
	clock.mu.Unlock()
	if queued != maxNotes {
		t.Fatalf("%d records queued behind a blocked logger, want the bound %d", queued, maxNotes)
	}
	time.Sleep(200 * time.Millisecond)
	handler.unblock()
	waitFor(t, "the drainer to finish", func() bool {
		clock.mu.Lock()
		defer clock.mu.Unlock()
		return !clock.draining && len(handler.lines()) > 0
	})
	records := handler.recorded()
	if len(records) != 1+maxNotes+1 {
		t.Fatalf("%d records written, want the first, the queue and one count", len(records))
	}
	last := records[len(records)-1]
	if !strings.Contains(last.message, "dropped") || last.dropped != 2*maxNotes {
		t.Fatalf("last record %q (dropped %d), want the count of %d dropped", last.message, last.dropped, 2*maxNotes)
	}
	for i := 1; i < len(records)-1; i++ {
		if records[i].signedAt <= records[i-1].signedAt {
			t.Fatalf("record %d signed_at_ms %d written after %d: out of the order decided", i, records[i].signedAt, records[i-1].signedAt)
		}
		if late := records[i].at.Sub(decided); late > 100*time.Millisecond {
			t.Fatalf("record %d stamped %v after it was decided, want the time it was decided", i, late)
		}
	}
}

// TestTheClocksRecordsHonourTheLoggersLevel logs through a handler at Warn
// while an episode opens and closes: the warning is written and the Info
// recovery line is not, since the drainer asks the handler first.
func TestTheClocksRecordsHonourTheLoggersLevel(t *testing.T) {
	timers := useFakeTimers(t)
	wall := newTestWall()
	mark := newMemMark(t, wall)
	logs := &syncBuffer{}
	clock := durableClock(t, wall, mark, slog.New(slog.NewTextHandler(logs, &slog.HandlerOptions{Level: slog.LevelWarn})))
	mark.setFail(true)
	wall.add(time.Hour)
	mark.next(t, clock)
	mark.setFail(false)
	recoverEpisode(t, timers, clock)
	if waitLogs(t, logs, "could not be persisted", 1) != 1 {
		t.Fatalf("the episode warning was not written:\n%s", logs.String())
	}
	if strings.Contains(logs.String(), "persisted again") {
		t.Fatalf("an Info record was written through a handler at Warn:\n%s", logs.String())
	}
}
