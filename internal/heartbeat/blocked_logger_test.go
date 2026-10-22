// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/asynclog"
)

// blockingHandler blocks every write, and every level check, until the test
// ends: a log destination that has stopped accepting writes.
type blockingHandler struct{ gate chan struct{} }

func newBlockingHandler(t *testing.T) *blockingHandler {
	h := &blockingHandler{gate: make(chan struct{})}
	t.Cleanup(func() { close(h.gate) })
	return h
}

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool  { <-h.gate; return true }
func (h *blockingHandler) Handle(context.Context, slog.Record) error { <-h.gate; return nil }
func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler             { return h }

// GW-6: a heartbeat loop whose every log line would block still reaches its
// failure limit and ends the process, within the limit's intervals and the
// exit flush bound.
func TestALoggerThatBlocksNeverKeepsTheHeartbeatFromItsExit(t *testing.T) {
	var fatalCalls atomic.Int32
	fatal := make(chan struct{})
	publisher, err := NewPublisher(func(context.Context, []byte) error {
		return errors.New("publish failed")
	}, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval:     time.Millisecond,
		FailureLimit: 3,
		Fatal: func(int) {
			if fatalCalls.Add(1) == 1 {
				close(fatal)
			}
		},
		Logger: slog.New(newBlockingHandler(t)),
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = publisher.Run(t.Context()) }()
	select {
	case <-fatal:
	case <-time.After(asynclog.ExitFlushBound + 5*time.Second):
		t.Fatal("a logger that blocks kept the heartbeat from its failure limit and exit")
	}
	if got := fatalCalls.Load(); got != 1 {
		t.Fatalf("fatal calls = %d, want 1", got)
	}
}

// Publishing follows the latest attempt at once: false before the first
// heartbeat, true right after a success, false right after a failure, well
// within the three intervals that would also mark it stale, true again after
// a success, and false once no heartbeat has gone out for three intervals.
func TestPublishingFollowsTheLatestAttempt(t *testing.T) {
	const interval = 200 * time.Millisecond
	var fail, stall atomic.Bool
	attempts := make(chan struct{}, 64)
	publisher, err := NewPublisher(func(ctx context.Context, _ []byte) error {
		defer func() { attempts <- struct{}{} }()
		if stall.Load() {
			<-ctx.Done()
			return ctx.Err()
		}
		if fail.Load() {
			return errors.New("publish failed")
		}
		return nil
	}, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval: interval, FailureLimit: 1000, Fatal: func(int) {},
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if publisher.Publishing() {
		t.Fatal("publishing before any heartbeat")
	}
	go func() { _ = publisher.Run(t.Context()) }()
	// afterAttempt waits for the next attempt to finish, then reads Publishing
	// at once, before the staleness window could decide it.
	afterAttempt := func(what string, want bool) {
		t.Helper()
		for len(attempts) > 0 {
			<-attempts
		}
		select {
		case <-attempts:
		case <-time.After(10 * time.Second):
			t.Fatalf("no attempt for %s", what)
		}
		// The attempt's outcome is recorded just after publish returns.
		deadline := time.Now().Add(interval / 2)
		for publisher.Publishing() != want {
			if time.Now().After(deadline) {
				t.Fatalf("Publishing() = %v right after %s, want %v", !want, what, want)
			}
			time.Sleep(time.Millisecond)
		}
	}
	afterAttempt("the first heartbeat", true)
	fail.Store(true)
	afterAttempt("a failed attempt", false)
	fail.Store(false)
	afterAttempt("a successful attempt", true)
	stall.Store(true)
	deadline := time.Now().Add(10 * time.Second)
	for publisher.Publishing() {
		if time.Now().After(deadline) {
			t.Fatal("still publishing after three intervals without a heartbeat")
		}
		time.Sleep(10 * time.Millisecond)
	}
}

// With a working logger, the line saying why the process ends is written
// before it ends: the exit waits for its queued records, up to the bound.
// It holds both for a plain logger and for one already queued, as the
// gateway's own logger is when runGateway hands it to the publisher.
func TestTheExitLineIsWrittenBeforeTheHeartbeatExits(t *testing.T) {
	for _, queued := range []bool{false, true} {
		t.Run(fmt.Sprintf("queued=%v", queued), func(t *testing.T) { exitLineWrittenBeforeExit(t, queued) })
	}
}

func exitLineWrittenBeforeExit(t *testing.T, queued bool) {
	var mu sync.Mutex
	var logs strings.Builder
	handler := slog.NewTextHandler(writerFunc(func(p []byte) (int, error) {
		mu.Lock()
		defer mu.Unlock()
		return logs.Write(p)
	}), nil)
	logger := slog.New(handler)
	if queued {
		logger, _ = asynclog.New(handler, "gateway log lines dropped")
	}
	written := make(chan bool, 1)
	publisher, err := NewPublisher(func(context.Context, []byte) error {
		return errors.New("publish failed")
	}, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval: time.Millisecond, FailureLimit: 2,
		Fatal: func(int) {
			mu.Lock()
			defer mu.Unlock()
			select {
			case written <- strings.Contains(logs.String(), "heartbeat failure limit reached, exiting"):
			default:
			}
		},
		Logger: logger,
	})
	if err != nil {
		t.Fatal(err)
	}
	go func() { _ = publisher.Run(t.Context()) }()
	select {
	case ok := <-written:
		if !ok {
			t.Fatal("the process ended before the line saying why was written")
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the heartbeat never reached its exit")
	}
}

type writerFunc func([]byte) (int, error)

func (f writerFunc) Write(p []byte) (int, error) { return f(p) }
