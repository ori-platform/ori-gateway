// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package asynclog

import (
	"bytes"
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"
)

// gatedHandler writes text records, but only once its gate opens; until then
// Handle, and Enabled when lockLevel is set, block.
type gatedHandler struct {
	mu        sync.Mutex
	buf       bytes.Buffer
	gate      chan struct{}
	level     slog.Level
	lockLevel bool
}

func newGatedHandler() *gatedHandler { return &gatedHandler{gate: make(chan struct{})} }

func (h *gatedHandler) Enabled(_ context.Context, level slog.Level) bool {
	if h.lockLevel {
		<-h.gate
	}
	return level >= h.level
}

func (h *gatedHandler) Handle(_ context.Context, r slog.Record) error {
	<-h.gate
	h.mu.Lock()
	defer h.mu.Unlock()
	h.buf.WriteString(r.Message)
	r.Attrs(func(a slog.Attr) bool {
		h.buf.WriteString(" ")
		h.buf.WriteString(a.String())
		return true
	})
	h.buf.WriteString("\n")
	return nil
}

func (h *gatedHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *gatedHandler) WithGroup(string) slog.Handler      { return h }

func (h *gatedHandler) String() string {
	h.mu.Lock()
	defer h.mu.Unlock()
	return h.buf.String()
}

func returnsWithin(t *testing.T, bound time.Duration, what string, f func()) {
	t.Helper()
	done := make(chan struct{})
	go func() { f(); close(done) }()
	select {
	case <-done:
	case <-time.After(bound):
		t.Fatalf("%s waited on a logger that blocks", what)
	}
}

func TestACallerNeverWaitsOnALoggerThatBlocks(t *testing.T) {
	for _, lockLevel := range []bool{false, true} {
		inner := newGatedHandler()
		inner.lockLevel = lockLevel
		log, _ := New(inner, "dropped")
		returnsWithin(t, time.Second, "logging", func() {
			for range 2 * MaxQueued {
				log.Warn("line")
			}
		})
		close(inner.gate)
	}
}

func TestRecordsAreWrittenInOrderOnceTheLoggerAccepts(t *testing.T) {
	inner := newGatedHandler()
	log, q := New(inner, "dropped")
	for i := range 10 {
		log.Info("line", "n", i)
	}
	close(inner.gate)
	if !q.Flush(5 * time.Second) {
		t.Fatal("flush did not finish once the logger accepted")
	}
	var want strings.Builder
	for i := range 10 {
		fmt.Fprintf(&want, "line n=%d\n", i)
	}
	if got := inner.String(); got != want.String() {
		t.Fatalf("records out of order:\n%s", got)
	}
}

func TestRecordsBehindABlockedLoggerAreBoundedAndCounted(t *testing.T) {
	inner := newGatedHandler()
	log, q := New(inner, "log lines dropped")
	for range MaxQueued + 1 + 10 {
		log.Warn("line")
	}
	close(inner.gate)
	if !q.Flush(5 * time.Second) {
		t.Fatal("flush did not finish")
	}
	got := inner.String()
	// One record may already be with the drainer, so 64 or 65 are written.
	if n := strings.Count(got, "line\n"); n < MaxQueued || n > MaxQueued+1 {
		t.Fatalf("%d records written, want %d or %d", n, MaxQueued, MaxQueued+1)
	}
	if !strings.HasSuffix(got, "log lines dropped dropped=10\n") && !strings.HasSuffix(got, "log lines dropped dropped=11\n") {
		t.Fatalf("the dropped count is not logged last:\n%s", got[max(0, len(got)-200):])
	}
}

func TestFlushEndsAtItsBoundBehindABlockedLogger(t *testing.T) {
	inner := newGatedHandler()
	log, q := New(inner, "dropped")
	log.Error("exiting")
	start := time.Now()
	if q.Flush(50 * time.Millisecond) {
		t.Fatal("flush reported a blocked record written")
	}
	if waited := time.Since(start); waited > 5*time.Second {
		t.Fatalf("flush waited %v", waited)
	}
	close(inner.gate)
	if !q.Flush(5*time.Second) || !strings.Contains(inner.String(), "exiting") {
		t.Fatal("the record was lost once the logger accepted")
	}
}

func TestFlushWithNothingQueuedReturnsAtOnce(t *testing.T) {
	_, q := New(newGatedHandler(), "dropped")
	if !q.Flush(time.Millisecond) {
		t.Fatal("an empty queue did not flush")
	}
}

func TestTheInnerLevelIsHonoured(t *testing.T) {
	inner := newGatedHandler()
	inner.level = slog.LevelError
	close(inner.gate)
	log, q := New(inner, "dropped")
	log.Info("quiet")
	log.Error("loud")
	q.Flush(5 * time.Second)
	if got := inner.String(); got != "loud\n" {
		t.Fatalf("got %q, want only the error", got)
	}
}
