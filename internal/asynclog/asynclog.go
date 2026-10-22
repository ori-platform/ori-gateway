// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package asynclog writes log records off every caller's path, so a log
// destination that stops accepting writes never holds up the code that logs:
// a heartbeat loop, a broker callback, or an escalation on its way to ending
// the process (GW-6).
package asynclog

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// MaxQueued bounds the records waiting behind a logger that blocks; later
// ones are counted, and the count is logged after the records queued before
// them.
const MaxQueued = 64

// ExitFlushBound is how long a path that is about to end the process waits
// for its queued records to be written: long enough for a working logger,
// short enough that a blocked one cannot keep the process alive.
const ExitFlushBound = time.Second

// Queue holds the records for one logger and writes them, in the order they
// were logged, from one drainer at a time.
type Queue struct {
	mu       sync.Mutex
	records  []queued
	draining bool
	dropped  int
	// counted is where the count of dropped records is written.
	counted slog.Handler
	// droppedMessage names whose records were dropped.
	droppedMessage string
	// idle is closed and replaced each time the drainer finds nothing left.
	idle chan struct{}
}

// queued is one record and the handler it is written to.
type queued struct {
	handler slog.Handler
	record  slog.Record
}

// New returns a logger whose records are queued for inner, and the queue that
// writes them. droppedMessage is the line logged with the count of records
// dropped behind a logger that blocked.
func New(inner slog.Handler, droppedMessage string) (*slog.Logger, *Queue) {
	q := &Queue{counted: inner, droppedMessage: droppedMessage, idle: make(chan struct{})}
	return slog.New(&handler{inner: inner, queue: q}), q
}

// Flush waits until every record queued so far has been written, or bound
// has passed, and reports whether they were all written.
func (q *Queue) Flush(bound time.Duration) bool {
	timer := time.NewTimer(bound)
	defer timer.Stop()
	for {
		q.mu.Lock()
		if !q.draining {
			q.mu.Unlock()
			return true
		}
		idle := q.idle
		q.mu.Unlock()
		select {
		case <-idle:
		case <-timer.C:
			return false
		}
	}
}

func (q *Queue) add(h slog.Handler, record slog.Record) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.records) >= MaxQueued {
		q.dropped++
		return
	}
	q.records = append(q.records, queued{handler: h, record: record})
	if !q.draining {
		q.draining = true
		go q.drain()
	}
}

// drain writes the queued records in order, each with the time it was logged,
// then the count of any dropped, until none is left. It asks each handler's
// level itself, so no caller waits on that either: a handler may hold one lock
// for both.
func (q *Queue) drain() {
	ctx := context.Background()
	for {
		q.mu.Lock()
		if len(q.records) > 0 {
			next := q.records[0]
			q.records = q.records[1:]
			q.mu.Unlock()
			if next.handler.Enabled(ctx, next.record.Level) {
				_ = next.handler.Handle(ctx, next.record)
			}
			continue
		}
		dropped := q.dropped
		q.dropped = 0
		if dropped == 0 {
			q.draining = false
			close(q.idle)
			q.idle = make(chan struct{})
			q.mu.Unlock()
			return
		}
		counted := q.counted
		q.mu.Unlock()
		record := slog.NewRecord(time.Now(), slog.LevelWarn, q.droppedMessage, 0)
		record.AddAttrs(slog.Int("dropped", dropped))
		if counted.Enabled(ctx, record.Level) {
			_ = counted.Handle(ctx, record)
		}
	}
}

// handler queues every record it is given for inner, so logging through it
// never waits on inner.
type handler struct {
	inner slog.Handler
	queue *Queue
}

func (h *handler) Enabled(context.Context, slog.Level) bool { return true }

func (h *handler) Handle(_ context.Context, record slog.Record) error {
	h.queue.add(h.inner, record.Clone())
	return nil
}

func (h *handler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &handler{inner: h.inner.WithAttrs(attrs), queue: h.queue}
}

func (h *handler) WithGroup(name string) slog.Handler {
	return &handler{inner: h.inner.WithGroup(name), queue: h.queue}
}
