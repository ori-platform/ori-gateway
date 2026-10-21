// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// maxNotes bounds the log records waiting behind a logger that blocks; later
// ones are counted, and the count is logged after the records queued before
// them.
const maxNotes = 64

// noteLog writes the client's log records off every caller's path: each is
// queued in the order it was decided, and one drainer at a time writes them,
// one by one, so a logger that blocks never holds up an export, its bound or
// the release of its pending state, nor the broker callback that delivers
// responses.
type noteLog struct {
	mu       sync.Mutex
	notes    []note
	draining bool
	dropped  int
	// counted is where the count of dropped records is written.
	counted slog.Handler
}

// note is one record and the handler it is written to.
type note struct {
	handler slog.Handler
	record  slog.Record
}

// queue adds a record for handler and starts the drainer if none is running.
func (n *noteLog) queue(handler slog.Handler, record slog.Record) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if n.counted == nil {
		n.counted = handler
	}
	if len(n.notes) >= maxNotes {
		n.dropped++
		return
	}
	n.notes = append(n.notes, note{handler: handler, record: record})
	if !n.draining {
		n.draining = true
		go n.drain()
	}
}

// drain writes the queued records in order, each with the time it was
// decided, then the count of any dropped, until none is left.
func (n *noteLog) drain() {
	ctx := context.Background()
	for {
		n.mu.Lock()
		if len(n.notes) > 0 {
			next := n.notes[0]
			n.notes = n.notes[1:]
			n.mu.Unlock()
			_ = next.handler.Handle(ctx, next.record)
			continue
		}
		dropped := n.dropped
		n.dropped = 0
		if dropped == 0 {
			n.draining = false
			n.mu.Unlock()
			return
		}
		counted := n.counted
		n.mu.Unlock()
		record := slog.NewRecord(time.Now(), slog.LevelWarn, "runtime export log lines dropped behind a logger that blocked", 0)
		record.AddAttrs(slog.Int("dropped", dropped))
		if counted.Enabled(ctx, record.Level) {
			_ = counted.Handle(ctx, record)
		}
	}
}

// queuedHandler is an slog.Handler that queues every record it is given for
// inner on notes, so logging through it never waits on inner.
type queuedHandler struct {
	inner slog.Handler
	notes *noteLog
}

func (h *queuedHandler) Enabled(ctx context.Context, level slog.Level) bool {
	return h.inner.Enabled(ctx, level)
}

func (h *queuedHandler) Handle(_ context.Context, record slog.Record) error {
	h.notes.queue(h.inner, record.Clone())
	return nil
}

func (h *queuedHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &queuedHandler{inner: h.inner.WithAttrs(attrs), notes: h.notes}
}

func (h *queuedHandler) WithGroup(name string) slog.Handler {
	return &queuedHandler{inner: h.inner.WithGroup(name), notes: h.notes}
}
