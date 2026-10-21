// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"log/slog"
	"sync"
	"time"
)

// maxNotes bounds the log records queued behind a logger that blocks; later
// ones are counted, and the count is logged after the records queued before
// them.
const maxNotes = 64

// noteLog writes the client's log records off every caller's path: each is
// stamped and queued in the order it was decided, and one drainer at a time
// writes them, so a logger that blocks never holds up an export, its bound or
// the release of its pending state, nor the broker callback that delivers
// responses.
type noteLog struct {
	mu       sync.Mutex
	notes    []slog.Record
	draining bool
	dropped  int
}

// warn queues a warning for log and starts the drainer if none is running.
func (n *noteLog) warn(log *slog.Logger, msg string, args ...any) {
	n.mu.Lock()
	defer n.mu.Unlock()
	if len(n.notes) >= maxNotes {
		n.dropped++
		return
	}
	record := slog.NewRecord(time.Now(), slog.LevelWarn, msg, 0)
	record.Add(args...)
	n.notes = append(n.notes, record)
	if !n.draining {
		n.draining = true
		go n.drain(log)
	}
}

// drain writes the queued records in order, each with the time it was
// decided, until none is left.
func (n *noteLog) drain(log *slog.Logger) {
	ctx := context.Background()
	handler := log.Handler()
	for {
		n.mu.Lock()
		notes, dropped := n.notes, n.dropped
		n.notes, n.dropped = nil, 0
		if len(notes) == 0 && dropped == 0 {
			n.draining = false
			n.mu.Unlock()
			return
		}
		n.mu.Unlock()
		for _, record := range notes {
			if handler.Enabled(ctx, record.Level) {
				_ = handler.Handle(ctx, record)
			}
		}
		if dropped > 0 {
			log.Warn("runtime export log lines dropped behind a logger that blocked", "dropped", dropped)
		}
	}
}
