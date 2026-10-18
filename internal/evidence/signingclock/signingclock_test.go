// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package signingclock

import (
	"testing"
	"time"
)

// TestNextAtLeastPersistsAMarkARestartResumesAbove issues a value no lower
// than the least asked for, persists it, and reopens the clock under a clock
// set back: Last is the persisted mark, and the next value is above it.
func TestNextAtLeastPersistsAMarkARestartResumesAbove(t *testing.T) {
	dir := t.TempDir()
	wall := time.UnixMilli(1_787_000_000_000)
	now := func() time.Time { return wall }
	clock, err := OpenMessages(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	least := wall.UnixMilli() + 1000
	at, err := clock.NextAtLeast(least)
	if err != nil || at < least || clock.Last() != at {
		t.Fatalf("NextAtLeast(%d) = %d, %v; Last %d", least, at, err, clock.Last())
	}
	wall = wall.Add(-time.Hour)
	reopened, err := OpenMessages(dir, now)
	if err != nil {
		t.Fatal(err)
	}
	if reopened.Last() != at {
		t.Fatalf("reopened mark %d, want %d", reopened.Last(), at)
	}
	if next, err := reopened.Next(); err != nil || next <= at {
		t.Fatalf("after reopening, Next = %d, %v; want above %d", next, err, at)
	}
}
