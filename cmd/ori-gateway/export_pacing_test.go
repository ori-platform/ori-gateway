// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

// instantExportPacing gives a runtime export client a pacing clock that a wait
// advances at once, for gateway tests that are not about pacing.
func instantExportPacing() runtimeclient.MQTTClientOption {
	var mu sync.Mutex
	at := time.Unix(1_787_000_000, 0)
	return runtimeclient.WithExportPacingClock(
		func() time.Time { mu.Lock(); defer mu.Unlock(); return at },
		func(_ context.Context, d time.Duration) error { mu.Lock(); at = at.Add(d); mu.Unlock(); return nil },
	)
}
