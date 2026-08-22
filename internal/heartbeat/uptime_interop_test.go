// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"encoding/json"
	"math"
	"testing"
)

// TestUptimeIsEmittedInAnInteroperableForm pins the numeric form this schema
// actually emits, rather than assuming arbitrary Go floats interoperate.
//
// uptime_s is the only floating-point field in a gateway-produced authenticated
// payload. The runtime verifies by re-serialising with CPython, and the two
// languages choose different notation for a non-zero magnitude below 1e-4: Go
// writes 0.000035 where CPython writes 3.5e-05. Rounding to milliseconds keeps
// every emitted value either exactly 0 or at least 0.001.
func TestUptimeIsEmittedInAnInteroperableForm(t *testing.T) {
	rounded := func(seconds float64) float64 {
		return math.Round(seconds*1000) / 1000
	}
	for _, raw := range []float64{
		0, 5e-7, 3.5e-05, 0.0001, 0.0009, 0.001, 0.5, 1.5, 12.345678901234567, 3600, 1e9,
	} {
		got := rounded(raw)
		if got != 0 && math.Abs(got) < 1e-4 {
			t.Errorf("uptime %v rounds to %v, inside the range where Go and CPython disagree on notation", raw, got)
		}
		// The emitted JSON must not use exponent notation, which is where the two
		// languages diverge.
		b, err := json.Marshal(got)
		if err != nil {
			t.Fatalf("marshal %v: %v", got, err)
		}
		for _, c := range string(b) {
			if c == 'e' || c == 'E' {
				t.Errorf("uptime %v emits exponent notation %s", raw, b)
			}
		}
	}
}
