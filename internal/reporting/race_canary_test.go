// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import "testing"

// TestRaceCanary is a deliberate data race. It exists only on a throwaway
// branch, to prove the CI race job fails on a race; it is never merged.
func TestRaceCanary(t *testing.T) {
	n := 0
	done := make(chan struct{})
	go func() {
		n++
		close(done)
	}()
	n++
	<-done
	_ = n
}
