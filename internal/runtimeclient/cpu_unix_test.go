// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package runtimeclient

import (
	"syscall"
	"testing"
	"time"
)

// processCPU is the CPU time the test process has spent, user and system.
func processCPU(t *testing.T) (time.Duration, bool) {
	t.Helper()
	var usage syscall.Rusage
	if err := syscall.Getrusage(syscall.RUSAGE_SELF, &usage); err != nil {
		t.Fatal(err)
	}
	return time.Duration(usage.Utime.Nano() + usage.Stime.Nano()), true
}
