// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build !unix

package runtimeclient

import (
	"testing"
	"time"
)

// processCPU reports no reading where the process's CPU time is not read.
func processCPU(*testing.T) (time.Duration, bool) { return 0, false }
