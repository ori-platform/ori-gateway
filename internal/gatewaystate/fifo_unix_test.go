// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package gatewaystate

import "syscall"

func makeFIFO(path string) error { return syscall.Mkfifo(path, 0o600) }
