// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package gatewaystate

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

const lockName = ".ori-gateway-state.lock"

// lockDirectory takes an exclusive, non-blocking flock on the state
// directory's lock file. The OS releases it when the holder exits, however it
// exits, so a crashed gateway never blocks the next start.
func lockDirectory(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open gateway state lock: %w", err)
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) {
			return nil, fmt.Errorf("gateway state directory is in use by another gateway")
		}
		return nil, fmt.Errorf("lock gateway state directory: %w", err)
	}
	return f, nil
}
