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

// lockDirectory takes an exclusive, non-blocking flock on the state
// directory's lock file. The OS releases it when the holder exits, however it
// exits, so a crashed gateway never blocks the next start.
// noFollow refuses to open a symbolic link.
const noFollow = syscall.O_NOFOLLOW

func lockDirectory(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, lockName), os.O_RDWR|os.O_CREATE|noFollow, 0o600)
	if err != nil {
		return nil, fmt.Errorf("open gateway state lock: %w", err)
	}
	if info, err := f.Stat(); err != nil || !info.Mode().IsRegular() {
		_ = f.Close()
		return nil, fmt.Errorf("gateway state lock is not a regular file")
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
