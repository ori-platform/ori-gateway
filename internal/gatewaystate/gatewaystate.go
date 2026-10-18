// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package gatewaystate owns the gateway's own durable state directory,
// gateway.state_directory. It holds the gateway's authentication state, today
// the mark of the signing clock its envelopes are signed from, independent of
// evidence forwarding. It confers no physical authority, and any further
// state kept here needs its own contract and reason.
package gatewaystate

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"time"

	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
)

const (
	markerName = ".ori-gateway-state-v1"
	markerBody = "ori-gateway state directory v1\n"
)

// Open prepares the state directory: it is created private (0700) when it
// does not exist; otherwise it must be a directory, not a symlink, owned by
// this process's user, with no group or other access, and either carry the
// state marker or be empty, as `install -d` leaves it. Anything else is
// refused, so the gateway never adopts a directory that is not its own.
func Open(dir string) (string, error) {
	dir = filepath.Clean(strings.TrimSpace(dir))
	if dir == "." || dir == "" || !filepath.IsAbs(dir) {
		return "", fmt.Errorf("gateway state directory must be an absolute path")
	}
	info, err := os.Lstat(dir)
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
			return "", fmt.Errorf("create gateway state parent directory: %w", err)
		}
		if err := os.Mkdir(dir, 0o700); err != nil && !errors.Is(err, os.ErrExist) {
			return "", fmt.Errorf("create gateway state directory: %w", err)
		}
		if err := os.Chmod(dir, 0o700); err != nil {
			return "", fmt.Errorf("protect gateway state directory: %w", err)
		}
		info, err = os.Lstat(dir)
		if err != nil {
			return "", fmt.Errorf("inspect gateway state directory: %w", err)
		}
	case err != nil:
		return "", fmt.Errorf("inspect gateway state directory: %w", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return "", fmt.Errorf("gateway state directory must be a directory, not a symlink or file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return "", fmt.Errorf("gateway state directory must not be accessible by group or other users")
	}
	if owner, ok := fileOwner(info); ok && owner != os.Geteuid() {
		return "", fmt.Errorf("gateway state directory must be owned by the user the gateway runs as")
	}
	if err := ensureMarker(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func ensureMarker(dir string) error {
	path := filepath.Join(dir, markerName)
	if body, err := os.ReadFile(path); err == nil {
		if string(body) != markerBody {
			return fmt.Errorf("gateway state directory marker is corrupt")
		}
		return nil
	} else if !errors.Is(err, os.ErrNotExist) {
		return fmt.Errorf("read gateway state directory marker: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read gateway state directory: %w", err)
	}
	if len(entries) != 0 {
		return fmt.Errorf("gateway state directory is not empty and is not a gateway state directory")
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err != nil {
		return fmt.Errorf("write gateway state directory marker: %w", err)
	}
	if _, err := f.WriteString(markerBody); err != nil {
		_ = f.Close()
		_ = os.Remove(path)
		return fmt.Errorf("write gateway state directory marker: %w", err)
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return fmt.Errorf("write gateway state directory marker: %w", err)
	}
	return f.Close()
}

// OpenSigningClock opens the signing clock's mark in the state directory and
// persists it once before any envelope is signed. That write proves the
// directory writable, and carries forward a mark an earlier release kept in
// the outbound evidence queue directory (legacyDir, empty when there is
// none), so upgrading never resets the clock. A corrupt or unreadable mark,
// in either place, or a directory that cannot be written refuses the start.
// Once persisted, the legacy mark is removed: the state directory is its one
// owner.
func OpenSigningClock(dir, legacyDir string, now func() time.Time) (*signingclock.Clock, error) {
	clock, err := signingclock.OpenMessages(dir, now)
	if err != nil {
		return nil, fmt.Errorf("open gateway signing clock: %w", err)
	}
	least := clock.Last() + 1
	var legacyPath string
	if legacyDir != "" {
		legacy, err := signingclock.OpenMessages(legacyDir, now)
		if err != nil {
			return nil, fmt.Errorf("open legacy gateway signing clock: %w", err)
		}
		if legacy.Last() > 0 {
			least = max(least, legacy.Last()+1)
			legacyPath = signingclock.MessagesPath(legacyDir)
		}
	}
	if _, err := clock.NextAtLeast(least); err != nil {
		return nil, fmt.Errorf("gateway state directory is not writable: %w", err)
	}
	if legacyPath != "" {
		if err := os.Remove(legacyPath); err != nil && !errors.Is(err, os.ErrNotExist) {
			return nil, fmt.Errorf("remove legacy gateway signing clock: %w", err)
		}
	}
	return clock, nil
}
