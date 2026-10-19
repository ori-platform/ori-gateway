// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

//go:build unix

package gatewaystate

import (
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
)

// TestEveryParentItCreatesIsPrivate opens a state directory under parents
// that do not exist with a umask that masks nothing: the directory and every
// parent the gateway creates grant no more than 0700.
func TestEveryParentItCreatesIsPrivate(t *testing.T) {
	old := syscall.Umask(0)
	defer syscall.Umask(old)
	base := t.TempDir()
	dir := filepath.Join(base, "a", "b", "state")
	if _, err := Open(dir); err != nil {
		t.Fatal(err)
	}
	for _, path := range []string{filepath.Join(base, "a"), filepath.Join(base, "a", "b"), dir} {
		info, err := os.Stat(path)
		if err != nil {
			t.Fatal(err)
		}
		if info.Mode().Perm()&^0o700 != 0 {
			t.Errorf("%s has mode %v, more than 0700", path, info.Mode().Perm())
		}
	}
}

// TestALockThatIsNotARegularFileRefusesTheStart puts a symlink, then a FIFO,
// where the lock file belongs: the start is refused, nothing is created
// through the link, and the FIFO never stalls it.
func TestALockThatIsNotARegularFileRefusesTheStart(t *testing.T) {
	for name, tc := range map[string]struct {
		setup func(dir, outside string) error
		want  string
	}{
		"a symlink": {func(dir, outside string) error { return os.Symlink(outside, filepath.Join(dir, lockName)) }, "open gateway state lock"},
		"a FIFO":    {func(dir, _ string) error { return makeFIFO(filepath.Join(dir, lockName)) }, "not a regular file"},
	} {
		t.Run(name, func(t *testing.T) {
			dir, err := Open(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			outside := filepath.Join(t.TempDir(), "created-through-the-link")
			if err := tc.setup(dir, outside); err != nil {
				t.Fatal(err)
			}
			owner, err := Acquire(dir)
			if err == nil {
				_ = owner.Close()
				t.Fatal("a lock that is not a regular file was taken")
			}
			if !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("Acquire = %v, want refused for %q", err, tc.want)
			}
			if _, err := os.Lstat(outside); !os.IsNotExist(err) {
				t.Fatalf("the lock was created through the link: %v", err)
			}
		})
	}
}
