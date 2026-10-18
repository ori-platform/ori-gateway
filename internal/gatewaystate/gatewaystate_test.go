// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package gatewaystate

import (
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
)

func TestOpenCreatesAPrivateStateDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "var", "lib", "ori-gateway", "state")
	got, err := Open(dir)
	if err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(got)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o700 {
		t.Fatalf("mode %v, want 0700", info.Mode().Perm())
	}
	if _, err := os.Stat(filepath.Join(got, markerName)); err != nil {
		t.Fatalf("no state marker: %v", err)
	}
	if _, err := Open(dir); err != nil {
		t.Fatalf("reopening its own directory: %v", err)
	}
}

func TestOpenAdoptsAnEmptyProvisionedDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := Open(dir); err != nil {
		t.Fatalf("an empty directory provisioned 0700 was refused: %v", err)
	}
}

func TestOpenRefusesAStateDirectoryThatIsNotTheGateways(t *testing.T) {
	cases := map[string]func(t *testing.T) string{
		"relative": func(t *testing.T) string { return "state" },
		"group readable": func(t *testing.T) string {
			dir := filepath.Join(t.TempDir(), "state")
			if err := os.Mkdir(dir, 0o750); err != nil {
				t.Fatal(err)
			}
			if err := os.Chmod(dir, 0o750); err != nil {
				t.Fatal(err)
			}
			return dir
		},
		"a symlink": func(t *testing.T) string {
			base := t.TempDir()
			target := filepath.Join(base, "real")
			if err := os.Mkdir(target, 0o700); err != nil {
				t.Fatal(err)
			}
			link := filepath.Join(base, "state")
			if err := os.Symlink(target, link); err != nil {
				t.Fatal(err)
			}
			return link
		},
		"a file": func(t *testing.T) string {
			path := filepath.Join(t.TempDir(), "state")
			if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		},
		"someone else's non-empty directory": func(t *testing.T) string {
			dir := filepath.Join(t.TempDir(), "state")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "other"), []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			return dir
		},
		"a corrupt marker": func(t *testing.T) string {
			dir := filepath.Join(t.TempDir(), "state")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, markerName), []byte("not ours"), 0o600); err != nil {
				t.Fatal(err)
			}
			return dir
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			if _, err := Open(setup(t)); err == nil {
				t.Fatal("the gateway adopted a state directory that is not its own")
			}
		})
	}
}

func TestOpenSigningClockRefusesACorruptMark(t *testing.T) {
	dir, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(signingclock.MessagesPath(dir), []byte("not a number"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir, "", time.Now); err == nil {
		t.Fatal("a corrupt signing clock mark was accepted")
	}
}

func TestOpenSigningClockRefusesAnUnwritableDirectory(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through directory permissions")
	}
	dir, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := Acquire(dir, "", time.Now); err == nil {
		t.Fatalf("Acquire = %v, want the unwritable directory refused", err)
	}
}

// TestUpgradingCarriesTheEvidenceDirectoryMarkForward stores a mark where an
// earlier build kept it, in the outbound evidence queue directory, ahead of
// the clock; the state directory's clock resumes above it, and the legacy
// mark is removed, so the state directory is its one owner.
func TestUpgradingCarriesTheEvidenceDirectoryMarkForward(t *testing.T) {
	base := t.TempDir()
	legacy := filepath.Join(base, "evidence-outbound")
	if err := os.Mkdir(legacy, 0o700); err != nil {
		t.Fatal(err)
	}
	wall := time.UnixMilli(1_787_000_000_000)
	now := func() time.Time { return wall }
	old, err := signingclock.OpenMessages(legacy, now)
	if err != nil {
		t.Fatal(err)
	}
	legacyMark, err := old.NextAtLeast(wall.Add(time.Hour).UnixMilli())
	if err != nil {
		t.Fatal(err)
	}
	dir := filepath.Join(base, "state")
	owner, err := Acquire(dir, legacy, now)
	if err != nil {
		t.Fatal(err)
	}
	first := owner.SigningClock().Last()
	if first <= legacyMark {
		t.Fatalf("state mark %d, not above the legacy mark %d", first, legacyMark)
	}
	if _, err := os.Stat(signingclock.MessagesPath(legacy)); !os.IsNotExist(err) {
		t.Fatalf("the legacy mark was left behind: %v", err)
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Acquire(dir, legacy, now)
	if err != nil || reopened.SigningClock().Last() <= first {
		t.Fatalf("reopened (%v), want a mark above %d", err, first)
	}
	_ = reopened.Close()
}

// TestASecondOwnerIsRefused acquires the state directory and then tries a
// second time, as a second gateway would: the second is refused before it
// reads or migrates any mark, and acquires once the first has closed.
func TestASecondOwnerIsRefused(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "state")
	legacy := filepath.Join(base, "evidence-outbound")
	if err := os.Mkdir(legacy, 0o700); err != nil {
		t.Fatal(err)
	}
	first, err := Acquire(dir, "", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(signingclock.MessagesPath(legacy), []byte("1"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir, legacy, time.Now); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("a second owner = %v, want refused as in use", err)
	}
	if _, err := os.Stat(signingclock.MessagesPath(legacy)); err != nil {
		t.Fatal("the refused owner migrated the legacy mark")
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	second, err := Acquire(dir, "", time.Now)
	if err != nil {
		t.Fatalf("after the first owner closed: %v", err)
	}
	_ = second.Close()
}

// TestADeadOwnerNeverBlocksTheNextStart acquires the state directory in a
// child process that exits without closing, as a crashed gateway would: the
// OS releases its hold, and the next start acquires the directory.
func TestADeadOwnerNeverBlocksTheNextStart(t *testing.T) {
	if dir := os.Getenv("ORI_GATEWAYSTATE_CHILD_DIR"); dir != "" {
		if _, err := Acquire(dir, "", time.Now); err != nil {
			os.Exit(2)
		}
		os.Exit(0) // exits holding the lock, never closing it
	}
	dir := filepath.Join(t.TempDir(), "state")
	child := exec.Command(os.Args[0], "-test.run=^TestADeadOwnerNeverBlocksTheNextStart$")
	child.Env = append(os.Environ(), "ORI_GATEWAYSTATE_CHILD_DIR="+dir)
	if out, err := child.CombinedOutput(); err != nil {
		t.Fatalf("child owner: %v\n%s", err, out)
	}
	owner, err := Acquire(dir, "", time.Now)
	if err != nil {
		t.Fatalf("after the owning process died: %v", err)
	}
	_ = owner.Close()
}
