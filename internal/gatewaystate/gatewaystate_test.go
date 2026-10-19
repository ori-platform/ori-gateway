// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package gatewaystate

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/mqttauth"
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

// TestOpenRefusesAStateDirectoryThatIsNotTheGateways refuses each kind of
// directory that is not the gateway's, each for its own reason, so no case
// passes because an earlier check refused it.
func TestOpenRefusesAStateDirectoryThatIsNotTheGateways(t *testing.T) {
	stateWith := func(t *testing.T, mode os.FileMode, setup func(dir string) error) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "state")
		if err := os.Mkdir(dir, 0o700); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(dir, mode); err != nil {
			t.Fatal(err)
		}
		if setup != nil {
			if err := setup(dir); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	marker := func(body string) func(string) error {
		return func(dir string) error { return os.WriteFile(filepath.Join(dir, markerName), []byte(body), 0o600) }
	}
	cases := map[string]struct {
		setup func(t *testing.T) string
		want  string
	}{
		"relative":       {func(t *testing.T) string { return "state" }, "absolute path"},
		"group readable": {func(t *testing.T) string { return stateWith(t, 0o750, nil) }, "group or other users"},
		"a symlink to a private directory": {func(t *testing.T) string {
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
		}, "not a symlink"},
		"a file": {func(t *testing.T) string {
			path := filepath.Join(t.TempDir(), "state")
			if err := os.WriteFile(path, []byte("x"), 0o600); err != nil {
				t.Fatal(err)
			}
			return path
		}, "not a symlink or file"},
		"someone else's non-empty directory": {func(t *testing.T) string {
			return stateWith(t, 0o700, func(dir string) error { return os.WriteFile(filepath.Join(dir, "other"), []byte("x"), 0o600) })
		}, "not empty"},
		"a staged marker name that is a directory": {func(t *testing.T) string {
			return stateWith(t, 0o700, func(dir string) error { return os.Mkdir(filepath.Join(dir, markerName+".x"), 0o700) })
		}, "not empty"},
		"a corrupt marker":               {func(t *testing.T) string { return stateWith(t, 0o700, marker("not ours")) }, "marker is corrupt"},
		"a marker without its line feed": {func(t *testing.T) string { return stateWith(t, 0o700, marker(strings.TrimSuffix(markerBody, "\n"))) }, "marker is corrupt"},
		"a marker ending CRLF": {func(t *testing.T) string {
			return stateWith(t, 0o700, marker(strings.TrimSuffix(markerBody, "\n")+"\r\n"))
		}, "marker is corrupt"},
		"a marker with a second line feed": {func(t *testing.T) string { return stateWith(t, 0o700, marker(markerBody+"\n")) }, "marker is corrupt"},
		"a marker with a leading space":    {func(t *testing.T) string { return stateWith(t, 0o700, marker(" "+markerBody)) }, "marker is corrupt"},
		"a marker that is a symlink": {func(t *testing.T) string {
			return stateWith(t, 0o700, func(dir string) error {
				target := filepath.Join(filepath.Dir(dir), "marker-elsewhere")
				if err := os.WriteFile(target, []byte(markerBody), 0o600); err != nil {
					return err
				}
				return os.Symlink(target, filepath.Join(dir, markerName))
			})
		}, "not a regular file"},
		"a marker that is a FIFO": {func(t *testing.T) string {
			return stateWith(t, 0o700, func(dir string) error { return makeFIFO(filepath.Join(dir, markerName)) })
		}, "not a regular file"},
	}
	for name, tc := range cases {
		t.Run(name, func(t *testing.T) {
			done := make(chan error, 1)
			dir := tc.setup(t)
			go func() { _, err := Open(dir); done <- err }()
			select {
			case err := <-done:
				if err == nil || !strings.Contains(err.Error(), tc.want) {
					t.Fatalf("Open = %v, want refused for %q", err, tc.want)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("Open never returned")
			}
		})
	}
}

// TestAnInterruptedAdoptionIsAdoptedAgain leaves what an adoption cut short
// may leave, the lock file and staged marker files, and nothing else: the
// directory is still adopted, with a complete marker. A file of any other
// name, a bare marker prefix among them, still refuses it.
func TestAnInterruptedAdoptionIsAdoptedAgain(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	for _, name := range []string{lockName, markerName + ".123456", markerName + ".x"} {
		if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := Open(dir); err != nil {
		t.Fatalf("an interrupted adoption was refused: %v", err)
	}
	body, err := os.ReadFile(filepath.Join(dir, markerName))
	if err != nil || string(body) != markerBody {
		t.Fatalf("marker %q, %v", body, err)
	}
	for _, name := range []string{markerName + ".", ".ori-signed-at-messages.1", "other"} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "state")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, name), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := Open(dir); err == nil {
				t.Fatalf("a directory holding %q was adopted", name)
			}
		})
	}
}

// TestAdoptionWritesTheMarkerThroughAStagedFile adopts an empty directory:
// the marker is private and exact, and no staged file is left beside it.
func TestAdoptionWritesTheMarkerThroughAStagedFile(t *testing.T) {
	dir, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	if len(entries) != 1 || entries[0].Name() != markerName {
		t.Fatalf("after adoption the directory holds %v", entries)
	}
	info, err := os.Stat(filepath.Join(dir, markerName))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("marker mode %v, %v", info.Mode().Perm(), err)
	}
}

// TestARefusedDirectoryGetsNoLockFile refuses a directory that is not the
// gateway's before the lock is taken, so no gateway file is written into it.
func TestARefusedDirectoryGetsNoLockFile(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, "other"), nil, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir); err == nil {
		t.Fatal("a directory that is not the gateway's was acquired")
	}
	if _, err := os.Stat(filepath.Join(dir, lockName)); !os.IsNotExist(err) {
		t.Fatalf("a lock file was written into a refused directory: %v", err)
	}
}

func writeMark(t *testing.T, dir, body string) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, markName), []byte(body), 0o600); err != nil {
		t.Fatal(err)
	}
}

// TestAMarkIsReadByItsGrammar accepts every spelling of a mark the contract
// admits and refuses every other as corrupt, the ceiling included.
func TestAMarkIsReadByItsGrammar(t *testing.T) {
	ceiling := strconv.FormatInt(mqttauth.SigningCeiling, 10)
	below := strconv.FormatInt(mqttauth.SigningCeiling-1, 10)
	valid := map[string]int64{
		"1787000000000\n": 1787000000000,
		"0":               0,
		"+0012":           12,
		" \t0042\r\n":     42,
		"\u00a07\u3000":   7,
		"\u0085+9\u2028":  9,
		below:             mqttauth.SigningCeiling - 1,
	}
	for body, want := range valid {
		t.Run("valid "+body, func(t *testing.T) {
			dir, err := Open(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			writeMark(t, dir, body)
			owner, err := Acquire(dir)
			if err != nil {
				t.Fatalf("a valid mark was refused: %v", err)
			}
			defer owner.Close()
			if got, ok := owner.Mark().Found(); !ok || got != want {
				t.Fatalf("found %d, %v; want %d", got, ok, want)
			}
		})
	}
	for _, body := range []string{"", " ", "not a number", "-0", "-5", "++5", "+", "+ 5", "+\t5", "\u200b5", "\ufeff5", "1.5", "1e3", "0x10", "1_000",
		"1 2", "\x1c5", "5\x00", ceiling, "9223372036854775807", "99999999999999999999"} {
		t.Run("corrupt "+body, func(t *testing.T) {
			dir, err := Open(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			writeMark(t, dir, body)
			if owner, err := Acquire(dir); err == nil {
				_ = owner.Close()
				t.Fatalf("a corrupt mark %q was accepted", body)
			}
		})
	}
}

// TestAMarkThatIsNotARegularFileIsCorrupt refuses a symlink or a directory
// where the mark belongs.
func TestAMarkThatIsNotARegularFileIsCorrupt(t *testing.T) {
	for name, setup := range map[string]func(dir string) error{
		"a symlink": func(dir string) error {
			target := filepath.Join(filepath.Dir(dir), "elsewhere")
			if err := os.WriteFile(target, []byte("5"), 0o600); err != nil {
				return err
			}
			return os.Symlink(target, filepath.Join(dir, markName))
		},
		"a directory": func(dir string) error { return os.Mkdir(filepath.Join(dir, markName), 0o700) },
	} {
		t.Run(name, func(t *testing.T) {
			dir, err := Open(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			if err := setup(dir); err != nil {
				t.Fatal(err)
			}
			if owner, err := Acquire(dir); err == nil {
				_ = owner.Close()
				t.Fatal("a mark that is not a regular file was accepted")
			}
		})
	}
}

// TestPersistWritesExactlyTheMarkAndNeverLowersIt writes marks through
// Persist: each is written exactly, a lower one is not written, the start's
// write of the mark found is not skipped, and one at the ceiling is refused.
func TestPersistWritesExactlyTheMarkAndNeverLowersIt(t *testing.T) {
	dir, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	writeMark(t, dir, " +0100 ")
	owner, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	mark := owner.Mark()
	read := func() string {
		body, err := os.ReadFile(filepath.Join(dir, markName))
		if err != nil {
			t.Fatal(err)
		}
		return string(body)
	}
	if err := mark.Persist(100); err != nil || read() != "100\n" {
		t.Fatalf("the mark found was not written again: %q, %v", read(), err)
	}
	if err := mark.Persist(250); err != nil || read() != "250\n" {
		t.Fatalf("Persist(250) left %q, %v", read(), err)
	}
	if err := mark.Persist(200); err != nil || read() != "250\n" {
		t.Fatalf("a lower mark was written: %q, %v", read(), err)
	}
	if err := mark.Persist(mqttauth.SigningCeiling); err == nil {
		t.Fatal("a mark at the ceiling was accepted")
	}
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	reopened, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer reopened.Close()
	if got, ok := reopened.Mark().Found(); !ok || got != 250 {
		t.Fatalf("reopened at %d, %v; want 250", got, ok)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), markName+".") {
			t.Fatalf("a staged mark file was left behind: %s", entry.Name())
		}
	}
}

// TestAStartThatCannotWriteItsMarkIsRefused makes the directory unwritable
// after it is acquired: the signing clock's first write fails, and the start
// is refused.
func TestAStartThatCannotWriteItsMarkIsRefused(t *testing.T) {
	if os.Geteuid() == 0 {
		t.Skip("root writes through directory permissions")
	}
	owner, err := Acquire(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := os.Chmod(owner.dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(owner.dir, 0o700) })
	if _, err := mqttauth.NewDurableSigningClock(time.Now, owner.Mark(), nil); err == nil {
		t.Fatal("a start whose mark could not be written was accepted")
	}
}

// TestASecondOwnerIsRefusedBeforeItReadsTheMark acquires the state directory
// and then corrupts the mark and tries a second time, as a second gateway
// would: the second is refused as in use, not for the mark, because the mark
// is read only under the lock; it acquires once the first has closed and the
// mark is repaired.
func TestASecondOwnerIsRefusedBeforeItReadsTheMark(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	first, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	writeMark(t, dir, "corrupt")
	if _, err := Acquire(dir); err == nil || !strings.Contains(err.Error(), "in use") {
		t.Fatalf("a second owner = %v, want refused as in use", err)
	}
	if err := first.Close(); err != nil {
		t.Fatal(err)
	}
	writeMark(t, dir, "5")
	second, err := Acquire(dir)
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
		if _, err := Acquire(dir); err != nil {
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
	owner, err := Acquire(dir)
	if err != nil {
		t.Fatalf("after the owning process died: %v", err)
	}
	_ = owner.Close()
}

// TestAnInterruptedWriteLeavesTheOldFileOrTheNew interrupts the marker's and
// the mark's writes after the staged file is written and before it is renamed
// into place: neither is left half-written, and the staged file is removed.
func TestAnInterruptedWriteLeavesTheOldFileOrTheNew(t *testing.T) {
	interrupted := errors.New("power lost")
	commitHook = func(string) error { return interrupted }
	defer func() { commitHook = nil }()
	dir := filepath.Join(t.TempDir(), "state")
	if _, err := Open(dir); !errors.Is(err, interrupted) {
		t.Fatalf("Open = %v, want the interrupted marker write", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil || len(entries) != 0 {
		t.Fatalf("an interrupted adoption left %v (%v)", entries, err)
	}
	commitHook = nil
	owner, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	defer owner.Close()
	if err := owner.Mark().Persist(100); err != nil {
		t.Fatal(err)
	}
	commitHook = func(name string) error {
		if name != markName {
			t.Errorf("a staged write for %q, want the mark", name)
		}
		return interrupted
	}
	if err := owner.Mark().Persist(200); !errors.Is(err, interrupted) {
		t.Fatalf("Persist = %v, want the interrupted write", err)
	}
	body, err := os.ReadFile(filepath.Join(dir, markName))
	if err != nil || string(body) != "100\n" {
		t.Fatalf("after an interrupted write the mark is %q (%v), want the old one", body, err)
	}
	entries, err = os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), markName+".") {
			t.Fatalf("a staged mark file was left behind: %s", entry.Name())
		}
	}
}

// TestADirectoryOwnedByAnotherUserIsRefused runs as if the gateway were
// another user: its own directory is then refused.
func TestADirectoryOwnedByAnotherUserIsRefused(t *testing.T) {
	dir, err := Open(filepath.Join(t.TempDir(), "state"))
	if err != nil {
		t.Fatal(err)
	}
	real := geteuid
	geteuid = func() int { return real() + 1 }
	defer func() { geteuid = real }()
	if _, err := Open(dir); err == nil || !strings.Contains(err.Error(), "owned by the user") {
		t.Fatalf("Open = %v, want a directory owned by another user refused", err)
	}
}

// TestOpenUsesTheCleanPath opens the state directory under an uncleaned
// spelling: the directory it opens and reports is the clean one.
func TestOpenUsesTheCleanPath(t *testing.T) {
	base := t.TempDir()
	got, err := Open(base + "//state/./sub/../")
	if err != nil {
		t.Fatal(err)
	}
	if want := filepath.Join(base, "state"); got != want {
		t.Fatalf("Open opened %q, want %q", got, want)
	}
}

// TestRestoringTheMarkerRecoversAUsedDirectory follows docs/OPERATIONS.md's
// recovery from a corrupt marker on a directory that holds a signing mark:
// deleting the marker leaves the directory refused, and restoring it exactly
// recovers it, with the clock resuming above the retained mark.
func TestRestoringTheMarkerRecoversAUsedDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "state")
	owner, err := Acquire(dir)
	if err != nil {
		t.Fatal(err)
	}
	clock, err := mqttauth.NewDurableSigningClock(time.Now, owner.Mark(), nil)
	if err != nil {
		t.Fatal(err)
	}
	last, err := clock.Next()
	if err != nil {
		t.Fatal(err)
	}
	clock.Close()
	if err := owner.Close(); err != nil {
		t.Fatal(err)
	}
	marker := filepath.Join(dir, markerName)
	if err := os.WriteFile(marker, []byte("damaged"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir); err == nil || !strings.Contains(err.Error(), "marker is corrupt") {
		t.Fatalf("a corrupt marker: %v", err)
	}
	if err := os.Remove(marker); err != nil {
		t.Fatal(err)
	}
	if _, err := Acquire(dir); err == nil || !strings.Contains(err.Error(), "not empty") {
		t.Fatalf("a deleted marker beside a mark: %v, want the directory still refused", err)
	}
	if err := os.WriteFile(marker, []byte("ori-gateway state directory v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	recovered, err := Acquire(dir)
	if err != nil {
		t.Fatalf("after restoring the marker: %v", err)
	}
	defer recovered.Close()
	resumed, err := mqttauth.NewDurableSigningClock(time.Now, recovered.Mark(), nil)
	if err != nil {
		t.Fatal(err)
	}
	defer resumed.Close()
	if at, err := resumed.Next(); err != nil || at <= last {
		t.Fatalf("after recovery the clock signed at %d (%v), not above %d", at, err, last)
	}
}

// TestAMarkIsAtMostSixtyFourBytes reads a valid mark padded to exactly 64
// bytes, and the same mark one byte longer: the first is read, the second is
// corrupt, and a mark far longer is refused without being read whole.
func TestAMarkIsAtMostSixtyFourBytes(t *testing.T) {
	pad := func(n int) string {
		return " +" + strings.Repeat("0", n-len(" +42\n")) + "42\n"
	}
	for _, tc := range []struct {
		name, body string
		valid      bool
	}{
		{"64 bytes", pad(64), true},
		{"65 bytes", pad(65), false},
		{"64 MiB", pad(64 << 20), false},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name != "64 MiB" && len(tc.body) != map[string]int{"64 bytes": 64, "65 bytes": 65}[tc.name] {
				t.Fatalf("built %d bytes", len(tc.body))
			}
			dir, err := Open(filepath.Join(t.TempDir(), "state"))
			if err != nil {
				t.Fatal(err)
			}
			writeMark(t, dir, tc.body)
			owner, err := Acquire(dir)
			if !tc.valid {
				if err == nil {
					_ = owner.Close()
					t.Fatal("a mark longer than 64 bytes was accepted")
				}
				if !strings.Contains(err.Error(), "longer than 64 bytes") {
					t.Fatalf("Acquire = %v, want refused for its length", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("a 64-byte mark was refused: %v", err)
			}
			defer owner.Close()
			if got, ok := owner.Mark().Found(); !ok || got != 42 {
				t.Fatalf("found %d, %v; want 42", got, ok)
			}
		})
	}
}
