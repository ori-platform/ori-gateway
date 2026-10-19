// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package gatewaystate owns the gateway's own durable state directory,
// gateway.state_directory (gateway-config/v2). It holds the mark of the clock
// the gateway's message envelopes are signed from, independent of evidence
// forwarding. It confers no authority, and any further state kept here needs
// its own contract and reason.
package gatewaystate

import (
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"

	"github.com/ori-platform/ori-gateway/internal/evidence/durable"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

const (
	markerName = ".ori-gateway-state-v1"
	markerBody = "ori-gateway state directory v1\n"
	lockName   = ".ori-gateway-state.lock"
	markName   = ".ori-signed-at-messages"
)

// Open prepares the state directory at the clean form of dir: it is created
// private (0700) when it does not exist, under parents granting no more than
// 0700; otherwise it must be a directory, not a symlink, owned by this
// process's user, with no group or other access. It must carry the state
// marker, or be empty but for the lock and staged marker files, as `install
// -d` or an interrupted adoption leaves it, and is then adopted by writing the
// marker. Anything else is refused, so the gateway never adopts a directory
// that is not its own.
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
	if owner, ok := fileOwner(info); ok && owner != geteuid() {
		return "", fmt.Errorf("gateway state directory must be owned by the user the gateway runs as")
	}
	if err := ensureMarker(dir); err != nil {
		return "", err
	}
	return dir, nil
}

func ensureMarker(dir string) error {
	path := filepath.Join(dir, markerName)
	info, err := os.Lstat(path)
	switch {
	case err == nil:
		// Only a regular file is read, and no more of it than a marker
		// holds, so a link, a FIFO or a device never stalls the start.
		if !info.Mode().IsRegular() {
			return fmt.Errorf("gateway state directory marker is corrupt: not a regular file")
		}
		body, err := readAtMost(path, len(markerBody)+1)
		if err != nil {
			return fmt.Errorf("read gateway state directory marker: %w", err)
		}
		if body != markerBody {
			return fmt.Errorf("gateway state directory marker is corrupt")
		}
		return nil
	case !errors.Is(err, os.ErrNotExist):
		return fmt.Errorf("read gateway state directory marker: %w", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		return fmt.Errorf("read gateway state directory: %w", err)
	}
	for _, entry := range entries {
		if !entry.Type().IsRegular() || !adoptionLeftover(entry.Name()) {
			return fmt.Errorf("gateway state directory is not empty and is not a gateway state directory")
		}
	}
	if err := writeAtomically(dir, markerName, markerBody); err != nil {
		return fmt.Errorf("write gateway state directory marker: %w", err)
	}
	return nil
}

// readAtMost reads up to limit bytes of the file at path, refusing to follow
// a link.
func readAtMost(path string, limit int) (string, error) {
	f, err := os.OpenFile(path, os.O_RDONLY|noFollow, 0)
	if err != nil {
		return "", err
	}
	defer f.Close()
	body, err := io.ReadAll(io.LimitReader(f, int64(limit)))
	return string(body), err
}

// geteuid is the user the gateway runs as; a test stands in another.
var geteuid = os.Geteuid

// commitHook, when set by a test, runs before a staged file is renamed into
// place; an error stands for a write interrupted there.
var commitHook func(name string) error

// adoptionLeftover reports whether name is one an interrupted adoption may
// leave behind, as a regular file: the lock file, or a staged marker file.
func adoptionLeftover(name string) bool {
	return name == lockName || (strings.HasPrefix(name, markerName+".") && len(name) > len(markerName)+1)
}

// writeAtomically replaces dir/name with body through a staged file in dir,
// so an interrupted write leaves the old file or the new one, never part of
// one, and the rename is durable before it returns.
func writeAtomically(dir, name, body string) error {
	tmp, err := os.CreateTemp(dir, name+".*")
	if err != nil {
		return fmt.Errorf("stage: %w", err)
	}
	tmpName := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(0o600); err != nil {
		return fmt.Errorf("protect: %w", err)
	}
	if _, err := tmp.WriteString(body); err != nil {
		return fmt.Errorf("write: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("sync: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("close: %w", err)
	}
	if commitHook != nil {
		if err := commitHook(name); err != nil {
			return err
		}
	}
	if err := os.Rename(tmpName, filepath.Join(dir, name)); err != nil {
		return fmt.Errorf("commit: %w", err)
	}
	committed = true
	if err := durable.SyncDirectory(dir); err != nil {
		return fmt.Errorf("sync directory: %w", err)
	}
	return nil
}

// Owner is the gateway's exclusive hold on its state directory for the life
// of a run. It holds an OS lock on the directory, released by the OS if the
// process dies, so two gateways never share one signing clock; the mark it
// reads is the run's only one. Close releases the lock; it is called once
// every signer has stopped.
type Owner struct {
	dir  string
	lock *os.File
	mark *Mark
}

// Acquire opens the state directory, then takes its exclusive lock, then reads
// the signing clock's mark: the marker is in place before the lock file is
// created, and the mark is read only under the lock. A corrupt or unreadable
// mark refuses the start.
func Acquire(dir string) (*Owner, error) {
	dir, err := Open(dir)
	if err != nil {
		return nil, err
	}
	lock, err := lockDirectory(dir)
	if err != nil {
		return nil, err
	}
	owner := &Owner{dir: dir, lock: lock}
	mark, err := readMark(dir)
	if err != nil {
		_ = owner.Close()
		return nil, err
	}
	owner.mark = mark
	return owner, nil
}

// Mark is the run's durable signing-clock mark.
func (o *Owner) Mark() *Mark { return o.mark }

// Close releases the exclusive lock. The caller has stopped every signer.
func (o *Owner) Close() error {
	if o == nil || o.lock == nil {
		return nil
	}
	err := o.lock.Close()
	o.lock = nil
	return err
}

// Mark is the persisted mark of the gateway's message signing clock, the file
// .ori-signed-at-messages. It satisfies mqttauth.DurableMark.
type Mark struct {
	dir   string
	found int64
	has   bool

	mu sync.Mutex
	// persisted is the highest mark this run wrote, -1 before its first.
	persisted int64
}

func readMark(dir string) (*Mark, error) {
	m := &Mark{dir: dir, persisted: -1}
	path := filepath.Join(dir, markName)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return m, nil
	}
	if err != nil {
		return nil, fmt.Errorf("read gateway signing clock mark: %w", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("gateway signing clock mark is corrupt: not a regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read gateway signing clock mark: %w", err)
	}
	value, ok := parseMark(string(body))
	if !ok {
		return nil, fmt.Errorf("gateway signing clock mark is corrupt")
	}
	// Nothing counts as persisted by this run until it writes, so the start's
	// first write is never skipped: it proves the directory writable.
	m.found, m.has = value, true
	return m, nil
}

// parseMark reads a mark: a non-negative decimal number of milliseconds,
// possibly with leading zeros, a leading '+' and surrounding Unicode
// White_Space, below mqttauth.SigningCeiling.
func parseMark(body string) (int64, bool) {
	digits := strings.TrimPrefix(strings.TrimSpace(body), "+")
	if digits == "" || strings.Trim(digits, "0123456789") != "" {
		return 0, false
	}
	value, err := strconv.ParseInt(digits, 10, 64)
	if err != nil || value >= mqttauth.SigningCeiling {
		return 0, false
	}
	return value, true
}

// Found is the mark read at the start, if there was one.
func (m *Mark) Found() (int64, bool) { return m.found, m.has }

// Persist durably writes mark, unless a mark at or above it is already
// persisted, so the persisted mark never falls. A mark at or above the
// ceiling is refused.
func (m *Mark) Persist(mark int64) error {
	if mark < 0 || mark >= mqttauth.SigningCeiling {
		return fmt.Errorf("gateway signing clock mark %d is outside its range", mark)
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if mark <= m.persisted {
		return nil
	}
	if err := writeAtomically(m.dir, markName, strconv.FormatInt(mark, 10)+"\n"); err != nil {
		return fmt.Errorf("persist gateway signing clock mark: %w", err)
	}
	m.persisted = mark
	return nil
}
