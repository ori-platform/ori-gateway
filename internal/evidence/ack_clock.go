// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"time"
)

// Each signed message family the gateway emits from durable state keeps its
// own high-water mark, named under one prefix the queue scan ignores.
const (
	signingClockPrefix       = ".ori-signed-at-"
	outboundAckSigningName   = signingClockPrefix + "outbound-ack"
	inboundReturnSigningName = signingClockPrefix + "inbound-return"
)

// AckSigningClock issues signed_at_ms for one family of gateway-signed
// messages: the current time, or one millisecond past the last one issued,
// whichever is later. The high-water mark is persisted before a message is
// signed, so a restarted gateway re-emitting the same durable entry under an
// unchanged or regressed clock still signs later than anything it signed
// before, and never recreates an earlier envelope byte for byte.
type AckSigningClock struct {
	path string
	now  func() time.Time

	mu   sync.Mutex
	last int64
}

// OpenAckSigningClock loads the outbound acknowledgement clock from the
// outbound queue directory.
func OpenAckSigningClock(dir string, now func() time.Time) (*AckSigningClock, error) {
	return openSigningClock(dir, outboundAckSigningName, now)
}

// OpenReturnSigningClock loads the inbound delivery clock from the return
// queue directory. A separate mark: the two families are signed under
// different message types and retired by different acknowledgements.
func OpenReturnSigningClock(dir string, now func() time.Time) (*AckSigningClock, error) {
	return openSigningClock(dir, inboundReturnSigningName, now)
}

func openSigningClock(dir, name string, now func() time.Time) (*AckSigningClock, error) {
	if dir == "" {
		return nil, fmt.Errorf("evidence: signing clock requires a directory")
	}
	if now == nil {
		now = time.Now
	}
	path := filepath.Join(dir, name)
	clock := &AckSigningClock{path: path, now: now}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		return clock, nil
	}
	if err != nil {
		return nil, fmt.Errorf("evidence: read acknowledgement clock: %w", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("evidence: acknowledgement clock is not a regular file")
	}
	body, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: read acknowledgement clock: %w", err)
	}
	last, err := strconv.ParseInt(strings.TrimSpace(string(body)), 10, 64)
	if err != nil || last < 0 {
		return nil, fmt.Errorf("evidence: acknowledgement clock is corrupt")
	}
	clock.last = last
	return clock, nil
}

// Next returns the next signing time after persisting it. An acknowledgement
// must not be signed with a time that could be issued again after a restart,
// so persistence failure refuses the time rather than returning it.
func (c *AckSigningClock) Next() (int64, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	at := c.now().UnixMilli()
	if at <= c.last {
		at = c.last + 1
	}
	if err := c.persist(at); err != nil {
		return 0, err
	}
	c.last = at
	return at, nil
}

func (c *AckSigningClock) persist(at int64) error {
	dir := filepath.Dir(c.path)
	tmp, err := os.CreateTemp(dir, filepath.Base(c.path)+".*")
	if err != nil {
		return fmt.Errorf("evidence: stage acknowledgement clock: %w", err)
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
		return fmt.Errorf("evidence: protect acknowledgement clock: %w", err)
	}
	if _, err := tmp.WriteString(strconv.FormatInt(at, 10) + "\n"); err != nil {
		return fmt.Errorf("evidence: write acknowledgement clock: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("evidence: sync acknowledgement clock: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("evidence: close acknowledgement clock: %w", err)
	}
	if err := os.Rename(tmpName, c.path); err != nil {
		return fmt.Errorf("evidence: commit acknowledgement clock: %w", err)
	}
	if err := syncDirectory(dir); err != nil {
		return fmt.Errorf("evidence: persist acknowledgement clock: %w", err)
	}
	committed = true
	return nil
}
