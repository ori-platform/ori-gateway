// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
)

const (
	holdRecordVersion = 1
	// holdFilePrefix names the refused-head record that sits beside a queue
	// record: holdFilePrefix + queue record id.
	holdFilePrefix = ".ori-evidence-hold-"
)

// HoldRecord is the durable diagnosis of a queue record the evidence authority
// refused terminally. It never changes the queued artifact; it records why the
// courier holds it, so a restart restores the hold instead of sending the
// bytes again.
type HoldRecord struct {
	V              int          `json:"v"`
	QueueRecord    string       `json:"queue_record"`
	ArtifactDigest string       `json:"artifact_digest"`
	ArtifactType   ArtifactType `json:"artifact_type"`
	// RefusalStatus is the HTTP status of the refusal, or 0 when the channel
	// reported none.
	RefusalStatus int `json:"refusal_status"`
	// Reason is one the refusal's status admits, or "unrecognised".
	Reason        string `json:"reason"`
	FirstHeldAtMS int64  `json:"first_held_at_ms"`
	// Acknowledged is false until an operator acknowledgement exists. No
	// surface can set it today.
	Acknowledged bool `json:"acknowledged"`
}

// Hold durably records that the queue record id is held. The record must be
// queued. The write is atomic: a crash leaves either no hold or a complete one.
func (q *DurableQueue) Hold(record HoldRecord) error {
	if q == nil {
		return fmt.Errorf("evidence: nil durable queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	queued, ok := q.entries[record.QueueRecord]
	if !ok {
		return ErrArtifactNotFound
	}
	record.V = holdRecordVersion
	if err := validateHoldRecord(record, queued); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("evidence: encode refused-head record: %w", err)
	}
	if err := q.writeFileAtomic(holdFilePrefix+record.QueueRecord, encoded); err != nil {
		return err
	}
	q.holds[record.QueueRecord] = record
	return nil
}

// HoldFor returns the durable hold on a queue record, if any.
func (q *DurableQueue) HoldFor(id string) (HoldRecord, bool) {
	if q == nil {
		return HoldRecord{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	record, ok := q.holds[id]
	return record, ok
}

// removeHoldLocked deletes a hold before its queue record is retired. Removing
// the hold first means a crash in between leaves a record without a hold, which
// is sent once more, never a hold without its record, which refuses startup.
func (q *DurableQueue) removeHoldLocked(id string) error {
	if _, ok := q.holds[id]; !ok {
		return nil
	}
	if err := os.Remove(filepath.Join(q.dir, holdFilePrefix+id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("evidence: remove refused-head record: %w", err)
	}
	if err := syncDirectory(q.dir); err != nil {
		return fmt.Errorf("evidence: persist refused-head removal: %w", err)
	}
	delete(q.holds, id)
	return nil
}

// loadHolds validates every refused-head record against the queue record it
// names. A hold that cannot be read, does not parse, names no queued record, or
// disagrees with that record's bytes refuses the queue: dropping it would send
// bytes already known to be refused, and trusting it would hold a record for a
// reason that is not its own.
func (q *DurableQueue) loadHolds(names []string) error {
	for _, name := range names {
		id := strings.TrimPrefix(name, holdFilePrefix)
		path := filepath.Join(q.dir, name)
		info, err := os.Lstat(path)
		if err != nil {
			return fmt.Errorf("evidence: inspect refused-head record %s: %w", id, err)
		}
		if info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("evidence: refused-head record %s must be a regular file", id)
		}
		if info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("evidence: refused-head record %s permissions are not private", id)
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return fmt.Errorf("evidence: read refused-head record %s: %w", id, err)
		}
		var record HoldRecord
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
			return fmt.Errorf("evidence: corrupt refused-head record %s", id)
		}
		if record.QueueRecord != id {
			return fmt.Errorf("evidence: refused-head record %s names queue record %q", id, record.QueueRecord)
		}
		queued, ok := q.entries[id]
		if !ok {
			return fmt.Errorf("evidence: refused-head record %s has no queue record; move it out with its record", id)
		}
		if err := validateHoldRecord(record, queued); err != nil {
			return fmt.Errorf("evidence: refused-head record %s does not match its queue record: %w", id, err)
		}
		q.holds[id] = record
	}
	return nil
}

const (
	backoffRecordVersion = 1
	// backoffFilePrefix names the back-off record beside a queue record:
	// backoffFilePrefix + queue record id.
	backoffFilePrefix = ".ori-evidence-backoff-"
)

// BackoffRecord persists when a backed-off head may next be sent, so a restart
// neither resends it early nor forgets how far its back-off has grown.
type BackoffRecord struct {
	V           int    `json:"v"`
	QueueRecord string `json:"queue_record"`
	NotBeforeMS int64  `json:"not_before_ms"`
	Attempts    int    `json:"attempts"`
}

// SetBackoff durably records a queued record's back-off. The write is atomic.
func (q *DurableQueue) SetBackoff(record BackoffRecord) error {
	if q == nil {
		return fmt.Errorf("evidence: nil durable queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.entries[record.QueueRecord]; !ok {
		return ErrArtifactNotFound
	}
	record.V = backoffRecordVersion
	if err := validateBackoffRecord(record); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("evidence: encode back-off record: %w", err)
	}
	if err := q.writeFileAtomic(backoffFilePrefix+record.QueueRecord, encoded); err != nil {
		return err
	}
	q.backoffs[record.QueueRecord] = record
	return nil
}

// ClearBackoff removes a queued record's back-off, if it has one.
func (q *DurableQueue) ClearBackoff(id string) error {
	if q == nil {
		return fmt.Errorf("evidence: nil durable queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if _, ok := q.backoffs[id]; !ok {
		return nil
	}
	err := q.removeBackoffLocked(id)
	q.noteStore(err)
	return err
}

// BackoffFor returns the durable back-off on a queue record, if any.
func (q *DurableQueue) BackoffFor(id string) (BackoffRecord, bool) {
	if q == nil {
		return BackoffRecord{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	record, ok := q.backoffs[id]
	return record, ok
}

func (q *DurableQueue) removeBackoffLocked(id string) error {
	if _, ok := q.backoffs[id]; !ok {
		return nil
	}
	if err := os.Remove(filepath.Join(q.dir, backoffFilePrefix+id)); err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("evidence: remove back-off record: %w", err)
	}
	if err := syncDirectory(q.dir); err != nil {
		return fmt.Errorf("evidence: persist back-off removal: %w", err)
	}
	delete(q.backoffs, id)
	return nil
}

// loadBackoffs validates every back-off record against the queue record it
// names, and refuses the queue on any that does not, as for holds.
func (q *DurableQueue) loadBackoffs(names []string) error {
	for _, name := range names {
		id := strings.TrimPrefix(name, backoffFilePrefix)
		raw, err := readPrivateSidecar(filepath.Join(q.dir, name), "back-off", id)
		if err != nil {
			return err
		}
		var record BackoffRecord
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
			return fmt.Errorf("evidence: corrupt back-off record %s", id)
		}
		if record.QueueRecord != id {
			return fmt.Errorf("evidence: back-off record %s names queue record %q", id, record.QueueRecord)
		}
		if _, ok := q.entries[id]; !ok {
			return fmt.Errorf("evidence: back-off record %s has no queue record; move it out with its record", id)
		}
		if err := validateBackoffRecord(record); err != nil {
			return fmt.Errorf("evidence: back-off record %s is invalid: %w", id, err)
		}
		q.backoffs[id] = record
	}
	return nil
}

func validateBackoffRecord(record BackoffRecord) error {
	switch {
	case record.V != backoffRecordVersion:
		return fmt.Errorf("unsupported version")
	case record.NotBeforeMS <= 0 || record.NotBeforeMS > maxSafeInteger:
		return fmt.Errorf("not-before time out of range")
	case record.Attempts < 1 || record.Attempts > maxBackoffAttempts:
		return fmt.Errorf("attempt count out of range")
	}
	return nil
}

// readPrivateSidecar reads a sidecar record that must be a private regular file.
func readPrivateSidecar(path, kind, id string) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: inspect %s record %s: %w", kind, id, err)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() {
		return nil, fmt.Errorf("evidence: %s record %s must be a regular file", kind, id)
	}
	if info.Mode().Perm()&0o077 != 0 {
		return nil, fmt.Errorf("evidence: %s record %s permissions are not private", kind, id)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("evidence: read %s record %s: %w", kind, id, err)
	}
	return raw, nil
}

func validateHoldRecord(record HoldRecord, queued queueRecord) error {
	switch {
	case record.V != holdRecordVersion:
		return fmt.Errorf("unsupported version")
	case record.QueueRecord != queued.ID:
		return fmt.Errorf("queue record mismatch")
	case record.ArtifactDigest != payloadDigest(queued.Payload):
		return fmt.Errorf("artifact digest mismatch")
	case record.ArtifactType != queued.Type:
		return fmt.Errorf("artifact type mismatch")
	case !heldReason(record.RefusalStatus, record.Reason):
		return fmt.Errorf("reason is not one at which its refusal status holds")
	case !heldRefusalStatus(record.RefusalStatus):
		return fmt.Errorf("refusal status cannot hold")
	case record.FirstHeldAtMS <= 0 || record.FirstHeldAtMS > maxSafeInteger:
		return fmt.Errorf("first-held time out of range")
	}
	return nil
}

// heldRefusalStatus is the closed set of statuses that can hold a head.
func heldRefusalStatus(status int) bool {
	switch status {
	case 0, 400, 409, 422:
		return true
	default:
		return false
	}
}
