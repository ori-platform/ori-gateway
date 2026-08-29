// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"time"
)

// ArtifactType is the closed set of device-signed artifacts the blind courier
// may carry outward. Authority artifacts travel in the opposite direction and
// never enter this queue.
type ArtifactType string

const (
	ArtifactDeliveryEnvelope       ArtifactType = "delivery_envelope"
	ArtifactAnchorRegistration     ArtifactType = "anchor_registration"
	ArtifactCheckpoint             ArtifactType = "checkpoint"
	artifactDeliveryReceipt        ArtifactType = "delivery_receipt"
	artifactEpochConfirmation      ArtifactType = "epoch_confirmation"
	artifactCustodyAcknowledgement ArtifactType = "custody_acknowledgement"

	queueRecordVersion = 1
	queueFileSuffix    = ".json"
	queueMarkerName    = ".ori-evidence-queue-v1"
	queueMarkerBody    = "ori evidence durable queue v1\n"
	queueTempPrefix    = ".ori-evidence-queue-tmp-"
)

var (
	// ErrQueueFull is an explicit retriable refusal. Callers must not acknowledge
	// custody when Enqueue returns this error.
	ErrQueueFull = errors.New("evidence: durable queue is full")
	// ErrArtifactNotFound means a caller tried to remove an entry the queue does
	// not hold. Treating that as success would hide a retirement race.
	ErrArtifactNotFound = errors.New("evidence: queued artifact not found")
)

// QueueFullError reports which configured bound refused the artifact without
// exposing queue paths or any evidence-store identity.
type QueueFullError struct {
	Limit string
}

func (e *QueueFullError) Error() string {
	return fmt.Sprintf("%s (%s limit)", ErrQueueFull, e.Limit)
}

func (e *QueueFullError) Unwrap() error { return ErrQueueFull }

// QueuedArtifact is one byte-preserving durable queue entry.
//
// Payload is the exact wire artifact received from the runtime. It is never
// decoded and re-encoded by the queue or delivery worker.
type QueuedArtifact struct {
	ID           string
	Type         ArtifactType
	Payload      []byte
	EnqueuedAtMS int64
}

type queueRecord struct {
	V            int          `json:"v"`
	ID           string       `json:"id"`
	Type         ArtifactType `json:"artifact_type"`
	QueueSeq     int64        `json:"queue_seq"`
	EnqueuedAtMS int64        `json:"enqueued_at_ms"`
	Payload      []byte       `json:"payload"`
}

// QueueOptions configure the on-disk evidence queue.
type QueueOptions struct {
	Directory string
	MaxItems  int
	MaxBytes  int64
	Now       func() time.Time
}

// DurableQueue stores artifacts as independently atomic records. A committed
// entry is fsynced before Enqueue returns, which is the boundary after which a
// custody acknowledgement may be issued.
type DurableQueue struct {
	mu       sync.Mutex
	dir      string
	maxItems int
	maxBytes int64
	now      func() time.Time
	entries  map[string]queueRecord
	sizes    map[string]int64
	order    []string
	bytes    int64
	nextSeq  int64
}

// OpenDurableQueue opens or creates a private queue and reconstructs its state.
// Corrupt, renamed, symlinked, or overly-permissive records fail startup rather
// than being skipped: silently skipping one would turn evidence loss into a
// healthy empty queue.
func OpenDurableQueue(opts QueueOptions) (*DurableQueue, error) {
	queue, err := openDurableQueue(opts)
	if err != nil {
		return nil, err
	}
	for _, id := range queue.order {
		if !validOutboundArtifactType(queue.entries[id].Type) {
			return nil, fmt.Errorf("evidence: outbound queue contains an authority artifact")
		}
	}
	return queue, nil
}

func openDurableQueue(opts QueueOptions) (*DurableQueue, error) {
	dir := filepath.Clean(strings.TrimSpace(opts.Directory))
	if dir == "." || dir == "" {
		return nil, fmt.Errorf("evidence: queue directory must not be empty")
	}
	if opts.MaxItems <= 0 {
		return nil, fmt.Errorf("evidence: queue max items must be positive")
	}
	if opts.MaxBytes <= 0 {
		return nil, fmt.Errorf("evidence: queue max bytes must be positive")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	if err := ensurePrivateQueueDirectory(dir); err != nil {
		return nil, err
	}

	q := &DurableQueue{
		dir:      dir,
		maxItems: opts.MaxItems,
		maxBytes: opts.MaxBytes,
		now:      now,
		entries:  make(map[string]queueRecord),
		sizes:    make(map[string]int64),
		nextSeq:  1,
	}
	if err := q.load(); err != nil {
		return nil, err
	}
	return q, nil
}

// Enqueue durably stores the exact artifact bytes. Re-enqueueing the same type
// and bytes is idempotent and returns the existing entry even when the queue is
// full; retrying a packet whose acknowledgement was lost must not consume the
// queue twice.
func (q *DurableQueue) Enqueue(kind ArtifactType, payload []byte) (QueuedArtifact, error) {
	if q == nil {
		return QueuedArtifact{}, fmt.Errorf("evidence: nil durable queue")
	}
	if !validOutboundArtifactType(kind) {
		return QueuedArtifact{}, fmt.Errorf("evidence: unsupported outbound artifact type %q", kind)
	}
	return q.enqueue(kind, payload)
}

func (q *DurableQueue) enqueue(kind ArtifactType, payload []byte) (QueuedArtifact, error) {
	if !validQueueArtifactType(kind) {
		return QueuedArtifact{}, fmt.Errorf("evidence: unsupported queue artifact type %q", kind)
	}
	if len(payload) == 0 {
		return QueuedArtifact{}, fmt.Errorf("evidence: artifact payload must not be empty")
	}
	id := artifactID(kind, payload)

	q.mu.Lock()
	defer q.mu.Unlock()
	if existing, ok := q.entries[id]; ok {
		return recordArtifact(existing), nil
	}
	if len(q.entries) >= q.maxItems {
		return QueuedArtifact{}, &QueueFullError{Limit: "item"}
	}

	enqueuedAtMS := q.now().UnixMilli()
	if enqueuedAtMS <= 0 {
		return QueuedArtifact{}, fmt.Errorf("evidence: queue clock must produce a positive timestamp")
	}
	record := queueRecord{
		V:            queueRecordVersion,
		ID:           id,
		Type:         kind,
		QueueSeq:     q.nextSeq,
		EnqueuedAtMS: enqueuedAtMS,
		Payload:      append([]byte(nil), payload...),
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return QueuedArtifact{}, fmt.Errorf("evidence: encode queue record: %w", err)
	}
	if q.bytes+int64(len(encoded)) > q.maxBytes {
		return QueuedArtifact{}, &QueueFullError{Limit: "byte"}
	}
	if err := q.writeRecord(record, encoded); err != nil {
		return QueuedArtifact{}, err
	}
	q.entries[id] = record
	q.sizes[id] = int64(len(encoded))
	q.order = append(q.order, id)
	q.sortOrder()
	q.bytes += int64(len(encoded))
	q.nextSeq++
	return recordArtifact(record), nil
}

// Peek returns the oldest queued artifact without retiring it.
func (q *DurableQueue) Peek() (QueuedArtifact, bool) {
	if q == nil {
		return QueuedArtifact{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	if len(q.order) == 0 {
		return QueuedArtifact{}, false
	}
	return recordArtifact(q.entries[q.order[0]]), true
}

// Remove durably retires one artifact. It is called only after the independent
// evidence channel has affirmatively accepted the exact queued bytes.
func (q *DurableQueue) Remove(id string) error {
	if q == nil {
		return fmt.Errorf("evidence: nil durable queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	record, ok := q.entries[id]
	if !ok {
		return ErrArtifactNotFound
	}
	path := q.recordPath(id)
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("evidence: encode queued artifact for retirement: %w", err)
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("evidence: retire queued artifact: %w", err)
	}
	if err := syncDirectory(q.dir); err != nil {
		// The unlink reached the filesystem but its directory entry was not
		// confirmed durable. Restore the record before returning so this process
		// does not hold an in-memory entry whose retry can never remove a file that
		// is already gone. A restored record may cause an idempotent redelivery;
		// losing one would be worse.
		if restoreErr := q.writeRecord(record, encoded); restoreErr != nil {
			return fmt.Errorf("evidence: persist queue retirement: %v; restore failed: %w", err, restoreErr)
		}
		return fmt.Errorf("evidence: persist queue retirement: %w", err)
	}
	delete(q.entries, id)
	storedSize := q.sizes[id]
	delete(q.sizes, id)
	for i, queuedID := range q.order {
		if queuedID == id {
			q.order = append(q.order[:i], q.order[i+1:]...)
			break
		}
	}
	q.bytes -= storedSize
	return nil
}

// Len is the number of durably queued artifacts.
func (q *DurableQueue) Len() int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return len(q.entries)
}

// StoredBytes is the number of encoded record bytes charged to the queue bound.
func (q *DurableQueue) StoredBytes() int64 {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.bytes
}

func (q *DurableQueue) load() error {
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		return fmt.Errorf("evidence: read durable queue: %w", err)
	}
	seenQueueSeq := make(map[int64]struct{})
	for _, entry := range entries {
		name := entry.Name()
		if name == queueMarkerName || strings.HasPrefix(name, ackSigningTimeName) {
			continue
		}
		if strings.HasPrefix(name, queueTempPrefix) {
			// A temp file was never committed by rename. Removing it cannot lose
			// an acknowledged artifact because Enqueue had not returned yet.
			if err := os.Remove(filepath.Join(q.dir, name)); err != nil {
				return fmt.Errorf("evidence: remove incomplete queue write: %w", err)
			}
			continue
		}
		if entry.IsDir() || !strings.HasSuffix(name, queueFileSuffix) {
			return fmt.Errorf("evidence: unexpected entry in durable queue")
		}
		info, err := entry.Info()
		if err != nil {
			return fmt.Errorf("evidence: inspect queue record: %w", err)
		}
		if info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() {
			return fmt.Errorf("evidence: queue record must be a regular file")
		}
		if info.Mode().Perm()&0o077 != 0 {
			return fmt.Errorf("evidence: queue record permissions are not private")
		}
		raw, err := os.ReadFile(filepath.Join(q.dir, name))
		if err != nil {
			return fmt.Errorf("evidence: read queue record: %w", err)
		}
		var record queueRecord
		if err := json.Unmarshal(raw, &record); err != nil {
			return fmt.Errorf("evidence: corrupt queue record: %w", err)
		}
		if record.V != queueRecordVersion || record.QueueSeq <= 0 || record.EnqueuedAtMS <= 0 || !validQueueArtifactType(record.Type) || len(record.Payload) == 0 {
			return fmt.Errorf("evidence: invalid queue record")
		}
		if record.ID != artifactID(record.Type, record.Payload) {
			return fmt.Errorf("evidence: queue record identity does not match its bytes")
		}
		if name != record.ID+queueFileSuffix {
			return fmt.Errorf("evidence: queue record filename does not match its identity")
		}
		if _, exists := q.entries[record.ID]; exists {
			return fmt.Errorf("evidence: duplicate queue record")
		}
		if _, exists := seenQueueSeq[record.QueueSeq]; exists {
			return fmt.Errorf("evidence: duplicate durable queue sequence")
		}
		seenQueueSeq[record.QueueSeq] = struct{}{}
		q.entries[record.ID] = record
		q.sizes[record.ID] = int64(len(raw))
		q.order = append(q.order, record.ID)
		q.bytes += int64(len(raw))
		if record.QueueSeq >= q.nextSeq {
			q.nextSeq = record.QueueSeq + 1
		}
	}
	q.sortOrder()
	if len(q.entries) > q.maxItems || q.bytes > q.maxBytes {
		return fmt.Errorf("evidence: existing durable queue exceeds configured bounds")
	}
	return nil
}

func (q *DurableQueue) writeRecord(record queueRecord, encoded []byte) error {
	tmp, err := os.CreateTemp(q.dir, queueTempPrefix)
	if err != nil {
		return fmt.Errorf("evidence: create queue record: %w", err)
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
		return fmt.Errorf("evidence: protect queue record: %w", err)
	}
	if _, err := tmp.Write(encoded); err != nil {
		return fmt.Errorf("evidence: write queue record: %w", err)
	}
	if err := tmp.Sync(); err != nil {
		return fmt.Errorf("evidence: sync queue record: %w", err)
	}
	if err := tmp.Close(); err != nil {
		return fmt.Errorf("evidence: close queue record: %w", err)
	}
	if err := os.Rename(tmpName, q.recordPath(record.ID)); err != nil {
		return fmt.Errorf("evidence: commit queue record: %w", err)
	}
	if err := syncDirectory(q.dir); err != nil {
		return fmt.Errorf("evidence: persist queue commit: %w", err)
	}
	committed = true
	return nil
}

func (q *DurableQueue) sortOrder() {
	sort.Slice(q.order, func(i, j int) bool {
		a, b := q.entries[q.order[i]], q.entries[q.order[j]]
		if a.QueueSeq == b.QueueSeq {
			return a.ID < b.ID
		}
		return a.QueueSeq < b.QueueSeq
	})
}

func (q *DurableQueue) recordPath(id string) string {
	return filepath.Join(q.dir, id+queueFileSuffix)
}

func ensurePrivateQueueDirectory(dir string) error {
	info, err := os.Lstat(dir)
	created := false
	switch {
	case errors.Is(err, os.ErrNotExist):
		if err := os.MkdirAll(filepath.Dir(dir), 0o700); err != nil {
			return fmt.Errorf("evidence: create queue parent directory: %w", err)
		}
		if err := os.Mkdir(dir, 0o700); err == nil {
			created = true
			if err := os.Chmod(dir, 0o700); err != nil {
				return fmt.Errorf("evidence: protect queue directory: %w", err)
			}
		} else if !errors.Is(err, os.ErrExist) {
			return fmt.Errorf("evidence: create queue directory: %w", err)
		}
		info, err = os.Lstat(dir)
	case err != nil:
		return fmt.Errorf("evidence: inspect queue directory: %w", err)
	}
	if err != nil {
		return fmt.Errorf("evidence: inspect queue directory: %w", err)
	}
	if info.Mode()&fs.ModeSymlink != 0 || !info.IsDir() {
		return fmt.Errorf("evidence: queue path must be a directory, not a symlink or file")
	}
	if info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("evidence: queue directory must not be accessible by group or other users")
	}
	return ensureQueueMarker(dir, created)
}

func ensureQueueMarker(dir string, mayCreate bool) error {
	path := filepath.Join(dir, queueMarkerName)
	if !mayCreate {
		if _, err := os.Lstat(path); errors.Is(err, os.ErrNotExist) {
			return fmt.Errorf("evidence: existing queue directory has no ownership marker")
		}
	}
	f, err := os.OpenFile(path, os.O_WRONLY|os.O_CREATE|os.O_EXCL, 0o600)
	if err == nil {
		if _, writeErr := f.WriteString(queueMarkerBody); writeErr != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return fmt.Errorf("evidence: write queue ownership marker: %w", writeErr)
		}
		if syncErr := f.Sync(); syncErr != nil {
			_ = f.Close()
			_ = os.Remove(path)
			return fmt.Errorf("evidence: sync queue ownership marker: %w", syncErr)
		}
		if closeErr := f.Close(); closeErr != nil {
			_ = os.Remove(path)
			return fmt.Errorf("evidence: close queue ownership marker: %w", closeErr)
		}
		if syncErr := syncDirectory(dir); syncErr != nil {
			return fmt.Errorf("evidence: persist queue ownership marker: %w", syncErr)
		}
		return nil
	}
	if !errors.Is(err, os.ErrExist) {
		return fmt.Errorf("evidence: create queue ownership marker: %w", err)
	}
	info, err := os.Lstat(path)
	if err != nil || info.Mode()&fs.ModeSymlink != 0 || !info.Mode().IsRegular() || info.Mode().Perm()&0o077 != 0 {
		return fmt.Errorf("evidence: invalid queue ownership marker")
	}
	body, err := os.ReadFile(path)
	if err != nil || string(body) != queueMarkerBody {
		return fmt.Errorf("evidence: queue ownership marker does not match")
	}
	return nil
}

func syncDirectory(dir string) error {
	f, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer f.Close()
	return f.Sync()
}

func validOutboundArtifactType(kind ArtifactType) bool {
	switch kind {
	case ArtifactDeliveryEnvelope, ArtifactAnchorRegistration, ArtifactCheckpoint:
		return true
	default:
		return false
	}
}

func validQueueArtifactType(kind ArtifactType) bool {
	return validOutboundArtifactType(kind) || kind == artifactDeliveryReceipt || kind == artifactEpochConfirmation || kind == artifactCustodyAcknowledgement
}

func artifactID(kind ArtifactType, payload []byte) string {
	h := sha256.New()
	_, _ = h.Write([]byte(kind))
	_, _ = h.Write([]byte{0})
	_, _ = h.Write(payload)
	return hex.EncodeToString(h.Sum(nil))
}

func recordArtifact(record queueRecord) QueuedArtifact {
	return QueuedArtifact{
		ID:           record.ID,
		Type:         record.Type,
		Payload:      append([]byte(nil), record.Payload...),
		EnqueuedAtMS: record.EnqueuedAtMS,
	}
}
