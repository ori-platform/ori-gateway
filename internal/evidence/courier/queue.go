// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/durable"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
	"io/fs"
	"maps"
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
	// Devices, when set, is the configured device set: a device not named is
	// refused admission. Left empty, every device is admitted.
	Devices []string
	// ReserveDeviceShares reserves capacity per named device: each may hold at
	// most MaxItems/len(Devices) records and MaxBytes/len(Devices) bytes, so one
	// device exhausting its share cannot refuse another's admissions
	// (gateway-config/v2, once a device declares gateway-evidence-carriage/v1).
	// Unset, the queue is one gateway-wide share, as a site that declares
	// nothing has always had.
	ReserveDeviceShares bool
	// Faults, when set, records store_unavailable under FaultSource while a
	// durable write, removal or directory sync fails, and clears it on the
	// next that succeeds.
	Faults      *faults.Recorder
	FaultSource string
	Now         func() time.Time
}

// DurableQueue stores artifacts as independently atomic records. A committed
// entry is fsynced before Enqueue returns, which is the boundary after which a
// custody acknowledgement may be issued.
type DurableQueue struct {
	mu       sync.Mutex
	dir      string
	maxItems int
	maxBytes int64
	// reserved is whether capacity is reserved per configured device.
	reserved bool
	now      func() time.Time
	entries  map[string]queueRecord
	holds    map[string]HoldRecord
	backoffs map[string]BackoffRecord
	sizes    map[string]int64
	order    []string
	bytes    int64
	nextSeq  int64

	// Per-lane delivery order and per-device reserved capacity. laneOf is
	// derived from each record's routing device_id and carriage artifact type;
	// nothing about it is stored separately, so a queue written before
	// delivery order was per device and lane needs no migration.
	laneOf      map[string]LaneKey
	laneItems   map[LaneKey]int
	laneBytes   map[LaneKey]int64
	deviceItems map[string]int
	deviceBytes map[string]int64
	configured  map[string]bool
	shareItems  int
	shareBytes  int64

	faults      *faults.Recorder
	faultSource string

	unconfigured         map[string]bool
	unconfiguredOverflow bool
}

// noteStore records the outcome of one durable-store operation: a failure
// raises the store's store_unavailable, and a success leaves it as it is.
// Only a successful probe of the store clears it (gateway-evidence-carriage/v1; see Probe).
func (q *DurableQueue) noteStore(err error) {
	if err != nil {
		q.faults.Note(faults.StoreUnavailable, q.faultSource, err)
	}
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
		dir:          dir,
		maxItems:     opts.MaxItems,
		maxBytes:     opts.MaxBytes,
		now:          now,
		entries:      make(map[string]queueRecord),
		holds:        make(map[string]HoldRecord),
		backoffs:     make(map[string]BackoffRecord),
		sizes:        make(map[string]int64),
		nextSeq:      1,
		laneOf:       make(map[string]LaneKey),
		laneItems:    make(map[LaneKey]int),
		laneBytes:    make(map[LaneKey]int64),
		deviceItems:  make(map[string]int),
		deviceBytes:  make(map[string]int64),
		shareItems:   opts.MaxItems,
		shareBytes:   opts.MaxBytes,
		faults:       opts.Faults,
		faultSource:  opts.FaultSource,
		unconfigured: make(map[string]bool),
	}
	if len(opts.Devices) > 0 {
		q.configured = make(map[string]bool, len(opts.Devices))
		for _, device := range opts.Devices {
			q.configured[device] = true
		}
	}
	if opts.ReserveDeviceShares {
		if len(q.configured) == 0 {
			return nil, fmt.Errorf("evidence: per-device queue shares require the configured devices")
		}
		q.reserved = true
		q.shareItems = opts.MaxItems / len(q.configured)
		q.shareBytes = opts.MaxBytes / int64(len(q.configured))
		if q.shareItems < MinDeviceShareItems || q.shareBytes < MinDeviceShareBytes {
			return nil, fmt.Errorf("evidence: per-device queue share (%d items, %d bytes over %d devices) cannot hold the registration reserve and one maximum-size evidence record",
				q.shareItems, q.shareBytes, len(q.configured))
		}
	}
	if err := q.load(); err != nil {
		return nil, err
	}
	// Probed immediately after opening; a failure is reported as the store's
	// store_unavailable, not as a failure to open.
	_ = q.Probe()
	return q, nil
}

// recordDevice is the device a record belongs to, from its routing fields.
func recordDevice(payload []byte) string {
	var routing struct {
		DeviceID string `json:"device_id"`
	}
	if err := decodeRouting(payload, &routing); err != nil {
		return ""
	}
	return routing.DeviceID
}

// Lane is one of a device's two delivery orders (evidence-transport/v2,
// refusal policy): its anchor registrations, and every other artifact it
// delivers.
type Lane string

const (
	LaneRegistration Lane = "registration"
	LaneEvidence     Lane = "evidence"
)

// LaneKey names one device's lane. Order, retry state and holds are kept per
// LaneKey; capacity is kept per device and shared by its two lanes.
type LaneKey struct {
	Device string
	Lane   Lane
}

// LaneOf is the lane an artifact of the given carriage type travels in. It is
// derived, never stored: an anchor registration is in the registration lane,
// and every other artifact in the evidence lane.
func LaneOf(kind ArtifactType) Lane {
	if kind == ArtifactAnchorRegistration {
		return LaneRegistration
	}
	return LaneEvidence
}

func recordLane(record queueRecord) LaneKey {
	return LaneKey{Device: recordDevice(record.Payload), Lane: LaneOf(record.Type)}
}

func (q *DurableQueue) indexLocked(record queueRecord, size int64) {
	key := recordLane(record)
	q.laneOf[record.ID] = key
	q.laneItems[key]++
	q.laneBytes[key] += size
	q.deviceItems[key.Device]++
	q.deviceBytes[key.Device] += size
}

func (q *DurableQueue) unindexLocked(id string, size int64) {
	key := q.laneOf[id]
	delete(q.laneOf, id)
	q.laneItems[key]--
	q.laneBytes[key] -= size
	if q.laneItems[key] <= 0 {
		delete(q.laneItems, key)
		delete(q.laneBytes, key)
	}
	device := key.Device
	q.deviceItems[device]--
	q.deviceBytes[device] -= size
	if q.deviceItems[device] <= 0 {
		delete(q.deviceItems, device)
		delete(q.deviceBytes, device)
	}
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
	device := recordDevice(payload)
	if q.configured != nil && !q.configured[device] {
		// Not queue_full: an unconfigured device holds no share to be full.
		return QueuedArtifact{}, faults.ErrUnconfiguredDevice
	}
	if len(q.entries) >= q.maxItems {
		return QueuedArtifact{}, &QueueFullError{Limit: "item"}
	}
	key := LaneKey{Device: device, Lane: LaneOf(kind)}
	if err := q.laneShareRefusalLocked(key, 0); err != nil {
		return QueuedArtifact{}, err
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
	if err := q.laneShareRefusalLocked(key, int64(len(encoded))); err != nil {
		return QueuedArtifact{}, err
	}
	if err := q.writeRecord(record, encoded); err != nil {
		return QueuedArtifact{}, err
	}
	q.entries[id] = record
	q.sizes[id] = int64(len(encoded))
	q.indexLocked(record, int64(len(encoded)))
	q.order = append(q.order, id)
	q.sortOrder()
	q.bytes += int64(len(encoded))
	q.nextSeq++
	return recordArtifact(record), nil
}

// laneShareRefusalLocked is the only place a lane's admission is refused for
// capacity (evidence-transport/v2). Capacity is reserved per device. With
// configured devices, every share reserves one anchor_registration slot and
// the maximum encoded registration record for registrations only: evidence
// admission may never take that reserve, so an evidence lane waiting on an
// unconfirmed epoch can never refuse the registration that would confirm it:
// the evidence lane holds at most the share less the reserve, and both lanes
// together at most the share. Without reserved shares the queue is one
// unreserved share. With size 0 it checks items; with a record's encoded size
// it checks bytes.
func (q *DurableQueue) laneShareRefusalLocked(key LaneKey, size int64) error {
	reserved := q.reserved && key.Lane != LaneRegistration
	if size == 0 {
		if q.deviceItems[key.Device] >= q.shareItems ||
			(reserved && q.laneItems[key] >= q.shareItems-1) {
			return &QueueFullError{Limit: "device item"}
		}
		return nil
	}
	if q.deviceBytes[key.Device]+size > q.shareBytes ||
		(reserved && q.laneBytes[key]+size > q.shareBytes-maxQueueRecordBytes) {
		return &QueueFullError{Limit: "device byte"}
	}
	return nil
}

// PeekLane returns the oldest queued artifact of one device's lane without
// retiring it. Delivery order is per device and lane: another device's
// records, and the device's other lane, never stand in front of it.
func (q *DurableQueue) PeekLane(key LaneKey) (QueuedArtifact, bool) {
	if q == nil {
		return QueuedArtifact{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, id := range q.order {
		if q.laneOf[id] == key {
			return recordArtifact(q.entries[id]), true
		}
	}
	return QueuedArtifact{}, false
}

// PeekDevice returns the oldest queued artifact of one device, in either lane.
func (q *DurableQueue) PeekDevice(device string) (QueuedArtifact, bool) {
	if q == nil {
		return QueuedArtifact{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	for _, id := range q.order {
		if q.laneOf[id].Device == device {
			return recordArtifact(q.entries[id]), true
		}
	}
	return QueuedArtifact{}, false
}

// Lanes lists every lane with queued records, sorted by device then lane.
func (q *DurableQueue) Lanes() []LaneKey {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]LaneKey, 0, len(q.laneItems))
	for key := range q.laneItems {
		out = append(out, key)
	}
	sortLaneKeys(out)
	return out
}

// LenLane is the number of records queued in one device's lane.
func (q *DurableQueue) LenLane(key LaneKey) int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.laneItems[key]
}

// laneCounts is the total and every lane's count, read under one lock so the
// lanes never sum to more than the total.
func (q *DurableQueue) laneCounts() (int, map[LaneKey]int) {
	q.mu.Lock()
	defer q.mu.Unlock()
	counts := make(map[LaneKey]int, len(q.laneItems))
	maps.Copy(counts, q.laneItems)
	return len(q.entries), counts
}

func sortLaneKeys(keys []LaneKey) {
	sort.Slice(keys, func(i, j int) bool {
		if keys[i].Device != keys[j].Device {
			return keys[i].Device < keys[j].Device
		}
		return keys[i].Lane < keys[j].Lane
	})
}

// Devices lists every device with queued records, in no particular order.
func (q *DurableQueue) Devices() []string {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	out := make([]string, 0, len(q.deviceItems))
	for device := range q.deviceItems {
		out = append(out, device)
	}
	sort.Strings(out)
	return out
}

// LenDevice is the number of records queued for one device.
func (q *DurableQueue) LenDevice(device string) int {
	if q == nil {
		return 0
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.deviceItems[device]
}

// Peek returns the oldest queued artifact of any device without retiring it.
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
	err := q.remove(id)
	if !errors.Is(err, ErrArtifactNotFound) {
		q.noteStore(err)
	}
	return err
}

func (q *DurableQueue) remove(id string) error {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.removeLocked(id)
}

// removeLocked durably retires one record and its sidecars and releases its
// capacity.
func (q *DurableQueue) removeLocked(id string) error {
	record, ok := q.entries[id]
	if !ok {
		return ErrArtifactNotFound
	}
	path := q.recordPath(id)
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("evidence: encode queued artifact for retirement: %w", err)
	}
	if err := q.removeHoldLocked(id); err != nil {
		return err
	}
	if err := q.removeBackoffLocked(id); err != nil {
		return err
	}
	if err := os.Remove(path); err != nil {
		return fmt.Errorf("evidence: retire queued artifact: %w", err)
	}
	if err := durable.SyncDirectory(q.dir); err != nil {
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
	q.unindexLocked(id, storedSize)
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
	var holdNames, backoffNames []string
	unconfiguredPresent := false
	for _, entry := range entries {
		name := entry.Name()
		if name == queueMarkerName || strings.HasPrefix(name, signingclock.FilePrefix) || name == probeFileName {
			continue
		}
		if name == unconfiguredFileName {
			unconfiguredPresent = true
			continue
		}
		if strings.HasPrefix(name, holdFilePrefix) {
			holdNames = append(holdNames, name)
			continue
		}
		if strings.HasPrefix(name, backoffFilePrefix) {
			backoffNames = append(backoffNames, name)
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
		q.indexLocked(record, int64(len(raw)))
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
	// Per-device shares gate new admissions only. A queue written while
	// capacity was gateway-wide may hold more for one device than its share;
	// it opens, delivers, and refuses that device's admissions until it drains
	// below its share, rather than refusing to start.
	if err := q.loadHolds(holdNames); err != nil {
		return err
	}
	if err := q.loadBackoffs(backoffNames); err != nil {
		return err
	}
	return q.loadUnconfigured(unconfiguredPresent)
}

func (q *DurableQueue) writeRecord(record queueRecord, encoded []byte) error {
	return q.writeFileAtomic(record.ID+queueFileSuffix, encoded)
}

// writeFileAtomic commits one private file into the queue directory: temp
// file, fsync, rename, directory fsync.
func (q *DurableQueue) writeFileAtomic(name string, encoded []byte) error {
	err := q.writeFileAtomicRaw(name, encoded)
	q.noteStore(err)
	return err
}

func (q *DurableQueue) writeFileAtomicRaw(name string, encoded []byte) error {
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
	if err := os.Rename(tmpName, filepath.Join(q.dir, name)); err != nil {
		return fmt.Errorf("evidence: commit queue record: %w", err)
	}
	if err := durable.SyncDirectory(q.dir); err != nil {
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
		if syncErr := durable.SyncDirectory(dir); syncErr != nil {
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
