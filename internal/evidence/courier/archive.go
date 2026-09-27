// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/custody"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"github.com/ori-platform/ori-gateway/internal/evidence/refusal"
	"io"
	"path/filepath"
	"sort"
	"strings"
)

const (
	archiveRecordVersion = 1
	// archiveFilePrefix names a terminal-registration archive record in the
	// outbound queue directory: archiveFilePrefix + queue record id.
	archiveFilePrefix = ".ori-evidence-archive-"
)

// errArchivedNotRetired means the archive record committed but the active
// record could not be retired. The record is terminal: it is never sent again,
// the move is retried, and a restart completes it.
var errArchivedNotRetired = errors.New("evidence: registration archived but its active record was not retired")

// ArchiveRecord is a terminal anchor-registration refusal moved out of the
// active registration lane (evidence-transport/v2, refusal policy). It keeps
// the exact bytes and is never delivered again. It is outside active capacity.
type ArchiveRecord struct {
	V              int          `json:"v"`
	QueueRecord    string       `json:"queue_record"`
	QueueSeq       int64        `json:"queue_seq"`
	DeviceID       string       `json:"device_id"`
	ArtifactType   ArtifactType `json:"artifact_type"`
	ArtifactDigest string       `json:"artifact_digest"`
	Payload        []byte       `json:"payload"`
	RefusalStatus  int          `json:"refusal_status"`
	Reason         string       `json:"reason"`
	RefusedAtMS    int64        `json:"refused_at_ms"`
	// Acknowledged is false until an operator acknowledgement exists.
	Acknowledged bool `json:"acknowledged"`
}

// Archive atomically moves a queued anchor registration the evidence
// authority refused terminally into the archive: the archive record is
// committed first, and only then is the active record retired and its
// capacity released. If the archive cannot commit, the active record is left
// exactly as it was. The same bytes archived again keep their first refusal
// time.
func (q *DurableQueue) Archive(id string, status int, reason string, refusedAtMS int64) error {
	if q == nil {
		return fmt.Errorf("evidence: nil durable queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	// A failing move holds store_unavailable under a source of its own, so a
	// successful write elsewhere in the store cannot clear it; only the move
	// completing does.
	source := q.faultSource + " archive " + id
	queued, ok := q.entries[id]
	if !ok {
		q.faults.Note(faults.StoreUnavailable, source, nil)
		return ErrArtifactNotFound
	}
	record := ArchiveRecord{
		V: archiveRecordVersion, QueueRecord: id, QueueSeq: queued.QueueSeq,
		DeviceID: recordDevice(queued.Payload), ArtifactType: queued.Type,
		ArtifactDigest: payloadDigest(queued.Payload), Payload: append([]byte(nil), queued.Payload...),
		RefusalStatus: status, Reason: reason, RefusedAtMS: refusedAtMS,
	}
	if earlier, ok := q.archives[id]; ok {
		record.RefusedAtMS = earlier.RefusedAtMS
	}
	if err := validateArchiveRecord(record); err != nil {
		return err
	}
	encoded, err := json.Marshal(record)
	if err != nil {
		return fmt.Errorf("evidence: encode archive record: %w", err)
	}
	if err := q.writeFileAtomic(archiveFilePrefix+id, encoded); err != nil {
		q.faults.Note(faults.StoreUnavailable, source, err)
		return err
	}
	q.archives[id] = record
	err = q.removeLocked(id)
	q.noteStore(err)
	q.faults.Note(faults.StoreUnavailable, source, err)
	if err != nil {
		return fmt.Errorf("%w: %v", errArchivedNotRetired, err)
	}
	return nil
}

// ArchivedFor returns the archive record of a queue record id, if any.
func (q *DurableQueue) ArchivedFor(id string) (ArchiveRecord, bool) {
	if q == nil {
		return ArchiveRecord{}, false
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	record, ok := q.archives[id]
	return record, ok
}

// Archived lists every archive record, without its bytes, by device, refusal
// time and digest.
func (q *DurableQueue) Archived() []ArchiveRecord {
	if q == nil {
		return nil
	}
	q.mu.Lock()
	out := make([]ArchiveRecord, 0, len(q.archives))
	for _, record := range q.archives {
		record.Payload = nil
		out = append(out, record)
	}
	q.mu.Unlock()
	sort.Slice(out, func(i, j int) bool {
		a, b := out[i], out[j]
		if a.DeviceID != b.DeviceID {
			return a.DeviceID < b.DeviceID
		}
		if a.RefusedAtMS != b.RefusedAtMS {
			return a.RefusedAtMS < b.RefusedAtMS
		}
		return a.ArtifactDigest < b.ArtifactDigest
	})
	return out
}

// loadArchives validates every archive record and refuses the queue on any
// that does not hold together: an archive is evidence, and dropping or
// misreading one would lose a terminal refusal. An archive whose active record
// is still queued under the same queue sequence is a move a crash interrupted
// after the archive committed; it is completed here. The same bytes queued
// under a later sequence are a new admission and stay active.
func (q *DurableQueue) loadArchives(names []string) error {
	for _, name := range names {
		id := strings.TrimPrefix(name, archiveFilePrefix)
		raw, err := readPrivateSidecar(filepath.Join(q.dir, name), "archive", id)
		if err != nil {
			return err
		}
		var record ArchiveRecord
		decoder := json.NewDecoder(bytes.NewReader(raw))
		decoder.DisallowUnknownFields()
		if err := decoder.Decode(&record); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
			return fmt.Errorf("evidence: corrupt archive record %s", id)
		}
		if record.QueueRecord != id {
			return fmt.Errorf("evidence: archive record %s names queue record %q", id, record.QueueRecord)
		}
		if err := validateArchiveRecord(record); err != nil {
			return fmt.Errorf("evidence: archive record %s is invalid: %w", id, err)
		}
		q.archives[id] = record
		if queued, ok := q.entries[id]; ok && queued.QueueSeq == record.QueueSeq {
			if err := q.removeLocked(id); err != nil {
				return fmt.Errorf("evidence: complete the archival of %s: %w", id, err)
			}
		}
	}
	return nil
}

func validateArchiveRecord(record ArchiveRecord) error {
	switch {
	case record.V != archiveRecordVersion:
		return fmt.Errorf("unsupported version")
	case record.ArtifactType != ArtifactAnchorRegistration:
		return fmt.Errorf("only an anchor registration is archived")
	case len(record.Payload) == 0 || len(record.Payload) > maxArtifactBytes:
		return fmt.Errorf("archived bytes out of range")
	case record.QueueRecord != artifactID(record.ArtifactType, record.Payload):
		return fmt.Errorf("queue record does not match the archived bytes")
	case record.ArtifactDigest != payloadDigest(record.Payload):
		return fmt.Errorf("artifact digest does not match the archived bytes")
	case record.DeviceID != recordDevice(record.Payload) || !validRoutingDeviceID(record.DeviceID):
		return fmt.Errorf("device does not match the archived bytes")
	case record.QueueSeq <= 0:
		return fmt.Errorf("queue sequence out of range")
	case !heldRefusalStatus(record.RefusalStatus) || !refusal.Held(record.RefusalStatus, record.Reason):
		return fmt.Errorf("reason is not one at which its refusal status holds")
	case record.RefusedAtMS <= 0 || record.RefusedAtMS > custody.MaxSafeInteger:
		return fmt.Errorf("refusal time out of range")
	}
	return nil
}
