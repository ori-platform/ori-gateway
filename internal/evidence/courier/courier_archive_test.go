// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// writeArchive writes an archive record by hand, in its on-disk format.
func writeArchive(t *testing.T, dir string, record ArchiveRecord) {
	t.Helper()
	raw, err := json.Marshal(record)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, archiveFilePrefix+record.QueueRecord), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func archiveOf(entry QueuedArtifact, seq int64) ArchiveRecord {
	return ArchiveRecord{
		V: archiveRecordVersion, QueueRecord: entry.ID, QueueSeq: seq, DeviceID: recordDevice(entry.Payload),
		ArtifactType: entry.Type, ArtifactDigest: payloadDigest(entry.Payload), Payload: entry.Payload,
		RefusalStatus: 422, Reason: "commissioning_digest_mismatch", RefusedAtMS: 1787000000000,
	}
}

// A crash after the archive committed and before the active record was
// retired leaves both; opening completes the move. The same bytes queued
// under a later sequence are a new admission and stay active.
func TestOpeningCompletesAnInterruptedArchival(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 100, 1<<24)
	entry, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	writeArchive(t, dir, archiveOf(entry, 1))
	reopened := openTestQueueAt(t, dir, 100, 1<<24)
	if reopened.Len() != 0 {
		t.Fatal("the interrupted archival left an active record")
	}
	if _, err := os.Stat(filepath.Join(dir, entry.ID+".json")); !os.IsNotExist(err) {
		t.Fatalf("the active record file survived: %v", err)
	}
	if _, ok := reopened.ArchivedFor(entry.ID); !ok {
		t.Fatal("the archive was lost")
	}

	// A re-offer of the archived bytes is a new admission, under a new
	// sequence, and survives a restart as active.
	again, err := reopened.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1))
	if err != nil || again.ID != entry.ID || reopened.Len() != 1 {
		t.Fatalf("re-offer = %#v, %v", again, err)
	}
	third := openTestQueueAt(t, dir, 100, 1<<24)
	if third.Len() != 1 {
		t.Fatal("a re-offered registration was taken for the interrupted archival")
	}
}

// A corrupt or inconsistent archive refuses the queue: an archive is a
// terminal refusal, and misreading it would lose or misreport one.
func TestInvalidArchiveRefusesTheQueue(t *testing.T) {
	for name, mutate := range map[string]func(*ArchiveRecord){
		"wrong version":         func(r *ArchiveRecord) { r.V = 2 },
		"not a registration":    func(r *ArchiveRecord) { r.ArtifactType = ArtifactCheckpoint },
		"bytes do not match":    func(r *ArchiveRecord) { r.Payload = []byte(`{"v":1,"device_id":"dev-a"}`) },
		"digest does not match": func(r *ArchiveRecord) { r.ArtifactDigest = "sha256:" + strings.Repeat("0", 64) },
		"device does not match": func(r *ArchiveRecord) { r.DeviceID = "dev-b" },
		"a status that never holds": func(r *ArchiveRecord) {
			r.RefusalStatus, r.Reason = 503, "unavailable"
		},
		"a backing-off reason": func(r *ArchiveRecord) {
			r.RefusalStatus, r.Reason = 409, "pending_registration_conflict"
		},
		"no refusal time":   func(r *ArchiveRecord) { r.RefusedAtMS = 0 },
		"no queue sequence": func(r *ArchiveRecord) { r.QueueSeq = 0 },
	} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "queue")
			q := openTestQueueAt(t, dir, 100, 1<<24)
			entry, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1))
			if err != nil {
				t.Fatal(err)
			}
			if err := q.Remove(entry.ID); err != nil {
				t.Fatal(err)
			}
			record := archiveOf(entry, 1)
			mutate(&record)
			writeArchive(t, dir, record)
			if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 24}); err == nil {
				t.Fatal("an invalid archive was adopted")
			}
		})
	}
	for name, raw := range map[string]string{
		"not JSON":      "{",
		"unknown field": `{"v":1,"x":1}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "queue")
			openTestQueueAt(t, dir, 100, 1<<24)
			if err := os.WriteFile(filepath.Join(dir, archiveFilePrefix+strings.Repeat("a", 64)), []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 24}); err == nil {
				t.Fatal("a corrupt archive was adopted")
			}
		})
	}
	t.Run("not private", func(t *testing.T) {
		dir := filepath.Join(t.TempDir(), "queue")
		q := openTestQueueAt(t, dir, 100, 1<<24)
		entry, _ := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1))
		_ = q.Remove(entry.ID)
		writeArchive(t, dir, archiveOf(entry, 1))
		if err := os.Chmod(filepath.Join(dir, archiveFilePrefix+entry.ID), 0o644); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 24}); err == nil {
			t.Fatal("a group-readable archive was adopted")
		}
	})
}

// Without a verified retention a terminal refusal is held in either lane and
// never archived (evidence-transport/v2, refusal policy): it stays at its
// lane's head, is not sent again, and blocks only its own lane, so the same
// device's other lane still delivers; a restart restores the hold from the
// store without sending it.
func TestATerminalRefusalWithoutRetentionIsHeldInEitherLane(t *testing.T) {
	for _, lane := range []LaneKey{regLane, evLane} {
		t.Run(string(lane.Lane), func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "queue")
			q := openTestQueueAt(t, dir, 100, 1<<24)
			authority := newLaneAuthority()
			authority.set(lane, &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "commissioning_digest_mismatch"})
			other := evLane
			if lane == evLane {
				other = regLane
			}
			if lane == regLane {
				for seq := 1; seq <= 2; seq++ {
					if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", seq)); err != nil {
						t.Fatal(err)
					}
				}
				enqueueEnvelopes(t, q, "dev-a", 1, 1)
			} else {
				enqueueEnvelopes(t, q, "dev-a", 1, 2)
				if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
					t.Fatal(err)
				}
			}
			worker := laneWorker(t, q, authority, nil, 10*time.Millisecond)
			running := startWorker(t, worker)
			waitFor(t, "the refused lane held", func() bool { e, ok := laneEntry(worker, lane); return ok && e.State == DeviceHeld })
			waitFor(t, "the other lane delivered", func() bool { return q.LenLane(other) == 0 })
			worker.NotifyDevice("dev-a")
			time.Sleep(30 * time.Millisecond)
			running.stop()
			if len(q.Archived()) != 0 || q.LenLane(lane) != 2 || authority.count(lane) != 1 {
				t.Fatalf("archived %d, queued %d, attempts %d", len(q.Archived()), q.LenLane(lane), authority.count(lane))
			}

			q = openTestQueueAt(t, dir, 100, 1<<24)
			worker = laneWorker(t, q, authority, nil, 10*time.Millisecond)
			running = startWorker(t, worker)
			waitFor(t, "the hold restored", func() bool { e, ok := laneEntry(worker, lane); return ok && e.State == DeviceHeld })
			time.Sleep(30 * time.Millisecond)
			running.stop()
			if authority.count(lane) != 1 || len(q.Archived()) != 0 {
				t.Fatalf("after a restart: attempts %d, archived %d", authority.count(lane), len(q.Archived()))
			}
		})
	}
}
