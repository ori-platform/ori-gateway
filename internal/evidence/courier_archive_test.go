// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"encoding/json"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

// If the archive cannot commit, the refused registration stays held in its
// lane, is never sent again by this process, and store_unavailable is raised;
// once the store recovers, the reminder completes the archival without
// sending the registration, and the lane moves on to the next registration.
func TestArchiveWriteFailureHoldsUntilTheStoreRecovers(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	faults := NewFaultRecorder()
	q, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 24, Faults: faults, FaultSource: "outbound"})
	if err != nil {
		t.Fatal(err)
	}
	refused := deviceRegistration("dev-a", 1)
	entry, err := q.Enqueue(ArtifactAnchorRegistration, refused)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 2)); err != nil {
		t.Fatal(err)
	}
	authority := newLaneAuthority()
	authority.setFor(refused, &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "commissioning_digest_mismatch"})
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
	})
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, channel, &lockedSink{}, DeliveryWorkerOptions{
		RetryInterval: time.Hour, BlockedReminderInterval: 150 * time.Millisecond, Faults: faults,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	restore := unwritable(t, dir)
	startWorker(t, worker)
	waitFor(t, "the registration held", func() bool { e, ok := laneEntry(worker, regLane); return ok && e.State == DeviceHeld })
	status := worker.Status()
	if len(status.Archived) != 0 || len(status.Faults) != 1 || status.Faults[0] != FaultStoreUnavailable ||
		status.Devices[0].Held.ArtifactType != ArtifactAnchorRegistration || status.Devices[0].Pending != 2 {
		t.Fatalf("status while the archive cannot commit = %#v", status)
	}
	worker.NotifyDevice("dev-a")
	time.Sleep(50 * time.Millisecond)
	if got := authority.count(regLane); got != 1 {
		t.Fatalf("a held registration was re-sent in process: %d attempts", got)
	}
	if strings.Contains(logs.String(), dir) {
		t.Fatal("a filesystem path reached the log")
	}
	restore()
	waitFor(t, "the archival on the reminder", func() bool { _, ok := q.ArchivedFor(entry.ID); return ok })
	waitFor(t, "the later registration delivered", func() bool { return q.Len() == 0 })
	if got := authority.count(regLane); got != 2 {
		t.Fatalf("attempts = %d, want the refused registration once and the later one once", got)
	}
	// The store's own fault clears on its next successful probe.
	if err := q.Probe(); err != nil {
		t.Fatal(err)
	}
	if faults := worker.Status().Faults; len(faults) != 0 {
		t.Fatalf("store_unavailable outlived the recovery: %v", faults)
	}
}

// The archive commits before the active record is retired: when only the
// archive commit fails, and the store could otherwise retire the record, the
// registration's active record and bytes stay exactly where they were.
func TestArchiveCommitFailureNeverReleasesTheActiveRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	faults := NewFaultRecorder()
	q, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 24, Faults: faults, FaultSource: "outbound"})
	if err != nil {
		t.Fatal(err)
	}
	refused := deviceRegistration("dev-a", 1)
	entry, err := q.Enqueue(ArtifactAnchorRegistration, refused)
	if err != nil {
		t.Fatal(err)
	}
	// A directory on the archive's path makes only the archive's rename fail.
	squat := filepath.Join(dir, archiveFilePrefix+entry.ID)
	if err := os.Mkdir(squat, 0o700); err != nil {
		t.Fatal(err)
	}
	authority := newLaneAuthority()
	authority.setFor(refused, &fakeRefusal{status: http.StatusBadRequest, reason: "malformed"})
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewDeliveryWorker(q, channel, &lockedSink{}, DeliveryWorkerOptions{
		RetryInterval: time.Hour, BlockedReminderInterval: 100 * time.Millisecond, Faults: faults,
		Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	waitFor(t, "the registration held", func() bool { e, ok := laneEntry(worker, regLane); return ok && e.State == DeviceHeld })
	if _, err := os.Stat(filepath.Join(dir, entry.ID+".json")); err != nil {
		t.Fatalf("the active record was released before its archive committed: %v", err)
	}
	if q.LenLane(regLane) != 1 {
		t.Fatal("the registration left the active lane without an archive")
	}
	if _, ok := q.ArchivedFor(entry.ID); ok {
		t.Fatal("an archive is reported that never committed")
	}
	if f := faults.Active(); len(f) != 1 || f[0] != FaultStoreUnavailable {
		t.Fatalf("faults = %v", f)
	}
	// Successful writes elsewhere in the store, an admission and a probe,
	// never clear store_unavailable while the move is still failing
	// (gateway-api/v1).
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-b", 1)); err != nil {
		t.Fatal(err)
	}
	if err := q.Probe(); err != nil {
		t.Fatal(err)
	}
	if f := worker.Status().Faults; len(f) != 1 || f[0] != FaultStoreUnavailable {
		t.Fatalf("store_unavailable cleared while the archive still cannot commit: %v", f)
	}
	// The store recovers; the reminder completes the move, which clears it.
	if err := os.Remove(squat); err != nil {
		t.Fatal(err)
	}
	waitFor(t, "the archival on the reminder", func() bool { _, ok := q.ArchivedFor(entry.ID); return ok })
	waitFor(t, "store_unavailable cleared", func() bool { return len(worker.Status().Faults) == 0 })
	if authority.count(regLane) != 1 {
		t.Fatalf("the held registration was re-sent: %d attempts", authority.count(regLane))
	}
}

// A re-offer of archived bytes is a new admission and is sent again; refused
// again, it is archived again as the same refusal: one entry, first refusal
// time kept, and the active record retired.
func TestReRefusedRegistrationKeepsItsFirstArchive(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	refused := deviceRegistration("dev-a", 1)
	authority := newLaneAuthority()
	authority.setFor(refused, &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "commissioning_digest_mismatch"})
	entry, err := q.Enqueue(ArtifactAnchorRegistration, refused)
	if err != nil {
		t.Fatal(err)
	}
	worker := laneWorker(t, q, authority, nil, time.Hour)
	startWorker(t, worker)
	waitFor(t, "the first archival", func() bool { _, ok := q.ArchivedFor(entry.ID); return ok && q.Len() == 0 })
	first, _ := q.ArchivedFor(entry.ID)
	time.Sleep(5 * time.Millisecond)
	if _, err := q.Enqueue(ArtifactAnchorRegistration, refused); err != nil || q.Len() != 1 {
		t.Fatalf("re-offer = %v, queued %d", err, q.Len())
	}
	worker.NotifyDevice("dev-a")
	waitFor(t, "the second refusal archived", func() bool { return authority.count(regLane) == 2 && q.Len() == 0 })
	again, ok := q.ArchivedFor(entry.ID)
	if !ok || again.RefusedAtMS != first.RefusedAtMS || again.QueueSeq == first.QueueSeq || len(q.Archived()) != 1 {
		t.Fatalf("first %#v, again %#v, %d archived", first, again, len(q.Archived()))
	}
}

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

// An evidence-lane hold is not archived: it stays at the head of its lane.
func TestEvidenceHoldIsNeverArchived(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newLaneAuthority()
	authority.set(evLane, &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "commissioning_digest_mismatch"})
	enqueueEnvelopes(t, q, "dev-a", 1, 2)
	worker := laneWorker(t, q, authority, nil, 10*time.Millisecond)
	startWorker(t, worker)
	waitFor(t, "the evidence lane held", func() bool { e, ok := laneEntry(worker, evLane); return ok && e.State == DeviceHeld })
	time.Sleep(30 * time.Millisecond)
	if len(q.Archived()) != 0 || q.LenLane(evLane) != 2 || authority.count(evLane) != 1 {
		t.Fatalf("archived %d, queued %d, attempts %d", len(q.Archived()), q.LenLane(evLane), authority.count(evLane))
	}
}
