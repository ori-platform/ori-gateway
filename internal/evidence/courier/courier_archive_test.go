// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"encoding/json"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

// No release writes an archive record: nothing leaves custody without a
// verified retention (evidence-transport/v2, refusal policy). A file named as
// one is an unexpected entry, and the queue refuses to open rather than
// release the active record it names.
func TestAnArchiveRecordNoReleaseWritesRefusesTheQueue(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 100, 1<<24)
	entry, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := json.Marshal(map[string]any{
		"v": 1, "queue_record": entry.ID, "queue_seq": 1, "device_id": "dev-a",
		"artifact_type": entry.Type, "artifact_digest": payloadDigest(entry.Payload), "payload": entry.Payload,
		"refusal_status": 422, "reason": "commissioning_digest_mismatch", "refused_at_ms": 1787000000000,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".ori-evidence-archive-"+entry.ID), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 24}); err == nil {
		t.Fatal("the queue opened with an archive record beside the record it names")
	}
	if _, err := os.Stat(filepath.Join(dir, entry.ID+queueFileSuffix)); err != nil {
		t.Fatalf("the active record was released: %v", err)
	}
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
			if q.LenLane(lane) != 2 || authority.count(lane) != 1 {
				t.Fatalf("queued %d, attempts %d; want the held head kept and sent once", q.LenLane(lane), authority.count(lane))
			}

			q = openTestQueueAt(t, dir, 100, 1<<24)
			worker = laneWorker(t, q, authority, nil, 10*time.Millisecond)
			running = startWorker(t, worker)
			waitFor(t, "the hold restored", func() bool { e, ok := laneEntry(worker, lane); return ok && e.State == DeviceHeld })
			time.Sleep(30 * time.Millisecond)
			running.stop()
			if authority.count(lane) != 1 || q.LenLane(lane) != 2 {
				t.Fatalf("after a restart: attempts %d, queued %d", authority.count(lane), q.LenLane(lane))
			}
		})
	}
}
