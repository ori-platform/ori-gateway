// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"errors"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"testing"
	"time"
)

const notPersistedMessage = "evidence delivery hold was not persisted: a restart will send the refused artifact once more"

// refusingQueue queues one artifact the fake authority refuses with 409 and one
// behind it, in dir.
func refusingQueue(t *testing.T, dir string, authority *fakeAuthority) (*DurableQueue, QueuedArtifact) {
	t.Helper()
	q := openTestQueueAt(t, dir, 100, 1<<20)
	refused := checkpointBytes(1)
	authority.setRefusal(payloadDigest(refused), &fakeRefusal{status: http.StatusConflict, reason: "conflict"})
	entry, err := q.Enqueue(ArtifactCheckpoint, refused)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ArtifactCheckpoint, checkpointBytes(2)); err != nil {
		t.Fatal(err)
	}
	return q, entry
}

// unwritable makes dir refuse new entries until the test ends, so the hold
// write fails the way a full or read-only filesystem would.
func unwritable(t *testing.T, dir string) func() {
	t.Helper()
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	probe, err := os.CreateTemp(dir, "probe-")
	if err == nil {
		_ = probe.Close()
		_ = os.Remove(probe.Name())
		_ = os.Chmod(dir, 0o700)
		t.Skip("directory permissions do not stop writes here (running as root?)")
	}
	restore := func() { _ = os.Chmod(dir, 0o700) }
	t.Cleanup(restore)
	return restore
}

func TestFailedHoldWriteNeverResendsInProcess(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	authority := newFakeAuthority()
	q, entry := refusingQueue(t, dir, authority)
	unwritable(t, dir)

	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, BlockedReminderInterval: 5 * time.Millisecond,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	waitFor(t, "the hold", func() bool { return worker.Status().Blocked })
	for i := 0; i < 10; i++ {
		worker.NotifyDevice("site-a-edge-01")
		time.Sleep(2 * time.Millisecond)
	}
	if got := authority.attemptsFor(payloadDigest(entry.Payload)); got != 1 {
		t.Fatalf("an unpersisted hold was re-sent in process: %d attempts", got)
	}
	waitFor(t, "reminders", func() bool { return len(logs.records(t, reminderMessage)) >= 3 })
	if got := authority.attemptsFor(payloadDigest(entry.Payload)); got != 1 {
		t.Fatalf("an unpersisted hold was re-sent by a reminder: %d attempts", got)
	}
	status := worker.Status()
	if !status.Blocked || status.Held == nil || status.Held.Persisted {
		t.Fatalf("status = %#v, held = %#v", status, status.Held)
	}
	lines := logs.records(t, notPersistedMessage)
	if len(lines) != 1 || lines[0]["artifact_digest"] != payloadDigest(entry.Payload) || lines[0]["hold_persisted"] != false {
		t.Fatalf("not-persisted lines = %v", lines)
	}
	if _, ok := q.HoldFor(entry.ID); ok {
		t.Fatal("queue reports a hold that was never written")
	}
	if _, err := os.Stat(filepath.Join(dir, holdFilePrefix+entry.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("hold file present after a failed write: %v", err)
	}
}

func TestCrashBeforeDurableHoldSendsAtMostOnceMore(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	authority := newFakeAuthority()
	q, entry := refusingQueue(t, dir, authority)
	digest := payloadDigest(entry.Payload)
	options := DeliveryWorkerOptions{RetryInterval: time.Millisecond, Logger: slog.New(slog.DiscardHandler)}

	// First process: refused, but the hold never reaches the disk.
	restore := unwritable(t, dir)
	first, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, options)
	if err != nil {
		t.Fatal(err)
	}
	running := startWorker(t, first)
	waitFor(t, "the first refusal", func() bool { return first.Status().Blocked })
	running.stop()
	restore()

	// Second process: nothing durable says the head is held, so it is sent
	// exactly once more, refused again, and this time held durably.
	second, err := NewDeliveryWorker(openTestQueueAt(t, dir, 100, 1<<20), fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, options)
	if err != nil {
		t.Fatal(err)
	}
	if second.Status().Blocked {
		t.Fatal("started blocked with no durable hold")
	}
	running = startWorker(t, second)
	waitFor(t, "the second refusal", func() bool { return second.Status().Blocked })
	for i := 0; i < 5; i++ {
		second.NotifyDevice("site-a-edge-01")
	}
	time.Sleep(20 * time.Millisecond)
	running.stop()
	if got := authority.attemptsFor(digest); got != 2 {
		t.Fatalf("attempts after one crash inside the window = %d, want exactly 2", got)
	}
	if st := second.Status(); st.Held == nil || !st.Held.Persisted {
		t.Fatalf("second refusal was not held durably: %#v", st.Held)
	}

	// Third process: the hold is durable, so nothing is sent.
	third, err := NewDeliveryWorker(openTestQueueAt(t, dir, 100, 1<<20), fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, options)
	if err != nil {
		t.Fatal(err)
	}
	if !third.Status().Blocked {
		t.Fatal("third start did not restore the hold")
	}
	startWorker(t, third)
	for i := 0; i < 5; i++ {
		third.NotifyDevice("site-a-edge-01")
	}
	time.Sleep(20 * time.Millisecond)
	if got := authority.attemptsFor(digest); got != 2 {
		t.Fatalf("a durable hold was re-sent on the third start: %d attempts", got)
	}
	if got := len(authority.accepted()); got != 0 {
		t.Fatalf("delivery continued past the held head: %d accepted", got)
	}
}

func TestEachProcessWithoutADurableHoldSendsOnceUntilOneIsWritten(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	authority := newFakeAuthority()
	_, entry := refusingQueue(t, dir, authority)
	digest := payloadDigest(entry.Payload)
	options := DeliveryWorkerOptions{RetryInterval: time.Millisecond, Logger: slog.New(slog.DiscardHandler)}

	runProcess := func(wantAttempts int, wantPersisted bool) {
		t.Helper()
		worker, err := NewDeliveryWorker(openTestQueueAt(t, dir, 100, 1<<20), fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, options)
		if err != nil {
			t.Fatal(err)
		}
		running := startWorker(t, worker)
		waitFor(t, "the hold", func() bool { return worker.Status().Blocked })
		for i := 0; i < 5; i++ {
			worker.NotifyDevice("site-a-edge-01")
		}
		time.Sleep(20 * time.Millisecond)
		running.stop()
		if got := authority.attemptsFor(digest); got != wantAttempts {
			t.Fatalf("after this process: %d attempts, want %d", got, wantAttempts)
		}
		if held := worker.Status().Held; held == nil || held.Persisted != wantPersisted {
			t.Fatalf("hold = %#v, want persisted=%v", held, wantPersisted)
		}
	}

	// Two processes in a row whose hold writes fail: each sends once.
	restore := unwritable(t, dir)
	runProcess(1, false)
	runProcess(2, false)
	restore()
	// A healthy process sends once more and writes the hold.
	runProcess(3, true)
	// Once the hold is durable, no later start sends it.
	runProcess(3, true)
	runProcess(3, true)
}

func TestLeftoverHoldTempFileIsCleanedNotTrusted(t *testing.T) {
	authority := newFakeAuthority()
	dir, entry, hold := heldQueue(t, authority)

	// A crash after the temp file was written and before the rename: the
	// complete hold bytes sit under the temp name, and no hold exists under
	// its own name. One variant is complete, one is torn.
	raw := []byte(`{"v":1,"queue_record":"` + hold.QueueRecord + `","artifact_digest":"` + hold.ArtifactDigest +
		`","artifact_type":"checkpoint","refusal_status":409,"reason":"conflict","first_held_at_ms":1,"acknowledged":false}`)
	if err := os.Remove(filepath.Join(dir, holdFilePrefix+entry.ID)); err != nil {
		t.Fatal(err)
	}
	for name, body := range map[string][]byte{
		queueTempPrefix + "1234567": raw,
		queueTempPrefix + "7654321": raw[:len(raw)/2],
	} {
		if err := os.WriteFile(filepath.Join(dir, name), body, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	q, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatalf("a crash while writing a hold stopped the queue from opening: %v", err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if len(e.Name()) >= len(queueTempPrefix) && e.Name()[:len(queueTempPrefix)] == queueTempPrefix {
			t.Fatalf("leftover temp file survived open: %s", e.Name())
		}
	}
	if _, ok := q.HoldFor(entry.ID); ok {
		t.Fatal("a temp file was read as a hold")
	}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status().Blocked {
		t.Fatal("started blocked from a temp file")
	}
	startWorker(t, worker)
	waitFor(t, "the refusal after the crash", func() bool { return worker.Status().Blocked })
	if got := authority.attemptsFor(hold.ArtifactDigest); got != 2 {
		t.Fatalf("attempts = %d, want the one before the crash and one after", got)
	}
}
