// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"errors"
	"os"
	"path/filepath"
	"strconv"
	"testing"
	"time"
)

func openTestQueue(t *testing.T, maxItems int, maxBytes int64) *DurableQueue {
	t.Helper()
	return openTestQueueAt(t, filepath.Join(t.TempDir(), "queue"), maxItems, maxBytes)
}

func openTestQueueAt(t *testing.T, dir string, maxItems int, maxBytes int64) *DurableQueue {
	t.Helper()
	q, err := OpenDurableQueue(QueueOptions{
		Directory: dir,
		MaxItems:  maxItems,
		MaxBytes:  maxBytes,
		Now:       func() time.Time { return time.UnixMilli(1787000000900) },
	})
	if err != nil {
		t.Fatal(err)
	}
	return q
}

func validEnvelopeBytes(seq int) []byte {
	return []byte(`{"v":1,"device_id":"site-a-edge-01","local_seq":` +
		strconv.Itoa(seq) +
		`,"signature":"ed25519:opaque"}`)
}

func TestEnvelopePassthroughUnaltered(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	payload := []byte("{\n  \"v\": 1, \"device_id\": \"dev-01\", \"local_seq\": 1, \"signature\": \"ed25519:x\"\n}")
	entry, err := q.Enqueue(ArtifactDeliveryEnvelope, payload)
	if err != nil {
		t.Fatal(err)
	}
	got, ok := q.Peek()
	if !ok {
		t.Fatal("queue is empty after enqueue")
	}
	if entry.ID != got.ID || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("queued bytes changed\n got %q\nwant %q", got.Payload, payload)
	}
}

func TestCustodySurvivesRestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	payload := validEnvelopeBytes(1)
	first := openTestQueueAt(t, dir, 10, 1<<20)
	entry, err := first.Enqueue(ArtifactDeliveryEnvelope, payload)
	if err != nil {
		t.Fatal(err)
	}

	reopened := openTestQueueAt(t, dir, 10, 1<<20)
	got, ok := reopened.Peek()
	if !ok {
		t.Fatal("acknowledged artifact disappeared across restart")
	}
	if got.ID != entry.ID || got.EnqueuedAtMS != entry.EnqueuedAtMS || !bytes.Equal(got.Payload, payload) {
		t.Fatalf("reopened entry differs: %#v", got)
	}
}

func TestQueueExhaustionRefusesExplicitly(t *testing.T) {
	q := openTestQueue(t, 1, 1<<20)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
		t.Fatal(err)
	}
	_, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(2))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("queue exhaustion = %v, want ErrQueueFull", err)
	}
	var full *QueueFullError
	if !errors.As(err, &full) || full.Limit != "item" {
		t.Fatalf("queue exhaustion did not name the item bound: %v", err)
	}
}

func TestQueueByteBoundIncludesDurableRecordOverhead(t *testing.T) {
	q := openTestQueue(t, 10, 1)
	_, err := q.Enqueue(ArtifactCheckpoint, []byte(`{"v":1,"device_id":"d"}`))
	if !errors.Is(err, ErrQueueFull) {
		t.Fatalf("byte exhaustion = %v, want ErrQueueFull", err)
	}
}

func TestDuplicateAdmissionIsIdempotentEvenWhenFull(t *testing.T) {
	q := openTestQueue(t, 1, 1<<20)
	payload := validEnvelopeBytes(1)
	first, err := q.Enqueue(ArtifactDeliveryEnvelope, payload)
	if err != nil {
		t.Fatal(err)
	}
	second, err := q.Enqueue(ArtifactDeliveryEnvelope, payload)
	if err != nil {
		t.Fatalf("idempotent retry refused by full queue: %v", err)
	}
	if first.ID != second.ID || q.Len() != 1 {
		t.Fatalf("duplicate consumed queue capacity: first=%s second=%s len=%d", first.ID, second.ID, q.Len())
	}
}

func TestQueuePreservesAdmissionOrderWhenTimestampsCollide(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20) // fixed clock: every entry has one timestamp
	first, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(2))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
		t.Fatal(err)
	}
	got, ok := q.Peek()
	if !ok || got.ID != first.ID {
		t.Fatalf("same-millisecond entries were reordered: got=%s want=%s", got.ID, first.ID)
	}
}

func TestCorruptRecordFailsRestartRatherThanDisappearing(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 10, 1<<20)
	entry, err := q.Enqueue(ArtifactDeliveryEnvelope, validEnvelopeBytes(1))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(q.recordPath(entry.ID), []byte("not-json"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 20}); err == nil {
		t.Fatal("corrupt acknowledged record was silently skipped")
	}
}

func TestQueueRefusesPublicDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	if err := os.Mkdir(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o755); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 1, MaxBytes: 1024}); err == nil {
		t.Fatal("public queue directory was accepted")
	}
}

func TestQueueRefusesToAdoptAnUnmarkedPrivateDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "not-a-queue")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 1, MaxBytes: 1024}); err == nil {
		t.Fatal("an arbitrary private directory was adopted as a queue")
	}
}

func TestIncompleteTempWriteIsNotRecoveredAsCustody(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 10, 1<<20)
	if err := os.WriteFile(filepath.Join(dir, queueTempPrefix+"crash"), []byte("partial"), 0o600); err != nil {
		t.Fatal(err)
	}
	reopened := openTestQueueAt(t, dir, 10, 1<<20)
	if reopened.Len() != 0 || q.Len() != 0 {
		t.Fatal("uncommitted temp write became a queued artifact")
	}
}
