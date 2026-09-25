// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

const restoredMessage = "evidence delivery blocked: restored the hold on a permanently refused artifact without sending it"

// heldQueue runs a worker until the head is refused with 409 conflict and held,
// then stops it. It returns the queue directory and the refused entry.
func heldQueue(t *testing.T, authority *fakeAuthority) (string, QueuedArtifact, HoldRecord) {
	t.Helper()
	dir := filepath.Join(t.TempDir(), "queue")
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
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	running := startWorker(t, worker)
	waitFor(t, "the hold", func() bool { return worker.Status().Blocked })
	running.stop()
	hold, ok := q.HoldFor(entry.ID)
	if !ok {
		t.Fatal("the hold was not persisted")
	}
	return dir, entry, hold
}

func TestRestartRestoresHoldWithoutSendingIt(t *testing.T) {
	authority := newFakeAuthority()
	dir, entry, hold := heldQueue(t, authority)
	if hold.Reason != "conflict" || hold.RefusalStatus != http.StatusConflict || hold.Acknowledged ||
		hold.ArtifactDigest != payloadDigest(entry.Payload) || hold.QueueRecord != entry.ID {
		t.Fatalf("persisted hold = %#v", hold)
	}
	time.Sleep(5 * time.Millisecond)

	q := openTestQueueAt(t, dir, 100, 1<<20)
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, BlockedReminderInterval: 5 * time.Millisecond,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	// Blocked before Run: health is right from startup.
	status := worker.Status()
	if !status.Blocked || !status.Degraded || status.LastError != "channel_permanent_refusal" || status.Held == nil {
		t.Fatalf("restored status = %#v", status)
	}
	first := time.UnixMilli(hold.FirstHeldAtMS)
	if !status.Held.FirstHeldAt.Equal(first) || !status.LastFailureAt.Equal(first) ||
		status.Held.ArtifactDigest != hold.ArtifactDigest || status.Held.Reason != "conflict" || !status.Held.Persisted {
		t.Fatalf("restored hold = %#v, want first held at %v", status.Held, first)
	}
	startWorker(t, worker)
	worker.NotifyDevice("site-a-edge-01")
	waitFor(t, "a reminder", func() bool { return len(logs.records(t, reminderMessage)) >= 2 })
	if got := authority.attemptsFor(hold.ArtifactDigest); got != 1 {
		t.Fatalf("restart sent the held artifact again: %d attempts", got)
	}
	restored := logs.records(t, restoredMessage)
	if len(restored) != 1 || restored[0]["artifact_digest"] != hold.ArtifactDigest || restored[0]["reason"] != "conflict" {
		t.Fatalf("restore lines = %v", restored)
	}
	if n := len(logs.records(t, blockedMessage)); n != 0 {
		t.Fatalf("restore was logged as a fresh refusal %d times", n)
	}
}

func TestQueueWithoutHoldsLoadsAndSendsTheHeadOnce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 100, 1<<20)
	refused := checkpointBytes(1)
	if _, err := q.Enqueue(ArtifactCheckpoint, refused); err != nil {
		t.Fatal(err)
	}
	// A queue written before refused-head records existed: records only.
	reopened := openTestQueueAt(t, dir, 100, 1<<20)
	authority := newFakeAuthority()
	authority.setRefusal(payloadDigest(refused), &fakeRefusal{status: http.StatusConflict, reason: "conflict"})
	worker, err := NewDeliveryWorker(reopened, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	if worker.Status().Blocked {
		t.Fatal("a queue with no refused-head record started blocked")
	}
	startWorker(t, worker)
	waitFor(t, "the hold", func() bool { return worker.Status().Blocked })
	if got := authority.attemptsFor(payloadDigest(refused)); got != 1 {
		t.Fatalf("attempts = %d, want 1", got)
	}
}

func TestInvalidHoldRefusesTheQueueAndKeepsTheRecord(t *testing.T) {
	for _, tc := range []struct {
		name   string
		mutate func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord)
		want   string
	}{
		{"corrupt", func(t *testing.T, dir string, entry QueuedArtifact, _ HoldRecord) {
			writeHold(t, dir, entry.ID, []byte("{not json"))
		}, "corrupt refused-head record"},
		{"unknown field", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, append(raw[:len(raw)-1], []byte(`,"extra":1}`)...))
		}, "corrupt refused-head record"},
		{"mismatched digest", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			hold.ArtifactDigest = payloadDigest([]byte("other bytes"))
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, raw)
		}, "does not match its queue record"},
		{"mismatched type", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			hold.ArtifactType = ArtifactDeliveryEnvelope
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, raw)
		}, "does not match its queue record"},
		{"reason not contract-owned", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			hold.Reason = "authority.internal"
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, raw)
		}, "does not match its queue record"},
		{"reason its status does not admit", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			hold.Reason = "unknown_key" // admitted by 422 and 401, never by 409
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, raw)
		}, "does not match its queue record"},
		{"a handoff reason recorded as a hold", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			hold.RefusalStatus, hold.Reason = http.StatusUnprocessableEntity, "unknown_key" // 422 unknown_key never holds
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, raw)
		}, "does not match its queue record"},
		{"status that cannot hold", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			hold.RefusalStatus = http.StatusUnauthorized
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, raw)
		}, "does not match its queue record"},
		{"names another record", func(t *testing.T, dir string, entry QueuedArtifact, hold HoldRecord) {
			hold.QueueRecord = strings.Repeat("0", 64)
			raw, _ := json.Marshal(hold)
			writeHold(t, dir, entry.ID, raw)
		}, "names queue record"},
		{"orphaned", func(t *testing.T, dir string, entry QueuedArtifact, _ HoldRecord) {
			if err := os.Rename(filepath.Join(dir, entry.ID+".json"), filepath.Join(t.TempDir(), entry.ID+".json")); err != nil {
				t.Fatal(err)
			}
		}, "has no queue record"},
		{"not private", func(t *testing.T, dir string, entry QueuedArtifact, _ HoldRecord) {
			if err := os.Chmod(filepath.Join(dir, holdFilePrefix+entry.ID), 0o644); err != nil {
				t.Fatal(err)
			}
		}, "permissions are not private"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			dir, entry, hold := heldQueue(t, newFakeAuthority())
			tc.mutate(t, dir, entry, hold)
			_, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 100, MaxBytes: 1 << 20})
			if err == nil || !strings.Contains(err.Error(), tc.want) || !strings.Contains(err.Error(), entry.ID) {
				t.Fatalf("open = %v, want refusal naming %s containing %q", err, entry.ID, tc.want)
			}
			if tc.name != "orphaned" {
				if _, statErr := os.Stat(filepath.Join(dir, entry.ID+".json")); statErr != nil {
					t.Fatal("the queue record was not kept")
				}
			}
			if _, statErr := os.Stat(filepath.Join(dir, holdFilePrefix+entry.ID)); statErr != nil {
				t.Fatal("the refused-head record was dropped")
			}
		})
	}
}

func writeHold(t *testing.T, dir, id string, raw []byte) {
	t.Helper()
	if err := os.WriteFile(filepath.Join(dir, holdFilePrefix+id), raw, 0o600); err != nil {
		t.Fatal(err)
	}
}

func TestHoldIsPrivateAtomicAndClearedWithItsRecord(t *testing.T) {
	dir, entry, _ := heldQueue(t, newFakeAuthority())
	info, err := os.Stat(filepath.Join(dir, holdFilePrefix+entry.ID))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("refused-head record mode = %v, %v", info, err)
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	for _, e := range entries {
		if strings.HasPrefix(e.Name(), queueTempPrefix) {
			t.Fatalf("temporary file left behind: %s", e.Name())
		}
	}
	q := openTestQueueAt(t, dir, 100, 1<<20)
	if err := q.Remove(entry.ID); err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(filepath.Join(dir, holdFilePrefix+entry.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retiring the record left its refused-head record: %v", err)
	}
	if _, ok := q.HoldFor(entry.ID); ok {
		t.Fatal("retired record still reported as held")
	}
	openTestQueueAt(t, dir, 100, 1<<20)
}

// stallOutcomes is every non-accepting answer the worker can meet, driven
// through the real HTTP channel.
var stallOutcomes = []struct {
	name    string
	status  int
	ctype   string
	body    string
	digest  bool
	header  http.Header
	hang    bool
	wantMsg string
}{
	{"409 conflict holds", 409, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"refused","reason":"conflict","retriable":false,"v":1}`, true, nil, false, blockedMessage},
	{"422 byte reason holds", 422, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"refused","reason":"bad_authenticator","retriable":false,"v":1}`, true, nil, false, blockedMessage},
	{"400 without digest holds", 400, "application/json", `{"authority_artifacts":[],"outcome":"refused","reason":"malformed","retriable":false,"v":1}`, false, nil, false, blockedMessage},
	{"422 unknown_key waits for handoff", 422, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"refused","reason":"unknown_key","retriable":false,"v":1}`, true, nil, false, stallMessage},
	{"401 waits for handoff", 401, "application/json", `{"authority_artifacts":[],"outcome":"refused","reason":"stale","retriable":false,"v":1}`, false, nil, false, stallMessage},
	{"401 retriable waits for handoff", 401, "application/json", `{"authority_artifacts":[],"outcome":"refused","reason":"stale","retriable":true,"v":1}`, false, nil, false, stallMessage},
	{"403 waits for handoff", 403, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"refused","reason":"not_authorized","retriable":false,"v":1}`, true, nil, false, stallMessage},
	{"429", 429, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"pending","reason":"rate_limited","retriable":true,"v":1}`, true, http.Header{"Retry-After": []string{"1"}}, false, stallMessage},
	{"503", 503, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"pending","reason":"unavailable","retriable":true,"v":1}`, true, nil, false, stallMessage},
	{"wrong content type", 200, "text/html", `<html>`, false, nil, false, stallMessage},
	{"oversized body", 200, "application/json", strings.Repeat(" ", maxResponseBytes+2), false, nil, false, stallMessage},
	{"hanging response", 200, "application/json", ``, false, nil, true, stallMessage},
	{"200 with a refusal body", 200, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"refused","reason":"conflict","retriable":false,"v":1}`, true, nil, false, stallMessage},
	{"422 with no reason holds as unrecognised", 422, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"refused","reason":"","retriable":false,"v":1}`, true, nil, false, blockedMessage},
	{"refused outcome under 503", 503, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"refused","reason":"unavailable","retriable":true,"v":1}`, true, nil, false, stallMessage},
	{"unknown status", 500, "application/json", `{"artifact_digest":%q,"authority_artifacts":[],"outcome":"pending","reason":"busy","retriable":true,"v":1}`, true, nil, false, stallMessage},
}

func TestEveryNonAcceptingOutcomeIsLogged(t *testing.T) {
	for _, tc := range stallOutcomes {
		t.Run(tc.name, func(t *testing.T) {
			q := openTestQueue(t, 10, 1<<20)
			payload := checkpointBytes(1)
			if _, err := q.Enqueue(ArtifactCheckpoint, payload); err != nil {
				t.Fatal(err)
			}
			digest := payloadDigest(payload)
			body := tc.body
			if strings.Contains(body, "%q") {
				body = strings.Replace(body, "%q", `"`+digest+`"`, 1)
			}
			client := &http.Client{Timeout: 20 * time.Millisecond, Transport: roundTripFunc(func(req *http.Request) (*http.Response, error) {
				if tc.hang {
					<-req.Context().Done()
					return nil, req.Context().Err()
				}
				header := http.Header{"Content-Type": []string{tc.ctype}}
				for k, v := range tc.header {
					header[k] = v
				}
				return &http.Response{StatusCode: tc.status, Header: header, Body: io.NopCloser(strings.NewReader(body))}, nil
			})}
			channel, err := NewHTTPChannel(HTTPChannelOptions{
				Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
				Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: client,
			})
			if err != nil {
				t.Fatal(err)
			}
			logs := &syncBuffer{}
			worker, err := NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
				RetryInterval: time.Millisecond, BlockedReminderInterval: time.Hour,
				Logger: slog.New(slog.NewJSONHandler(logs, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			startWorker(t, worker)
			waitFor(t, "a log line for "+tc.name, func() bool { return len(logs.records(t, tc.wantMsg)) >= 1 })
			// Let an interval failure repeat many times: it still enters once.
			time.Sleep(60 * time.Millisecond)
			lines := logs.records(t, tc.wantMsg)
			if len(lines) != 1 {
				t.Fatalf("%q logged %d times, want once on entry", tc.wantMsg, len(lines))
			}
			if lines[0]["artifact_digest"] != digest {
				t.Fatalf("line = %v", lines[0])
			}
			if strings.Contains(logs.String(), "authority.invalid") {
				t.Fatal("log names the evidence authority's endpoint")
			}
		})
	}
}

func TestHandoffStallRemindsWithoutRetrying(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	courier, err := NewCourier(q, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := newFakeAuthority()
	head := checkpointBytes(1)
	authority.setRefusal(payloadDigest(head), &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "unknown_key"})
	if _, err := courier.Admit(ArtifactCheckpoint, head); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, BlockedReminderInterval: 10 * time.Millisecond,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	waitFor(t, "reminders", func() bool { return len(logs.records(t, stallReminderMessage)) >= 3 })
	if got := authority.attemptsFor(payloadDigest(head)); got != 1 {
		t.Fatalf("receiver-state head retried by the reminder timer: %d attempts", got)
	}
	reminder := logs.records(t, stallReminderMessage)[0]
	if reminder["reason"] != "unknown_key" || reminder["retry"] != "next_handoff" || reminder["artifact_digest"] != payloadDigest(head) {
		t.Fatalf("reminder = %v", reminder)
	}
	authority.setRefusal(payloadDigest(head), nil)
	handoff(t, courier, worker, checkpointBytes(2))
	waitFor(t, "the drain", func() bool { return q.Len() == 0 })
	if resumed := logs.records(t, resumedMessage); len(resumed) != 1 {
		t.Fatalf("resumed lines = %v", resumed)
	}
	if n := len(logs.records(t, stallMessage)); n != 1 {
		t.Fatalf("stall entry logged %d times, want once", n)
	}
}

func TestIntervalStallLogsOnceThenOnTheReminderInterval(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, checkpointBytes(1)); err != nil {
		t.Fatal(err)
	}
	channel := &fakeEvidenceChannel{err: errors.New("dial tcp authority.invalid:443: refused")}
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, channel, nil, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, BlockedReminderInterval: 30 * time.Millisecond,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	running := startWorker(t, worker)
	time.Sleep(100 * time.Millisecond)
	running.stop()
	entries, reminders := len(logs.records(t, stallMessage)), len(logs.records(t, stallReminderMessage))
	if entries != 1 || reminders < 1 || reminders > 4 {
		t.Fatalf("entry lines %d, reminders %d over ~100 attempts: want 1 and a few", entries, reminders)
	}
	if strings.Contains(logs.String(), "authority.invalid") {
		t.Fatal("transport error text reached the log")
	}
}

func TestHandoffsFasterThanTheReminderDoNotSuppressIt(t *testing.T) {
	q := openTestQueue(t, 1000, 1<<24)
	courier, err := NewCourier(q, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := newFakeAuthority()
	head := checkpointBytes(0)
	authority.setRefusal(payloadDigest(head), &fakeRefusal{status: http.StatusConflict, reason: "conflict"})
	if _, err := courier.Admit(ArtifactCheckpoint, head); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, BlockedReminderInterval: 40 * time.Millisecond,
		Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	waitFor(t, "the hold", func() bool { return worker.Status().Blocked })
	ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
	defer cancel()
	for i := 1; ctx.Err() == nil; i++ {
		handoff(t, courier, worker, checkpointBytes(i))
		time.Sleep(5 * time.Millisecond)
	}
	if n := len(logs.records(t, reminderMessage)); n < 2 {
		t.Fatalf("handoffs every 5ms suppressed the 40ms reminder: %d reminders in 200ms", n)
	}
}
