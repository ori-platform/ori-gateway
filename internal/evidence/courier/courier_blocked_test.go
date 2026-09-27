// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/refusal"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"
)

const (
	blockedMessage   = "evidence delivery blocked: the evidence authority permanently refused the queue head"
	reminderMessage  = "evidence delivery still blocked behind a permanently refused artifact"
	unblockedMessage = "evidence delivery unblocked: the refused artifact is no longer at the queue head"
	fakeAuthorityURL = "https://authority.invalid/v1/evidence/artifacts"
)

type syncBuffer struct {
	mu  sync.Mutex
	buf bytes.Buffer
}

func (b *syncBuffer) Write(p []byte) (int, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.Write(p)
}

func (b *syncBuffer) String() string {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.buf.String()
}

// records returns every JSON log record whose message is msg.
func (b *syncBuffer) records(t *testing.T, msg string) []map[string]any {
	t.Helper()
	var out []map[string]any
	scanner := bufio.NewScanner(strings.NewReader(b.String()))
	for scanner.Scan() {
		var record map[string]any
		if err := json.Unmarshal(scanner.Bytes(), &record); err != nil {
			t.Fatalf("log line is not JSON: %q", scanner.Text())
		}
		if record["msg"] == msg {
			out = append(out, record)
		}
	}
	return out
}

// fakeAuthority answers the real HTTPChannel as the evidence authority does:
// a refusal for the digests in refuse, and acceptance for everything else.
type fakeAuthority struct {
	mu       sync.Mutex
	refuse   map[string]fakeRefusal
	attempts map[string]int
	order    []string
}

type fakeRefusal struct {
	status int
	reason string
}

func newFakeAuthority() *fakeAuthority {
	return &fakeAuthority{refuse: map[string]fakeRefusal{}, attempts: map[string]int{}}
}

func (a *fakeAuthority) setRefusal(digest string, refusal *fakeRefusal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if refusal == nil {
		delete(a.refuse, digest)
		return
	}
	a.refuse[digest] = *refusal
}

func (a *fakeAuthority) attemptsFor(digest string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.attempts[digest]
}

func (a *fakeAuthority) accepted() []string {
	a.mu.Lock()
	defer a.mu.Unlock()
	var out []string
	for _, digest := range a.order {
		if _, refused := a.refuse[digest]; !refused {
			out = append(out, digest)
		}
	}
	return out
}

func (a *fakeAuthority) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	digest := payloadDigest(body)
	a.mu.Lock()
	a.attempts[digest]++
	a.order = append(a.order, digest)
	refusal, refused := a.refuse[digest]
	a.mu.Unlock()
	status, outcome, reason := http.StatusOK, "accepted", ""
	if refused {
		status, outcome, reason = refusal.status, "refused", refusal.reason
	}
	respBody := fmt.Sprintf(
		`{"artifact_digest":%q,"authority_artifacts":[],"outcome":%q,"reason":%q,"retriable":false,"v":1}`,
		digest, outcome, reason,
	)
	return &http.Response{
		StatusCode: status,
		Header:     http.Header{"Content-Type": []string{"application/json"}},
		Body:       io.NopCloser(strings.NewReader(respBody)),
	}, nil
}

func fakeAuthorityChannel(t *testing.T, authority *fakeAuthority) *HTTPChannel {
	t.Helper()
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint:   fakeAuthorityURL,
		ClientID:   "site-a-gateway",
		Secret:     "evidence-ingest-secret-with-at-least-32-bytes",
		HTTPClient: &http.Client{Transport: authority},
	})
	if err != nil {
		t.Fatal(err)
	}
	return channel
}

func checkpointBytes(highWater int) []byte {
	return fmt.Appendf(nil, `{"v":1,"device_id":"site-a-edge-01","high_water_seq":%d,"signature":"ed25519:opaque"}`, highWater)
}

type runningWorker struct {
	cancel context.CancelFunc
	done   chan error
	once   sync.Once
}

func startWorker(t *testing.T, worker *DeliveryWorker) *runningWorker {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	running := &runningWorker{cancel: cancel, done: make(chan error, 1)}
	go func() { running.done <- worker.Run(ctx) }()
	t.Cleanup(running.stop)
	return running
}

func (r *runningWorker) stop() {
	r.once.Do(func() {
		r.cancel()
		<-r.done
	})
}

func waitFor(t *testing.T, what string, cond func() bool) {
	t.Helper()
	deadline := time.Now().Add(2 * time.Second)
	for !cond() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(time.Millisecond)
	}
}

// handoff admits one artifact the way the runtime ingress does and wakes the
// worker, as the outbound subscription does after every admission.
func handoff(t *testing.T, courier *Courier, worker *DeliveryWorker, payload []byte) QueuedArtifact {
	t.Helper()
	admission, err := courier.Admit(ArtifactCheckpoint, payload)
	if err != nil {
		t.Fatal(err)
	}
	worker.NotifyDevice("site-a-edge-01")
	return admission.Queued
}

func TestTerminalRefusalIsAttemptedOnceHoldsTheQueueAndIsLoggedOnce(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 100, 1<<20)
	courier, err := NewCourier(q, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := newFakeAuthority()
	refused := checkpointBytes(1)
	refusedDigest := payloadDigest(refused)
	authority.setRefusal(refusedDigest, &fakeRefusal{status: http.StatusConflict, reason: "conflict"})

	var later [][]byte
	refusedEntry, err := courier.Admit(ArtifactCheckpoint, refused)
	if err != nil {
		t.Fatal(err)
	}
	for i := 2; i <= 7; i++ {
		later = append(later, checkpointBytes(i))
		if _, err := courier.Admit(ArtifactCheckpoint, later[len(later)-1]); err != nil {
			t.Fatal(err)
		}
	}

	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval:           time.Millisecond,
		BlockedReminderInterval: 25 * time.Millisecond,
		Logger:                  slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	running := startWorker(t, worker)
	waitFor(t, "the refusal", func() bool { return worker.Status().Blocked })

	// Every later handoff wakes the worker. None may re-send the held head.
	for i := 8; i <= 10; i++ {
		later = append(later, checkpointBytes(i))
		handoff(t, courier, worker, later[len(later)-1])
	}
	waitFor(t, "a reminder", func() bool { return len(logs.records(t, reminderMessage)) >= 2 })

	if got := authority.attemptsFor(refusedDigest); got != 1 {
		t.Fatalf("permanently refused artifact was delivered %d times, want exactly one", got)
	}
	if accepted := authority.accepted(); len(accepted) != 0 {
		t.Fatalf("delivery continued past a held head: %v", accepted)
	}
	blocked := logs.records(t, blockedMessage)
	if len(blocked) != 1 {
		t.Fatalf("blocked transition logged %d times, want once:\n%s", len(blocked), logs.String())
	}
	if n := len(logs.records(t, restoredMessage)); n != 0 {
		t.Fatalf("the running process re-read its own hold %d times", n)
	}
	line := blocked[0]
	if line["level"] != "ERROR" || line["artifact_digest"] != refusedDigest || line["reason"] != "conflict" ||
		line["queued_behind"] != float64(6) || line["device_id"] != "site-a-edge-01" ||
		line["queue_record"] != refusedEntry.Queued.ID || line["artifact_type"] != "checkpoint" {
		t.Fatalf("blocked transition line = %v", line)
	}
	reminders := logs.records(t, reminderMessage)
	reminder := reminders[len(reminders)-1]
	if reminder["level"] != "ERROR" || reminder["artifact_digest"] != refusedDigest || reminder["queued_behind"] != float64(9) {
		t.Fatalf("reminder line = %v", reminder)
	}
	if strings.Contains(logs.String(), "authority.invalid") {
		t.Fatal("log names the evidence authority's endpoint")
	}
	status := worker.Status()
	if !status.Blocked || !status.Degraded || status.LastError != "channel_permanent_refusal" || status.Pending != 10 {
		t.Fatalf("status = %#v", status)
	}
	running.stop()

	// The documented exit: with the gateway stopped, move the refused-head
	// record and then the queue record into quarantine and start again.
	// Nothing behind it is lost.
	quarantine := t.TempDir()
	holdName := holdFilePrefix + refusedEntry.Queued.ID
	if err := os.Rename(filepath.Join(dir, holdName), filepath.Join(quarantine, holdName)); err != nil {
		t.Fatal(err)
	}
	retained := filepath.Join(quarantine, refusedEntry.Queued.ID+".json")
	if err := os.Rename(filepath.Join(dir, refusedEntry.Queued.ID+".json"), retained); err != nil {
		t.Fatal(err)
	}
	reopened := openTestQueueAt(t, dir, 100, 1<<20)
	if reopened.Len() != 9 {
		t.Fatalf("queue behind the refused head holds %d artifacts, want 9", reopened.Len())
	}
	next, err := NewDeliveryWorker(reopened, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, next)
	waitFor(t, "the queue to drain", func() bool { return reopened.Len() == 0 })
	accepted := authority.accepted()
	if len(accepted) != len(later) {
		t.Fatalf("delivered %d artifacts after the exit, want %d", len(accepted), len(later))
	}
	for i, payload := range later {
		if accepted[i] != payloadDigest(payload) {
			t.Fatalf("delivery %d was %s, want %s in admission order", i, accepted[i], payloadDigest(payload))
		}
	}
	if got := authority.attemptsFor(refusedDigest); got != 1 {
		t.Fatalf("refused artifact re-sent after the exit: %d attempts", got)
	}
	var record queueRecord
	raw, err := os.ReadFile(retained)
	if err != nil || json.Unmarshal(raw, &record) != nil || !bytes.Equal(record.Payload, refused) {
		t.Fatal("the moved record does not retain the refused artifact's exact bytes")
	}
}

func TestReceiverStateRefusalRetriesOnNextHandoffAndNeverBlocks(t *testing.T) {
	q := openTestQueue(t, 100, 1<<20)
	courier, err := NewCourier(q, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := newFakeAuthority()
	pending := checkpointBytes(1)
	pendingDigest := payloadDigest(pending)
	authority.setRefusal(pendingDigest, &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "unknown_key"})
	if _, err := courier.Admit(ArtifactCheckpoint, pending); err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval:           time.Millisecond,
		BlockedReminderInterval: time.Millisecond,
		Logger:                  slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	waitFor(t, "the first attempt", func() bool { return authority.attemptsFor(pendingDigest) == 1 })
	waitFor(t, "the refusal to be recorded", func() bool { return worker.Status().Degraded })
	// Many retry intervals pass. A receiver-state refusal is not retried on a timer.
	time.Sleep(50 * time.Millisecond)
	if got := authority.attemptsFor(pendingDigest); got != 1 {
		t.Fatalf("receiver-state refusal retried on an interval: %d attempts", got)
	}
	status := worker.Status()
	if status.Blocked || status.LastError != "channel_receiver_state_refusal" {
		t.Fatalf("receiver-state refusal status = %#v", status)
	}

	handoff(t, courier, worker, checkpointBytes(2))
	waitFor(t, "the retry on handoff", func() bool { return authority.attemptsFor(pendingDigest) == 2 })
	if worker.Status().Blocked {
		t.Fatal("a repeated receiver-state refusal marked the queue blocked")
	}

	// The receiver state is repaired elsewhere; the next handoff delivers.
	authority.setRefusal(pendingDigest, nil)
	handoff(t, courier, worker, checkpointBytes(3))
	waitFor(t, "the queue to drain", func() bool { return q.Len() == 0 })
	if got := authority.attemptsFor(pendingDigest); got != 3 {
		t.Fatalf("repaired artifact attempts = %d, want 3", got)
	}
	if status := worker.Status(); status.Blocked || status.Degraded {
		t.Fatalf("drained queue status = %#v", status)
	}
	for _, msg := range []string{blockedMessage, reminderMessage, unblockedMessage} {
		if n := len(logs.records(t, msg)); n != 0 {
			t.Fatalf("receiver-state refusal logged %q %d times", msg, n)
		}
	}
}

func TestReleasedHeldHeadLogsUnblockedOnce(t *testing.T) {
	q := openTestQueue(t, 100, 1<<20)
	courier, err := NewCourier(q, nil)
	if err != nil {
		t.Fatal(err)
	}
	authority := newFakeAuthority()
	refused := checkpointBytes(1)
	authority.setRefusal(payloadDigest(refused), &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "bad_authenticator"})
	head, err := courier.Admit(ArtifactCheckpoint, refused)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, fakeAuthorityChannel(t, authority), &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Millisecond, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	waitFor(t, "the refusal", func() bool { return worker.Status().Blocked })

	// Anything that takes the held artifact off the head releases the hold.
	if err := q.Remove(head.Queued.ID); err != nil {
		t.Fatal(err)
	}
	handoff(t, courier, worker, checkpointBytes(2))
	waitFor(t, "the next artifact", func() bool { return q.Len() == 0 })
	if worker.Status().Blocked {
		t.Fatal("hold survived its head leaving the queue")
	}
	released := logs.records(t, unblockedMessage)
	if len(released) != 1 || released[0]["artifact_digest"] != payloadDigest(refused) || released[0]["reason"] != "bad_authenticator" {
		t.Fatalf("unblocked lines = %v", released)
	}
	if len(logs.records(t, blockedMessage)) != 1 {
		t.Fatal("blocked transition was not logged exactly once")
	}
}

// contractReasons is every 422 reason evidence-transport/v1 owns, with whether
// it names receiver state. It is written out here rather than read from
// refusal.go so a reason moved between the sets fails this test.
var contractReasons = []struct {
	reason        string
	receiverState bool
}{
	{"bad_authenticator", false},
	{"binding_mismatch", false},
	{"commissioning_digest_mismatch", false},
	{"unrecognised_version", true},
	{"unknown_key", true},
}

// returnPathReasons belong to the runtime-gateway return path, not to this
// hop, and malformed bytes are a 400: at 422 each holds as unrecognised.
var returnPathReasons = []string{"wrong_purpose", "retired_key", "unknown_sequence", "non_contiguous_range", "malformed"}

func TestReturnPathReasonsHoldAsUnrecognisedAt422(t *testing.T) {
	for _, reason := range returnPathReasons {
		result := deliverRefusal(t, http.StatusUnprocessableEntity, reason)
		if result.ReceiverState || result.Retriable || result.RefusalReason != "unrecognised" {
			t.Fatalf("422 %s = %#v, want a hold recorded unrecognised", reason, result)
		}
	}
}

// hostileReasons are far-end reasons no contract owns, several shaped to pass
// a character-class check.
var hostileReasons = []string{
	"authority.internal",
	"ingest-01.evidence.example.net:8443",
	"10.0.4.17",
	"sk_" + "live_4eC39HqLyjWDarjtT1zdp7dc",
	"hkdf-sha256:0123456789abcdef0123456789abcdef",
	"conflict at https://authority.example/v1 token=abc",
	"line\nbreak",
	"MALFORMED",
	"malformed ",
	"unsupported",
	"",
}

func TestRefusalReasonVocabularyIsContractOwned(t *testing.T) {
	for _, tc := range contractReasons {
		if got := refusal.RecordedReason(http.StatusUnprocessableEntity, tc.reason); got != tc.reason {
			t.Fatalf("422 reason %q recorded as %q", tc.reason, got)
		}
		if got := refusal.RecordedReason(0, tc.reason); got != tc.reason {
			t.Fatalf("contract reason %q reduced to %q", tc.reason, got)
		}
	}
	for status, reason := range map[int]string{
		http.StatusBadRequest: "malformed", http.StatusUnauthorized: "replay", http.StatusForbidden: "not_authorized",
		http.StatusConflict: "conflict", http.StatusTooManyRequests: "rate_limited", http.StatusServiceUnavailable: "unavailable",
	} {
		if got := refusal.RecordedReason(status, reason); got != reason {
			t.Fatalf("%d %q recorded as %q", status, reason, got)
		}
	}
	// A reason is admitted only under its own status: an invalid pair never
	// looks like a valid one.
	for status, reason := range map[int]string{
		http.StatusBadRequest: "unknown_key", http.StatusConflict: "unknown_key", http.StatusForbidden: "conflict",
		http.StatusUnauthorized: "not_authorized", http.StatusTooManyRequests: "unavailable",
		http.StatusServiceUnavailable: "rate_limited", http.StatusUnprocessableEntity: "conflict",
	} {
		if got := refusal.RecordedReason(status, reason); got != "unrecognised" {
			t.Fatalf("invalid pair %d %q recorded as %q", status, reason, got)
		}
	}
	for _, status := range []int{0, 400, 401, 403, 409, 422, 429, 503} {
		for _, reason := range hostileReasons {
			if got := refusal.RecordedReason(status, reason); got != "unrecognised" {
				t.Fatalf("%d reason %q reached the log as %q", status, reason, got)
			}
		}
	}
}

func deliverRefusal(t *testing.T, status int, reason string) DeliveryResult {
	t.Helper()
	digest := payloadDigest(checkpointBytes(1))
	if status == http.StatusUnauthorized {
		digest = ""
	}
	return deliverRefusalWith(t, status, reason, digest, false)
}

func deliverRefusalWith(t *testing.T, status int, reason, digest string, retriable bool) DeliveryResult {
	t.Helper()
	payload := checkpointBytes(1)
	body, err := json.Marshal(map[string]any{
		"v": 1, "artifact_digest": digest, "outcome": "refused", "reason": reason,
		"retriable": retriable, "authority_artifacts": []any{},
	})
	if err != nil {
		t.Fatal(err)
	}
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		return &http.Response{
			StatusCode: status,
			Header:     http.Header{"Content-Type": []string{"application/json"}},
			Body:       io.NopCloser(bytes.NewReader(body)),
		}, nil
	})}
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	result, err := channel.Deliver(context.Background(), QueuedArtifact{Type: ArtifactCheckpoint, Payload: payload})
	if err != nil {
		t.Fatalf("status %d reason %q: %v", status, reason, err)
	}
	if result.Accepted || result.Retriable {
		t.Fatalf("status %d reason %q: result = %#v", status, reason, result)
	}
	return result
}

func TestHTTPChannelClassifiesRefusalsByContractReason(t *testing.T) {
	// 422 is decided by reason.
	for _, tc := range contractReasons {
		result := deliverRefusal(t, http.StatusUnprocessableEntity, tc.reason)
		if result.ReceiverState != tc.receiverState || result.RefusalReason != tc.reason {
			t.Fatalf("422 %s: receiver state %v reason %q, want %v", tc.reason, result.ReceiverState, result.RefusalReason, tc.receiverState)
		}
	}
	// 400 always holds; only malformed is a 400 reason.
	for _, tc := range append(contractReasons, struct {
		reason        string
		receiverState bool
	}{"malformed", false}) {
		result := deliverRefusal(t, http.StatusBadRequest, tc.reason)
		wantReason := "unrecognised"
		if tc.reason == "malformed" {
			wantReason = "malformed"
		}
		if result.ReceiverState || result.RefusalReason != wantReason {
			t.Fatalf("400 %s: receiver state %v reason %q, want hold recording %q", tc.reason, result.ReceiverState, result.RefusalReason, wantReason)
		}
	}
	for _, status := range []int{http.StatusBadRequest, http.StatusUnprocessableEntity} {
		for _, reason := range append([]string{"conflict", "replay"}, hostileReasons[:len(hostileReasons)-1]...) {
			result := deliverRefusal(t, status, reason)
			if result.ReceiverState || result.RefusalReason != "unrecognised" {
				t.Fatalf("%d %q outside the status's reasons was not held as unrecognised: %#v", status, reason, result)
			}
		}
	}
	// 409 holds whatever reason accompanies it.
	for _, reason := range []string{"conflict", "unknown_key", "retired_key", "authority.internal"} {
		if deliverRefusal(t, http.StatusConflict, reason).ReceiverState {
			t.Fatalf("409 %q was not held", reason)
		}
	}
	// 401 and 403 are credential and authorisation state whatever the reason.
	for _, status := range []int{http.StatusUnauthorized, http.StatusForbidden} {
		for _, reason := range []string{"unknown_key", "replay", "not_authorized", "malformed", "authority.internal"} {
			if !deliverRefusal(t, status, reason).ReceiverState {
				t.Fatalf("%d %q was held rather than retried on handoff", status, reason)
			}
		}
	}
	// A 401 marked retriable is still credential state: retried on handoff,
	// never on the delivery interval.
	if r := deliverRefusalWith(t, http.StatusUnauthorized, "stale", "", true); r.Retriable || !r.ReceiverState {
		t.Fatalf("401 retriable = %#v", r)
	}
	// A 400 refused before authentication carries no digest, and still holds.
	for _, tc := range contractReasons {
		r := deliverRefusalWith(t, http.StatusBadRequest, tc.reason, "", false)
		if r.ReceiverState || r.RefusalStatus != http.StatusBadRequest {
			t.Fatalf("400 without digest %s = %#v", tc.reason, r)
		}
	}
	for _, reason := range hostileReasons[:len(hostileReasons)-1] {
		if got := deliverRefusal(t, http.StatusUnprocessableEntity, reason).RefusalReason; got != "unrecognised" {
			t.Fatalf("channel passed far-end reason %q through as %q", reason, got)
		}
	}
}

func TestUnrecognisedReasonHoldsTheQueueAndLeaksNothing(t *testing.T) {
	for _, reason := range []string{"authority.internal", "sk_" + "live_4eC39HqLyjWDarjtT1zdp7dc"} {
		t.Run(reason, func(t *testing.T) {
			q := openTestQueue(t, 100, 1<<20)
			courier, err := NewCourier(q, nil)
			if err != nil {
				t.Fatal(err)
			}
			authority := newFakeAuthority()
			head := checkpointBytes(1)
			authority.setRefusal(payloadDigest(head), &fakeRefusal{status: http.StatusUnprocessableEntity, reason: reason})
			if _, err := courier.Admit(ArtifactCheckpoint, head); err != nil {
				t.Fatal(err)
			}
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
			handoff(t, courier, worker, checkpointBytes(2))
			waitFor(t, "a reminder", func() bool { return len(logs.records(t, reminderMessage)) >= 2 })
			if got := authority.attemptsFor(payloadDigest(head)); got != 1 {
				t.Fatalf("unrecognised refusal attempted %d times, want once", got)
			}
			if q.Len() != 2 {
				t.Fatalf("queue holds %d artifacts, want 2", q.Len())
			}
			blocked := logs.records(t, blockedMessage)
			if len(blocked) != 1 || blocked[0]["reason"] != "unrecognised" {
				t.Fatalf("blocked lines = %v", blocked)
			}
			status := worker.Status()
			if strings.Contains(logs.String(), reason) || strings.Contains(fmt.Sprintf("%#v", status), reason) {
				t.Fatalf("far-end reason %q reached the log or status", reason)
			}
		})
	}
}
