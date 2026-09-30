// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"encoding/json"
	"errors"
	"github.com/ori-platform/ori-gateway/internal/evidence/custody"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"log/slog"
	"math"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// deviceAuthority answers each device from its own refusal table, keyed by the
// device named in the request body, and counts attempts per device.
type deviceAuthority struct {
	mu       sync.Mutex
	status   map[string]fakeRefusal
	attempts map[string]int
}

func newDeviceAuthority() *deviceAuthority {
	return &deviceAuthority{status: map[string]fakeRefusal{}, attempts: map[string]int{}}
}

func (a *deviceAuthority) refuse(device string, r *fakeRefusal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r == nil {
		delete(a.status, device)
		return
	}
	a.status[device] = *r
}

func (a *deviceAuthority) count(device string) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.attempts[device]
}

func (a *deviceAuthority) RoundTrip(req *http.Request) (*http.Response, error) {
	var body bytes.Buffer
	if _, err := body.ReadFrom(req.Body); err != nil {
		return nil, err
	}
	device := recordDevice(body.Bytes())
	a.mu.Lock()
	a.attempts[device]++
	r, refused := a.status[device]
	a.mu.Unlock()
	status, outcome, reason := http.StatusOK, "accepted", ""
	digest := payloadDigest(body.Bytes())
	if refused {
		status, reason = r.status, r.reason
		outcome = "refused"
		if status == http.StatusServiceUnavailable || status == http.StatusTooManyRequests {
			outcome = "pending"
		}
	}
	return vectorHTTPResponse(status, "json", &outcome, reason, status != http.StatusUnauthorized, false, "", digest, nil, nil), nil
}

func deviceWorker(t *testing.T, q *DurableQueue, authority http.RoundTripper, logs *syncBuffer, retry time.Duration) *DeliveryWorker {
	t.Helper()
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
	})
	if err != nil {
		t.Fatal(err)
	}
	var handler slog.Handler = slog.DiscardHandler
	if logs != nil {
		handler = slog.NewJSONHandler(logs, nil)
	}
	worker, err := NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: retry, MaxBackoff: 8 * retry, BlockedReminderInterval: time.Hour, Logger: slog.New(handler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

func TestHoldOnOneDeviceNeverDelaysAnother(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newDeviceAuthority()
	authority.refuse("dev-a", &fakeRefusal{status: http.StatusConflict, reason: "conflict"})
	// dev-a's refused artifact is the oldest in the whole queue.
	for i := 1; i <= 3; i++ {
		if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", i)); err != nil {
			t.Fatal(err)
		}
		if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-b", i)); err != nil {
			t.Fatal(err)
		}
	}
	worker := deviceWorker(t, q, authority, nil, 10*time.Millisecond)
	startWorker(t, worker)
	waitFor(t, "dev-b to drain", func() bool { return q.LenDevice("dev-b") == 0 })
	waitFor(t, "dev-a held", func() bool { e, ok := laneState(worker, "dev-a"); return ok && e.State == DeviceHeld })
	worker.NotifyDevice("dev-a")
	worker.NotifyDevice("dev-b")
	time.Sleep(30 * time.Millisecond)
	if got := authority.count("dev-a"); got != 1 {
		t.Fatalf("held device attempted %d times", got)
	}
	if q.LenDevice("dev-a") != 3 {
		t.Fatalf("held device lost evidence: %d queued", q.LenDevice("dev-a"))
	}
	status := worker.Status()
	if !status.Blocked || !status.Degraded || len(status.Devices) != 1 || status.Devices[0].DeviceID != "dev-a" {
		t.Fatalf("status = %#v", status)
	}
}

func TestBackoffOnOneDeviceNeverDelaysAnother(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newDeviceAuthority()
	authority.refuse("dev-a", &fakeRefusal{status: http.StatusServiceUnavailable, reason: "unavailable"})
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	worker := deviceWorker(t, q, authority, nil, time.Hour)
	startWorker(t, worker)
	waitFor(t, "dev-a backing off", func() bool { e, ok := laneState(worker, "dev-a"); return ok && e.State == DeviceBackingOff })
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-b", 1)); err != nil {
		t.Fatal(err)
	}
	worker.NotifyDevice("dev-b")
	waitFor(t, "dev-b delivered during dev-a's back-off", func() bool { return q.LenDevice("dev-b") == 0 })
	status := worker.Status()
	if status.Blocked || !status.Degraded || len(status.Devices) != 1 || status.Devices[0].State != DeviceBackingOff {
		t.Fatalf("status = %#v", status)
	}
}

func TestHandoffForOneDeviceNeverRetriesAnother(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newDeviceAuthority()
	authority.refuse("dev-a", &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "unknown_key"})
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	worker := deviceWorker(t, q, authority, nil, 10*time.Millisecond)
	startWorker(t, worker)
	waitFor(t, "dev-a waiting", func() bool { e, ok := laneState(worker, "dev-a"); return ok && e.State == DeviceWaitingHandoff })
	for i := 1; i <= 5; i++ {
		if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-b", i)); err != nil {
			t.Fatal(err)
		}
		worker.NotifyDevice("dev-b")
	}
	waitFor(t, "dev-b drained", func() bool { return q.LenDevice("dev-b") == 0 })
	time.Sleep(20 * time.Millisecond)
	if got := authority.count("dev-a"); got != 1 {
		t.Fatalf("another device's handoff retried dev-a: %d attempts", got)
	}
	worker.NotifyDevice("dev-a")
	waitFor(t, "dev-a's own handoff", func() bool { return authority.count("dev-a") == 2 })
}

func TestPerDeviceCapacityRefusesOnlyTheExhaustedDevice(t *testing.T) {
	q, err := OpenDurableQueue(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "queue"), MaxItems: 4, MaxBytes: 4 * maxQueueRecordBytes,
		Devices: []string{"dev-a", "dev-b"}, ReserveDeviceShares: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	// A share of two items: one for evidence, one reserved for registration.
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	_, err = q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 2))
	var full *QueueFullError
	if !errors.As(err, &full) || full.Limit != "device item" {
		t.Fatalf("dev-a evidence into its registration reserve = %v", err)
	}
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
		t.Fatalf("registration refused its reserve: %v", err)
	}
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 2)); !errors.As(err, &full) || full.Limit != "device item" {
		t.Fatalf("dev-a past its whole share = %v", err)
	}
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-b", 1)); err != nil {
		t.Fatalf("dev-b refused while dev-a is full: %v", err)
	}
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-c", 1)); !errors.Is(err, faults.ErrUnconfiguredDevice) || errors.Is(err, ErrQueueFull) {
		t.Fatalf("unconfigured device = %v, want faults.ErrUnconfiguredDevice and never queue_full", err)
	}
	// An idempotent re-admission of held bytes still succeeds at the limit.
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatalf("idempotent re-admission refused: %v", err)
	}
}

func TestPerDeviceByteShareRefusesOnlyTheExhaustedDevice(t *testing.T) {
	q, err := OpenDurableQueue(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "queue"), MaxItems: 100, MaxBytes: 2 * MinDeviceShareBytes,
		Devices: []string{"dev-a", "dev-b"}, ReserveDeviceShares: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	big := func(device string, n int) []byte {
		prefix := `{"v":1,"device_id":"` + device + `","high_water_seq":` + string(rune('0'+n)) + `,"pad":"`
		return []byte(prefix + strings.Repeat("x", maxArtifactBytes-len(prefix)-2) + `"}`)
	}
	bigRegistration := func(device string, n int) []byte {
		prefix := `{"v":1,"device_id":"` + device + `","anchor_epoch_id":"e` + string(rune('0'+n)) + `","pad":"`
		return []byte(prefix + strings.Repeat("x", maxArtifactBytes-len(prefix)-2) + `"}`)
	}
	if _, err := q.Enqueue(ArtifactCheckpoint, big("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	var full *QueueFullError
	if _, err := q.Enqueue(ArtifactCheckpoint, big("dev-a", 2)); !errors.As(err, &full) || full.Limit != "device byte" {
		t.Fatalf("dev-a evidence into its registration byte reserve = %v", err)
	}
	if _, err := q.Enqueue(ArtifactAnchorRegistration, bigRegistration("dev-a", 1)); err != nil {
		t.Fatalf("a maximum-size registration refused its byte reserve: %v", err)
	}
	if _, err := q.Enqueue(ArtifactCheckpoint, big("dev-b", 1)); err != nil {
		t.Fatalf("dev-b refused while dev-a's bytes are full: %v", err)
	}
}

// TestQueueRefusesAShareWithoutRoomForTheReserveAndOneRecord: each share must
// hold two items and the encoded bytes of one maximum-size registration record
// plus one maximum-size evidence record, and a share exactly at that bound
// opens.
func TestQueueRefusesAShareWithoutRoomForTheReserveAndOneRecord(t *testing.T) {
	devices := []string{"dev-a", "dev-b"}
	for name, tc := range map[string]struct {
		items int
		bytes int64
		opens bool
	}{
		"exact":              {4, 2 * MinDeviceShareBytes, true},
		"one byte short":     {4, 2*MinDeviceShareBytes - 2, false},
		"one item per share": {3, 2 * MinDeviceShareBytes, false},
	} {
		_, err := OpenDurableQueue(QueueOptions{
			Directory: filepath.Join(t.TempDir(), "queue"), MaxItems: tc.items, MaxBytes: tc.bytes, Devices: devices, ReserveDeviceShares: true,
		})
		if tc.opens != (err == nil) || (err != nil && !strings.Contains(err.Error(), "per-device queue share")) {
			t.Fatalf("%s: open = %v", name, err)
		}
	}
}

// TestMaxQueueRecordBytesIsExact: the largest record the queue can write is
// exactly maxQueueRecordBytes: a maximum-size registration with the largest
// queue_seq and enqueued_at_ms. No evidence record is larger.
func TestMaxQueueRecordBytesIsExact(t *testing.T) {
	payload := bytes.Repeat([]byte("x"), maxArtifactBytes)
	for _, kind := range []ArtifactType{ArtifactAnchorRegistration, ArtifactDeliveryEnvelope, ArtifactCheckpoint} {
		encoded, err := json.Marshal(queueRecord{
			V: queueRecordVersion, ID: artifactID(kind, payload), Type: kind,
			QueueSeq: math.MaxInt64, EnqueuedAtMS: math.MaxInt64, Payload: payload,
		})
		if err != nil {
			t.Fatal(err)
		}
		n := int64(len(encoded))
		switch kind {
		case ArtifactAnchorRegistration:
			if n != maxQueueRecordBytes {
				t.Fatalf("maximum registration record is %d bytes, bound %d", n, maxQueueRecordBytes)
			}
		case ArtifactDeliveryEnvelope:
			if n != maxEvidenceRecordBytes {
				t.Fatalf("maximum envelope record is %d bytes, bound %d", n, maxEvidenceRecordBytes)
			}
		default:
			if n > maxEvidenceRecordBytes {
				t.Fatalf("%s record is %d bytes, above the evidence bound %d", kind, n, maxEvidenceRecordBytes)
			}
		}
	}
	// One maximum registration record plus one maximum evidence record
	// (evidence-transport/v2, gateway-config/v2), each measured above.
	if MinDeviceShareBytes != maxQueueRecordBytes+maxEvidenceRecordBytes {
		t.Fatalf("the least byte share %d is not one registration plus one evidence record", MinDeviceShareBytes)
	}
	if MinDeviceShareBytes != 2796604 {
		t.Fatalf("the least byte share is %d; docs/OPERATIONS.md and gateway.yaml.example state 2,796,604 and must change with it", MinDeviceShareBytes)
	}
}

func TestAdmissionRefusesAnArtifactOverTheTransportMaximum(t *testing.T) {
	q := openTestQueue(t, 10, 4*maxQueueRecordBytes)
	signer, err := custody.NewSigner("published-test-custody-secret-with-enough-entropy-0123456789")
	if err != nil {
		t.Fatal(err)
	}
	courier, err := NewCourier(q, signer)
	if err != nil {
		t.Fatal(err)
	}
	prefix := `{"v":1,"device_id":"site-a-edge-01","local_seq":1,"pad":"`
	exact := []byte(prefix + strings.Repeat("x", maxArtifactBytes-len(prefix)-2) + `"}`)
	if len(exact) != maxArtifactBytes {
		t.Fatalf("fixture is %d bytes", len(exact))
	}
	over := append(append([]byte(nil), exact[:len(exact)-2]...), []byte(`x"}`)...)
	admission, err := courier.Admit(ArtifactDeliveryEnvelope, over)
	if err == nil || admission.Custody != nil || q.Len() != 0 {
		t.Fatalf("over-size admission = %#v, %v; queued %d", admission, err, q.Len())
	}
	if admission, err := courier.Admit(ArtifactDeliveryEnvelope, exact); err != nil || admission.Custody == nil {
		t.Fatalf("maximum-size admission = %v", err)
	}
}

// TestGatewayWideQueueOpensPerDevice is the migration: a queue and hold
// written while order and capacity were gateway-wide open unchanged, the
// old hold becomes its device's hold, and the other device delivers.
func TestGatewayWideQueueOpensPerDevice(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	old := openTestQueueAt(t, dir, 100, 1<<24) // no Devices: one gateway-wide share
	for i := 1; i <= 5; i++ {
		if _, err := old.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", i)); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := old.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-b", 1)); err != nil {
		t.Fatal(err)
	}
	head, _ := old.Peek()
	if err := old.Hold(HoldRecord{
		QueueRecord: head.ID, ArtifactDigest: payloadDigest(head.Payload), ArtifactType: head.Type,
		RefusalStatus: http.StatusConflict, Reason: "conflict", FirstHeldAtMS: 1787000000000,
	}); err != nil {
		t.Fatal(err)
	}

	// Reopened with per-device shares of 3 items: dev-a already holds 5.
	q, err := OpenDurableQueue(QueueOptions{
		Directory: dir, MaxItems: 6, MaxBytes: 2 * maxQueueRecordBytes * 3, Devices: []string{"dev-a", "dev-b"}, ReserveDeviceShares: true,
	})
	if err != nil {
		t.Fatalf("a gateway-wide queue over a device's share refused to open: %v", err)
	}
	var full *QueueFullError
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 6)); !errors.As(err, &full) {
		t.Fatalf("dev-a over its share admitted: %v", err)
	}
	authority := newDeviceAuthority()
	worker := deviceWorker(t, q, authority, nil, 10*time.Millisecond)
	if e, ok := laneState(worker, "dev-a"); !ok || e.State != DeviceHeld || e.Held.FirstHeldAt.UnixMilli() != 1787000000000 {
		t.Fatalf("the gateway-wide hold did not become dev-a's: %#v", e)
	}
	startWorker(t, worker)
	waitFor(t, "dev-b delivered", func() bool { return q.LenDevice("dev-b") == 0 })
	time.Sleep(20 * time.Millisecond)
	if authority.count("dev-a") != 0 || q.LenDevice("dev-a") != 5 {
		t.Fatalf("dev-a attempts %d, queued %d", authority.count("dev-a"), q.LenDevice("dev-a"))
	}
}

func TestRestartNeverRestoresAnEarlierBackoff(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	worker := deviceWorker(t, q, newDeviceAuthority(), nil, time.Second)
	lane := worker.lane(LaneKey{Device: "dev-a", Lane: LaneEvidence})
	head, ok := lane.head()
	if !ok {
		t.Fatal("no head")
	}
	lane.attempted = head
	// A deadline on a whole millisecond is kept exactly by any encoding, so
	// one back-off proves nothing; across many, one falls between. Each is a
	// first back-off, well inside the restore's bound.
	for range 20 {
		lane.clearBackoff()
		lane.scheduleBackoff(errChannelRefused)
		scheduled := worker.nextAttempt("dev-a")
		restored := deviceWorker(t, q, newDeviceAuthority(), nil, time.Second).nextAttempt("dev-a")
		if restored.Before(scheduled) {
			t.Fatalf("a restart brought the back-off forward by %s", scheduled.Sub(restored))
		}
	}
}

func TestATimerNeverSendsBeforeTheWallClockBackoff(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	authority := newDeviceAuthority()
	authority.refuse("dev-a", &fakeRefusal{status: http.StatusServiceUnavailable, reason: "unavailable"})
	worker := deviceWorker(t, q, authority, nil, 300*time.Millisecond)
	startWorker(t, worker)
	waitFor(t, "the back-off", func() bool { return worker.nextAttempt("dev-a").After(time.Now()) })
	time.Sleep(50 * time.Millisecond)
	// A wall clock running slow against the timer: the deadline moves later
	// than the timer already armed for it.
	lane := worker.lane(LaneKey{Device: "dev-a", Lane: LaneEvidence})
	lane.mu.Lock()
	deadline := lane.notBefore.Add(400 * time.Millisecond)
	lane.notBefore = deadline
	lane.mu.Unlock()
	for time.Now().Before(deadline) {
		if authority.count("dev-a") != 1 {
			t.Fatalf("retried %s before the back-off", time.Until(deadline))
		}
		time.Sleep(time.Millisecond)
	}
	waitFor(t, "the retry after the back-off", func() bool { return authority.count("dev-a") == 2 })
}

// rateLimiter answers the first refusals requests with a 429 whose
// Retry-After is retryAfterS seconds, and accepts after that. It records each
// request's wall-clock arrival.
type rateLimiter struct {
	mu          sync.Mutex
	refusals    int
	retryAfterS int
	arrived     []time.Time
}

func (a *rateLimiter) RoundTrip(req *http.Request) (*http.Response, error) {
	var body bytes.Buffer
	if _, err := body.ReadFrom(req.Body); err != nil {
		return nil, err
	}
	a.mu.Lock()
	a.arrived = append(a.arrived, time.Now().Round(0))
	refuse := len(a.arrived) <= a.refusals
	a.mu.Unlock()
	digest := payloadDigest(body.Bytes())
	if refuse {
		outcome := "pending"
		return vectorHTTPResponse(http.StatusTooManyRequests, "json", &outcome, "rate_limited", true, false,
			strconv.Itoa(a.retryAfterS), digest, nil, nil), nil
	}
	outcome := "accepted"
	return vectorHTTPResponse(http.StatusOK, "json", &outcome, "", true, false, "", digest, nil, nil), nil
}

func (a *rateLimiter) arrivals() []time.Time {
	a.mu.Lock()
	defer a.mu.Unlock()
	return append([]time.Time(nil), a.arrived...)
}

// boundedWorker backs off from 100ms to a 200ms bound, well under a
// one-second Retry-After.
func boundedWorker(t *testing.T, q *DurableQueue, authority http.RoundTripper) *DeliveryWorker {
	t.Helper()
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: 100 * time.Millisecond, MaxBackoff: 200 * time.Millisecond,
		BlockedReminderInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

// awaitRetryNoEarlierThan hands the device off throughout the wait, and
// requires the retry to arrive no earlier than the persisted deadline.
func awaitRetryNoEarlierThan(t *testing.T, worker *DeliveryWorker, authority *rateLimiter, notBefore time.Time) {
	t.Helper()
	give := time.Now().Add(time.Until(notBefore) + 2*time.Second)
	for len(authority.arrivals()) < 2 && time.Now().Before(give) {
		worker.NotifyDevice("dev-a")
		time.Sleep(5 * time.Millisecond)
	}
	arrived := authority.arrivals()
	if len(arrived) != 2 {
		t.Fatalf("%d attempts, want the refused one and its retry", len(arrived))
	}
	if arrived[1].Before(notBefore) {
		t.Fatalf("retried %s before the Retry-After", notBefore.Sub(arrived[1]))
	}
}

func TestRetryAfterBeyondTheBoundIsWaitedInFull(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	entry, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	authority := &rateLimiter{refusals: 1, retryAfterS: 1}
	worker := boundedWorker(t, q, authority)
	startWorker(t, worker)
	waitFor(t, "the back-off", func() bool { _, ok := q.BackoffFor(entry.ID); return ok })
	record, _ := q.BackoffFor(entry.ID)
	notBefore := time.UnixMilli(record.NotBeforeMS)
	if record.RetryAfterMS != 1000 || notBefore.Before(authority.arrivals()[0].Add(time.Second)) {
		t.Fatalf("persisted back-off = %#v", record)
	}
	awaitRetryNoEarlierThan(t, worker, authority, notBefore)
}

func TestRetryAfterBeyondTheBoundSurvivesARestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 100, 1<<24)
	entry, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	authority := &rateLimiter{refusals: 1, retryAfterS: 1}
	running := startWorker(t, boundedWorker(t, q, authority))
	waitFor(t, "the back-off", func() bool { _, ok := q.BackoffFor(entry.ID); return ok })
	running.stop()
	record, _ := q.BackoffFor(entry.ID)
	notBefore := time.UnixMilli(record.NotBeforeMS)

	next := boundedWorker(t, openTestQueueAt(t, dir, 100, 1<<24), authority)
	if restored := next.nextAttempt("dev-a"); !restored.Equal(notBefore) {
		t.Fatalf("restored deadline %s, persisted %s", restored.Format(time.RFC3339Nano), notBefore.Format(time.RFC3339Nano))
	}
	startWorker(t, next)
	awaitRetryNoEarlierThan(t, next, authority, notBefore)
}

func TestABackwardsClockNeverShortensARetryAfter(t *testing.T) {
	const far = time.Hour
	for _, tc := range []struct {
		name       string
		retryAfter time.Duration
		bound      time.Duration
	}{
		{"with a Retry-After beyond the bound", 1500 * time.Millisecond, 1500 * time.Millisecond},
		{"without one", 0, 200 * time.Millisecond},
	} {
		t.Run(tc.name, func(t *testing.T) {
			near := func(got time.Time) bool {
				want := time.Now().Add(tc.bound)
				return !got.After(want) && got.After(want.Add(-100*time.Millisecond))
			}
			// A deadline an hour away is one a clock that moved backwards
			// produced: restored, it is clamped to the longest wait the
			// back-off could have.
			q := openTestQueue(t, 10, 1<<20)
			entry, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1))
			if err != nil {
				t.Fatal(err)
			}
			if err := q.SetBackoff(BackoffRecord{
				QueueRecord: entry.ID, NotBeforeMS: time.Now().Add(far).UnixMilli(), Attempts: 1,
				RetryAfterMS: tc.retryAfter.Milliseconds(),
			}); err != nil {
				t.Fatal(err)
			}
			worker := boundedWorker(t, q, &rateLimiter{})
			if got := worker.nextAttempt("dev-a"); !near(got) {
				t.Fatalf("restored deadline in %s, want %s", time.Until(got), tc.bound)
			}
			// And in this process, where every timer and handoff asks.
			lane := worker.lane(LaneKey{Device: "dev-a", Lane: LaneEvidence})
			lane.mu.Lock()
			lane.notBefore, lane.retryAfter = time.Now().Round(0).Add(far), tc.retryAfter
			lane.mu.Unlock()
			if wait := lane.untilDue(); wait > tc.bound || wait < tc.bound-100*time.Millisecond {
				t.Fatalf("waits %s, want %s", wait, tc.bound)
			}
			if got := worker.nextAttempt("dev-a"); !near(got) {
				t.Fatalf("deadline in %s, want %s", time.Until(got), tc.bound)
			}
		})
	}
}

func TestBackoffSurvivesARestart(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 100, 1<<24)
	authority := newDeviceAuthority()
	authority.refuse("dev-a", &fakeRefusal{status: http.StatusServiceUnavailable, reason: "unavailable"})
	entry, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	// The back-off is the delivery interval: long enough that stopping,
	// reopening and restarting fall well inside it on any machine.
	worker := deviceWorker(t, q, authority, nil, time.Second)
	running := startWorker(t, worker)
	waitFor(t, "the back-off", func() bool { _, ok := q.BackoffFor(entry.ID); return ok })
	running.stop()
	record, _ := q.BackoffFor(entry.ID)
	if record.Attempts != 1 || time.UnixMilli(record.NotBeforeMS).Before(time.Now()) {
		t.Fatalf("persisted back-off = %#v", record)
	}

	reopened := openTestQueueAt(t, dir, 100, 1<<24)
	next := deviceWorker(t, reopened, authority, nil, time.Second)
	if e, ok := laneState(next, "dev-a"); !ok || e.State != DeviceBackingOff {
		t.Fatalf("restart forgot the back-off: %#v", e)
	}
	startWorker(t, next)
	next.NotifyDevice("dev-a")
	// Observe before the persisted back-off elapses: an attempt then is the
	// restart resending early.
	notBefore := time.UnixMilli(record.NotBeforeMS)
	time.Sleep(min(100*time.Millisecond, time.Until(notBefore)-50*time.Millisecond))
	if time.Now().After(notBefore) {
		t.Fatal("setup outlasted the persisted back-off, so the restart was not observed before it")
	}
	if authority.count("dev-a") != 1 {
		t.Fatalf("restart resent before the back-off elapsed: %d attempts", authority.count("dev-a"))
	}
	authority.refuse("dev-a", nil)
	waitFor(t, "the retry after the back-off", func() bool { return reopened.Len() == 0 })
	if _, err := os.Stat(filepath.Join(dir, backoffFilePrefix+entry.ID)); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("retirement left the back-off record: %v", err)
	}
}

func TestInvalidBackoffRefusesTheQueue(t *testing.T) {
	for name, body := range map[string]string{
		"corrupt":              "{",
		"zero attempts":        `{"v":1,"queue_record":"%s","not_before_ms":1,"attempts":0}`,
		"wrong version":        `{"v":2,"queue_record":"%s","not_before_ms":1,"attempts":1}`,
		"unknown field":        `{"v":1,"queue_record":"%s","not_before_ms":1,"attempts":1,"x":1}`,
		"names another":        `{"v":1,"queue_record":"` + strings.Repeat("0", 64) + `","not_before_ms":1,"attempts":1}`,
		"negative before":      `{"v":1,"queue_record":"%s","not_before_ms":-1,"attempts":1}`,
		"negative retry-after": `{"v":1,"queue_record":"%s","not_before_ms":1,"attempts":1,"retry_after_ms":-1}`,
		"retry-after unparsed": `{"v":1,"queue_record":"%s","not_before_ms":1,"attempts":1,"retry_after_ms":2147483648000}`,
	} {
		t.Run(name, func(t *testing.T) {
			dir := filepath.Join(t.TempDir(), "queue")
			q := openTestQueueAt(t, dir, 10, 1<<20)
			entry, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1))
			if err != nil {
				t.Fatal(err)
			}
			raw := strings.ReplaceAll(body, "%s", entry.ID)
			if err := os.WriteFile(filepath.Join(dir, backoffFilePrefix+entry.ID), []byte(raw), 0o600); err != nil {
				t.Fatal(err)
			}
			if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 20}); err == nil ||
				!strings.Contains(err.Error(), entry.ID) {
				t.Fatalf("open = %v", err)
			}
		})
	}
}

// TestEveryBackoffWaitsAtLeastTheDeliveryInterval: the worker raises a base or
// bound below the delivery interval to meet bound >= base >= interval, and a
// Retry-After is an additional floor.
func TestEveryBackoffWaitsAtLeastTheDeliveryInterval(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	w, err := NewDeliveryWorker(q, &fakeEvidenceChannel{}, nil, DeliveryWorkerOptions{
		RetryInterval: 50 * time.Millisecond, BackoffBase: 10 * time.Millisecond, MaxBackoff: 20 * time.Millisecond,
	})
	if err != nil {
		t.Fatal(err)
	}
	if w.backoffBase != 50*time.Millisecond || w.maxBackoff != 50*time.Millisecond {
		t.Fatalf("base %v bound %v, want both raised to the 50ms interval", w.backoffBase, w.maxBackoff)
	}
	for attempts := 1; attempts <= 5; attempts++ {
		if got := w.backoffDelay(attempts, 0); got < 50*time.Millisecond {
			t.Fatalf("attempt %d waits %v, under the delivery interval", attempts, got)
		}
	}
	w = &DeliveryWorker{retry: 50 * time.Millisecond, backoffBase: 10 * time.Millisecond, maxBackoff: 20 * time.Millisecond}
	if got := w.backoffDelay(1, 0); got != 50*time.Millisecond {
		t.Fatalf("an unnormalised base still waits %v, want the 50ms interval", got)
	}
	if got := w.backoffDelay(1, time.Second); got != time.Second {
		t.Fatalf("Retry-After floor = %v", got)
	}
}

func TestUnrecognisedOutcomesBackOffExponentiallyAndBounded(t *testing.T) {
	w := &DeliveryWorker{retry: 10 * time.Millisecond, backoffBase: 10 * time.Millisecond, maxBackoff: 70 * time.Millisecond}
	want := []time.Duration{10, 20, 40, 70, 70, 70}
	for i, d := range want {
		if got := w.backoffDelay(i+1, 0); got != d*time.Millisecond {
			t.Fatalf("attempt %d: %v, want %v", i+1, got, d*time.Millisecond)
		}
	}
	if got := w.backoffDelay(1, time.Second); got != time.Second {
		t.Fatalf("Retry-After floor ignored: %v", got)
	}
	if got := w.backoffDelay(maxBackoffAttempts, 0); got != 70*time.Millisecond {
		t.Fatalf("unbounded back-off: %v", got)
	}

	// Through the worker: a 413 is attempted at growing intervals, never
	// retired, never held.
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	var mu sync.Mutex
	var at []time.Time
	client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
		mu.Lock()
		at = append(at, time.Now())
		mu.Unlock()
		return vectorHTTPResponse(http.StatusRequestEntityTooLarge, "text", nil, "", false, false, "", "", nil, nil), nil
	})}
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: client,
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: 20 * time.Millisecond, MaxBackoff: 80 * time.Millisecond, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	waitFor(t, "five attempts", func() bool { mu.Lock(); defer mu.Unlock(); return len(at) >= 5 })
	mu.Lock()
	gaps := []time.Duration{at[1].Sub(at[0]), at[2].Sub(at[1]), at[3].Sub(at[2]), at[4].Sub(at[3])}
	mu.Unlock()
	floors := []time.Duration{20, 40, 80, 80}
	for i, gap := range gaps {
		if gap < floors[i]*time.Millisecond {
			t.Fatalf("gap %d was %v, want at least %v", i, gap, floors[i]*time.Millisecond)
		}
	}
	if st := worker.Status(); st.Blocked || q.Len() != 1 || st.Devices[0].State != DeviceBackingOff {
		t.Fatalf("status = %#v", st)
	}
}
