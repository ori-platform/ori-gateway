// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
)

// laneAuthority answers each device and lane from its own table. The lane is
// read from the request's artifact type header, not from the queue. A 200 for
// a delivery envelope carries a receipt covering its local_seq.
type laneAuthority struct {
	mu       sync.Mutex
	answer   map[LaneKey]fakeRefusal
	byDigest map[string]fakeRefusal
	attempts map[LaneKey]int
	// confirm answers a registration's 200 with an epoch confirmation;
	// otherwise the registration is accepted pending, with none.
	confirm bool
}

func newLaneAuthority() *laneAuthority {
	return &laneAuthority{answer: map[LaneKey]fakeRefusal{}, byDigest: map[string]fakeRefusal{}, attempts: map[LaneKey]int{}}
}

// setFor answers one artifact's bytes, ahead of its lane's answer.

func (a *laneAuthority) set(key LaneKey, r *fakeRefusal) {
	a.mu.Lock()
	defer a.mu.Unlock()
	if r == nil {
		delete(a.answer, key)
		return
	}
	a.answer[key] = *r
}

func (a *laneAuthority) count(key LaneKey) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.attempts[key]
}

func (a *laneAuthority) RoundTrip(req *http.Request) (*http.Response, error) {
	var body bytes.Buffer
	if _, err := body.ReadFrom(req.Body); err != nil {
		return nil, err
	}
	kind := req.Header.Get("X-Ori-Evidence-Artifact-Type")
	lane := LaneEvidence
	if kind == "anchor_registration" {
		lane = LaneRegistration
	}
	var routing struct {
		DeviceID string `json:"device_id"`
		LocalSeq int    `json:"local_seq"`
	}
	_ = json.Unmarshal(body.Bytes(), &routing)
	key := LaneKey{Device: routing.DeviceID, Lane: lane}
	digest := payloadDigest(body.Bytes())
	a.mu.Lock()
	a.attempts[key]++
	r, refused := a.byDigest[digest]
	if !refused {
		r, refused = a.answer[key]
	}
	confirm := a.confirm
	a.mu.Unlock()
	if refused {
		outcome := "refused"
		if r.status == http.StatusServiceUnavailable || r.status == http.StatusTooManyRequests {
			outcome = "pending"
		}
		return vectorHTTPResponse(r.status, "json", &outcome, r.reason, r.status != http.StatusUnauthorized, false, "", digest, nil, nil), nil
	}
	outcome := "accepted"
	if kind == "anchor_registration" && confirm {
		confirmation := fmt.Sprintf(`{"v":1,"device_id":%q,"anchor_epoch_id":"epoch-1","signature":"ed25519:authority"}`, routing.DeviceID)
		raw, _ := json.Marshal(map[string]any{
			"v": 1, "artifact_digest": digest, "outcome": "accepted", "reason": "", "retriable": false,
			"authority_artifacts": []any{map[string]any{
				"artifact_type": "epoch_confirmation", "artifact_b64": base64.StdEncoding.EncodeToString([]byte(confirmation)),
			}},
		})
		return &http.Response{
			StatusCode: http.StatusOK, Header: http.Header{"Content-Type": []string{"application/json"}},
			Body: io.NopCloser(bytes.NewReader(raw)),
		}, nil
	}
	var receipt []byte
	if kind == "delivery_envelope" {
		receipt = validReceiptBytes(routing.DeviceID, routing.LocalSeq, routing.LocalSeq)
	}
	return vectorHTTPResponse(http.StatusOK, "json", &outcome, "", true, false, "", digest, receipt, nil), nil
}

// lockedSink is an AuthoritySink safe for lanes storing concurrently.
type lockedSink struct {
	mu     sync.Mutex
	stored []AuthorityArtifact
}

func (s *lockedSink) Store(_ context.Context, artifact AuthorityArtifact) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.stored = append(s.stored, artifact)
	return nil
}

func laneWorker(t *testing.T, q *DurableQueue, authority http.RoundTripper, logs *syncBuffer, retry time.Duration) *DeliveryWorker {
	t.Helper()
	return laneWorkerSink(t, q, authority, logs, retry, &lockedSink{})
}

func laneWorkerSink(t *testing.T, q *DurableQueue, authority http.RoundTripper, logs *syncBuffer, retry time.Duration, sink AuthoritySink) *DeliveryWorker {
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
	worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{
		RetryInterval: retry, MaxBackoff: 8 * retry, BlockedReminderInterval: time.Hour, Logger: slog.New(handler),
	})
	if err != nil {
		t.Fatal(err)
	}
	return worker
}

var (
	regLane = LaneKey{Device: "dev-a", Lane: LaneRegistration}
	evLane  = LaneKey{Device: "dev-a", Lane: LaneEvidence}
)

func enqueueEnvelopes(t *testing.T, q *DurableQueue, device string, from, to int) {
	t.Helper()
	for seq := from; seq <= to; seq++ {
		if _, err := q.Enqueue(ArtifactDeliveryEnvelope, deviceEnvelope(device, seq)); err != nil {
			t.Fatal(err)
		}
	}
}

// A registration the authority refuses 409 pending_registration_conflict is
// retained and backed off in the registration lane, is reported, and never
// delays the same device's envelopes queued behind it.
func TestRegistrationConflictBacksOffWhileTheDevicesEnvelopesDeliver(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newLaneAuthority()
	authority.set(regLane, &fakeRefusal{status: http.StatusConflict, reason: "pending_registration_conflict"})
	registration, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q, "dev-a", 1, 3)
	logs := &syncBuffer{}
	worker := laneWorker(t, q, authority, logs, time.Hour)
	startWorker(t, worker)
	waitFor(t, "the envelopes to drain", func() bool { return q.LenLane(evLane) == 0 })
	waitFor(t, "the registration backing off", func() bool {
		e, ok := laneEntry(worker, regLane)
		return ok && e.State == DeviceBackingOff
	})
	// Handoffs never bring the back-off forward.
	for seq := 4; seq <= 5; seq++ {
		enqueueEnvelopes(t, q, "dev-a", seq, seq)
		worker.NotifyDevice("dev-a")
	}
	waitFor(t, "the later envelopes to drain", func() bool { return q.LenLane(evLane) == 0 })
	time.Sleep(30 * time.Millisecond)
	if got := authority.count(regLane); got != 1 {
		t.Fatalf("registration attempted %d times, want once", got)
	}
	if got := authority.count(evLane); got != 5 {
		t.Fatalf("envelopes attempted %d times, want 5", got)
	}
	if q.LenLane(regLane) != 1 {
		t.Fatal("the backed-off registration was not retained")
	}
	if _, ok := q.HoldFor(registration.ID); ok {
		t.Fatal("pending_registration_conflict was held")
	}
	if record, ok := q.BackoffFor(registration.ID); !ok || record.Attempts != 1 {
		t.Fatalf("back-off not persisted: %#v %v", record, ok)
	}
	status := worker.Status()
	if status.Blocked || !status.Degraded || len(status.Devices) != 1 ||
		status.Devices[0].Lane != LaneRegistration || status.Devices[0].Pending != 1 {
		t.Fatalf("status = %#v", status)
	}
	lines := logs.records(t, stallMessage)
	if len(lines) != 1 || lines[0]["reason"] != "pending_registration_conflict" || lines[0]["lane"] != "registration" {
		t.Fatalf("the conflict was not reported: %v", lines)
	}
}

// A 200 retiring an anchor registration is a handoff for the device's
// evidence lane: an envelope waiting for a handoff is retried with no new
// admission and no timer.
//
// Both 200s do so: one carrying the epoch confirmation, and one accepting the
// registration pending with none. Each retires the entry; the two are told
// apart only by the declared authority-artifact type, which decides what is
// staged for the runtime, never whether the entry retires.
func TestRetiredRegistrationReleasesTheEvidenceLanesHandoffWait(t *testing.T) {
	for _, confirm := range []bool{true, false} {
		t.Run(fmt.Sprintf("confirmed=%v", confirm), func(t *testing.T) {
			retiredRegistrationReleasesHandoffWait(t, confirm)
		})
	}
}

func retiredRegistrationReleasesHandoffWait(t *testing.T, confirm bool) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newLaneAuthority()
	authority.confirm = confirm
	authority.set(regLane, &fakeRefusal{status: http.StatusServiceUnavailable, reason: "unavailable"})
	authority.set(evLane, &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "unknown_key"})
	registration := deviceRegistration("dev-a", 1)
	if _, err := q.Enqueue(ArtifactAnchorRegistration, registration); err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q, "dev-a", 1, 1)
	sink := &lockedSink{}
	worker := laneWorkerSink(t, q, authority, nil, 300*time.Millisecond, sink)
	startWorker(t, worker)
	waitFor(t, "the envelope waiting for a handoff", func() bool {
		e, ok := laneEntry(worker, evLane)
		return ok && e.State == DeviceWaitingHandoff
	})
	waitFor(t, "the registration backing off", func() bool {
		e, ok := laneEntry(worker, regLane)
		return ok && e.State == DeviceBackingOff
	})
	if authority.count(evLane) != 1 || authority.count(regLane) != 1 {
		t.Fatalf("attempts before the back-off elapsed: %d, %d", authority.count(evLane), authority.count(regLane))
	}
	authority.set(regLane, nil)
	authority.set(evLane, nil)
	waitFor(t, "the registration retired", func() bool { return q.LenLane(regLane) == 0 })
	waitFor(t, "the envelope retried and retired", func() bool { return q.LenLane(evLane) == 0 })
	if got := authority.count(evLane); got != 2 {
		t.Fatalf("envelope attempted %d times, want 2", got)
	}
	confirmations := 0
	sink.mu.Lock()
	for _, stored := range sink.stored {
		if stored.Type == AuthorityEpochConfirmation {
			confirmations++
		}
	}
	sink.mu.Unlock()
	if want := map[bool]int{true: 1, false: 0}[confirm]; confirmations != want {
		t.Fatalf("staged %d epoch confirmations, want %d", confirmations, want)
	}
	// A re-offer of the same bytes after a pending acceptance is a new
	// admission, not a recovered entry.
	if _, err := q.Enqueue(ArtifactAnchorRegistration, registration); err != nil || q.LenLane(regLane) != 1 {
		t.Fatalf("re-offer = %v, queued %d", err, q.LenLane(regLane))
	}
	worker.NotifyDevice("dev-a")
	waitFor(t, "the re-offer delivered", func() bool { return q.LenLane(regLane) == 0 && authority.count(regLane) == 3 })
}

// A newly admitted artifact is a handoff for both of its device's lanes: an
// envelope's admission retries a registration waiting for a handoff, and a
// registration's admission retries an envelope waiting for one.
func TestNewArtifactIsAHandoffForBothOfTheDevicesLanes(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newLaneAuthority()
	authority.set(regLane, &fakeRefusal{status: http.StatusForbidden, reason: "not_authorized"})
	authority.set(evLane, &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "unknown_key"})
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q, "dev-a", 1, 1)
	worker := laneWorker(t, q, authority, nil, time.Hour)
	startWorker(t, worker)
	waitFor(t, "both lanes waiting", func() bool {
		r, okR := laneEntry(worker, regLane)
		e, okE := laneEntry(worker, evLane)
		return okR && okE && r.State == DeviceWaitingHandoff && e.State == DeviceWaitingHandoff
	})
	// Another device's admission is no handoff for dev-a.
	enqueueEnvelopes(t, q, "dev-b", 1, 1)
	worker.NotifyDevice("dev-b")
	waitFor(t, "dev-b delivered", func() bool { return q.LenLane(LaneKey{Device: "dev-b", Lane: LaneEvidence}) == 0 })
	if authority.count(regLane) != 1 || authority.count(evLane) != 1 {
		t.Fatalf("another device's handoff retried dev-a: %d, %d", authority.count(regLane), authority.count(evLane))
	}
	enqueueEnvelopes(t, q, "dev-a", 2, 2)
	worker.NotifyDevice("dev-a")
	waitFor(t, "the registration retried on the envelope's admission", func() bool { return authority.count(regLane) == 2 })
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 2)); err != nil {
		t.Fatal(err)
	}
	before := authority.count(evLane)
	worker.NotifyDevice("dev-a")
	waitFor(t, "the envelope retried on the registration's admission", func() bool { return authority.count(evLane) > before })
}

// A retired envelope is not a handoff for the registration lane: only a new
// artifact, a retired registration, or a process start is.
func TestRetiredEnvelopeIsNotAHandoffForTheRegistrationLane(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	authority := newLaneAuthority()
	authority.set(regLane, &fakeRefusal{status: http.StatusForbidden, reason: "not_authorized"})
	authority.set(evLane, &fakeRefusal{status: http.StatusServiceUnavailable, reason: "unavailable"})
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q, "dev-a", 1, 1)
	worker := laneWorker(t, q, authority, nil, 100*time.Millisecond)
	startWorker(t, worker)
	waitFor(t, "the registration waiting", func() bool { e, ok := laneEntry(worker, regLane); return ok && e.State == DeviceWaitingHandoff })
	authority.set(evLane, nil)
	waitFor(t, "the envelope retired", func() bool { return q.LenLane(evLane) == 0 })
	time.Sleep(50 * time.Millisecond)
	if got := authority.count(regLane); got != 1 {
		t.Fatalf("registration attempted %d times after an envelope retired", got)
	}
}

// Per-lane state is restored across a restart: the registration lane's
// back-off and the evidence lane's hold on the same device each come back in
// their own lane, and neither is sent early.
func TestRestartRestoresPerLaneState(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	q := openTestQueueAt(t, dir, 100, 1<<24)
	authority := newLaneAuthority()
	authority.set(regLane, &fakeRefusal{status: http.StatusConflict, reason: "pending_registration_conflict"})
	authority.set(evLane, &fakeRefusal{status: http.StatusConflict, reason: "conflict"})
	registration, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1))
	if err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q, "dev-a", 1, 2)
	envelope, _ := q.PeekLane(evLane)
	worker := laneWorker(t, q, authority, nil, 400*time.Millisecond)
	running := startWorker(t, worker)
	waitFor(t, "the persisted back-off", func() bool { _, ok := q.BackoffFor(registration.ID); return ok })
	waitFor(t, "the persisted hold", func() bool { _, ok := q.HoldFor(envelope.ID); return ok })
	running.stop()

	reopened := openTestQueueAt(t, dir, 100, 1<<24)
	next := laneWorker(t, reopened, authority, nil, 400*time.Millisecond)
	reg, okReg := laneEntry(next, regLane)
	ev, okEv := laneEntry(next, evLane)
	if !okReg || reg.State != DeviceBackingOff || reg.Pending != 1 {
		t.Fatalf("registration lane after restart: %#v", reg)
	}
	if !okEv || ev.State != DeviceHeld || ev.Pending != 2 || ev.Held.QueueRecord != envelope.ID {
		t.Fatalf("evidence lane after restart: %#v", ev)
	}
	startWorker(t, next)
	next.NotifyDevice("dev-a")
	time.Sleep(100 * time.Millisecond)
	if authority.count(regLane) != 1 || authority.count(evLane) != 1 {
		t.Fatalf("restart sent early: registration %d, envelope %d", authority.count(regLane), authority.count(evLane))
	}
	authority.set(regLane, nil)
	waitFor(t, "the registration after its back-off", func() bool { return reopened.LenLane(regLane) == 0 })
	time.Sleep(50 * time.Millisecond)
	if authority.count(evLane) != 1 || reopened.LenLane(evLane) != 2 {
		t.Fatalf("the held lane moved: %d attempts, %d queued", authority.count(evLane), reopened.LenLane(evLane))
	}
}

// Capacity is reserved per device: one device's registrations and envelopes
// together fill at most its share, never another's, and the evidence lane
// never takes the registration reserve.
func TestTwoLanesShareTheDevicesCapacity(t *testing.T) {
	q, err := OpenDurableQueue(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "queue"), MaxItems: 4, MaxBytes: 4 * maxQueueRecordBytes,
		Devices: []string{"dev-a", "dev-b"}, ReserveDeviceShares: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, deviceEnvelope("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	var full *QueueFullError
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, deviceEnvelope("dev-a", 2)); !errors.As(err, &full) || full.Limit != "device item" {
		t.Fatalf("an envelope past the device's shared share = %v", err)
	}
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 2)); !errors.As(err, &full) || full.Limit != "device item" {
		t.Fatalf("a registration past the device's shared share = %v", err)
	}
	for i := 1; i <= 2; i++ {
		if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-b", i)); err != nil {
			t.Fatalf("dev-b refused while dev-a is full: %v", err)
		}
	}
	if q.LenLane(regLane) != 1 || q.LenLane(evLane) != 1 {
		t.Fatalf("lanes = %d, %d", q.LenLane(regLane), q.LenLane(evLane))
	}
}

// An evidence lane filled while its epoch is unconfirmed can never refuse the
// registration that would confirm it: evidence stops one item short of the
// share, and the registration takes the reserve. A registration already
// queued does not shrink the evidence lane's own limit.
func TestEvidenceNeverTakesTheRegistrationReserve(t *testing.T) {
	q, err := OpenDurableQueue(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "queue"), MaxItems: 8, MaxBytes: 2 * MinDeviceShareBytes,
		Devices: []string{"dev-a", "dev-b"}, ReserveDeviceShares: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q, "dev-a", 1, 3)
	var full *QueueFullError
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, deviceEnvelope("dev-a", 4)); !errors.As(err, &full) {
		t.Fatalf("the fourth envelope took the registration reserve: %v", err)
	}
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
		t.Fatalf("the confirming registration was refused: %v", err)
	}

	// Registrations first: the evidence lane still gets its three items.
	q2, err := OpenDurableQueue(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "queue"), MaxItems: 8, MaxBytes: 2 * MinDeviceShareBytes,
		Devices: []string{"dev-a", "dev-b"}, ReserveDeviceShares: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q2.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q2, "dev-a", 1, 3)
	if _, err := q2.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 2)); !errors.As(err, &full) {
		t.Fatalf("a registration past the whole share: %v", err)
	}
}

// A queue directory written before lanes existed opens into lanes. Records
// and their hold are written here byte for byte in the on-disk format, not by
// the queue, and a record the queue writes carries no lane field: the lane is
// derived from the carriage artifact type alone.
func TestQueueWrittenWithoutLanesOpensIntoLanes(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	if err := os.Mkdir(dir, 0o700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(dir, ".ori-evidence-queue-v1"), []byte("ori evidence durable queue v1\n"), 0o600); err != nil {
		t.Fatal(err)
	}
	write := func(seq int, kind string, payload []byte) string {
		h := sha256.New()
		h.Write([]byte(kind))
		h.Write([]byte{0})
		h.Write(payload)
		id := hex.EncodeToString(h.Sum(nil))
		raw := fmt.Sprintf(`{"v":1,"id":%q,"artifact_type":%q,"queue_seq":%d,"enqueued_at_ms":%d,"payload":%q}`,
			id, kind, seq, 1787000000000+seq, base64.StdEncoding.EncodeToString(payload))
		if err := os.WriteFile(filepath.Join(dir, id+".json"), []byte(raw), 0o600); err != nil {
			t.Fatal(err)
		}
		return id
	}
	registration := deviceRegistration("dev-a", 1)
	regID := write(1, "anchor_registration", registration)
	write(2, "delivery_envelope", deviceEnvelope("dev-a", 1))
	write(3, "checkpoint", deviceCheckpoint("dev-a", 1))
	hold := fmt.Sprintf(`{"v":1,"queue_record":%q,"artifact_digest":%q,"artifact_type":"anchor_registration","refusal_status":409,"reason":"conflict","first_held_at_ms":1787000000500,"acknowledged":false}`,
		regID, payloadDigest(registration))
	if err := os.WriteFile(filepath.Join(dir, holdFilePrefix+regID), []byte(hold), 0o600); err != nil {
		t.Fatal(err)
	}

	q := openTestQueueAt(t, dir, 100, 1<<24)
	lanes := q.Lanes()
	if fmt.Sprint(lanes) != fmt.Sprint([]LaneKey{evLane, regLane}) {
		t.Fatalf("lanes = %v", lanes)
	}
	if head, _ := q.PeekLane(evLane); head.Type != ArtifactDeliveryEnvelope {
		t.Fatalf("evidence lane head = %s", head.Type)
	}
	if q.LenLane(evLane) != 2 || q.LenLane(regLane) != 1 {
		t.Fatalf("lane lengths = %d, %d", q.LenLane(evLane), q.LenLane(regLane))
	}
	authority := newLaneAuthority()
	// A durable hold on a registration is restored as a hold: kept, never
	// sent, and holding only its own lane.
	worker := laneWorker(t, q, authority, nil, 10*time.Millisecond)
	running := startWorker(t, worker)
	waitFor(t, "the evidence lane to drain", func() bool { return q.LenLane(evLane) == 0 })
	waitFor(t, "the registration hold restored", func() bool { e, ok := laneEntry(worker, regLane); return ok && e.State == DeviceHeld })
	running.stop()
	if q.LenLane(regLane) != 1 || authority.count(regLane) != 0 {
		t.Fatalf("registration lane %d, attempts %d", q.LenLane(regLane), authority.count(regLane))
	}
	if _, err := os.Stat(filepath.Join(dir, holdFilePrefix+regID)); err != nil {
		t.Fatalf("the restored hold record was removed: %v", err)
	}

	// A record the queue writes today has exactly the fields it always had.
	fresh, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 2))
	if err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(dir, fresh.ID+".json"))
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(raw, &fields); err != nil {
		t.Fatal(err)
	}
	keys := make([]string, 0, len(fields))
	for k := range fields {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	if strings.Join(keys, ",") != "artifact_type,enqueued_at_ms,id,payload,queue_seq,v" {
		t.Fatalf("a queue record now persists %v", keys)
	}
}
