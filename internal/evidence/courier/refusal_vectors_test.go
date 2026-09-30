// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
	"io"
	"log/slog"
	"net/http"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

type refusalVectorResponse struct {
	Status      int    `json:"status"`
	Reason      string `json:"reason"`
	Outcome     string `json:"outcome"`
	RetryAfterS int    `json:"retry_after_s"`
	// Retention is the result of the gateway's retention checks on a
	// retained refusal, `verified` or the defect that failed them.
	Retention *string `json:"retention"`
}

type refusalVectorCase struct {
	Name   string `json:"name"`
	Status int    `json:"status"`
	Media  string `json:"media"`
	// Outcome is nil when the vector's body has no outcome field at all.
	Outcome   *string `json:"outcome"`
	Reason    string  `json:"reason"`
	Digest    bool    `json:"digest"`
	Retriable bool    `json:"retriable"`
	Complete  *bool   `json:"complete"`
	// Staged is whether the gateway durably staged every authority artifact
	// the response carried; Retention the result of its retention checks.
	Staged    *bool   `json:"staged"`
	Retention *string `json:"retention"`
	// RetryAfter is sent as the header's text exactly as the vector gives it:
	// a number of seconds, or a value that is not one.
	RetryAfter json.RawMessage `json:"retry_after_s"`
	Expected   struct {
		Class    string `json:"class"`
		Reason   string `json:"reason"`
		MinWaitS int    `json:"min_wait_s"`
	} `json:"expected"`
}

type refusalVectorStep struct {
	Event  string `json:"event"`
	Device string `json:"device"`
	// Lane is the device's lane the step concerns; absent, the evidence lane.
	Lane          string                 `json:"lane"`
	Expect        string                 `json:"expect"`
	Response      *refusalVectorResponse `json:"response"`
	HoldPersisted *bool                  `json:"hold_persisted"`
	// ArchivePersisted false is an archive whose commit failed; Then is the
	// state the artifact reaches after the step.
	ArchivePersisted *bool  `json:"archive_persisted"`
	Then             string `json:"then"`
}

// refusalVectors is decoded strictly: a field the corpus gains that this test
// does not model fails the test rather than being ignored.
type refusalVectors struct {
	Contract  string              `json:"contract"`
	Note      string              `json:"note"`
	Cases     []refusalVectorCase `json:"cases"`
	Sequences []struct {
		Name  string              `json:"name"`
		Steps []refusalVectorStep `json:"steps"`
	} `json:"sequences"`
}

func loadRefusalVectors(t *testing.T) refusalVectors {
	t.Helper()
	raw, err := specvectors.Read("evidence-transport/vectors/refusal-policy-v2.json")
	if err != nil {
		t.Fatalf("the evidence-transport refusal-policy vectors are required at evidence-transport/vectors/refusal-policy-v2.json: %v", err)
	}
	var vectors refusalVectors
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&vectors); err != nil {
		t.Fatalf("decode refusal-policy vectors: %v", err)
	}
	if len(vectors.Cases) == 0 || len(vectors.Sequences) == 0 {
		t.Fatal("refusal-policy vectors carry no cases or sequences")
	}
	return vectors
}

// vectorHTTPResponse builds the authority's answer exactly as described: a nil
// outcome is a body with no outcome field.
func vectorHTTPResponse(status int, media string, outcome *string, reason string, digest, retriable bool, retryAfter string, artifactDigest string, receipt []byte, returned []byte) *http.Response {
	header := http.Header{}
	if retryAfter != "" {
		header.Set("Retry-After", retryAfter)
	}
	if media == "text" {
		header.Set("Content-Type", "text/plain; charset=utf-8")
		return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(http.StatusText(status)))}
	}
	header.Set("Content-Type", "application/json")
	body := map[string]any{"v": 1, "reason": reason, "retriable": retriable}
	if outcome != nil {
		body["outcome"] = *outcome
	}
	if digest {
		body["artifact_digest"] = artifactDigest
		artifacts := []any{}
		if outcome != nil && *outcome == "accepted" && receipt != nil {
			artifacts = append(artifacts, map[string]any{
				"artifact_type": "delivery_receipt",
				"artifact_b64":  base64Std(receipt),
			})
		}
		// A refusal carrying a return set: evidence-transport/v2 carries
		// authority artifacts on any response.
		if returned != nil && (outcome == nil || *outcome != "accepted") {
			artifacts = append(artifacts, map[string]any{
				"artifact_type": "delivery_receipt",
				"artifact_b64":  base64Std(returned),
			})
		}
		body["authority_artifacts"] = artifacts
	}
	raw, _ := json.Marshal(body)
	return &http.Response{StatusCode: status, Header: header, Body: io.NopCloser(strings.NewReader(string(raw)))}
}

func base64Std(b []byte) string {
	raw, _ := json.Marshal(b) // encoding/json writes []byte as standard Base64
	return strings.Trim(string(raw), `"`)
}

func deviceEnvelope(device string, seq int) []byte {
	return fmt.Appendf(nil, `{"v":1,"device_id":%q,"local_seq":%d,"signature":"ed25519:opaque"}`, device, seq)
}

func deviceCheckpoint(device string, highWater int) []byte {
	return fmt.Appendf(nil, `{"v":1,"device_id":%q,"high_water_seq":%d,"signature":"ed25519:opaque"}`, device, highWater)
}

func deviceRegistration(device string, n int) []byte {
	return fmt.Appendf(nil, `{"v":1,"device_id":%q,"anchor_epoch_id":"epoch-%d","signature":"ed25519:opaque"}`, device, n)
}

// laneState is a device's evidence-lane entry, where every checkpoint and
// envelope travels.
// quiesceLanes holds each lane's delivery lock, waiting out any attempt in
// flight, until the returned release: no attempt runs meanwhile, and the
// first attempt after the release consumes every wake pending until then.
func quiesceLanes(worker *DeliveryWorker, keys ...LaneKey) func() {
	lanes := make([]*deliveryLane, 0, len(keys))
	for _, key := range keys {
		lane := worker.lane(key)
		lane.deliveryMu.Lock()
		lanes = append(lanes, lane)
	}
	var once sync.Once
	return func() {
		once.Do(func() {
			for _, lane := range lanes {
				lane.deliveryMu.Unlock()
			}
		})
	}
}

func laneState(worker *DeliveryWorker, device string) (DeviceDeliveryStatus, bool) {
	return laneEntry(worker, LaneKey{Device: device, Lane: LaneEvidence})
}

// laneEntry is one device and lane's entry. It fails the lookup when two
// entries share a device and lane.
func laneEntry(worker *DeliveryWorker, key LaneKey) (DeviceDeliveryStatus, bool) {
	var found DeviceDeliveryStatus
	n := 0
	for _, entry := range worker.Status().Devices {
		if entry.DeviceID == key.Device && entry.Lane == key.Lane {
			found = entry
			n++
		}
	}
	return found, n == 1
}

func (w *DeliveryWorker) nextAttempt(device string) time.Time {
	return w.nextAttemptIn(LaneKey{Device: device, Lane: LaneEvidence})
}

func (w *DeliveryWorker) nextAttemptIn(key LaneKey) time.Time {
	l := w.lane(key)
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.notBefore
}

// refusalCasePins are the refusal-policy cases this gateway does not yet
// satisfy, each with its missing requirement and the exact deviation observed.
var refusalCasePins = map[string]vectorPin{
	"400 malformed retained": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects archive",
	},
	"400 malformed retained without its digest": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "unadmitted reason \"malformed\" reached the log",
	},
	"409 conflict refused carrying a retention": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state held, vector expects backoff",
	},
	"409 conflict retained": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects archive",
	},
	"409 conflict retained marked retriable": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects archive",
	},
	"422 bad_authenticator retained": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects archive",
	},
	"422 binding_mismatch retained": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects archive",
	},
	"422 commissioning_digest_mismatch retained": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects archive",
	},
	"503 retention_capacity_unavailable": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "recorded reason \"unrecognised\", vector expects \"retention_capacity_unavailable\"",
	},
	"503 retention_capacity_unavailable not marked retriable": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "recorded reason \"unrecognised\", vector expects \"retention_capacity_unavailable\"",
	},
	"507 retention_capacity_exhausted": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects hold",
	},
	"507 retention_capacity_exhausted marked retriable": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects hold",
	},
	"507 unknown reason": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "device state backing_off, vector expects hold",
	},
}

func TestRefusalPolicyVectorCases(t *testing.T) {
	vectors := loadRefusalVectors(t)
	names := make([]string, 0, len(vectors.Cases))
	for _, tc := range vectors.Cases {
		names = append(names, tc.Name)
	}
	checkPinsExist(t, refusalCasePins, names)
	const device = "site-a-edge-01"
	for _, tc := range vectors.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			report := func(format string, args ...any) {
				t.Helper()
				checkPin(t, refusalCasePins, tc.Name, fmt.Sprintf(format, args...))
			}
			// An acceptance that must carry a receipt is exercised with a
			// delivery envelope; every other case with a checkpoint.
			payload := deviceCheckpoint(device, 1)
			kind := ArtifactCheckpoint
			var receipt []byte
			if tc.Complete != nil {
				payload, kind = deviceEnvelope(device, 1), ArtifactDeliveryEnvelope
				if *tc.Complete {
					receipt = validReceiptBytes(device, 1, 1)
				}
			}
			digest := payloadDigest(payload)
			// A case that says its returned artifacts were not staged is a
			// response that carries one.
			var returned []byte
			if tc.Staged != nil && !*tc.Staged {
				returned = validReceiptBytes(device, 1, 1)
			}
			var mu sync.Mutex
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				return vectorHTTPResponse(tc.Status, tc.Media, tc.Outcome, tc.Reason, tc.Digest, tc.Retriable,
					string(tc.RetryAfter), digest, receipt, returned), nil
			})}
			channel, err := NewHTTPChannel(HTTPChannelOptions{
				Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
				Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: client,
			})
			if err != nil {
				t.Fatal(err)
			}
			q := openTestQueue(t, 10, 1<<24)
			if _, err := q.Enqueue(kind, payload); err != nil {
				t.Fatal(err)
			}
			logs := &syncBuffer{}
			// A case whose returned artifacts were not staged is a sink whose
			// durable write fails.
			sink := &fakeAuthoritySink{}
			if tc.Staged != nil && !*tc.Staged {
				sink.err = errors.New("the authority artifact could not be staged")
			}
			worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{
				RetryInterval: time.Second, BlockedReminderInterval: time.Hour,
				Logger: slog.New(slog.NewJSONHandler(logs, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			running := startWorker(t, worker)
			if tc.Expected.Class == "retire" {
				retired := waitUntil(func() bool { return q.Len() == 0 })
				running.stop()
				if !retired {
					report("not retired")
					return
				}
				if calls != 1 {
					report("%d attempts to retire", calls)
					return
				}
				report("")
				return
			}
			if !waitUntil(func() bool { return worker.Status().LastError != "" }) {
				running.stop()
				if q.Len() == 0 {
					report("retired, vector expects %s", tc.Expected.Class)
				} else {
					report("no outcome recorded, vector expects %s", tc.Expected.Class)
				}
				return
			}
			// A handoff: only a handoff-only outcome may use it.
			worker.NotifyDevice(device)
			time.Sleep(30 * time.Millisecond)
			running.stop()

			entry, ok := laneState(worker, device)
			if !ok {
				report("device reported clean, vector expects %s", tc.Expected.Class)
				return
			}
			wantState := map[string]string{
				"hold": DeviceHeld, "handoff": DeviceWaitingHandoff,
				"retry_after": DeviceBackingOff, "backoff": DeviceBackingOff,
			}[tc.Expected.Class]
			if entry.State != wantState {
				report("device state %s, vector expects %s", entry.State, tc.Expected.Class)
				return
			}
			if q.Len() != 1 {
				report("a non-accepting outcome retired the entry")
				return
			}
			var recorded string
			if entry.Held != nil {
				recorded = entry.Held.Reason
			} else {
				lines := logs.records(t, stallMessage)
				if len(lines) != 1 {
					report("%d stall lines, want one", len(lines))
					return
				}
				recorded, _ = lines[0]["reason"].(string)
			}
			if recorded != tc.Expected.Reason {
				report("recorded reason %q, vector expects %q", recorded, tc.Expected.Reason)
				return
			}
			if tc.Reason != "" && tc.Reason != recorded && strings.Contains(logs.String(), tc.Reason) {
				report("unadmitted reason %q reached the log", tc.Reason)
				return
			}
			want := 1
			if tc.Expected.Class == "handoff" {
				want = 2
			}
			mu.Lock()
			got := calls
			mu.Unlock()
			if got != want {
				report("%d attempts after one outcome and one handoff, want %d", got, want)
				return
			}
			if wantState == DeviceBackingOff {
				wait := worker.nextAttempt(device).Sub(started)
				seconds, _ := strconv.Atoi(string(tc.RetryAfter))
				floor := time.Duration(max(seconds, 0)) * time.Second
				if tc.Expected.MinWaitS > 0 {
					floor = time.Duration(tc.Expected.MinWaitS) * time.Second
				}
				if tc.Expected.Class == "backoff" && tc.Expected.MinWaitS == 0 {
					floor = 0
				}
				if wait < floor || wait < 900*time.Millisecond {
					report("next attempt too soon, want at least %v and never immediate", floor)
					return
				}
			}
			report("")
		})
	}
}

// vectorAuthority answers each device and lane with the response the current
// step set for it, and records every request per device and lane. The lane is
// read from the request's artifact type header, independently of the queue.
type vectorAuthority struct {
	mu        sync.Mutex
	responses map[LaneKey]refusalVectorResponse
	nonces    map[LaneKey][]string
	bodies    map[LaneKey][]string
	// persisted is the lane's persisted back-off deadline, read as each
	// request arrives; early records, per request, how long before that
	// deadline it arrived, or zero.
	persisted func(LaneKey) (time.Time, bool)
	early     map[LaneKey][]time.Duration
}

func newVectorAuthority() *vectorAuthority {
	return &vectorAuthority{
		responses: map[LaneKey]refusalVectorResponse{}, nonces: map[LaneKey][]string{}, bodies: map[LaneKey][]string{},
		early: map[LaneKey][]time.Duration{},
	}
}

func (a *vectorAuthority) set(key LaneKey, r *refusalVectorResponse) {
	if r == nil {
		return
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.responses[key] = *r
}

func (a *vectorAuthority) attempts(key LaneKey) int {
	a.mu.Lock()
	defer a.mu.Unlock()
	return len(a.bodies[key])
}

// earlyBy is how long before its persisted back-off deadline the lane's nth
// request arrived, counting from zero, or zero when it did not.
func (a *vectorAuthority) earlyBy(key LaneKey, n int) time.Duration {
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.early[key][n]
}

// anyEarly names a request that arrived before its persisted back-off
// deadline, or returns "".
func (a *vectorAuthority) anyEarly() string {
	a.mu.Lock()
	defer a.mu.Unlock()
	for key, early := range a.early {
		for n, by := range early {
			if by > 0 {
				return fmt.Sprintf("%s %s request %d arrived %s before its persisted back-off elapsed", key.Device, key.Lane, n, by)
			}
		}
	}
	return ""
}

func (a *vectorAuthority) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	lane := LaneEvidence
	if req.Header.Get("X-Ori-Evidence-Artifact-Type") == "anchor_registration" {
		lane = LaneRegistration
	}
	key := LaneKey{Device: recordDevice(body), Lane: lane}
	a.mu.Lock()
	defer a.mu.Unlock()
	a.nonces[key] = append(a.nonces[key], req.Header.Get("X-Ori-Evidence-Nonce"))
	a.bodies[key] = append(a.bodies[key], string(body))
	// The deadline is persisted in wall-clock milliseconds, so the arrival is
	// compared with it on the wall clock, exactly.
	arrived := time.Now().Round(0)
	var early time.Duration
	if a.persisted != nil {
		if notBefore, ok := a.persisted(key); ok && arrived.Before(notBefore) {
			early = notBefore.Sub(arrived)
		}
	}
	a.early[key] = append(a.early[key], early)
	r, set := a.responses[key]
	if !set {
		// A lane no step has answered, such as the other lane's artifact in
		// a cross-lane handoff, is accepted.
		r = refusalVectorResponse{Status: http.StatusOK}
	}
	media := "json"
	if r.Status != http.StatusOK && r.Reason == "" && r.Outcome == "" {
		// A status with no body semantics in the step, such as a framework
		// 413, is answered as the framework would: plain text.
		media = "text"
	}
	digestPresent := r.Status != http.StatusUnauthorized
	outcome := r.Outcome
	if outcome == "" {
		switch r.Status {
		case http.StatusOK:
			outcome = "accepted"
		case http.StatusTooManyRequests, http.StatusServiceUnavailable:
			outcome = "pending"
		default:
			outcome = "refused"
		}
	}
	retryAfter := ""
	if r.RetryAfterS != 0 {
		retryAfter = strconv.Itoa(r.RetryAfterS)
	}
	return vectorHTTPResponse(r.Status, media, &outcome, r.Reason, digestPresent, false, retryAfter, payloadDigest(body), nil, nil), nil
}

// scaledRetryAfter shortens a Retry-After so a sequence runs in test time. The
// ordering the sequence asserts is unchanged.
const scaledRetryAfterS = 3

// refusalSequencePins are the refusal-policy sequences this gateway does not yet
// satisfy, each with its missing requirement and the exact deviation observed.
var refusalSequencePins = map[string]vectorPin{
	"a 503 retention_capacity_unavailable backs off without custody until capacity is restored, never by eviction": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 3 (backoff_elapsed site-a-edge-01 evidence): not archived",
	},
	"a 507 holds only that device's lane while its other lane and another device deliver": {
		Requirement: "missing_requirement:retention-capacity",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 0 (deliver device-a evidence): not held, state backing_off",
	},
	"a registration conflict backs off in its own lane and resolves once commissioning does": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 4 (backoff_elapsed site-a-edge-01 registration): not archived",
	},
	"a retained checkpoint rollback is archived and the next envelope is delivered": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 1 (deliver site-a-edge-01 evidence): not archived",
	},
	"a retention that does not bind never releases the gateway copy, and the verified retry archives it": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 4 (backoff_elapsed site-a-edge-01 evidence): not archived",
	},
	"a terminal registration refusal is archived and never blocks a later registration": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 0 (deliver site-a-edge-01 registration): not archived",
	},
	"an archive survives a restart and is never re-sent": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 0 (deliver site-a-edge-01 evidence): not archived",
	},
	"an archive that cannot commit leaves the head held, and the identical retry after a restart archives it": {
		Requirement: "missing_requirement:retention-verification",
		Issue:       "https://github.com/ori-platform/ori-gateway/issues/102",
		Observed:    "step 0 (deliver site-a-edge-01 evidence): not held, state backing_off",
	},
}

func TestRefusalPolicyVectorSequences(t *testing.T) {
	vectors := loadRefusalVectors(t)
	names := make([]string, 0, len(vectors.Sequences))
	for _, seq := range vectors.Sequences {
		names = append(names, seq.Name)
	}
	checkPinsExist(t, refusalSequencePins, names)
	const retry = 400 * time.Millisecond
	// The back-off is long enough that a restart, and the observation after
	// it, fall inside it on all but a heavily loaded machine. Its bound is
	// the base, well under the scaled Retry-After, which a bound never
	// shortens.
	const backoff = time.Second
	const maxBackoff = backoff
	const plays = 5
	const defaultDevice = "site-a-edge-01"
	for _, seq := range vectors.Sequences {
		t.Run(seq.Name, func(t *testing.T) {
			// play runs the sequence once. It is inconclusive when the
			// machine outran an observation that must land before a
			// back-off elapses; nothing was observed, so it is played again.
			play := func() (observed string, inconclusive bool) {
				dir := filepath.Join(t.TempDir(), "queue")
				authority := newVectorAuthority()
				channel, err := NewHTTPChannel(HTTPChannelOptions{
					Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
					Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
				})
				if err != nil {
					t.Fatal(err)
				}
				q := openTestQueueAt(t, dir, 100, 1<<24)
				// The deadline in force for a lane is the persisted back-off of
				// the head it sends. q is reopened by a restart while no
				// worker runs.
				persistedFor := func(key LaneKey) (time.Time, bool) {
					record, ok := q.BackoffFor(q.firstID(key))
					return time.UnixMilli(record.NotBeforeMS), ok
				}
				authority.persisted = persistedFor
				var worker *DeliveryWorker
				var running *runningWorker
				newWorker := func() {
					worker, err = NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
						RetryInterval: retry, BackoffBase: backoff, MaxBackoff: maxBackoff, BlockedReminderInterval: time.Hour,
						Logger: slog.New(slog.DiscardHandler),
					})
					if err != nil {
						t.Fatal(err)
					}
					running = startWorker(t, worker)
				}
				var restore func()
				keyOf := func(step refusalVectorStep) LaneKey {
					device := step.Device
					if device == "" {
						device = defaultDevice
					}
					switch step.Lane {
					case "", string(LaneEvidence):
						return LaneKey{Device: device, Lane: LaneEvidence}
					case string(LaneRegistration):
						return LaneKey{Device: device, Lane: LaneRegistration}
					default:
						t.Fatalf("lane %q the worker cannot express", step.Lane)
						return LaneKey{}
					}
				}
				// A registration_retired step is the consequence of the step
				// before it, whose 200 retires the registration: the evidence
				// lane's response is set, and its attempts counted, before that
				// step runs.
				consequenceBefore := map[LaneKey]int{}
				// generation picks a lane's artifact bytes. A deliver after a
				// retirement re-offers the same bytes, a new admission of them;
				// a deliver after an archival is a later registration.
				generation := map[LaneKey]int{}
				runStep := func(i int, step refusalVectorStep) string {
					key := keyOf(step)
					// waitArchived holds this lane's current artifact to an
					// archival. The gateway keeps no archive, so none is ever
					// observed; an artifact that leaves its lane anyway has left
					// custody, which is reported as its own deviation.
					waitArchived := func(label string) string {
						released := waitUntil(func() bool {
							_, listed := laneEntry(worker, key)
							return q.LenLane(key) == 0 && !listed
						})
						generation[key]++
						if released {
							return label + ": released from custody with no archive"
						}
						return label + ": not archived"
					}
					if step.Response != nil && step.Response.RetryAfterS > 0 {
						step.Response.RetryAfterS = scaledRetryAfterS
					}
					before, preset := consequenceBefore[key]
					if step.Event == "registration_retired" {
						if !preset || key.Lane != LaneEvidence {
							t.Fatalf("step %d: registration_retired must follow the step that retires a registration", i)
						}
						delete(consequenceBefore, key)
					} else {
						authority.set(key, step.Response)
						before = authority.attempts(key)
					}
					if i+1 < len(seq.Steps) && seq.Steps[i+1].Event == "registration_retired" {
						next := keyOf(seq.Steps[i+1])
						authority.set(next, seq.Steps[i+1].Response)
						consequenceBefore[next] = authority.attempts(next)
					}
					label := fmt.Sprintf("step %d (%s %s %s)", i, step.Event, key.Device, key.Lane)
					persistFails := (step.HoldPersisted != nil && !*step.HoldPersisted) ||
						(step.ArchivePersisted != nil && !*step.ArchivePersisted)
					// A newly admitted artifact is a handoff for both of its
					// device's lanes: a lane of the same device waiting for a
					// handoff is retried once.
					other := LaneKey{Device: key.Device, Lane: LaneRegistration}
					if key.Lane == LaneRegistration {
						other.Lane = LaneEvidence
					}
					otherBefore := authority.attempts(other)
					otherWaits := false
					if worker != nil {
						e, ok := laneEntry(worker, other)
						otherWaits = ok && e.State == DeviceWaitingHandoff
					}
					// A step that signals a handoff runs against quiescent lanes:
					// no attempt is in flight, and the first attempt after the
					// release answers every wake pending until then, so an attempt
					// counted here is one this step caused.
					release := func() {}
					switch step.Event {
					case "deliver", "handoff", "artifact_handed_off_in_other_lane", "queue_full":
						if worker != nil {
							release = quiesceLanes(worker, key, other)
						}
					}
					defer release()
					if step.Event == "deliver" {
						kind, payload := laneArtifact(key, generation[key])
						if _, err := q.Enqueue(kind, payload); err != nil {
							t.Fatal(err)
						}
					}
					if step.Event == "artifact_handed_off_in_other_lane" {
						// A fresh artifact in the device's other lane, admitted
						// and handed off; this lane's retry is the expectation.
						kind, payload := laneArtifact(other, 1000+generation[other])
						if _, err := q.Enqueue(kind, payload); err != nil {
							t.Fatal(err)
						}
						generation[other]++
					}
					if persistFails {
						restore = unwritable(t, dir)
					}
					switch step.Event {
					case "deliver":
						if worker == nil {
							newWorker()
						} else {
							worker.NotifyDevice(key.Device)
						}
						release()
						if !waitUntil(func() bool { return authority.attempts(key) == before+1 }) {
							return fmt.Sprintf("%s: %d attempts, want one", label, authority.attempts(key)-before)
						}
						if otherWaits && !waitUntil(func() bool { return authority.attempts(other) == otherBefore+1 }) {
							return fmt.Sprintf("%s: the other lane was retried %d times on the handoff, want once", label, authority.attempts(other)-otherBefore)
						}
					case "handoff":
						worker.NotifyDevice(key.Device)
						release()
					case "timer_elapsed":
						time.Sleep(3 * retry)
					case "restart":
						running.stop()
						q = openTestQueueAt(t, dir, 100, 1<<24)
						newWorker()
					case "artifact_handed_off_in_other_lane":
						worker.NotifyDevice(key.Device)
						release()
					case "queue_full":
						// An authenticated queue_full for a configured device is a
						// handoff: the outbound subscription notifies the device
						// after the ingress publishes the refusal.
						worker.NotifyDevice(key.Device)
						release()
					case "queue_full_unconfigured_device", "admission_unauthenticated":
						// Refused before the ingress admits anything: the
						// subscription returns without notifying the device.
					case "retry_after_elapsed", "backoff_elapsed", "registration_retired":
						// Waited below, by the expectation.
					default:
						t.Fatalf("%s: event the worker cannot express", label)
					}
					switch step.Expect {
					case "retained":
						if !waitUntil(func() bool { _, ok := laneEntry(worker, key); return ok }) {
							return label + ": no outcome recorded"
						}
						entry, _ := laneEntry(worker, key)
						if entry.State == DeviceHeld || q.LenLane(key) != 1 {
							return fmt.Sprintf("%s: not retained, state %s", label, entry.State)
						}
					case "held":
						if !waitUntil(func() bool { e, ok := laneEntry(worker, key); return ok && e.State == DeviceHeld }) {
							e, _ := laneEntry(worker, key)
							return fmt.Sprintf("%s: not held, state %s", label, e.State)
						}
					case "retired":
						if !waitUntil(func() bool { return q.LenLane(key) == 0 }) {
							return label + ": not retired"
						}
					case "no_attempt":
						window := 100 * time.Millisecond
						if step.Event == "restart" {
							window = 3 * retry
						}
						time.Sleep(window)
						if got := authority.attempts(key); got != before {
							return fmt.Sprintf("%s: %d attempts, want none", label, got-before)
						}
					case "no_attempt_before_retry_after", "no_attempt_before_backoff":
						// Observe before the persisted back-off elapses: an attempt
						// then is this step bringing it forward. One the back-off
						// allowed means the machine outran the observation. Every
						// attempt is also held to the deadline it arrived under.
						notBefore, ok := persistedFor(key)
						if ok {
							time.Sleep(min(100*time.Millisecond, time.Until(notBefore)-50*time.Millisecond))
						}
						if got := authority.attempts(key); got != before {
							if authority.earlyBy(key, before) > 0 {
								return fmt.Sprintf("%s: %d attempts, want none", label, got-before)
							}
							inconclusive = true
							return label + ": the back-off elapsed before the step was observed"
						}
						if !ok {
							return label + ": no persisted back-off"
						}
						if time.Now().After(notBefore) {
							inconclusive = true
							return label + ": the back-off elapsed before the step was observed"
						}
					case "attempt_fresh_envelope_same_bytes":
						wait := 3 * time.Second
						if step.Event == "retry_after_elapsed" {
							wait = (scaledRetryAfterS + 3) * time.Second
						}
						deadline := time.Now().Add(wait)
						// An elapsed back-off is the lane's own schedule: a lane that
						// kept backing off while earlier steps waited is due when
						// its next attempt is, not after a fixed window.
						if step.Event == "backoff_elapsed" {
							if due := worker.nextAttemptIn(key).Add(3 * time.Second); due.After(deadline) {
								deadline = due
							}
						}
						for authority.attempts(key) == before && time.Now().Before(deadline) {
							time.Sleep(time.Millisecond)
						}
						if authority.attempts(key) != before+1 {
							return fmt.Sprintf("%s: %d attempts, want one", label, authority.attempts(key)-before)
						}
						if by := authority.earlyBy(key, before); by > 0 {
							return fmt.Sprintf("%s: attempted %s before the persisted back-off elapsed", label, by)
						}
						authority.mu.Lock()
						bodies, nonces := authority.bodies[key], authority.nonces[key]
						n := len(bodies)
						sameBytes := bodies[n-1] == bodies[n-2]
						fresh := nonces[n-1] != nonces[n-2]
						authority.mu.Unlock()
						if !sameBytes || !fresh {
							return fmt.Sprintf("%s: same bytes %v, fresh envelope %v", label, sameBytes, fresh)
						}
						if step.Response != nil && step.Response.Status == http.StatusOK && step.Then == "" {
							if !waitUntil(func() bool { return q.LenLane(key) == 0 }) {
								return label + ": not retired"
							}
						}
						if step.Response != nil && vectorHolds(*step.Response) && step.Then == "" {
							// A terminal refusal archives only on a retention; without
							// one it is held, in either lane (evidence-transport/v2).
							if step.Response.Outcome == "refused_retained" && !persistFails {
								if d := waitArchived(label); d != "" {
									return d
								}
							} else if !waitUntil(func() bool { e, ok := laneEntry(worker, key); return ok && e.State == DeviceHeld }) {
								return label + ": not held"
							}
						}
					case "archived":
						if d := waitArchived(label); d != "" {
							return d
						}
					default:
						t.Fatalf("%s: expectation %q the worker cannot express", label, step.Expect)
					}
					// then is the state the artifact reaches once the step's
					// response has been acted on.
					switch step.Then {
					case "":
					case "retained":
						if !waitUntil(func() bool {
							e, ok := laneEntry(worker, key)
							return ok && e.State != DeviceHeld && q.LenLane(key) == 1
						}) {
							e, _ := laneEntry(worker, key)
							return fmt.Sprintf("%s: then not retained, state %s", label, e.State)
						}
					case "retired":
						if !waitUntil(func() bool { return q.LenLane(key) == 0 }) {
							return label + ": then not retired"
						}
					case "archived":
						if d := waitArchived(label); d != "" {
							return d
						}
					default:
						t.Fatalf("%s: then %q the worker cannot express", label, step.Then)
					}
					if persistFails {
						restore()
						id := q.firstID(key)
						if _, ok := q.HoldFor(id); ok {
							return label + ": a hold was persisted while the write was made to fail"
						}
					}
					return ""
				}
				for i, step := range seq.Steps {
					observed = runStep(i, step)
					// An attempt before its deadline, in any lane, is a
					// violation, and a violation is never inconclusive.
					if early := authority.anyEarly(); early != "" && (observed == "" || inconclusive) {
						observed = fmt.Sprintf("step %d (%s): %s", i, step.Event, early)
						inconclusive = false
					}
					if observed != "" {
						break
					}
				}
				if observed == "" && len(consequenceBefore) != 0 {
					t.Fatal("a registration_retired consequence was never checked")
				}
				if running != nil {
					running.stop()
				}
				return observed, inconclusive
			}
			observed, conclusive := playUntilConclusive(plays, play, func(observed string) { t.Logf("played again: %s", observed) })
			if !conclusive {
				t.Fatalf("inconclusive in %d plays: %s", plays, observed)
			}
			checkPin(t, refusalSequencePins, seq.Name, observed)
		})
	}
}

// playUntilConclusive plays until a play is conclusive, at most plays times.
// A conclusive play, a violation included, is never played again.
func playUntilConclusive(plays int, play func() (string, bool), again func(string)) (string, bool) {
	observed, inconclusive := play()
	for n := 1; inconclusive && n < plays; n++ {
		again(observed)
		observed, inconclusive = play()
	}
	return observed, !inconclusive
}

func TestPlayUntilConclusiveNeverReplaysAViolation(t *testing.T) {
	plays := 0
	play := func(results ...bool) func() (string, bool) {
		plays = 0
		return func() (string, bool) {
			inconclusive := results[min(plays, len(results)-1)]
			plays++
			return fmt.Sprintf("play %d", plays), inconclusive
		}
	}
	if observed, ok := playUntilConclusive(5, play(false), func(string) {}); !ok || observed != "play 1" || plays != 1 {
		t.Fatalf("a conclusive play was played again: %q after %d plays", observed, plays)
	}
	if observed, ok := playUntilConclusive(5, play(true, true, false), func(string) {}); !ok || observed != "play 3" || plays != 3 {
		t.Fatalf("inconclusive plays: %q after %d plays", observed, plays)
	}
	if _, ok := playUntilConclusive(5, play(true), func(string) {}); ok || plays != 5 {
		t.Fatalf("an always inconclusive sequence was played %d times, conclusive %v", plays, ok)
	}
}

// vectorHolds is the evidence-transport/v2 table's hold rows, written from the
// contract and not from the code under test.
func vectorHolds(r refusalVectorResponse) bool {
	switch r.Status {
	case http.StatusBadRequest:
		return true
	case http.StatusConflict:
		return r.Reason != "pending_registration_conflict"
	case http.StatusUnprocessableEntity:
		return r.Reason != "unknown_key" && r.Reason != "unrecognised_version"
	case http.StatusInsufficientStorage:
		return true
	default:
		return false
	}
}

// laneArtifact is a lane's n-th distinct artifact: a registration in the
// registration lane, a checkpoint in the evidence lane.
func laneArtifact(key LaneKey, n int) (ArtifactType, []byte) {
	if key.Lane == LaneRegistration {
		return ArtifactAnchorRegistration, deviceRegistration(key.Device, n+1)
	}
	return ArtifactCheckpoint, deviceCheckpoint(key.Device, n+1)
}

func (q *DurableQueue) firstID(key LaneKey) string {
	head, _ := q.PeekLane(key)
	return head.ID
}
