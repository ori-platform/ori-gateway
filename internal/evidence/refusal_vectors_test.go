// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"
)

// refusalPolicyVectorPath is the draft refusal-policy corpus in a sibling
// ori-specs checkout. It is read in place until the contract merges and the
// file is vendored into testdata under MANIFEST.json like the other vectors.
var refusalPolicyVectorPath = filepath.Join("..", "..", "..", "ori-specs", "evidence-transport", "vectors", "refusal-policy.json")

type refusalVectorResponse struct {
	Status      int    `json:"status"`
	Reason      string `json:"reason"`
	Outcome     string `json:"outcome"`
	RetryAfterS int    `json:"retry_after_s"`
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
	raw, err := os.ReadFile(refusalPolicyVectorPath)
	if err != nil {
		t.Fatalf("the evidence-transport refusal-policy vectors are required at %s: %v", refusalPolicyVectorPath, err)
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
func vectorHTTPResponse(status int, media string, outcome *string, reason string, digest, retriable bool, retryAfter string, artifactDigest string, receipt []byte) *http.Response {
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
	return []byte(fmt.Sprintf(`{"v":1,"device_id":%q,"local_seq":%d,"signature":"ed25519:opaque"}`, device, seq))
}

func deviceCheckpoint(device string, highWater int) []byte {
	return []byte(fmt.Sprintf(`{"v":1,"device_id":%q,"high_water_seq":%d,"signature":"ed25519:opaque"}`, device, highWater))
}

func deviceRegistration(device string, n int) []byte {
	return []byte(fmt.Sprintf(`{"v":1,"device_id":%q,"anchor_epoch_id":"epoch-%d","signature":"ed25519:opaque"}`, device, n))
}

// laneState is a device's evidence-lane entry, where every checkpoint and
// envelope travels.
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

func TestRefusalPolicyVectorCases(t *testing.T) {
	vectors := loadRefusalVectors(t)
	const device = "site-a-edge-01"
	for _, tc := range vectors.Cases {
		t.Run(tc.Name, func(t *testing.T) {
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
			var mu sync.Mutex
			calls := 0
			client := &http.Client{Transport: roundTripFunc(func(*http.Request) (*http.Response, error) {
				mu.Lock()
				calls++
				mu.Unlock()
				return vectorHTTPResponse(tc.Status, tc.Media, tc.Outcome, tc.Reason, tc.Digest, tc.Retriable,
					string(tc.RetryAfter), digest, receipt), nil
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
			worker, err := NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
				RetryInterval: time.Second, BlockedReminderInterval: time.Hour,
				Logger: slog.New(slog.NewJSONHandler(logs, nil)),
			})
			if err != nil {
				t.Fatal(err)
			}
			started := time.Now()
			running := startWorker(t, worker)
			if tc.Expected.Class == "retire" {
				waitFor(t, "retirement", func() bool { return q.Len() == 0 })
				running.stop()
				if calls != 1 {
					t.Fatalf("%d attempts to retire", calls)
				}
				return
			}
			waitFor(t, "a failure", func() bool { return worker.Status().LastError != "" })
			// A handoff: only a handoff-only outcome may use it.
			worker.NotifyDevice(device)
			time.Sleep(30 * time.Millisecond)
			running.stop()

			entry, ok := laneState(worker, device)
			if !ok {
				t.Fatalf("device reported clean after %s", tc.Expected.Class)
			}
			wantState := map[string]string{
				"hold": DeviceHeld, "handoff": DeviceWaitingHandoff,
				"retry_after": DeviceBackingOff, "backoff": DeviceBackingOff,
			}[tc.Expected.Class]
			if entry.State != wantState {
				t.Fatalf("device state %q, vector expects %s", entry.State, tc.Expected.Class)
			}
			if q.Len() != 1 {
				t.Fatal("a non-accepting outcome retired the entry")
			}
			var recorded string
			if entry.Held != nil {
				recorded = entry.Held.Reason
			} else {
				lines := logs.records(t, stallMessage)
				if len(lines) != 1 {
					t.Fatalf("stall lines = %v", lines)
				}
				recorded, _ = lines[0]["reason"].(string)
			}
			if recorded != tc.Expected.Reason {
				t.Fatalf("recorded reason %q, vector expects %q", recorded, tc.Expected.Reason)
			}
			if tc.Reason != "" && tc.Reason != recorded && strings.Contains(logs.String(), tc.Reason) {
				t.Fatalf("unadmitted reason %q reached the log", tc.Reason)
			}
			want := 1
			if tc.Expected.Class == "handoff" {
				want = 2
			}
			mu.Lock()
			got := calls
			mu.Unlock()
			if got != want {
				t.Fatalf("%d attempts after one outcome and one handoff, want %d", got, want)
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
					t.Fatalf("next attempt in %v, want at least %v and never immediate", wait, floor)
				}
			}
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
}

func newVectorAuthority() *vectorAuthority {
	return &vectorAuthority{
		responses: map[LaneKey]refusalVectorResponse{}, nonces: map[LaneKey][]string{}, bodies: map[LaneKey][]string{},
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
	return vectorHTTPResponse(r.Status, media, &outcome, r.Reason, digestPresent, false, retryAfter, payloadDigest(body), nil), nil
}

// scaledRetryAfter shortens a Retry-After so a sequence runs in test time. The
// ordering the sequence asserts is unchanged.
const scaledRetryAfterS = 3

func TestRefusalPolicyVectorSequences(t *testing.T) {
	vectors := loadRefusalVectors(t)
	const retry = 400 * time.Millisecond
	const defaultDevice = "site-a-edge-01"
	for _, seq := range vectors.Sequences {
		t.Run(seq.Name, func(t *testing.T) {
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
			var worker *DeliveryWorker
			var running *runningWorker
			newWorker := func() {
				worker, err = NewDeliveryWorker(q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
					RetryInterval: retry, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
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
			for i, step := range seq.Steps {
				key := keyOf(step)
				// waitArchived holds this lane's current artifact to an
				// archival: out of the active lane, in the archive, and not
				// reported as a lane state.
				waitArchived := func(label string) {
					kind, payload := laneArtifact(key, generation[key])
					id := artifactID(kind, payload)
					waitFor(t, label+" archival", func() bool {
						_, archived := q.ArchivedFor(id)
						_, listed := laneEntry(worker, key)
						return archived && q.LenLane(key) == 0 && !listed
					})
					generation[key]++
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
				persistFails := step.HoldPersisted != nil && !*step.HoldPersisted
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
					waitFor(t, label+" attempt", func() bool { return authority.attempts(key) == before+1 })
					if otherWaits {
						waitFor(t, label+" handoff retry of the other lane", func() bool { return authority.attempts(other) == otherBefore+1 })
					}
				case "handoff":
					worker.NotifyDevice(key.Device)
				case "timer_elapsed":
					time.Sleep(3 * retry)
				case "restart":
					running.stop()
					q = openTestQueueAt(t, dir, 100, 1<<24)
					newWorker()
				case "artifact_handed_off_in_other_lane":
					worker.NotifyDevice(key.Device)
				case "retry_after_elapsed", "backoff_elapsed", "registration_retired":
					// Waited below, by the expectation.
				default:
					t.Fatalf("%s: event the worker cannot express", label)
				}
				switch step.Expect {
				case "retained":
					waitFor(t, label+" failure", func() bool { _, ok := laneEntry(worker, key); return ok })
					entry, _ := laneEntry(worker, key)
					if entry.State == DeviceHeld || q.LenLane(key) != 1 {
						t.Fatalf("%s: not retained: %#v", label, entry)
					}
				case "held":
					waitFor(t, label+" hold", func() bool { e, ok := laneEntry(worker, key); return ok && e.State == DeviceHeld })
				case "retired":
					waitFor(t, label+" retirement", func() bool { return q.LenLane(key) == 0 })
				case "no_attempt", "no_attempt_before_retry_after", "no_attempt_before_backoff":
					window := 100 * time.Millisecond
					if step.Expect == "no_attempt" && step.Event == "restart" {
						window = 3 * retry
					}
					time.Sleep(window)
					if got := authority.attempts(key); got != before {
						t.Fatalf("%s: %d attempts, want none", label, got-before)
					}
				case "attempt_fresh_envelope_same_bytes":
					wait := 3 * time.Second
					if step.Event == "retry_after_elapsed" {
						wait = (scaledRetryAfterS + 3) * time.Second
					}
					deadline := time.Now().Add(wait)
					for authority.attempts(key) == before && time.Now().Before(deadline) {
						time.Sleep(time.Millisecond)
					}
					if authority.attempts(key) != before+1 {
						t.Fatalf("%s: %d attempts, want one", label, authority.attempts(key)-before)
					}
					authority.mu.Lock()
					bodies, nonces := authority.bodies[key], authority.nonces[key]
					n := len(bodies)
					sameBytes := bodies[n-1] == bodies[n-2]
					fresh := nonces[n-1] != nonces[n-2]
					authority.mu.Unlock()
					if !sameBytes || !fresh {
						t.Fatalf("%s: same bytes %v, fresh envelope %v", label, sameBytes, fresh)
					}
					if step.Response != nil && step.Response.Status == http.StatusOK {
						waitFor(t, label+" retirement", func() bool { return q.LenLane(key) == 0 })
					}
					if step.Response != nil && vectorHolds(*step.Response) {
						if key.Lane == LaneRegistration && !persistFails {
							// A registration-lane hold is an archival.
							waitArchived(label)
						} else {
							waitFor(t, label+" hold", func() bool { e, ok := laneEntry(worker, key); return ok && e.State == DeviceHeld })
						}
					}
				case "archived":
					waitArchived(label)
				default:
					t.Fatalf("%s: expectation %q the worker cannot express", label, step.Expect)
				}
				if persistFails {
					restore()
					id := q.firstID(key)
					if _, ok := q.HoldFor(id); ok {
						t.Fatalf("%s: a hold was persisted while the write was made to fail", label)
					}
					if _, ok := q.ArchivedFor(id); ok {
						t.Fatalf("%s: an archive was committed while the write was made to fail", label)
					}
				}
			}
			if len(consequenceBefore) != 0 {
				t.Fatal("a registration_retired consequence was never checked")
			}
		})
	}
}

// vectorHolds is the evidence-transport/v1 table's hold rows, written from the
// contract and not from the code under test.
func vectorHolds(r refusalVectorResponse) bool {
	switch r.Status {
	case http.StatusBadRequest:
		return true
	case http.StatusConflict:
		return r.Reason != "pending_registration_conflict"
	case http.StatusUnprocessableEntity:
		return r.Reason != "unknown_key" && r.Reason != "unrecognised_version"
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
