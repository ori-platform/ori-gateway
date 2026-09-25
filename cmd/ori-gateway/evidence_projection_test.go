// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"regexp"
	"sort"
	"strings"
	"sync"
	"testing"
	"time"
	"unicode"
	"unicode/utf8"

	"github.com/ori-platform/ori-gateway/internal/evidence"
)

// siteHealthVectorPath is the draft projection corpus in a sibling ori-specs
// checkout, read in place until the contract merges and it is vendored.
var siteHealthVectorPath = filepath.Join("..", "..", "..", "ori-specs", "gateway-api", "vectors", "site-health-evidence-delivery.json")

// projectionRefusal names the gateway-api/v1 rule a projection breaks.
type projectionRefusal struct{ rule, detail string }

func (r *projectionRefusal) Error() string { return r.rule + ": " + r.detail }

// The oracle below is written from gateway-api/v1 and the evidence-transport/v1
// refusal policy, independently of the code under test.
var (
	projectionStates    = map[string]bool{"held": true, "waiting_handoff": true, "backing_off": true}
	projectionTypes     = map[string]bool{"anchor_registration": true, "delivery_envelope": true, "checkpoint": true}
	projectionHeldAt    = map[int]map[string]bool{400: {"malformed": true, "unrecognised": true}, 409: {"conflict": true, "unrecognised": true}, 422: {"bad_authenticator": true, "binding_mismatch": true, "commissioning_digest_mismatch": true, "unrecognised": true}}
	projectionEntryKeys = []string{"device_id", "lane", "pending", "state"}
	projectionLanes     = map[string]bool{"registration": true, "evidence": true}
	projectionTopKeys   = map[string]bool{"pending": true, "degraded": true, "blocked": true, "last_failure_at_ms": true, "last_error": true, "faults": true, "devices": true, "archived_registrations": true}
	// projectionArchivedKeys is an archived refusal's closed field set, sorted.
	projectionArchivedKeys = []string{"acknowledged", "artifact_digest", "device_id", "reason", "refusal_status", "refused_at_ms"}
	projectionFaults       = map[string]bool{
		"store_unavailable": true, "admission_failed": true,
		"delivery_impaired": true, "device_unconfigured": true,
	}
	projectionLastErrors = map[string]bool{
		"channel_unavailable": true, "channel_refused": true, "channel_permanent_refusal": true,
		"channel_receiver_state_refusal": true, "malformed_channel_response": true,
		"runtime_sink_unavailable": true, "queue_retirement_failed": true, "delivery_pending": true,
	}
	projectionHeldKeys = []string{"acknowledged", "artifact_digest", "artifact_type", "first_held_at_ms", "queue_record", "reason", "refusal_status"}
	projectionRecordRE = regexp.MustCompile(`^[0-9a-f]{64}$`)
	projectionDigestRE = regexp.MustCompile(`^sha256:[0-9a-f]{64}$`)
)

// projectionDeviceID is the device_id domain: 1 to 128 Unicode scalar values
// with no control character (Cc), no White_Space character, "/", "+" or "#".
func projectionDeviceID(s string) bool {
	if !utf8.ValidString(s) {
		return false
	}
	n := 0
	for _, r := range s {
		if unicode.Is(unicode.Cc, r) || unicode.Is(unicode.White_Space, r) || r == '/' || r == '+' || r == '#' {
			return false
		}
		n++
	}
	return n >= 1 && n <= 128
}

func refuse(rule, format string, args ...any) error {
	return &projectionRefusal{rule: rule, detail: fmt.Sprintf(format, args...)}
}

func keysOf(m map[string]any) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

func intValue(v any, minimum int64) (int64, bool) {
	number, ok := v.(json.Number)
	if !ok {
		return 0, false
	}
	n, err := number.Int64()
	return n, err == nil && n >= minimum
}

// validateProjection applies every gateway-api/v1 rule for evidence_delivery.
func validateProjection(p map[string]any) error {
	for _, key := range []string{"pending", "degraded", "blocked"} {
		if _, ok := p[key]; !ok {
			return refuse("fields", "projection lacks %s", key)
		}
	}
	for key := range p {
		if !projectionTopKeys[key] {
			return refuse("fields", "projection carries %s", key)
		}
	}
	if lastError, ok := p["last_error"]; ok {
		if text, _ := lastError.(string); !projectionLastErrors[text] {
			return refuse("last_error", "last_error %v", lastError)
		}
	}
	if at, ok := p["last_failure_at_ms"]; ok {
		if _, ok := intValue(at, 1); !ok {
			return refuse("fields", "last_failure_at_ms is a positive integer")
		}
	}
	rawFaults, hasFaults := p["faults"]
	var faults []any
	if hasFaults {
		list, ok := rawFaults.([]any)
		if !ok {
			return refuse("faults", "faults is not a list")
		}
		seenFault := map[string]bool{}
		for _, f := range list {
			text, _ := f.(string)
			if !projectionFaults[text] {
				return refuse("faults", "fault %v outside the closed vocabulary", f)
			}
			if seenFault[text] {
				return refuse("faults", "a fault repeats")
			}
			seenFault[text] = true
		}
		faults = list
	}
	rawDevices, hasDevices := p["devices"]
	var devices []any
	if hasDevices {
		list, ok := rawDevices.([]any)
		if !ok {
			return refuse("fields", "devices is not a list")
		}
		devices = list
		// A present devices is complete, so it never stands beside
		// delivery_impaired.
		for _, f := range faults {
			if f == "delivery_impaired" {
				return refuse("delivery_impaired", "delivery_impaired beside devices")
			}
		}
	}
	degraded, okD := p["degraded"].(bool)
	blocked, okB := p["blocked"].(bool)
	if !okD || !okB {
		return refuse("fields", "degraded and blocked are booleans")
	}
	seen := map[string]bool{}
	var listed int64
	allHeld := true
	for _, raw := range devices {
		entry, ok := raw.(map[string]any)
		if !ok {
			return refuse("fields", "an entry is not an object")
		}
		state, _ := entry["state"].(string)
		if !projectionStates[state] {
			return refuse("state", "state %v", entry["state"])
		}
		_, hasHeld := entry["held"]
		if hasHeld != (state == "held") {
			return refuse("held_presence", "held present exactly when the state is held")
		}
		want := append([]string(nil), projectionEntryKeys...)
		if state == "held" {
			want = append(want, "held")
		}
		sort.Strings(want)
		if strings.Join(keysOf(entry), ",") != strings.Join(want, ",") {
			return refuse("fields", "entry fields %v", keysOf(entry))
		}
		device, _ := entry["device_id"].(string)
		if !projectionDeviceID(device) {
			return refuse("device_id", "device_id %v", entry["device_id"])
		}
		lane, _ := entry["lane"].(string)
		if !projectionLanes[lane] {
			return refuse("lane", "lane %v", entry["lane"])
		}
		if seen[device+"\x00"+lane] {
			return refuse("device_id", "device %s lane %s listed twice", device, lane)
		}
		seen[device+"\x00"+lane] = true
		pending, ok := intValue(entry["pending"], 1)
		if !ok {
			return refuse("pending", "an entry's pending is a positive integer")
		}
		listed += pending
		if state != "held" {
			allHeld = false
			continue
		}
		held, ok := entry["held"].(map[string]any)
		if !ok || strings.Join(keysOf(held), ",") != strings.Join(projectionHeldKeys, ",") {
			return refuse("fields", "held fields")
		}
		if record, _ := held["queue_record"].(string); !projectionRecordRE.MatchString(record) {
			return refuse("queue_record", "queue_record is not 64 lowercase hex")
		}
		if digest, _ := held["artifact_digest"].(string); !projectionDigestRE.MatchString(digest) {
			return refuse("fields", "artifact_digest is not sha256 hex")
		}
		kind, _ := held["artifact_type"].(string)
		if !projectionTypes[kind] {
			return refuse("artifact_type", "artifact_type %v", held["artifact_type"])
		}
		// The held head is an anchor_registration in the registration lane
		// and never in the evidence lane.
		if (lane == "registration") != (kind == "anchor_registration") {
			return refuse("lane", "%s cannot head the %s lane", kind, lane)
		}
		status, ok := intValue(held["refusal_status"], 0)
		if !ok || projectionHeldAt[int(status)] == nil {
			return refuse("refusal_status", "status %v never holds", held["refusal_status"])
		}
		if reason, _ := held["reason"].(string); !projectionHeldAt[int(status)][reason] {
			return refuse("reason", "%d does not hold at %v", status, held["reason"])
		}
		if _, ok := intValue(held["first_held_at_ms"], 1); !ok {
			return refuse("first_held_at_ms", "first_held_at_ms is a positive integer")
		}
		if ack, ok := held["acknowledged"].(bool); !ok || ack {
			return refuse("acknowledged", "acknowledged is false until an acknowledgement surface exists")
		}
	}
	total, ok := intValue(p["pending"], 0)
	if !ok || listed > total {
		return refuse("pending", "the entries' pending exceed the gateway-level pending")
	}
	wantBlocked := hasDevices && total > 0 && listed == total && allHeld
	if blocked != wantBlocked {
		return refuse("blocked", "blocked must be %v", wantBlocked)
	}
	rawArchived, hasArchived := p["archived_registrations"]
	var archived []any
	if hasArchived {
		list, ok := rawArchived.([]any)
		if !ok {
			return refuse("archived", "archived_registrations is not a list")
		}
		archived = list
		if err := validateArchived(archived); err != nil {
			return err
		}
	}
	// With faults, devices and archived_registrations all absent, degraded
	// keeps an older producer's advisory meaning and is not constrained.
	if hasFaults || hasDevices || hasArchived {
		want := len(faults) > 0 || len(devices) > 0 || len(archived) > 0
		if degraded != want {
			return refuse("degraded", "degraded must be %v", want)
		}
	}
	return nil
}

// validateArchived applies gateway-api/v1's archived_registrations rules: the
// closed field set, the device_id domain, one entry per digest, a status and
// reason at which the refusal policy holds, and acknowledged false. An
// archived refusal's bytes are never projected.
func validateArchived(archived []any) error {
	seen := map[string]bool{}
	for _, raw := range archived {
		entry, ok := raw.(map[string]any)
		if !ok || strings.Join(keysOf(entry), ",") != strings.Join(projectionArchivedKeys, ",") {
			return refuse("archived", "archived entry fields %v", raw)
		}
		if device, _ := entry["device_id"].(string); !projectionDeviceID(device) {
			return refuse("device_id", "device_id %v", entry["device_id"])
		}
		digest, _ := entry["artifact_digest"].(string)
		if !projectionDigestRE.MatchString(digest) {
			return refuse("archived", "artifact_digest is not sha256 hex")
		}
		if seen[digest] {
			return refuse("archived", "an archived refusal is listed twice")
		}
		seen[digest] = true
		status, ok := intValue(entry["refusal_status"], 0)
		if !ok || projectionHeldAt[int(status)] == nil {
			return refuse("refusal_status", "status %v never holds", entry["refusal_status"])
		}
		if reason, _ := entry["reason"].(string); !projectionHeldAt[int(status)][reason] {
			return refuse("reason", "%d does not hold at %v", status, entry["reason"])
		}
		if _, ok := intValue(entry["refused_at_ms"], 1); !ok {
			return refuse("archived", "refused_at_ms is a positive integer")
		}
		if ack, ok := entry["acknowledged"].(bool); !ok || ack {
			return refuse("acknowledged", "acknowledged is false until an acknowledgement surface exists")
		}
	}
	return nil
}

func decodeProjection(t *testing.T, raw []byte) map[string]any {
	t.Helper()
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var out map[string]any
	if err := decoder.Decode(&out); err != nil {
		t.Fatal(err)
	}
	return out
}

// TestProjectionOracleAgreesWithTheVectors holds the oracle to the draft
// corpus: every accepted projection passes, every refused one fails for the
// rule it names.
func TestProjectionOracleAgreesWithTheVectors(t *testing.T) {
	raw, err := os.ReadFile(siteHealthVectorPath)
	if err != nil {
		t.Fatalf("the gateway-api site-health vectors are required at %s: %v", siteHealthVectorPath, err)
	}
	var doc struct {
		Cases []struct {
			Name       string          `json:"name"`
			Valid      bool            `json:"valid"`
			Rule       string          `json:"rule"`
			Projection json.RawMessage `json:"projection"`
		} `json:"cases"`
	}
	if err := json.Unmarshal(raw, &doc); err != nil {
		t.Fatal(err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("no projection vectors")
	}
	for _, tc := range doc.Cases {
		err := validateProjection(decodeProjection(t, tc.Projection))
		refusal, _ := err.(*projectionRefusal)
		switch {
		case tc.Valid && err != nil:
			t.Errorf("%s: declared valid, refused: %v", tc.Name, err)
		case !tc.Valid && err == nil:
			t.Errorf("%s: declared invalid for %s, accepted", tc.Name, tc.Rule)
		case !tc.Valid && refusal.rule != tc.Rule:
			t.Errorf("%s: refused for %s, vector names %s", tc.Name, refusal.rule, tc.Rule)
		}
	}
}

// projectionAuthority answers each device from its own table and can hold a
// device's delivery open, so another device is observably "still delivering".
type projectionAuthority struct {
	mu      sync.Mutex
	answer  map[string][3]any // status, outcome, reason
	stalled map[string]chan struct{}
}

func (a *projectionAuthority) RoundTrip(req *http.Request) (*http.Response, error) {
	body, err := io.ReadAll(req.Body)
	if err != nil {
		return nil, err
	}
	var routing struct {
		DeviceID string `json:"device_id"`
	}
	_ = json.Unmarshal(body, &routing)
	a.mu.Lock()
	// A device's registrations are answered under "<device>:registration",
	// its other artifacts under the device alone.
	key := routing.DeviceID
	if req.Header.Get("X-Ori-Evidence-Artifact-Type") == "anchor_registration" {
		key += ":registration"
	}
	answer, ok := a.answer[key]
	stall := a.stalled[key]
	a.mu.Unlock()
	if stall != nil {
		select {
		case <-stall:
		case <-req.Context().Done():
			return nil, req.Context().Err()
		}
	}
	if !ok {
		answer = [3]any{200, "accepted", ""}
	}
	sum := sha256.Sum256(body)
	digest := "sha256:" + hex.EncodeToString(sum[:])
	out := map[string]any{"v": 1, "outcome": answer[1], "reason": answer[2], "retriable": false}
	if answer[0].(int) != http.StatusUnauthorized {
		out["artifact_digest"], out["authority_artifacts"] = digest, []any{}
	}
	raw, _ := json.Marshal(out)
	return &http.Response{
		StatusCode: answer[0].(int), Header: http.Header{"Content-Type": []string{"application/json"}},
		Body: io.NopCloser(strings.NewReader(string(raw))),
	}, nil
}

func checkpointFor(device string, n int) []byte {
	return []byte(fmt.Sprintf(`{"v":1,"device_id":%q,"high_water_seq":%d,"signature":"ed25519:opaque"}`, device, n))
}

// TestRealProjectionWithAStoreFault drives a real store failure: the hold
// write fails, so store_unavailable is active beside the held device. The
// projection is degraded by both, and blocked by the held device alone; once
// the store recovers the fault clears.
func TestRealProjectionWithAStoreFault(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "queue")
	faults := evidence.NewFaultRecorder()
	q, err := evidence.OpenDurableQueue(evidence.QueueOptions{
		Directory: dir, MaxItems: 100, MaxBytes: 1 << 26, Devices: []string{"dev-a", "dev-b"},
		Faults: faults, FaultSource: "outbound",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := q.Enqueue(evidence.ArtifactCheckpoint, checkpointFor("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o500); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if probe, err := os.CreateTemp(dir, "probe-"); err == nil {
		_ = probe.Close()
		t.Skip("directory permissions do not stop writes here")
	}
	authority := &projectionAuthority{answer: map[string][3]any{"dev-a": {409, "refused", "conflict"}}}
	channel, err := evidence.NewHTTPChannel(evidence.HTTPChannelOptions{
		Endpoint: "https://authority.invalid/v1/evidence/artifacts", ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := evidence.NewDeliveryWorker(q, channel, nil, evidence.DeliveryWorkerOptions{
		RetryInterval: time.Hour, Faults: faults, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- worker.Run(ctx) }()
	defer func() { cancel(); <-done }()
	deadline := time.Now().Add(2 * time.Second)
	for len(worker.Status().Faults) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	raw, _ := json.Marshal(evidenceDeliveryView(worker))
	if err := validateProjection(decodeProjection(t, raw)); err != nil {
		t.Fatalf("projection breaks the contract: %v\n%s", err, raw)
	}
	view := evidenceDeliveryView(worker)
	if len(view.Faults) != 1 || view.Faults[0] != "store_unavailable" || !view.Degraded || !view.Blocked {
		t.Fatalf("projection = %s", raw)
	}
	// The store recovers. A durable write leaves the fault raised; only the
	// store's next successful probe clears it (gateway-api/v1).
	_ = os.Chmod(dir, 0o700)
	if _, err := q.Enqueue(evidence.ArtifactCheckpoint, checkpointFor("dev-b", 1)); err != nil {
		t.Fatal(err)
	}
	if faults := worker.Status().Faults; len(faults) != 1 || faults[0] != "store_unavailable" {
		t.Fatalf("faults after a successful write = %v, want store_unavailable", faults)
	}
	if err := q.Probe(); err != nil {
		t.Fatal(err)
	}
	if faults := worker.Status().Faults; len(faults) != 0 {
		t.Fatalf("faults after a successful probe = %v", faults)
	}
}

// TestRealProjectionsSatisfyTheContract drives the real courier into each
// state the contract describes and validates the projection it reports.
func TestRealProjectionsSatisfyTheContract(t *testing.T) {
	for _, tc := range []struct {
		name        string
		answer      map[string][3]any
		stall       []string
		queued      map[string]int
		wantDevices int
		wantBlocked bool
		settle      func(evidence.DeliveryStatus) bool
		// wantStates is each listed entry's state, keyed "<device>/<lane>".
		wantStates map[string]string
	}{
		{"clean", nil, nil, map[string]int{}, 0, false, func(s evidence.DeliveryStatus) bool { return true }, nil},
		{"one held beside one still delivering",
			map[string][3]any{"dev-a": {409, "refused", "conflict"}}, []string{"dev-b"},
			map[string]int{"dev-a": 3, "dev-b": 2}, 1, false,
			func(s evidence.DeliveryStatus) bool { return len(s.Devices) == 1 },
			map[string]string{"dev-a/evidence": "held"}},
		{"every device with pending held",
			map[string][3]any{"dev-a": {422, "refused", "bad_authenticator"}, "dev-b": {409, "refused", "conflict"}}, nil,
			map[string]int{"dev-a": 2, "dev-b": 1}, 2, true,
			func(s evidence.DeliveryStatus) bool { return len(s.Devices) == 2 }, nil},
		{"waiting for a handoff and backing off",
			map[string][3]any{"dev-a": {422, "refused", "unknown_key"}, "dev-b": {503, "pending", "unavailable"}}, nil,
			map[string]int{"dev-a": 1, "dev-b": 3}, 2, false,
			func(s evidence.DeliveryStatus) bool { return len(s.Devices) == 2 }, nil},
		{"an unrecognised reason held",
			map[string][3]any{"dev-a": {400, "refused", "authority.internal"}}, nil,
			map[string]int{"dev-a": 1}, 1, true,
			func(s evidence.DeliveryStatus) bool { return len(s.Devices) == 1 }, nil},
		{"a registration backing off beside the same device's evidence still delivering",
			map[string][3]any{"dev-a:registration": {409, "refused", "pending_registration_conflict"}}, []string{"dev-a"},
			map[string]int{"dev-a": 2, "dev-a:registration": 1}, 1, false,
			func(s evidence.DeliveryStatus) bool { return len(s.Devices) == 1 },
			map[string]string{"dev-a/registration": "backing_off"}},
		// A terminal registration refusal is archived: never in devices, never
		// in pending, never blocking, always degrading.
		{"an archived registration beside the same device's evidence still delivering",
			map[string][3]any{"dev-a:registration": {422, "refused", "commissioning_digest_mismatch"}}, []string{"dev-a"},
			map[string]int{"dev-a": 2, "dev-a:registration": 1}, 0, false,
			func(s evidence.DeliveryStatus) bool { return len(s.Archived) == 1 },
			map[string]string{}},
		{"an archived registration beside a held evidence lane covering everything is blocked",
			map[string][3]any{"dev-a:registration": {422, "refused", "commissioning_digest_mismatch"}, "dev-a": {409, "refused", "conflict"}}, nil,
			map[string]int{"dev-a": 2, "dev-a:registration": 1}, 1, true,
			func(s evidence.DeliveryStatus) bool { return len(s.Archived) == 1 && s.Blocked },
			map[string]string{"dev-a/evidence": "held"}},
		{"an archived registration beside the same device's evidence backing off",
			map[string][3]any{"dev-a:registration": {422, "refused", "commissioning_digest_mismatch"}, "dev-a": {503, "pending", "unavailable"}}, nil,
			map[string]int{"dev-a": 2, "dev-a:registration": 1}, 1, false,
			func(s evidence.DeliveryStatus) bool { return len(s.Archived) == 1 && len(s.Devices) == 1 },
			map[string]string{"dev-a/evidence": "backing_off"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			authority := &projectionAuthority{answer: tc.answer, stalled: map[string]chan struct{}{}}
			release := make(chan struct{})
			for _, device := range tc.stall {
				authority.stalled[device] = release
			}
			defer close(release)
			q, err := evidence.OpenDurableQueue(evidence.QueueOptions{
				Directory: filepath.Join(t.TempDir(), "queue"), MaxItems: 100, MaxBytes: 1 << 26,
				Devices: []string{"dev-a", "dev-b"},
			})
			if err != nil {
				t.Fatal(err)
			}
			for key, n := range tc.queued {
				device, registration := strings.CutSuffix(key, ":registration")
				for i := 1; i <= n; i++ {
					kind, payload := evidence.ArtifactCheckpoint, checkpointFor(device, i)
					if registration {
						kind = evidence.ArtifactAnchorRegistration
						payload = []byte(fmt.Sprintf(`{"v":1,"device_id":%q,"anchor_epoch_id":"epoch-%d"}`, device, i))
					}
					if _, err := q.Enqueue(kind, payload); err != nil {
						t.Fatal(err)
					}
				}
			}
			channel, err := evidence.NewHTTPChannel(evidence.HTTPChannelOptions{
				Endpoint: "https://authority.invalid/v1/evidence/artifacts", ClientID: "site-a-gateway",
				Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
			})
			if err != nil {
				t.Fatal(err)
			}
			worker, err := evidence.NewDeliveryWorker(q, channel, nil, evidence.DeliveryWorkerOptions{
				RetryInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
			})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- worker.Run(ctx) }()
			defer func() { cancel(); <-done }()
			deadline := time.Now().Add(2 * time.Second)
			for !tc.settle(worker.Status()) && time.Now().Before(deadline) {
				time.Sleep(time.Millisecond)
			}
			view := evidenceDeliveryView(worker)
			raw, err := json.Marshal(view)
			if err != nil {
				t.Fatal(err)
			}
			projection := decodeProjection(t, raw)
			if err := validateProjection(projection); err != nil {
				t.Fatalf("real projection breaks the contract: %v\n%s", err, raw)
			}
			// A producer implementing this revision always carries faults and,
			// as this one does, devices.
			for _, key := range []string{"faults", "devices"} {
				if _, ok := projection[key].([]any); !ok {
					t.Fatalf("projection omits %s: %s", key, raw)
				}
			}
			if len(view.Devices) != tc.wantDevices || view.Blocked != tc.wantBlocked {
				t.Fatalf("projection = %s", raw)
			}
			if tc.wantStates != nil {
				got := map[string]string{}
				for _, entry := range view.Devices {
					got[entry.DeviceID+"/"+entry.Lane] = entry.State
				}
				if fmt.Sprint(got) != fmt.Sprint(tc.wantStates) {
					t.Fatalf("lane states = %v, want %v: %s", got, tc.wantStates, raw)
				}
			}
			// Every registration answered with a hold is archived, and only those.
			wantArchived := 0
			for key, answer := range tc.answer {
				status, reason := answer[0].(int), answer[2].(string)
				holds := status == 400 || (status == 409 && reason != "pending_registration_conflict") ||
					(status == 422 && reason != "unknown_key" && reason != "unrecognised_version")
				if strings.HasSuffix(key, ":registration") && holds {
					wantArchived += tc.queued[key]
				}
			}
			if len(view.ArchivedRegistrations) != wantArchived {
				t.Fatalf("archived_registrations = %d, want %d: %s", len(view.ArchivedRegistrations), wantArchived, raw)
			}
			if wantArchived > 0 && !view.Degraded {
				t.Fatalf("an archived refusal did not degrade delivery: %s", raw)
			}
			if _, ok := projection["archived_registrations"].([]any); !ok {
				t.Fatalf("projection omits archived_registrations: %s", raw)
			}
			for _, forbidden := range []string{"authority.invalid", "authority.internal", "https"} {
				if strings.Contains(string(raw), forbidden) {
					t.Fatalf("projection carries %q: %s", forbidden, raw)
				}
			}
		})
	}
}
