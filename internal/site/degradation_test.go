// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

const testTopic = "ori/dev-01/runtime/heartbeat"

// heartbeatMap builds a payload as a map so tests can carry malformed
// degradation_reasons values the typed contract model cannot express.
func heartbeatMap(status string, nowMS int64) map[string]any {
	return map[string]any{
		"device_id":       "dev-01",
		"status":          status,
		"last_seen_ms":    nowMS,
		"gateway_seen_ms": 0,
		"active_triggers": []string{},
	}
}

func mustJSON(t *testing.T, v any) []byte {
	t.Helper()
	b, err := json.Marshal(v)
	if err != nil {
		t.Fatal(err)
	}
	return b
}

func newHandler(t *testing.T, now time.Time) (*RuntimeHeartbeatHandler, *Registry) {
	t.Helper()
	reg := NewRegistry()
	h, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	return h, reg
}

// Every closed verdict is reachable, and each is produced by the check it
// names. Asserting on the verdict rather than the error string is deliberate:
// a message-substring assertion silently starts passing when wording changes.
func TestDegradationReasonsVerdicts(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	cases := []struct {
		name    string
		status  string
		reasons any
		verdict string
	}{
		{"not an array (object)", NodeStatusDegraded, map[string]any{"a": 1}, VerdictDegradationReasonsNotArray},
		{"not an array (string)", NodeStatusDegraded, "firmware_liveness_degraded", VerdictDegradationReasonsNotArray},
		{"explicit null is not absence", NodeStatusDegraded, nil, VerdictDegradationReasonsNotArray},
		{"present empty", NodeStatusDegraded, []any{}, VerdictDegradationReasonsLengthInvalid},
		{"element not a string (number)", NodeStatusDegraded, []any{42}, VerdictDegradationReasonNotString},
		// null into a Go string is a successful no-op, so an element-wise
		// string unmarshal turns [null] into "" and reports it as an unknown
		// token. The type must be asserted, not inferred from decode success.
		{"element not a string (null)", NodeStatusDegraded, []any{nil}, VerdictDegradationReasonNotString},
		{"element not a string (bool)", NodeStatusDegraded, []any{true}, VerdictDegradationReasonNotString},
		{"element not a string (object)", NodeStatusDegraded, []any{map[string]any{}}, VerdictDegradationReasonNotString},
		{"element not a string (nested array)", NodeStatusDegraded, []any{[]any{}}, VerdictDegradationReasonNotString},
		{"unknown token", NodeStatusDegraded, []any{"not_a_real_reason"}, VerdictDegradationReasonUnknown},
		{"reasons with healthy status", NodeStatusHealthy, []any{"firmware_liveness_degraded"}, VerdictDegradationReasonsStatusMismatch},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newHandler(t, now)
			payload := heartbeatMap(tc.status, now.UnixMilli())
			payload["degradation_reasons"] = tc.reasons
			err := h.Handle(testTopic, mustJSON(t, payload))
			if got := DegradationVerdict(err); got != tc.verdict {
				t.Fatalf("verdict = %q, want %q (err=%v)", got, tc.verdict, err)
			}
		})
	}
}

// Uniqueness and ordering have distinct verdicts. Sharing one would leave a
// conformance failure ambiguous about which rule is missing.
func TestDegradationReasonsUniquenessAndOrderingAreDistinct(t *testing.T) {
	now := time.UnixMilli(1234567890000)

	h, _ := newHandler(t, now)
	dup := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	dup["degradation_reasons"] = []any{"firmware_liveness_degraded", "firmware_liveness_degraded"}
	if got := DegradationVerdict(h.Handle(testTopic, mustJSON(t, dup))); got != VerdictDegradationReasonsNotUnique {
		t.Fatalf("duplicate verdict = %q", got)
	}

	h2, _ := newHandler(t, now)
	unordered := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	// Unique, both unknown, but out of order: ordering precedes vocabulary,
	// so this must be refused as not_ordered rather than unknown.
	unordered["degradation_reasons"] = []any{"z_token", "a_token"}
	if got := DegradationVerdict(h2.Handle(testTopic, mustJSON(t, unordered))); got != VerdictDegradationReasonsNotOrdered {
		t.Fatalf("ordering verdict = %q", got)
	}
}

// Precedence, proven with inputs that violate several rules at once. Without a
// fixed order these would be refused by whichever check happened to run first,
// and a test asserting only "refused" would pass against an implementation
// missing the earlier guard entirely.
func TestDegradationReasonsPrecedence(t *testing.T) {
	now := time.UnixMilli(1234567890000)

	// 17 duplicated unknown tokens: violates length, uniqueness and
	// vocabulary. Length is checked first, so length must win.
	overlong := make([]any, 17)
	for i := range overlong {
		overlong[i] = "not_a_real_reason"
	}
	// Also violates status implication, on a healthy heartbeat.
	cases := []struct {
		name    string
		status  string
		reasons any
		verdict string
	}{
		{"over-length beats uniqueness and vocabulary", NodeStatusDegraded, overlong, VerdictDegradationReasonsLengthInvalid},
		{"length beats element type", NodeStatusDegraded, []any{}, VerdictDegradationReasonsLengthInvalid},
		{"element type beats uniqueness", NodeStatusDegraded, []any{1, 1}, VerdictDegradationReasonNotString},
		{"uniqueness beats vocabulary", NodeStatusDegraded, []any{"zz_unknown", "zz_unknown"}, VerdictDegradationReasonsNotUnique},
		{"ordering beats vocabulary", NodeStatusDegraded, []any{"zz_unknown", "aa_unknown"}, VerdictDegradationReasonsNotOrdered},
		{"vocabulary beats status mismatch", NodeStatusHealthy, []any{"zz_unknown"}, VerdictDegradationReasonUnknown},
		{"array type beats everything", NodeStatusHealthy, "nope", VerdictDegradationReasonsNotArray},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newHandler(t, now)
			payload := heartbeatMap(tc.status, now.UnixMilli())
			payload["degradation_reasons"] = tc.reasons
			err := h.Handle(testTopic, mustJSON(t, payload))
			if got := DegradationVerdict(err); got != tc.verdict {
				t.Fatalf("verdict = %q, want %q (err=%v)", got, tc.verdict, err)
			}
		})
	}
}

// Raw classification runs before the typed decode, so an unrelated typed
// failure cannot pre-empt a degradation defect and deny it its verdict.
func TestDegradationClassificationPrecedesTypedDecode(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	cases := []struct {
		name    string
		mutate  func(map[string]any)
		verdict string
	}{
		{"invalid status does not pre-empt", func(m map[string]any) { m["status"] = "not_a_status" }, VerdictDegradationReasonUnknown},
		{"status of wrong type does not pre-empt", func(m map[string]any) { m["status"] = 42 }, VerdictDegradationReasonUnknown},
		{"invalid last_seen_ms does not pre-empt", func(m map[string]any) { m["last_seen_ms"] = 0 }, VerdictDegradationReasonUnknown},
		{"device mismatch does not pre-empt", func(m map[string]any) { m["device_id"] = "someone-else" }, VerdictDegradationReasonUnknown},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			h, _ := newHandler(t, now)
			payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
			payload["degradation_reasons"] = []any{"zz_unknown_token"}
			tc.mutate(payload)
			err := h.Handle(testTopic, mustJSON(t, payload))
			if got := DegradationVerdict(err); got != tc.verdict {
				t.Fatalf("verdict = %q, want %q (err=%v)", got, tc.verdict, err)
			}
		})
	}
}

// A malformed status is not "degraded", so reasons carried alongside one are
// refused by the status-implication check rather than silently accepted.
func TestDegradationReasonsWithUnusableStatus(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	for _, status := range []any{42, nil, map[string]any{}} {
		h, _ := newHandler(t, now)
		payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
		payload["status"] = status
		payload["degradation_reasons"] = []any{"firmware_liveness_degraded"}
		err := h.Handle(testTopic, mustJSON(t, payload))
		if got := DegradationVerdict(err); got != VerdictDegradationReasonsStatusMismatch {
			t.Fatalf("status %#v: verdict = %q, want %q", status, got, VerdictDegradationReasonsStatusMismatch)
		}
	}
}

// Absent and present-empty are different states, through the real handler.
func TestDegradationReasonsAbsentVersusPresentEmpty(t *testing.T) {
	now := time.UnixMilli(1234567890000)

	h, reg := newHandler(t, now)
	absent := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	if err := h.Handle(testTopic, mustJSON(t, absent)); err != nil {
		t.Fatalf("absent field must be accepted: %v", err)
	}
	if got := reg.Snapshot()[0].DegradationReasons; got != nil {
		t.Fatalf("absent must stay nil, got %#v", got)
	}

	h2, _ := newHandler(t, now)
	empty := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	empty["degradation_reasons"] = []any{}
	if got := DegradationVerdict(h2.Handle(testTopic, mustJSON(t, empty))); got != VerdictDegradationReasonsLengthInvalid {
		t.Fatalf("present-empty verdict = %q", got)
	}
}

// A conforming heartbeat reaches the registry and the projection intact.
func TestDegradationReasonsAcceptedAndProjected(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	h, reg := newHandler(t, now)
	payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	payload["degradation_reasons"] = []any{"firmware_liveness_degraded"}
	if err := h.Handle(testTopic, mustJSON(t, payload)); err != nil {
		t.Fatal(err)
	}

	got := reg.Snapshot()[0].DegradationReasons
	if len(got) != 1 || got[0] != "firmware_liveness_degraded" {
		t.Fatalf("registry reasons = %#v", got)
	}

	view := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"dev-01"}}).Project(now, GatewayView{})
	if len(view.Nodes) != 1 {
		t.Fatalf("nodes = %d", len(view.Nodes))
	}
	if len(view.Nodes[0].DegradationReasons) != 1 ||
		view.Nodes[0].DegradationReasons[0] != "firmware_liveness_degraded" {
		t.Fatalf("projected reasons = %#v", view.Nodes[0].DegradationReasons)
	}
}

// Registry snapshots must not alias registry state: a caller mutating what it
// received must not reach back into the registry.
func TestDegradationReasonsSnapshotDoesNotAlias(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	h, reg := newHandler(t, now)
	payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	payload["degradation_reasons"] = []any{"firmware_liveness_degraded"}
	if err := h.Handle(testTopic, mustJSON(t, payload)); err != nil {
		t.Fatal(err)
	}

	first := reg.Snapshot()[0]
	first.DegradationReasons[0] = "mutated_by_caller"

	if got := reg.Snapshot()[0].DegradationReasons[0]; got != "firmware_liveness_degraded" {
		t.Fatalf("registry state was aliased and mutated: %q", got)
	}
}

// The projected output carries no subordinate firmware-device identity, broker
// URL, filesystem path or exception text. The runtime node's own device_id is
// expected and unchanged.
func TestDegradationReasonsProjectionCarriesNoSensitiveData(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	h, reg := newHandler(t, now)
	payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	payload["degradation_reasons"] = []any{"firmware_liveness_degraded"}
	if err := h.Handle(testTopic, mustJSON(t, payload)); err != nil {
		t.Fatal(err)
	}

	encoded := string(mustJSON(t, NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"dev-01"}}).Project(now, GatewayView{})))
	for _, forbidden := range []string{
		"ori-fw-", "mqtt://", "mqtts://", "/run/", "/etc/", "Traceback", "exception",
		"supervised_device_count", "expiring_device_count", "worst_case_device_latency_s",
	} {
		if strings.Contains(encoded, forbidden) {
			t.Fatalf("projection leaked %q: %s", forbidden, encoded)
		}
	}
	if !strings.Contains(encoded, `"device_id":"dev-01"`) {
		t.Fatalf("runtime node device_id must remain projected: %s", encoded)
	}
}

// Ordering failures surface at two different layers. An implementation testing
// only the first leaves semantic ordering unenforced for any publisher that
// correctly signs a malformed list.
func TestDegradationReasonsOrderingFailsAtTwoLayers(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	const secret = "site-local-secret"

	signedHandler := func(t *testing.T) (*RuntimeHeartbeatHandler, *Registry) {
		t.Helper()
		verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: secret, Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		reg := NewRegistry()
		h, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{
			AuthVerifier: verifier,
			Now:          func() time.Time { return now },
		})
		if err != nil {
			t.Fatal(err)
		}
		return h, reg
	}

	sign := func(t *testing.T, payload map[string]any) []byte {
		t.Helper()
		auth, err := mqttauth.Sign(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), secret)
		if err != nil {
			t.Fatal(err)
		}
		payload["auth"] = auth
		return mustJSON(t, payload)
	}

	t.Run("already-signed list reordered in transit fails authentication", func(t *testing.T) {
		h, _ := signedHandler(t)
		payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
		payload["degradation_reasons"] = []any{"aa_token", "zz_token"}
		body := sign(t, payload)

		// Swap the two tokens in the signed bytes. Authentication must fail on
		// changed canonical bytes, before any semantic check runs.
		tampered := strings.Replace(string(body),
			`["aa_token","zz_token"]`, `["zz_token","aa_token"]`, 1)
		if tampered == string(body) {
			t.Fatal("tamper did not apply; test would prove nothing")
		}

		err := h.Handle(testTopic, []byte(tampered))
		if err == nil {
			t.Fatal("reordered signed payload must be refused")
		}
		if got := DegradationVerdict(err); got != "" {
			t.Fatalf("must fail authentication, not semantic validation; got verdict %q", got)
		}
	})

	t.Run("freshly signed non-lexicographic list passes auth then fails ordering", func(t *testing.T) {
		h, _ := signedHandler(t)
		payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
		payload["degradation_reasons"] = []any{"zz_token", "aa_token"}
		body := sign(t, payload)

		err := h.Handle(testTopic, body)
		if got := DegradationVerdict(err); got != VerdictDegradationReasonsNotOrdered {
			t.Fatalf("verdict = %q, want %q (err=%v)", got, VerdictDegradationReasonsNotOrdered, err)
		}
	})

	t.Run("conforming signed heartbeat is accepted", func(t *testing.T) {
		h, reg := signedHandler(t)
		payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
		payload["degradation_reasons"] = []any{"firmware_liveness_degraded"}
		if err := h.Handle(testTopic, sign(t, payload)); err != nil {
			t.Fatal(err)
		}
		if got := reg.Snapshot()[0].DegradationReasons; len(got) != 1 {
			t.Fatalf("reasons = %#v", got)
		}
	})
}

// Rolling upgrade: an older runtime reports degraded without naming a reason.
// Refusing it would turn an upgrade window into an outage.
func TestDegradedWithFieldAbsentRemainsAccepted(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	for _, status := range []string{NodeStatusHealthy, NodeStatusDegraded} {
		t.Run(status, func(t *testing.T) {
			h, reg := newHandler(t, now)
			if err := h.Handle(testTopic, mustJSON(t, heartbeatMap(status, now.UnixMilli()))); err != nil {
				t.Fatalf("field absent must be accepted: %v", err)
			}
			if got := reg.Snapshot()[0].Status; got != status {
				t.Fatalf("status = %q", got)
			}
		})
	}
}

// Additive fields from a newer runtime must not be refused; the gateway
// deliberately does not use DisallowUnknownFields.
func TestUnknownAdditiveFieldIsIgnored(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	h, _ := newHandler(t, now)
	payload := heartbeatMap(NodeStatusDegraded, now.UnixMilli())
	payload["some_future_field"] = map[string]any{"x": 1}
	if err := h.Handle(testTopic, mustJSON(t, payload)); err != nil {
		t.Fatalf("additive field must be tolerated: %v", err)
	}
}
