// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sort"
	"testing"
	"time"
)

// contractTokenSeverity is gateway-api/v2's degradation_reasons table, written
// out from the contract rather than derived from the implementation's map.
var contractTokenSeverity = map[string]string{
	"action_log_records_lost":              NodeStatusDegraded,
	"alert_delivery_overdue":               NodeStatusDegraded,
	"action_log_records_stalled":           NodeStatusDegraded,
	"commissioning_problem":                NodeStatusDegraded,
	"community_skills_refused":             NodeStatusDegraded,
	"config_unsigned_value_source":         NodeStatusDegraded,
	"decision_record_lost":                 NodeStatusCritical,
	"evidence_contributor_link_unresolved": NodeStatusDegraded,
	"evidence_courier_answer_unrecognised": NodeStatusDegraded,
	"evidence_posture_problem":             NodeStatusDegraded,
	"firmware_liveness_degraded":           "",
	"firmware_liveness_device_expired":     NodeStatusCritical,
	"firmware_liveness_over_capacity":      NodeStatusDegraded,
	"firmware_liveness_stopped":            NodeStatusCritical,
	"firmware_liveness_tick_failures":      NodeStatusDegraded,
	"firmware_liveness_tick_stale":         NodeStatusCritical,
	"safety_zone_activation_refused":       NodeStatusDegraded,
	"safety_zone_active_unprotected":       NodeStatusDegraded,
	"safety_zone_trip_unresolved":          NodeStatusCritical,
	"sensor_stale":                         NodeStatusDegraded,
	"sensor_measurement_degraded":          NodeStatusDegraded,
	"sensor_unconnected":                   NodeStatusDegraded,
	"health_snapshot_invalid":              NodeStatusCritical,
	"subsystem_starting":                   NodeStatusDegraded,
	"subsystem_state_unknown":              NodeStatusDegraded,
	"subsystem_state_unknown_critical":     NodeStatusCritical,
	"tier_c_records_degraded":              NodeStatusDegraded,
	"skill_optional_hooks_unavailable":     NodeStatusDegraded,
	"skill_required_hooks_unavailable":     NodeStatusCritical,
	"tier_c_recovery_failed":               NodeStatusDegraded,
	"tier_d_alert_delivery_unconfirmed":    NodeStatusCritical,
}

func handleReasons(t *testing.T, status string, reasons any) (*Registry, error) {
	t.Helper()
	now := time.UnixMilli(1234567890000)
	h, reg := newHandler(t, now)
	payload := heartbeatMap(status, now.UnixMilli())
	if reasons != nil {
		payload["degradation_reasons"] = reasons
	}
	return reg, h.Handle(testTopic, mustJSON(t, payload))
}

// Every token is accepted at its own severity and above, and refused below it
// by step 11 — health_snapshot_invalid by step 9, which precedes it.
func TestEveryTokenAcceptedAtItsSeverityAndRefusedBelow(t *testing.T) {
	if len(contractTokenSeverity) != 31 {
		t.Fatalf("contract table holds 31 tokens, test table has %d", len(contractTokenSeverity))
	}
	for token, severity := range contractTokenSeverity {
		for _, status := range []string{NodeStatusHealthy, NodeStatusDegraded, NodeStatusCritical} {
			t.Run(token+"/"+status, func(t *testing.T) {
				reg, err := handleReasons(t, status, []any{token})
				var want string
				switch {
				case token == healthSnapshotInvalid && status != NodeStatusCritical:
					want = VerdictDegradationReasonNotAlone
				case status == NodeStatusHealthy:
					want = VerdictDegradationReasonsStatusMismatch
				case severity == NodeStatusCritical && status == NodeStatusDegraded:
					want = VerdictDegradationSeverityUnderstated
				}
				if got := DegradationVerdict(err); got != want {
					t.Fatalf("verdict = %q, want %q (err=%v)", got, want, err)
				}
				if want == "" {
					if err != nil {
						t.Fatal(err)
					}
					if got := reg.Snapshot()[0].Status; got != status {
						t.Fatalf("registered status = %q, want %q", got, status)
					}
				}
			})
		}
	}
	// The implementation holds no token the contract does not.
	for token := range degradationReasonSeverity {
		if _, ok := contractTokenSeverity[token]; !ok {
			t.Errorf("implementation vocabulary holds %q, which the contract does not define", token)
		}
	}
}

// The fallback heartbeat of runtime-health/v4: critical, health_snapshot_invalid
// alone, no active triggers, no evidence block.
func TestFallbackHeartbeatAcceptedAndProjectedCritical(t *testing.T) {
	reg, err := handleReasons(t, NodeStatusCritical, []any{healthSnapshotInvalid})
	if err != nil {
		t.Fatalf("fallback heartbeat refused: %v", err)
	}
	got := reg.Snapshot()[0]
	if got.Status != NodeStatusCritical || got.Evidence != nil ||
		len(got.DegradationReasons) != 1 || got.DegradationReasons[0] != healthSnapshotInvalid {
		t.Fatalf("registered = %#v", got)
	}
	out := getHealth(t, reg, []string{"dev-01"}, time.UnixMilli(1234567890000))
	if out.Status != SiteStatusCritical || out.Nodes[0].Status != SiteStatusCritical {
		t.Fatalf("site %q node %q, want critical", out.Status, out.Nodes[0].Status)
	}
}

// Step 9 refuses health_snapshot_invalid beside any other token, in either
// order position, and on any non-critical status.
func TestHealthSnapshotInvalidNotAlone(t *testing.T) {
	cases := []struct {
		name    string
		status  string
		reasons []any
	}{
		{"before another token", NodeStatusCritical, []any{"decision_record_lost", healthSnapshotInvalid}},
		{"after another token", NodeStatusCritical, []any{healthSnapshotInvalid, "sensor_stale"}},
		{"beside the umbrella", NodeStatusCritical, []any{"firmware_liveness_degraded", healthSnapshotInvalid}},
		{"on degraded", NodeStatusDegraded, []any{healthSnapshotInvalid}},
		{"on healthy, before the status implication", NodeStatusHealthy, []any{healthSnapshotInvalid}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg, err := handleReasons(t, tc.status, tc.reasons)
			if got := DegradationVerdict(err); got != VerdictDegradationReasonNotAlone {
				t.Fatalf("verdict = %q, want %q (err=%v)", got, VerdictDegradationReasonNotAlone, err)
			}
			if len(reg.Snapshot()) != 0 {
				t.Fatal("refused heartbeat reached the registry")
			}
		})
	}
}

// Step 11 refuses a critical condition under degraded; a status above its
// tokens, and an older runtime's degraded umbrella alone, are accepted.
func TestSeverityUnderstated(t *testing.T) {
	refused := [][]any{
		{"decision_record_lost"},
		{"decision_record_lost", "sensor_unconnected"},
		{"firmware_liveness_degraded", "firmware_liveness_stopped"},
		{"sensor_stale", "tier_d_alert_delivery_unconfirmed"},
	}
	for _, reasons := range refused {
		reg, err := handleReasons(t, NodeStatusDegraded, reasons)
		if got := DegradationVerdict(err); got != VerdictDegradationSeverityUnderstated {
			t.Fatalf("%v: verdict = %q, want %q (err=%v)", reasons, got, VerdictDegradationSeverityUnderstated, err)
		}
		if len(reg.Snapshot()) != 0 {
			t.Fatalf("%v: refused heartbeat reached the registry", reasons)
		}
	}
	accepted := []struct {
		status  string
		reasons []any
	}{
		{NodeStatusCritical, []any{"decision_record_lost", "sensor_unconnected"}},
		{NodeStatusCritical, []any{"sensor_unconnected"}},
		{NodeStatusCritical, []any{"firmware_liveness_degraded"}},
		{NodeStatusDegraded, []any{"firmware_liveness_degraded"}},
		{NodeStatusDegraded, []any{"firmware_liveness_degraded", "firmware_liveness_tick_failures"}},
	}
	for _, tc := range accepted {
		if _, err := handleReasons(t, tc.status, tc.reasons); err != nil {
			t.Fatalf("%s %v refused: %v", tc.status, tc.reasons, err)
		}
	}
}

// The bound is exactly 32: 32 elements pass the length step and 33 do not.
func TestDegradationReasonsLengthBoundIsExact(t *testing.T) {
	tokens := func(n int) []any {
		names := make([]string, n)
		for i := range names {
			names[i] = fmt.Sprintf("zz_unknown_%02d", i)
		}
		sort.Strings(names)
		out := make([]any, n)
		for i, name := range names {
			out[i] = name
		}
		return out
	}
	if _, err := handleReasons(t, NodeStatusCritical, tokens(32)); DegradationVerdict(err) != VerdictDegradationReasonUnknown {
		t.Fatalf("32 unique ordered unknown tokens: verdict = %q, want unknown", DegradationVerdict(err))
	}
	if _, err := handleReasons(t, NodeStatusCritical, tokens(33)); DegradationVerdict(err) != VerdictDegradationReasonsLengthInvalid {
		t.Fatalf("33 tokens: verdict = %q, want length_invalid", DegradationVerdict(err))
	}
}

// A critical heartbeat without reasons is accepted: absence terminates
// validation.
func TestCriticalWithoutReasonsAccepted(t *testing.T) {
	reg, err := handleReasons(t, NodeStatusCritical, nil)
	if err != nil {
		t.Fatal(err)
	}
	if got := reg.Snapshot()[0]; got.Status != NodeStatusCritical || got.DegradationReasons != nil {
		t.Fatalf("registered = %#v", got)
	}
}

func getHealth(t *testing.T, reg *Registry, devices []string, now time.Time) SiteHealth {
	t.Helper()
	return getHealthWithGateway(t, reg, devices, now, healthyGateway())
}

func getHealthWithGateway(t *testing.T, reg *Registry, devices []string, now time.Time, gw GatewayView) SiteHealth {
	t.Helper()
	viewer := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: devices, NodeTTLMS: 5000})
	h := NewHealthHandler(viewer, func() GatewayView { return gw }, "127.0.0.1:0", func() time.Time { return now })
	rec := httptest.NewRecorder()
	h.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/health", nil))
	if rec.Code != http.StatusOK {
		t.Fatalf("GET /health = %d", rec.Code)
	}
	var out SiteHealth
	if err := json.Unmarshal(rec.Body.Bytes(), &out); err != nil {
		t.Fatal(err)
	}
	return out
}

// A critical heartbeat accepted through the handler is critical in GET /health,
// and a site with one critical and one degraded node reports critical.
func TestCriticalHeartbeatKeptThroughGetHealth(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	reg := NewRegistry()
	h, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	for _, beat := range []struct {
		device, status string
		reasons        []any
	}{
		{"node-a", NodeStatusCritical, []any{"decision_record_lost", "sensor_unconnected"}},
		{"node-b", NodeStatusDegraded, []any{"sensor_stale"}},
		{"node-c", NodeStatusHealthy, nil},
	} {
		payload := heartbeatMap(beat.status, now.UnixMilli())
		payload["device_id"] = beat.device
		if beat.reasons != nil {
			payload["degradation_reasons"] = beat.reasons
		}
		if err := h.Handle("ori/"+beat.device+"/runtime/heartbeat", mustJSON(t, payload)); err != nil {
			t.Fatalf("%s: %v", beat.device, err)
		}
	}
	out := getHealth(t, reg, []string{"node-a", "node-b", "node-c"}, now)
	if out.Status != SiteStatusCritical {
		t.Fatalf("site status = %q, want critical", out.Status)
	}
	want := map[string]string{"node-a": SiteStatusCritical, "node-b": SiteStatusDegraded, "node-c": SiteStatusHealthy}
	for _, n := range out.Nodes {
		if n.Status != want[n.DeviceID] {
			t.Errorf("%s: status %q, want %q", n.DeviceID, n.Status, want[n.DeviceID])
		}
	}
	if r := out.Nodes[0].DegradationReasons; len(r) != 2 || r[0] != "decision_record_lost" {
		t.Errorf("node-a reasons = %#v", r)
	}
}

// Nothing the projection adds lowers a critical node: not staleness, not an
// evidence-integrity problem, not a degraded gateway, not a missing node.
func TestCriticalNodeNeverFoldedBelowCritical(t *testing.T) {
	nowMS := int64(1234567890000)
	now := time.UnixMilli(nowMS)
	degradedDelivery := &GatewayEvidenceDeliveryView{Degraded: true, Blocked: true}
	cases := []struct {
		name string
		beat NodeHeartbeat
		gw   GatewayView
	}{
		{"fresh", NodeHeartbeat{LastSeenMS: nowMS}, healthyGateway()},
		{"stale", NodeHeartbeat{LastSeenMS: nowMS - 60000}, healthyGateway()},
		{"future-dated", NodeHeartbeat{LastSeenMS: nowMS + 60000}, healthyGateway()},
		{"evidence unavailable", NodeHeartbeat{LastSeenMS: nowMS, Evidence: &NodeEvidence{Available: false}}, healthyGateway()},
		{"evidence regressed", NodeHeartbeat{LastSeenMS: nowMS, Evidence: &NodeEvidence{Available: true, HeadRegressed: true, TruncationSuspected: true}}, healthyGateway()},
		{"gateway degraded", NodeHeartbeat{LastSeenMS: nowMS}, GatewayView{Status: SiteStatusDegraded, EvidenceDelivery: degradedDelivery}},
		{"gateway starting", NodeHeartbeat{LastSeenMS: nowMS}, GatewayView{Status: "starting"}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			reg := NewRegistry()
			beat := tc.beat
			beat.DeviceID = "node-a"
			beat.Status = NodeStatusCritical
			reg.Upsert(beat)
			out := getHealthWithGateway(t, reg, []string{"node-a", "node-missing"}, now, tc.gw)
			if out.Status != SiteStatusCritical {
				t.Errorf("site status = %q, want critical", out.Status)
			}
			if out.Nodes[0].Status != SiteStatusCritical {
				t.Errorf("node status = %q, want critical", out.Nodes[0].Status)
			}
			if out.Nodes[1].Status != SiteStatusDegraded {
				t.Errorf("missing node status = %q, want degraded", out.Nodes[1].Status)
			}
		})
	}
}

// A critical node whose next heartbeat omits the evidence block, as the
// fallback heartbeat does after evidence history, stays critical.
func TestFallbackAfterEvidenceHistoryStaysCritical(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	h, reg := newHandler(t, now)
	withEvidence := heartbeatMap(NodeStatusHealthy, now.UnixMilli())
	withEvidence["evidence"] = map[string]any{
		"chain_head_hash": "", "attestation_gap_count": 0, "available": true, "action_event_type": "",
	}
	if err := h.Handle(testTopic, mustJSON(t, withEvidence)); err != nil {
		t.Fatal(err)
	}
	fallback := heartbeatMap(NodeStatusCritical, now.UnixMilli())
	fallback["degradation_reasons"] = []any{healthSnapshotInvalid}
	if err := h.Handle(testTopic, mustJSON(t, fallback)); err != nil {
		t.Fatal(err)
	}
	out := getHealth(t, reg, []string{"dev-01"}, now)
	n := out.Nodes[0]
	if n.Status != SiteStatusCritical || out.Status != SiteStatusCritical {
		t.Fatalf("site %q node %q, want critical", out.Status, n.Status)
	}
	if n.Evidence == nil || n.Evidence.Available {
		t.Fatalf("missing evidence block must read as unavailable: %#v", n.Evidence)
	}
}

// A projected severity never reads an unrecognised status as healthy.
func TestProjectedSeverityUnknownIsDegraded(t *testing.T) {
	for _, status := range []string{"", "starting", "Critical", "unknown"} {
		if got := siteStatusName(projectedSeverity(status)); got != SiteStatusDegraded {
			t.Errorf("%q projects as %q, want degraded", status, got)
		}
	}
}

// silenceHarness drives heartbeats through the real handler at a movable
// clock, evicts as the gateway's eviction loop does, and reads GET /health.
type silenceHarness struct {
	t   *testing.T
	now time.Time
	h   *RuntimeHeartbeatHandler
	reg *Registry
}

const silenceTTLMS = 5000

func newSilenceHarness(t *testing.T) *silenceHarness {
	t.Helper()
	s := &silenceHarness{t: t, now: time.UnixMilli(1234567890000), reg: NewRegistry()}
	h, err := NewRuntimeHeartbeatHandler(s.reg, RuntimeHeartbeatHandlerOptions{Now: func() time.Time { return s.now }})
	if err != nil {
		t.Fatal(err)
	}
	s.h = h
	return s
}

func (s *silenceHarness) beat(status string, reasons []any) {
	s.t.Helper()
	payload := heartbeatMap(status, s.now.UnixMilli())
	if reasons != nil {
		payload["degradation_reasons"] = reasons
	}
	if err := s.h.Handle(testTopic, mustJSON(s.t, payload)); err != nil {
		s.t.Fatal(err)
	}
}

func (s *silenceHarness) silence(d time.Duration) (evicted, retained []NodeHeartbeat) {
	s.now = s.now.Add(d)
	return s.reg.EvictStaleRetainingCritical(s.now.UnixMilli(), silenceTTLMS)
}

func (s *silenceHarness) health() SiteHealth {
	s.t.Helper()
	return getHealth(s.t, s.reg, []string{"dev-01"}, s.now)
}

// Critical, then silence past eviction: still critical, reported once; a fresh
// healthy heartbeat clears it, and a healthy node going silent is evicted.
func TestSilentCriticalNodeStaysCriticalUntilItReportsAgain(t *testing.T) {
	s := newSilenceHarness(t)
	s.beat(NodeStatusCritical, []any{"decision_record_lost"})

	evicted, retained := s.silence(10 * silenceTTLMS * time.Millisecond)
	if len(evicted) != 0 || len(retained) != 1 || retained[0].DeviceID != "dev-01" {
		t.Fatalf("first eviction past TTL: evicted %v retained %v", evicted, retained)
	}
	out := s.health()
	n := out.Nodes[0]
	if out.Status != SiteStatusCritical || n.Status != SiteStatusCritical || !n.Stale || !n.GatewayObserved {
		t.Fatalf("silent critical node projected as site %q node %#v", out.Status, n)
	}
	if len(n.DegradationReasons) != 1 || n.DegradationReasons[0] != "decision_record_lost" {
		t.Fatalf("silent critical node lost its reasons: %#v", n.DegradationReasons)
	}

	evicted, retained = s.silence(1000 * silenceTTLMS * time.Millisecond)
	if len(evicted) != 0 || len(retained) != 0 {
		t.Fatalf("later eviction: evicted %v, retained %v (retention is reported once)", evicted, retained)
	}
	if out := s.health(); out.Status != SiteStatusCritical || out.Nodes[0].Status != SiteStatusCritical {
		t.Fatalf("critical decayed with time: %#v", out)
	}

	s.beat(NodeStatusHealthy, nil)
	if out := s.health(); out.Status != SiteStatusHealthy || out.Nodes[0].Status != SiteStatusHealthy || out.Nodes[0].Stale {
		t.Fatalf("fresh healthy heartbeat did not clear critical: %#v", out)
	}

	evicted, retained = s.silence(2 * silenceTTLMS * time.Millisecond)
	if len(evicted) != 1 || len(retained) != 0 {
		t.Fatalf("silent healthy node: evicted %v retained %v", evicted, retained)
	}
	if n := s.health().Nodes[0]; n.Status != SiteStatusDegraded || n.GatewayObserved {
		t.Fatalf("silent healthy node after eviction: %#v", n)
	}
}

// A node that goes silent after critical a second time is reported again.
func TestSilentCriticalRetentionReportedPerSilence(t *testing.T) {
	s := newSilenceHarness(t)
	s.beat(NodeStatusCritical, nil)
	if _, retained := s.silence(2 * silenceTTLMS * time.Millisecond); len(retained) != 1 {
		t.Fatalf("first silence retained %v", retained)
	}
	s.beat(NodeStatusCritical, nil)
	if _, retained := s.silence(silenceTTLMS * time.Millisecond); len(retained) != 0 {
		t.Fatalf("a node at the TTL boundary is not silent: retained %v", retained)
	}
	if _, retained := s.silence(time.Millisecond); len(retained) != 1 {
		t.Fatalf("second silence retained %v", retained)
	}
}

// Retention follows the last heartbeat only: degraded after critical, then
// silence, is today's behaviour, evicted and unobserved degraded.
func TestSilenceAfterDegradedIsEvicted(t *testing.T) {
	for _, status := range []string{NodeStatusDegraded, NodeStatusHealthy} {
		t.Run(status, func(t *testing.T) {
			s := newSilenceHarness(t)
			s.beat(NodeStatusCritical, nil)
			s.beat(status, nil)
			evicted, retained := s.silence(2 * silenceTTLMS * time.Millisecond)
			if len(evicted) != 1 || len(retained) != 0 {
				t.Fatalf("evicted %v retained %v", evicted, retained)
			}
			out := s.health()
			if out.Status != SiteStatusDegraded || out.Nodes[0].Status != SiteStatusDegraded || out.Nodes[0].GatewayObserved {
				t.Fatalf("site %q node %#v", out.Status, out.Nodes[0])
			}
		})
	}
}

// A future-dated critical node, which eviction otherwise drops at once, is
// retained critical as well, and EvictStale keeps it too.
func TestFutureDatedCriticalNodeRetained(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "dev-01", Status: NodeStatusCritical, LastSeenMS: 2000})
	reg.Upsert(NodeHeartbeat{DeviceID: "dev-02", Status: NodeStatusHealthy, LastSeenMS: 2000})
	if evicted := reg.EvictStale(1000, 100); len(evicted) != 1 || evicted[0].DeviceID != "dev-02" {
		t.Fatalf("evicted %v", evicted)
	}
	out := getHealth(t, reg, []string{"dev-01", "dev-02"}, time.UnixMilli(1000))
	if out.Status != SiteStatusCritical || out.Nodes[0].Status != SiteStatusCritical || !out.Nodes[0].Stale {
		t.Fatalf("future-dated critical node: %#v", out)
	}
}

// A retained node keeps its evidence history, so a chain that comes back
// truncated after the silence is still caught.
func TestRetainedCriticalNodeKeepsEvidenceHistory(t *testing.T) {
	reg := NewRegistry()
	beat := evidenceBeat("dev-01", headHash(1), 0, true)
	beat.Status = NodeStatusCritical
	reg.Upsert(beat)
	if evicted, retained := reg.EvictStaleRetainingCritical(beat.LastSeenMS+10_000, 100); len(evicted) != 0 || len(retained) != 1 {
		t.Fatalf("evicted %v retained %v", evicted, retained)
	}
	reg.Upsert(evidenceBeat("dev-01", "", 0, true))
	if got := reg.Snapshot()[0].Evidence; !got.TruncationSuspected {
		t.Fatalf("truncation after silence not detected: %#v", got)
	}
}

// Silence is judged on the gateway's receipt time, not the producer's clock: a
// critical node still reporting with its clock ahead or behind is not silent
// within the TTL of its arrival, and is reported once past it.
func TestSilentCriticalJudgedOnGatewayReceiptNotProducerClock(t *testing.T) {
	for _, skew := range []time.Duration{2 * time.Second, -200 * time.Second} {
		t.Run(skew.String(), func(t *testing.T) {
			s := newSilenceHarness(t)
			payload := heartbeatMap(NodeStatusCritical, s.now.Add(skew).UnixMilli())
			if err := s.h.Handle(testTopic, mustJSON(t, payload)); err != nil {
				t.Fatal(err)
			}
			for range 3 {
				if evicted, retained := s.reg.EvictStaleRetainingCritical(s.now.UnixMilli(), silenceTTLMS); len(evicted) != 0 || len(retained) != 0 {
					t.Fatalf("tick at arrival: evicted %v retained %v", evicted, retained)
				}
			}
			if _, retained := s.silence(silenceTTLMS * time.Millisecond); len(retained) != 0 {
				t.Fatalf("tick at the TTL of arrival: retained %v", retained)
			}
			if _, retained := s.silence(time.Millisecond); len(retained) != 1 {
				t.Fatalf("tick past the TTL of arrival: retained %v, want once", retained)
			}
			if _, retained := s.silence(time.Millisecond); len(retained) != 0 {
				t.Fatalf("later tick: retained %v, want nothing more", retained)
			}
			if out := s.health(); out.Status != SiteStatusCritical {
				t.Fatalf("site status %q", out.Status)
			}
		})
	}
}
