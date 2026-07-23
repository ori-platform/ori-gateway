// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

type fakeBroker struct {
	mu              sync.Mutex
	handlers        map[string]broker.MessageHandler
	subscribeCounts map[string]int
	publishes       []publishedMessage
	publishHook     func(topic string, payload []byte)
}

type publishedMessage struct {
	topic   string
	qos     byte
	retain  bool
	payload []byte
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		handlers:        make(map[string]broker.MessageHandler),
		subscribeCounts: make(map[string]int),
	}
}

func (f *fakeBroker) Subscribe(_ context.Context, topic string, _ byte, handler broker.MessageHandler) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.handlers[topic] = handler
	f.subscribeCounts[topic]++
	return nil
}

func (f *fakeBroker) Publish(_ context.Context, topic string, qos byte, retain bool, payload []byte) error {
	f.mu.Lock()
	f.publishes = append(f.publishes, publishedMessage{
		topic:   topic,
		qos:     qos,
		retain:  retain,
		payload: append([]byte(nil), payload...),
	})
	hook := f.publishHook
	f.mu.Unlock()
	if hook != nil {
		hook(topic, payload)
	}
	return nil
}

func (f *fakeBroker) respond(deviceID string, requestID string, payload map[string]any) {
	payload["request_id"] = requestID
	if _, ok := payload["device_id"]; !ok {
		payload["device_id"] = deviceID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(err)
	}
	f.respondRaw(deviceID, requestID, encoded)
}

func (f *fakeBroker) respondRaw(deviceID string, requestID string, encoded []byte) {
	topic, err := contracts.ExportResponseTopicFilter(deviceID)
	if err != nil {
		panic(err)
	}
	f.mu.Lock()
	handler := f.handlers[topic]
	f.mu.Unlock()
	if handler == nil {
		panic("missing response handler")
	}
	handler("ori/"+deviceID+"/export/response/"+requestID, encoded)
}

func (f *fakeBroker) subscribeCount(topic string) int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.subscribeCounts[topic]
}

func (f *fakeBroker) publishedRequests() []exportRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	out := make([]exportRequest, 0, len(f.publishes))
	for _, msg := range f.publishes {
		var req exportRequest
		if err := json.Unmarshal(msg.payload, &req); err != nil {
			panic(err)
		}
		out = append(out, req)
	}
	return out
}

func signedExportPayload(t *testing.T, payload map[string]any, secret string, now time.Time) []byte {
	t.Helper()
	auth, err := mqttauth.Sign(payload, contracts.ExportResponseMessageType, fmt.Sprint(payload["device_id"]), fmt.Sprint(payload["request_id"]), now.UnixMilli(), secret)
	if err != nil {
		t.Fatal(err)
	}
	payload["auth"] = auth
	encoded, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}

func newTestMQTTClient(t *testing.T, b *fakeBroker, ids ...string) *MQTTClient {
	t.Helper()
	idx := 0
	client, err := NewMQTTClient(b, WithRequestIDFunc(func() (string, error) {
		if idx >= len(ids) {
			return "extra-id", nil
		}
		id := ids[idx]
		idx++
		return id, nil
	}))
	if err != nil {
		t.Fatal(err)
	}
	return client
}

func TestMQTTRuntimeClientHealth(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "health-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "health",
			"complete":    true,
			"items": []any{map[string]any{
				"device_id":          "edge-1",
				"uptime_s":           42.5,
				"capability_posture": map[string]any{"gateway_reachable": true},
				"sensors":            []any{map[string]any{"last_seen_ms": float64(1234)}},
				"device_policy":      map[string]any{"tier": "trial"},
				"gateway_broker_posture": map[string]any{
					"available":              true,
					"gateway_enabled":        true,
					"deployment_check":       "required",
					"anonymous_access":       "disabled",
					"acl_policy":             "per_device_required",
					"require_credentials":    true,
					"credentials_configured": true,
					"requires_acl_hardening": false,
				},
				"state_store_encryption": map[string]any{
					"available":              true,
					"mode":                   "filesystem_required",
					"satisfied":              true,
					"marker_configured":      true,
					"path_prefix_configured": true,
				},
				"alert_outbox": map[string]any{
					"backlog_count":                     float64(2),
					"oldest_queued_original_ts":         float64(123456),
					"oldest_queued_age_ms":              float64(60000),
					"retry_interval_minutes":            0.5,
					"max_non_tier_d_attempts":           float64(10),
					"tier_d_critical_warning_threshold": float64(3),
					"batch_size":                        float64(50),
				},
				"remote_command_lockout": map[string]any{"senders": []any{map[string]any{
					"channel":       "sms",
					"from_number":   "+2348012345678",
					"risk_level":    "elevated",
					"locked_out":    false,
					"stale":         false,
					"checked_at_ms": float64(99),
				}}},
			}},
		})
	}

	health, err := client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
	if err != nil {
		t.Fatal(err)
	}
	if health.DeviceID != "edge-1" || health.UptimeS != 42.5 || !health.GatewaySeen {
		t.Fatalf("unexpected health: %#v", health)
	}
	if health.LastReadingMS != 1234 || health.PolicyStatus != "trial" {
		t.Fatalf("runtime health fields not mapped: %#v", health)
	}
	if health.GatewayBrokerPosture == nil || !health.GatewayBrokerPosture.Available || health.GatewayBrokerPosture.RequiresACLHardening {
		t.Fatalf("gateway broker posture not mapped: %#v", health.GatewayBrokerPosture)
	}
	if health.GatewayBrokerPosture.ACLPolicy != "per_device_required" || !health.GatewayBrokerPosture.CredentialsConfigured {
		t.Fatalf("gateway broker posture fields not mapped: %#v", health.GatewayBrokerPosture)
	}
	if health.StateStoreEncryption == nil || !health.StateStoreEncryption.Satisfied || health.StateStoreEncryption.Mode != "filesystem_required" {
		t.Fatalf("state store encryption posture not mapped: %#v", health.StateStoreEncryption)
	}
	if !health.StateStoreEncryption.PathPrefixConfigured {
		t.Fatalf("state store encryption path prefix flag not mapped: %#v", health.StateStoreEncryption)
	}
	if health.AlertOutbox == nil || !health.AlertOutbox.Available || health.AlertOutbox.BacklogCount != 2 {
		t.Fatalf("alert outbox posture not mapped: %#v", health.AlertOutbox)
	}
	if health.AlertOutbox.OldestQueuedOriginalMS != 123456 || health.AlertOutbox.OldestQueuedAgeMS != 60000 {
		t.Fatalf("alert outbox timestamp fields not mapped: %#v", health.AlertOutbox)
	}
	state := health.LockoutRiskLevels["sms:+2348012345678"]
	if state.RiskLevel != "elevated" || state.CheckedAtMS != 99 {
		t.Fatalf("lockout state not mapped: %#v", health.LockoutRiskLevels)
	}

	requests := b.publishedRequests()
	if len(requests) != 1 {
		t.Fatalf("published requests = %d, want 1", len(requests))
	}
	if requests[0].ExportType != "health" || requests[0].DeviceID != "edge-1" {
		t.Fatalf("unexpected request: %#v", requests[0])
	}
	if b.publishes[0].topic != "ori/edge-1/export/request" {
		t.Fatalf("unexpected publish topic: %s", b.publishes[0].topic)
	}
}

func TestMQTTRuntimeClientHealthLeavesAbsentPostureNil(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "health-legacy")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "health",
			"complete":    true,
			"items": []any{map[string]any{
				"device_id": "edge-1",
				"status":    "healthy",
			}},
		})
	}

	health, err := client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
	if err != nil {
		t.Fatal(err)
	}
	if health.GatewayBrokerPosture != nil {
		t.Fatalf("gateway broker posture should be nil when absent: %#v", health.GatewayBrokerPosture)
	}
	if health.StateStoreEncryption != nil {
		t.Fatalf("state store encryption posture should be nil when absent: %#v", health.StateStoreEncryption)
	}
	if health.AlertOutbox != nil {
		t.Fatalf("alert outbox posture should be nil when absent: %#v", health.AlertOutbox)
	}
}

func TestMQTTRuntimeClientSensorHistoryBucketed(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "history-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "sensor_history",
			"complete":    true,
			"items": []any{map[string]any{
				"sensor_id":    "current-main",
				"sensor_type":  "current_clamp",
				"unit":         "A",
				"start_ms":     float64(1000),
				"end_ms":       float64(2000),
				"avg_value":    nil,
				"value":        4.2,
				"min_value":    3.9,
				"max_value":    4.8,
				"sample_count": float64(12),
				"quality":      0.98,
			}},
		})
	}

	rows, err := client.SensorHistory(context.Background(), SensorHistoryRequest{
		BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, UntilMS: 2, Limit: 50},
		SensorID:      "current-main",
		BucketMS:      DefaultWeeklyReportBucketMS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AvgValue != 4.2 || rows[0].Samples != 12 {
		t.Fatalf("unexpected history rows: %#v", rows)
	}

	requests := b.publishedRequests()
	if got := requests[0].Params["bucket_ms"]; got != float64(DefaultWeeklyReportBucketMS) {
		t.Fatalf("bucket_ms = %#v, want %d", got, DefaultWeeklyReportBucketMS)
	}
	if got := requests[0].Params["sensor_id"]; got != "current-main" {
		t.Fatalf("sensor_id = %#v", got)
	}
}

func TestMQTTRuntimeClientActionLog(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "actions-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "action_log",
			"complete":    true,
			"items": []any{map[string]any{
				"device_id":               "edge-1",
				"timestamp":               float64(3000),
				"action_name":             "alert_whatsapp",
				"tier":                    "A",
				"trigger_name":            "overcurrent",
				"sensor_type":             "current_clamp",
				"safe_default_used":       false,
				"executed":                true,
				"attestation_status":      "signed",
				"attestation_seq":         float64(42),
				"input_attestation_grade": "attested",
				"input_posture":           "hardware_key",
			}},
		})
	}

	rows, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ActionName != "alert_whatsapp" || !rows[0].Success {
		t.Fatalf("unexpected action rows: %#v", rows)
	}
	if rows[0].AttestationStatus != "signed" || rows[0].AttestationSeq == nil || *rows[0].AttestationSeq != 42 {
		t.Fatalf("action attestation fields not mapped: %#v", rows[0])
	}
	if rows[0].InputAttestationGrade != "attested" || rows[0].InputPosture != "hardware_key" {
		t.Fatalf("action input evidence fields not mapped: %#v", rows[0])
	}
}

func TestMQTTRuntimeClientActionLogPreservesNullAttestationSequence(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "actions-pending")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "action_log",
			"complete":    true,
			"items": []any{map[string]any{
				"device_id":          "edge-1",
				"timestamp":          float64(3000),
				"action_name":        "open_relay",
				"tier":               "C",
				"attestation_status": "pending",
				"attestation_seq":    nil,
			}},
		})
	}

	rows, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AttestationStatus != "pending" || rows[0].AttestationSeq != nil {
		t.Fatalf("null attestation sequence not preserved: %#v", rows)
	}
}

func TestMQTTRuntimeClientTierCDecisionLog(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "tierc-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "tier_c_decision_log",
			"complete":    true,
			"items": []any{map[string]any{
				"device_id":           "edge-1",
				"proposal_id":         "abc123",
				"created_at":          float64(4000),
				"skill_name":          "energy-anomaly-detector",
				"trigger_name":        "overcurrent",
				"sensor_id":           "current-main",
				"sensor_type":         "current_clamp",
				"reading_value":       7.5,
				"history_window":      []any{map[string]any{"sensor_id": "current-main", "timestamp": float64(3900), "value": 4.2}},
				"proposed_action":     "open safety circuit",
				"confidence":          0.82,
				"operator_decision":   "approved",
				"decision_latency_s":  12.5,
				"safe_default_used":   false,
				"final_action_result": map[string]any{"relay": "open"},
				"site_type":           "pharmacy",
				"location":            "Lagos",
				"timezone":            "Africa/Lagos",
				"later_outcome":       map[string]any{"stable": true},
			}},
		})
	}

	rows, err := client.TierCDecisionLog(context.Background(), TierCDecisionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].ProposalID != "abc123" || rows[0].HistoryWindow[0].Value != 4.2 {
		t.Fatalf("unexpected Tier C rows: %#v", rows)
	}
	if rows[0].FinalActionResult["relay"] != "open" || rows[0].LaterOutcome["stable"] != true {
		t.Fatalf("nested maps not preserved: %#v", rows[0])
	}
}

func TestMQTTRuntimeClientReasoningLog(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "reasoning-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		if req.ExportType != "reasoning_log" {
			t.Fatalf("export_type = %q", req.ExportType)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "reasoning_log",
			"complete":    true,
			"items": []any{map[string]any{
				"device_id":        "edge-1",
				"created_at_ms":    float64(5000),
				"skill_name":       "energy-anomaly-detector",
				"trigger_name":     "overcurrent",
				"sensor_id":        "current-main",
				"sensor_type":      "current_clamp",
				"tier_used":        "gateway",
				"model":            "llama-3.2-3b",
				"prompt_text":      "explain",
				"reasoning_text":   "voltage sag caused current spike",
				"confidence":       0.73,
				"proposed_action":  "notify operator",
				"action_tier":      "A",
				"token_count":      float64(42),
				"latency_ms":       float64(1250),
				"reasoning_status": "complete",
				"correlation_id":   "corr-1",
			}},
		})
	}

	rows, err := client.ReasoningLog(context.Background(), ReasoningLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 {
		t.Fatalf("rows = %d", len(rows))
	}
	row := rows[0]
	if row.CorrelationID != "corr-1" || row.ReasoningStatus != "complete" || row.TokenCount != 42 || row.LatencyMS != 1250 {
		t.Fatalf("reasoning row not mapped: %#v", row)
	}
	if row.ReasoningText != "voltage sag caused current spike" || row.Model != "llama-3.2-3b" {
		t.Fatalf("reasoning text/model not mapped: %#v", row)
	}
}

func TestMQTTRuntimeClientReasoningLogFiltersForwarded(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "reasoning-filter-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		if req.ExportType != "reasoning_log" {
			t.Fatalf("export_type = %q", req.ExportType)
		}
		wantParams := map[string]any{
			"tier_used":        "gateway",
			"action_tier":      "B",
			"reasoning_status": "incomplete",
			"correlation_id":   "corr-1",
		}
		for key, want := range wantParams {
			if got := req.Params[key]; got != want {
				t.Fatalf("param %s = %#v, want %#v in %#v", key, got, want, req.Params)
			}
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "reasoning_log",
			"complete":    true,
			"items":       []any{},
		})
	}

	rows, err := client.ReasoningLog(context.Background(), ReasoningLogRequest{
		BoundedWindow:   BoundedWindow{DeviceID: "edge-1", SinceMS: 1, UntilMS: 2, Limit: 10},
		TierUsed:        "gateway",
		ActionTier:      "B",
		ReasoningStatus: "incomplete",
		CorrelationID:   "corr-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 0 {
		t.Fatalf("expected empty rows, got %#v", rows)
	}
}

func TestMQTTRuntimeClientReasoningLogRuntimeError(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "reasoning-error-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "reasoning_log",
			"complete":    true,
			"error":       "reasoning log unavailable",
			"items":       []any{},
		})
	}

	_, err := client.ReasoningLog(context.Background(), ReasoningLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err == nil || !strings.Contains(err.Error(), "reasoning log unavailable") {
		t.Fatalf("expected runtime error, got %v", err)
	}
}

func TestMQTTRuntimeClientIgnoresUnmatchedResponseID(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "wanted-id")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, "other-id", map[string]any{
			"export_type": "health",
			"complete":    true,
			"items":       []any{map[string]any{"device_id": "wrong"}},
		})
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "health",
			"complete":    true,
			"items":       []any{map[string]any{"device_id": "edge-1", "status": "healthy"}},
		})
	}

	health, err := client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
	if err != nil {
		t.Fatal(err)
	}
	if health.DeviceID != "edge-1" || health.Status != "healthy" {
		t.Fatalf("unmatched response was not ignored: %#v", health)
	}
}

func TestMQTTRuntimeClientSubscribesOnceForConcurrentFirstRequests(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "concurrent-health", "concurrent-actions")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		switch req.ExportType {
		case "health":
			b.respond(req.DeviceID, req.RequestID, map[string]any{
				"export_type": "health",
				"complete":    true,
				"items":       []any{map[string]any{"device_id": "edge-1", "status": "healthy"}},
			})
		case "action_log":
			b.respond(req.DeviceID, req.RequestID, map[string]any{
				"export_type": "action_log",
				"complete":    true,
				"items":       []any{},
			})
		}
	}

	var wg sync.WaitGroup
	wg.Add(2)
	errCh := make(chan error, 2)
	go func() {
		defer wg.Done()
		_, err := client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
		errCh <- err
	}()
	go func() {
		defer wg.Done()
		_, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
		errCh <- err
	}()
	wg.Wait()
	close(errCh)
	for err := range errCh {
		if err != nil {
			t.Fatal(err)
		}
	}
	if got := b.subscribeCount("ori/edge-1/export/response/+"); got != 1 {
		t.Fatalf("subscribe count = %d, want 1", got)
	}
}

func TestMQTTRuntimeClientPaginatesUntilComplete(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "page-1", "page-2")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		if req.PageToken == "" {
			b.respond(req.DeviceID, req.RequestID, map[string]any{
				"export_type":     "action_log",
				"complete":        false,
				"next_page_token": "1",
				"items":           []any{map[string]any{"action_name": "first", "timestamp": float64(1)}},
			})
			return
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "action_log",
			"complete":    true,
			"items":       []any{map[string]any{"action_name": "second", "timestamp": float64(2)}},
		})
	}

	rows, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 1}})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 2 || rows[0].ActionName != "first" || rows[1].ActionName != "second" {
		t.Fatalf("pagination not assembled: %#v", rows)
	}
	requests := b.publishedRequests()
	if len(requests) != 2 || requests[1].PageToken != "1" {
		t.Fatalf("unexpected paged requests: %#v", requests)
	}
}

func TestMQTTRuntimeClientTimeout(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "timeout-1")
	ctx, cancel := context.WithTimeout(context.Background(), time.Millisecond)
	defer cancel()

	_, err := client.Health(ctx, HealthRequest{DeviceID: "edge-1"})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("got %v, want deadline exceeded", err)
	}
}

func TestMQTTRuntimeClientRejectsRepeatedPageToken(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "repeat-1", "repeat-2")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type":     "action_log",
			"complete":        false,
			"next_page_token": "same-token",
			"items":           []any{},
		})
	}

	_, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err == nil || !strings.Contains(err.Error(), "repeated next_page_token") {
		t.Fatalf("expected repeated page token error, got %v", err)
	}
}

func TestMQTTRuntimeClientRuntimeError(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "error-1")
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "health",
			"complete":    true,
			"error":       "state store unavailable",
			"items":       []any{},
		})
	}

	_, err := client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
	if err == nil || !strings.Contains(err.Error(), "state store unavailable") {
		t.Fatalf("expected runtime error, got %v", err)
	}
}

func TestMQTTRuntimeClientSignsExportRequestAndVerifiesResponse(t *testing.T) {
	b := newFakeBroker()
	now := time.UnixMilli(1234567890000)
	client, err := NewMQTTClient(
		b,
		WithRequestIDFunc(func() (string, error) { return "health-signed", nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
	)
	if err != nil {
		t.Fatal(err)
	}
	b.publishHook = func(_ string, payload []byte) {
		verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "current-secret", Now: func() time.Time { return now }})
		if err != nil {
			t.Fatal(err)
		}
		unsigned, err := verifier.VerifyJSON(payload, contracts.ExportRequestMessageType, "edge-1", "health-signed")
		if err != nil {
			t.Fatal(err)
		}
		if unsigned["export_type"] != "health" {
			t.Fatalf("unexpected export request: %#v", unsigned)
		}
		resp := map[string]any{"request_id": "health-signed", "device_id": "edge-1", "export_type": "health", "complete": true, "items": []any{map[string]any{"device_id": "edge-1", "status": "healthy"}}}
		b.respondRaw("edge-1", "health-signed", signedExportPayload(t, resp, "current-secret", now))
	}
	if _, err := client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
		t.Fatal(err)
	}
}

func TestMQTTRuntimeClientAcceptsPreviousSecretForResponse(t *testing.T) {
	b := newFakeBroker()
	now := time.UnixMilli(1234567890000)
	client, err := NewMQTTClient(
		b,
		WithRequestIDFunc(func() (string, error) { return "health-prev", nil }),
		WithMessageAuth("current-secret", "previous-secret", func() time.Time { return now }),
	)
	if err != nil {
		t.Fatal(err)
	}
	b.publishHook = func(_ string, _ []byte) {
		resp := map[string]any{"request_id": "health-prev", "device_id": "edge-1", "export_type": "health", "complete": true, "items": []any{map[string]any{"device_id": "edge-1", "status": "healthy"}}}
		b.respondRaw("edge-1", "health-prev", signedExportPayload(t, resp, "previous-secret", now))
	}
	if _, err := client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
		t.Fatal(err)
	}
}

func TestMQTTRuntimeClientRejectsUnsignedResponseWhenAuthEnabled(t *testing.T) {
	b := newFakeBroker()
	now := time.UnixMilli(1234567890000)
	client, err := NewMQTTClient(
		b,
		WithRequestIDFunc(func() (string, error) { return "health-unsigned", nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
	)
	if err != nil {
		t.Fatal(err)
	}
	b.publishHook = func(_ string, _ []byte) {
		b.respond("edge-1", "health-unsigned", map[string]any{"export_type": "health", "complete": true, "items": []any{map[string]any{"device_id": "edge-1"}}})
	}
	_, err = client.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
	if err == nil || !strings.Contains(err.Error(), "verify export response") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMQTTRuntimeClientDecryptsSensitiveExportResponse(t *testing.T) {
	b := newFakeBroker()
	now := time.UnixMilli(1234567890000)
	client, err := NewMQTTClient(
		b,
		WithRequestIDFunc(func() (string, error) { return "history-secure", nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithMessageEncryption("current-secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	b.publishHook = func(_ string, _ []byte) {
		encryptor, err := mqttauth.NewEncryptor("current-secret")
		if err != nil {
			t.Fatal(err)
		}
		plain := map[string]any{"request_id": "history-secure", "device_id": "edge-1", "export_type": "sensor_history", "complete": true, "items": []any{map[string]any{"sensor_id": "current-main", "value": 4.2}}}
		encrypted, err := encryptor.Encrypt(plain, contracts.ExportResponseMessageType, []byte("123456789012"))
		if err != nil {
			t.Fatal(err)
		}
		b.respondRaw("edge-1", "history-secure", signedExportPayload(t, encrypted, "current-secret", now))
	}
	rows, err := client.SensorHistory(context.Background(), SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, UntilMS: 2, Limit: 10}, SensorID: "current-main"})
	if err != nil {
		t.Fatal(err)
	}
	if len(rows) != 1 || rows[0].AvgValue != 4.2 {
		t.Fatalf("unexpected rows: %#v", rows)
	}
}

func TestMQTTRuntimeClientRejectsTamperedEncryptedExportResponse(t *testing.T) {
	b := newFakeBroker()
	now := time.UnixMilli(1234567890000)
	client, err := NewMQTTClient(
		b,
		WithRequestIDFunc(func() (string, error) { return "history-tampered", nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithMessageEncryption("current-secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	b.publishHook = func(_ string, _ []byte) {
		encryptor, err := mqttauth.NewEncryptor("current-secret")
		if err != nil {
			t.Fatal(err)
		}
		plain := map[string]any{"request_id": "history-tampered", "device_id": "edge-1", "export_type": "sensor_history", "complete": true, "items": []any{}}
		encrypted, err := encryptor.Encrypt(plain, contracts.ExportResponseMessageType, []byte("123456789012"))
		if err != nil {
			t.Fatal(err)
		}
		envelope := encrypted["encryption"].(map[string]any)
		envelope["ciphertext"] = "AAAA" + envelope["ciphertext"].(string)
		b.respondRaw("edge-1", "history-tampered", signedExportPayload(t, encrypted, "current-secret", now))
	}
	_, err = client.SensorHistory(context.Background(), SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, UntilMS: 2, Limit: 10}, SensorID: "current-main"})
	if err == nil || !strings.Contains(err.Error(), "decrypt export response") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMQTTRuntimeClientRejectsPlainSensitiveExportWhenEncryptionEnabled(t *testing.T) {
	b := newFakeBroker()
	now := time.UnixMilli(1234567890000)
	client, err := NewMQTTClient(
		b,
		WithRequestIDFunc(func() (string, error) { return "history-plain", nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithMessageEncryption("current-secret"),
	)
	if err != nil {
		t.Fatal(err)
	}
	b.publishHook = func(_ string, _ []byte) {
		plain := map[string]any{"request_id": "history-plain", "device_id": "edge-1", "export_type": "sensor_history", "complete": true, "items": []any{}}
		b.respondRaw("edge-1", "history-plain", signedExportPayload(t, plain, "current-secret", now))
	}
	_, err = client.SensorHistory(context.Background(), SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, UntilMS: 2, Limit: 10}, SensorID: "current-main"})
	if err == nil || !strings.Contains(err.Error(), "not encrypted") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMQTTRuntimeClientEncryptionRequiresAuth(t *testing.T) {
	b := newFakeBroker()
	_, err := NewMQTTClient(b, WithMessageEncryption("current-secret"))
	if err == nil || !strings.Contains(err.Error(), "encryption requires message auth") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMQTTRuntimeClientPreservesAuthOptionError(t *testing.T) {
	b := newFakeBroker()
	_, err := NewMQTTClient(b, WithMessageAuth("same-secret", "same-secret", time.Now))
	if err == nil || !strings.Contains(err.Error(), "previous shared secret") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestMQTTRuntimeClientPreservesEncryptionOptionError(t *testing.T) {
	b := newFakeBroker()
	_, err := NewMQTTClient(b, WithMessageEncryption(""))
	if err == nil || !strings.Contains(err.Error(), "shared secret must not be empty") {
		t.Fatalf("unexpected error: %v", err)
	}
}
