// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
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
	payload["request_id"] = requestID
	if _, ok := payload["device_id"]; !ok {
		payload["device_id"] = deviceID
	}
	encoded, err := json.Marshal(payload)
	if err != nil {
		panic(err)
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
				"device_id":         "edge-1",
				"timestamp":         float64(3000),
				"action_name":       "alert_whatsapp",
				"tier":              "A",
				"trigger_name":      "overcurrent",
				"sensor_type":       "current_clamp",
				"safe_default_used": false,
				"executed":          true,
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
