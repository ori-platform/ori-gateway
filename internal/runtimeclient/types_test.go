// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"errors"
	"testing"
)

func validWindow() BoundedWindow {
	return BoundedWindow{DeviceID: "edge-1", SinceMS: 1, UntilMS: 2, Limit: 10}
}

func TestHealthRequestValidation(t *testing.T) {
	if _, err := NormalizeHealthRequest(HealthRequest{DeviceID: "edge-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := NormalizeHealthRequest(HealthRequest{}); err == nil {
		t.Fatal("expected missing device_id to fail")
	}
}

func TestSensorHistoryRequestValidation(t *testing.T) {
	_, err := NormalizeSensorHistoryRequest(SensorHistoryRequest{BoundedWindow: validWindow(), SensorID: "sensor-1"})
	if err != nil {
		t.Fatal(err)
	}

	cases := []struct {
		name string
		req  SensorHistoryRequest
	}{
		{name: "missing device", req: SensorHistoryRequest{BoundedWindow: BoundedWindow{SinceMS: 1, Limit: 10}, SensorID: "sensor-1"}},
		{name: "missing since", req: SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", Limit: 10}, SensorID: "sensor-1"}},
		{name: "until equal since", req: SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 10, UntilMS: 10, Limit: 10}, SensorID: "sensor-1"}},
		{name: "until before since", req: SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 10, UntilMS: 9, Limit: 10}, SensorID: "sensor-1"}},
		{name: "zero limit", req: SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1}, SensorID: "sensor-1"}},
		{name: "negative limit", req: SensorHistoryRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: -1}, SensorID: "sensor-1"}},
		{name: "missing sensor", req: SensorHistoryRequest{BoundedWindow: validWindow()}},
		{name: "negative bucket", req: SensorHistoryRequest{BoundedWindow: validWindow(), SensorID: "sensor-1", BucketMS: -1}},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NormalizeSensorHistoryRequest(tc.req); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestSensorHistoryRequestCarriesBucketMS(t *testing.T) {
	req, err := NormalizeSensorHistoryRequest(SensorHistoryRequest{
		BoundedWindow: validWindow(),
		SensorID:      "current-main",
		BucketMS:      DefaultWeeklyReportBucketMS,
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.BucketMS != DefaultWeeklyReportBucketMS {
		t.Fatalf("bucket_ms = %d, want %d", req.BucketMS, DefaultWeeklyReportBucketMS)
	}
}

func TestActionLogRequestValidation(t *testing.T) {
	req, err := NormalizeActionLogRequest(ActionLogRequest{BoundedWindow: validWindow()})
	if err != nil {
		t.Fatal(err)
	}
	if req.Limit != 10 {
		t.Fatalf("limit = %d, want 10", req.Limit)
	}

	if _, err := NormalizeActionLogRequest(ActionLogRequest{BoundedWindow: BoundedWindow{SinceMS: 1, Limit: 10}}); err == nil {
		t.Fatal("expected missing device_id to fail")
	}
	if _, err := NormalizeActionLogRequest(ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1}}); err == nil {
		t.Fatal("expected missing limit to fail")
	}
	if _, err := NormalizeActionLogRequest(ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 5, UntilMS: 4, Limit: 10}}); err == nil {
		t.Fatal("expected invalid window to fail")
	}
}

func TestTierCDecisionLogRequestValidation(t *testing.T) {
	req, err := NormalizeTierCDecisionLogRequest(TierCDecisionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: MaxLimit + 50}})
	if err != nil {
		t.Fatal(err)
	}
	if req.Limit != MaxLimit {
		t.Fatalf("limit = %d, want cap %d", req.Limit, MaxLimit)
	}

	if _, err := NormalizeTierCDecisionLogRequest(TierCDecisionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: -1, Limit: 10}}); err == nil {
		t.Fatal("expected negative since_ms to fail")
	}
	if _, err := NormalizeTierCDecisionLogRequest(TierCDecisionLogRequest{BoundedWindow: BoundedWindow{SinceMS: 1, Limit: 10}}); err == nil {
		t.Fatal("expected missing device_id to fail")
	}
}

func TestFakeRuntimeDataClient(t *testing.T) {
	client := &FakeClient{
		HealthSnapshot: HealthSnapshot{DeviceID: "edge-1", Status: "healthy"},
		SensorHistoryRows: []SensorAggregate{{
			DeviceID: "edge-1",
			SensorID: "current-main",
			AvgValue: 4.2,
			Metadata: map[string]any{"phase": "a"},
		}},
		ActionLogRows: []ActionLogEntry{{
			DeviceID:   "edge-1",
			ActionName: "alert_whatsapp",
			Tier:       "A",
			Result:     map[string]any{"sent": true},
		}},
		TierCDecisionLogRows: []TierCDecisionEntry{{
			DeviceID:          "edge-1",
			ProposalID:        "abc123",
			OperatorDecision:  "approved",
			HistoryWindow:     []HistorySample{{SensorID: "current-main", Value: 4.2}},
			FinalActionResult: map[string]any{"relay": "open"},
			LaterOutcome:      map[string]any{"stable": true},
		}},
		ReasoningLogRows: []ReasoningLogEntry{{
			DeviceID:        "edge-1",
			TierUsed:        "gateway",
			ReasoningStatus: "complete",
			CorrelationID:   "corr-1",
		}},
	}

	ctx := context.Background()
	health, err := client.Health(ctx, HealthRequest{DeviceID: "edge-1"})
	if err != nil {
		t.Fatal(err)
	}
	if health.DeviceID != "edge-1" {
		t.Fatalf("unexpected health device: %q", health.DeviceID)
	}

	history, err := client.SensorHistory(ctx, SensorHistoryRequest{BoundedWindow: validWindow(), SensorID: "current-main", BucketMS: DefaultWeeklyReportBucketMS})
	if err != nil {
		t.Fatal(err)
	}
	if len(history) != 1 || history[0].AvgValue != 4.2 {
		t.Fatalf("unexpected history: %#v", history)
	}
	if client.LastSensorHistoryRequest().BucketMS != DefaultWeeklyReportBucketMS {
		t.Fatalf("fake did not preserve bucket_ms: %d", client.LastSensorHistoryRequest().BucketMS)
	}
	if client.LastSensorHistoryRequest().Limit != 10 {
		t.Fatalf("fake did not preserve normalized request limit: %d", client.LastSensorHistoryRequest().Limit)
	}

	actions, err := client.ActionLog(ctx, ActionLogRequest{BoundedWindow: validWindow()})
	if err != nil {
		t.Fatal(err)
	}
	if len(actions) != 1 || actions[0].ActionName != "alert_whatsapp" {
		t.Fatalf("unexpected actions: %#v", actions)
	}

	decisions, err := client.TierCDecisionLog(ctx, TierCDecisionLogRequest{BoundedWindow: validWindow()})
	if err != nil {
		t.Fatal(err)
	}
	if len(decisions) != 1 || decisions[0].ProposalID != "abc123" {
		t.Fatalf("unexpected decisions: %#v", decisions)
	}

	reasoning, err := client.ReasoningLog(ctx, ReasoningLogRequest{
		BoundedWindow:   validWindow(),
		TierUsed:        "gateway",
		ReasoningStatus: "complete",
		CorrelationID:   "corr-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(reasoning) != 1 || reasoning[0].CorrelationID != "corr-1" {
		t.Fatalf("unexpected reasoning rows: %#v", reasoning)
	}
	if client.LastReasoningLogRequest().TierUsed != "gateway" {
		t.Fatalf("fake did not preserve reasoning filter: %#v", client.LastReasoningLogRequest())
	}
}

func TestFakeRuntimeDataClientReturnsCopies(t *testing.T) {
	client := &FakeClient{
		SensorHistoryRows: []SensorAggregate{{Metadata: map[string]any{"phase": "a"}}},
		ActionLogRows:     []ActionLogEntry{{Result: map[string]any{"sent": true}}},
		TierCDecisionLogRows: []TierCDecisionEntry{{
			HistoryWindow:     []HistorySample{{SensorID: "current-main", Value: 4.2}},
			FinalActionResult: map[string]any{"relay": "open"},
			LaterOutcome:      map[string]any{"stable": true},
		}},
		ReasoningLogRows: []ReasoningLogEntry{{CorrelationID: "corr-1", ReasoningText: "original"}},
	}

	history, err := client.SensorHistory(context.Background(), SensorHistoryRequest{BoundedWindow: validWindow(), SensorID: "current-main", BucketMS: DefaultWeeklyReportBucketMS})
	if err != nil {
		t.Fatal(err)
	}
	history[0].Metadata["phase"] = "tampered"
	if got := client.SensorHistoryRows[0].Metadata["phase"]; got != "a" {
		t.Fatalf("history metadata aliased into fake storage: %v", got)
	}

	actions, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: validWindow()})
	if err != nil {
		t.Fatal(err)
	}
	actions[0].Result["sent"] = false
	if got := client.ActionLogRows[0].Result["sent"]; got != true {
		t.Fatalf("action result aliased into fake storage: %v", got)
	}

	decisions, err := client.TierCDecisionLog(context.Background(), TierCDecisionLogRequest{BoundedWindow: validWindow()})
	if err != nil {
		t.Fatal(err)
	}
	decisions[0].HistoryWindow[0].Value = 99
	decisions[0].FinalActionResult["relay"] = "closed"
	decisions[0].LaterOutcome["stable"] = false
	stored := client.TierCDecisionLogRows[0]
	if stored.HistoryWindow[0].Value != 4.2 {
		t.Fatalf("history window aliased into fake storage: %v", stored.HistoryWindow[0].Value)
	}
	if stored.FinalActionResult["relay"] != "open" {
		t.Fatalf("final result aliased into fake storage: %v", stored.FinalActionResult["relay"])
	}
	if stored.LaterOutcome["stable"] != true {
		t.Fatalf("later outcome aliased into fake storage: %v", stored.LaterOutcome["stable"])
	}

	reasoning, err := client.ReasoningLog(context.Background(), ReasoningLogRequest{BoundedWindow: validWindow()})
	if err != nil {
		t.Fatal(err)
	}
	reasoning[0].ReasoningText = "tampered"
	if client.ReasoningLogRows[0].ReasoningText != "original" {
		t.Fatalf("reasoning rows aliased into fake storage: %v", client.ReasoningLogRows[0].ReasoningText)
	}
}

func TestFakeRuntimeDataClientValidatesBeforeContext(t *testing.T) {
	client := &FakeClient{}
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	_, err := client.SensorHistory(ctx, SensorHistoryRequest{})
	if err == nil || errors.Is(err, context.Canceled) {
		t.Fatalf("expected validation error before context cancellation, got %v", err)
	}
}

func TestFakeRuntimeDataClientPropagatesErrorsAndContext(t *testing.T) {
	wantErr := errors.New("runtime unavailable")
	client := &FakeClient{ActionLogErr: wantErr}

	_, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: validWindow()})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want %v", err, wantErr)
	}

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err = client.Health(ctx, HealthRequest{DeviceID: "edge-1"})
	if !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestReasoningLogRequestValidation(t *testing.T) {
	req, err := NormalizeReasoningLogRequest(ReasoningLogRequest{
		BoundedWindow:   BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: MaxLimit + 50},
		TierUsed:        "gateway",
		ActionTier:      "B",
		ReasoningStatus: "incomplete",
		CorrelationID:   "corr-1",
	})
	if err != nil {
		t.Fatal(err)
	}
	if req.Limit != MaxLimit {
		t.Fatalf("limit = %d, want cap %d", req.Limit, MaxLimit)
	}

	cases := []struct {
		name string
		req  ReasoningLogRequest
	}{
		{name: "bad tier", req: ReasoningLogRequest{BoundedWindow: validWindow(), TierUsed: "cloud"}},
		{name: "bad action tier", req: ReasoningLogRequest{BoundedWindow: validWindow(), ActionTier: "Z"}},
		{name: "bad status", req: ReasoningLogRequest{BoundedWindow: validWindow(), ReasoningStatus: "unknown"}},
		{name: "bad window", req: ReasoningLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1}}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, err := NormalizeReasoningLogRequest(tc.req); err == nil {
				t.Fatal("expected validation error")
			}
		})
	}
}

func TestRuntimeDataClientLimitCap(t *testing.T) {
	client := &FakeClient{}
	_, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: MaxLimit + 1}})
	if err != nil {
		t.Fatal(err)
	}
	if client.LastActionLogRequest().Limit != MaxLimit {
		t.Fatalf("limit = %d, want %d", client.LastActionLogRequest().Limit, MaxLimit)
	}

	_, err = client.ReasoningLog(context.Background(), ReasoningLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: MaxLimit + 1}})
	if err != nil {
		t.Fatal(err)
	}
	if client.LastReasoningLogRequest().Limit != MaxLimit {
		t.Fatalf("reasoning limit = %d, want %d", client.LastReasoningLogRequest().Limit, MaxLimit)
	}
}
