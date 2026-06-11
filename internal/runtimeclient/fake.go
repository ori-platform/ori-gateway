// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"sync"
)

// FakeClient is an in-memory Client implementation for gateway product/report tests.
// It still runs request validation so tests exercise the same bounded-export contract.
type FakeClient struct {
	mu sync.Mutex

	HealthSnapshot       HealthSnapshot
	HealthErr            error
	SensorHistoryRows    []SensorAggregate
	SensorHistoryErr     error
	ActionLogRows        []ActionLogEntry
	ActionLogErr         error
	TierCDecisionLogRows []TierCDecisionEntry
	TierCDecisionLogErr  error
	ReasoningLogRows     []ReasoningLogEntry
	ReasoningLogErr      error

	lastHealthRequest           HealthRequest
	lastSensorHistoryRequest    SensorHistoryRequest
	lastActionLogRequest        ActionLogRequest
	lastTierCDecisionLogRequest TierCDecisionLogRequest
	lastReasoningLogRequest     ReasoningLogRequest
}

func (f *FakeClient) Health(ctx context.Context, req HealthRequest) (HealthSnapshot, error) {
	normalized, err := NormalizeHealthRequest(req)
	if err != nil {
		return HealthSnapshot{}, err
	}
	if err := ctx.Err(); err != nil {
		return HealthSnapshot{}, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastHealthRequest = normalized
	if f.HealthErr != nil {
		return HealthSnapshot{}, f.HealthErr
	}
	return f.HealthSnapshot, nil
}

func (f *FakeClient) SensorHistory(ctx context.Context, req SensorHistoryRequest) ([]SensorAggregate, error) {
	normalized, err := NormalizeSensorHistoryRequest(req)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastSensorHistoryRequest = normalized
	if f.SensorHistoryErr != nil {
		return nil, f.SensorHistoryErr
	}
	return copySensorAggregates(f.SensorHistoryRows), nil
}

func (f *FakeClient) ActionLog(ctx context.Context, req ActionLogRequest) ([]ActionLogEntry, error) {
	normalized, err := NormalizeActionLogRequest(req)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastActionLogRequest = normalized
	if f.ActionLogErr != nil {
		return nil, f.ActionLogErr
	}
	return copyActionLogEntries(f.ActionLogRows), nil
}

func (f *FakeClient) TierCDecisionLog(ctx context.Context, req TierCDecisionLogRequest) ([]TierCDecisionEntry, error) {
	normalized, err := NormalizeTierCDecisionLogRequest(req)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastTierCDecisionLogRequest = normalized
	if f.TierCDecisionLogErr != nil {
		return nil, f.TierCDecisionLogErr
	}
	return copyTierCDecisionEntries(f.TierCDecisionLogRows), nil
}

func (f *FakeClient) ReasoningLog(ctx context.Context, req ReasoningLogRequest) ([]ReasoningLogEntry, error) {
	normalized, err := NormalizeReasoningLogRequest(req)
	if err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	f.mu.Lock()
	defer f.mu.Unlock()
	f.lastReasoningLogRequest = normalized
	if f.ReasoningLogErr != nil {
		return nil, f.ReasoningLogErr
	}
	return copyReasoningLogEntries(f.ReasoningLogRows), nil
}

func (f *FakeClient) LastHealthRequest() HealthRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastHealthRequest
}

func (f *FakeClient) LastSensorHistoryRequest() SensorHistoryRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastSensorHistoryRequest
}

func (f *FakeClient) LastActionLogRequest() ActionLogRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastActionLogRequest
}

func (f *FakeClient) LastTierCDecisionLogRequest() TierCDecisionLogRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastTierCDecisionLogRequest
}

func (f *FakeClient) LastReasoningLogRequest() ReasoningLogRequest {
	f.mu.Lock()
	defer f.mu.Unlock()
	return f.lastReasoningLogRequest
}

func copySensorAggregates(rows []SensorAggregate) []SensorAggregate {
	out := append([]SensorAggregate(nil), rows...)
	for i := range out {
		out[i].Metadata = copyMap(out[i].Metadata)
	}
	return out
}

func copyActionLogEntries(rows []ActionLogEntry) []ActionLogEntry {
	out := append([]ActionLogEntry(nil), rows...)
	for i := range out {
		out[i].Result = copyMap(out[i].Result)
	}
	return out
}

func copyTierCDecisionEntries(rows []TierCDecisionEntry) []TierCDecisionEntry {
	out := append([]TierCDecisionEntry(nil), rows...)
	for i := range out {
		out[i].HistoryWindow = append([]HistorySample(nil), out[i].HistoryWindow...)
		out[i].FinalActionResult = copyMap(out[i].FinalActionResult)
		out[i].LaterOutcome = copyMap(out[i].LaterOutcome)
	}
	return out
}

func copyReasoningLogEntries(rows []ReasoningLogEntry) []ReasoningLogEntry {
	return append([]ReasoningLogEntry(nil), rows...)
}

func copyMap(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = v
	}
	return out
}
