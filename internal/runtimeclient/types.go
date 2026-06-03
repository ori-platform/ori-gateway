// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package runtimeclient defines gateway-facing access to runtime-owned data.
//
// The gateway must not read ori-runtime SQLite files directly. Runtime owns
// local state semantics, schema evolution, and safety-critical persistence.
// Gateway features such as customer reports should depend on this bounded
// export contract instead of storage internals.
package runtimeclient

import (
	"context"
	"fmt"
)

const (
	// DefaultWeeklyReportBucketMS keeps seven-day report inputs compact: 7 * 24 rows per sensor.
	DefaultWeeklyReportBucketMS int64 = 3_600_000
	// MaxLimit caps every export request so product features cannot request unbounded runtime data.
	MaxLimit = 1000
)

// Client is the stable gateway dependency for runtime health and bounded data exports.
type Client interface {
	Health(ctx context.Context, req HealthRequest) (HealthSnapshot, error)
	SensorHistory(ctx context.Context, req SensorHistoryRequest) ([]SensorAggregate, error)
	ActionLog(ctx context.Context, req ActionLogRequest) ([]ActionLogEntry, error)
	TierCDecisionLog(ctx context.Context, req TierCDecisionLogRequest) ([]TierCDecisionEntry, error)
}

// HealthRequest asks runtime for a device-scoped health snapshot.
type HealthRequest struct {
	DeviceID string
}

// HealthSnapshot is a gateway-consumable subset of runtime health.
type HealthSnapshot struct {
	DeviceID          string                      `json:"device_id"`
	Status            string                      `json:"status"`
	UptimeS           float64                     `json:"uptime_s"`
	LastReadingMS     int64                       `json:"last_reading_ms"`
	GatewaySeen       bool                        `json:"gateway_seen"`
	PolicyStatus      string                      `json:"policy_status"`
	LockoutRiskLevels map[string]LockoutRiskState `json:"lockout_risk_levels,omitempty"`
}

// LockoutRiskState is advisory remote-command sender risk state from runtime health.
type LockoutRiskState struct {
	RiskLevel   string `json:"risk_level"`
	LockedOut   bool   `json:"locked_out"`
	Stale       bool   `json:"stale"`
	CheckedAtMS int64  `json:"checked_at_ms"`
}

// BoundedWindow scopes every runtime export request to one device, a time range, and a capped limit.
type BoundedWindow struct {
	DeviceID string
	SinceMS  int64
	UntilMS  int64
	Limit    int
}

// SensorHistoryRequest asks runtime for bounded sensor aggregates.
type SensorHistoryRequest struct {
	BoundedWindow
	SensorID string
	// BucketMS requests runtime-side aggregation. Use 3_600_000 for weekly reports.
	BucketMS int64
}

// SensorAggregate is an already-aggregated sensor reading returned by runtime.
type SensorAggregate struct {
	DeviceID string
	SensorID string
	Type     string
	Unit     string
	StartMS  int64
	EndMS    int64
	AvgValue float64
	MinValue float64
	MaxValue float64
	Samples  int
	Quality  float64
	Metadata map[string]any
}

// ActionLogRequest asks runtime for bounded action history.
type ActionLogRequest struct {
	BoundedWindow
}

// ActionLogEntry is a runtime action log row shaped for gateway reporting.
type ActionLogEntry struct {
	DeviceID        string
	CreatedAtMS     int64
	ActionName      string
	Tier            string
	SkillName       string
	TriggerName     string
	SensorType      string
	SafeDefaultUsed bool
	Success         bool
	Result          map[string]any
}

// TierCDecisionLogRequest asks runtime for bounded Tier C approval history.
type TierCDecisionLogRequest struct {
	BoundedWindow
}

// HistorySample is the reporting-safe history context attached to a Tier C decision.
type HistorySample struct {
	SensorID    string
	SensorType  string
	Unit        string
	TimestampMS int64
	Value       float64
	Quality     float64
}

// TierCDecisionEntry is a runtime Tier C decision row shaped for gateway reporting.
type TierCDecisionEntry struct {
	DeviceID          string
	ProposalID        string
	CreatedAtMS       int64
	SkillName         string
	TriggerName       string
	SensorID          string
	SensorType        string
	ReadingValue      float64
	HistoryWindow     []HistorySample
	ProposedAction    string
	Confidence        float64
	OperatorDecision  string
	DecisionLatencyS  float64
	SafeDefaultUsed   bool
	FinalActionResult map[string]any
	SiteType          string
	Location          string
	Timezone          string
	LaterOutcome      map[string]any
}

// NormalizeHealthRequest validates req and returns a copy.
func NormalizeHealthRequest(req HealthRequest) (HealthRequest, error) {
	if req.DeviceID == "" {
		return HealthRequest{}, fmt.Errorf("device_id must not be empty")
	}
	return req, nil
}

// NormalizeSensorHistoryRequest validates req and returns a copy with capped limit.
func NormalizeSensorHistoryRequest(req SensorHistoryRequest) (SensorHistoryRequest, error) {
	window, err := NormalizeBoundedWindow(req.BoundedWindow)
	if err != nil {
		return SensorHistoryRequest{}, err
	}
	if req.SensorID == "" {
		return SensorHistoryRequest{}, fmt.Errorf("sensor_id must not be empty")
	}
	if req.BucketMS < 0 {
		return SensorHistoryRequest{}, fmt.Errorf("bucket_ms must not be negative")
	}
	req.BoundedWindow = window
	return req, nil
}

// NormalizeActionLogRequest validates req and returns a copy with capped limit.
func NormalizeActionLogRequest(req ActionLogRequest) (ActionLogRequest, error) {
	window, err := NormalizeBoundedWindow(req.BoundedWindow)
	if err != nil {
		return ActionLogRequest{}, err
	}
	req.BoundedWindow = window
	return req, nil
}

// NormalizeTierCDecisionLogRequest validates req and returns a copy with capped limit.
func NormalizeTierCDecisionLogRequest(req TierCDecisionLogRequest) (TierCDecisionLogRequest, error) {
	window, err := NormalizeBoundedWindow(req.BoundedWindow)
	if err != nil {
		return TierCDecisionLogRequest{}, err
	}
	req.BoundedWindow = window
	return req, nil
}

// NormalizeBoundedWindow validates window and returns a copy with the limit capped.
func NormalizeBoundedWindow(window BoundedWindow) (BoundedWindow, error) {
	if window.DeviceID == "" {
		return BoundedWindow{}, fmt.Errorf("device_id must not be empty")
	}
	if window.SinceMS <= 0 {
		return BoundedWindow{}, fmt.Errorf("since_ms must be positive")
	}
	if window.UntilMS != 0 && window.UntilMS <= window.SinceMS {
		return BoundedWindow{}, fmt.Errorf("until_ms must be greater than since_ms")
	}
	if window.Limit <= 0 {
		return BoundedWindow{}, fmt.Errorf("limit must be positive")
	}
	if window.Limit > MaxLimit {
		window.Limit = MaxLimit
	}
	return window, nil
}
