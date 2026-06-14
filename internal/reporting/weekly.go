// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package reporting builds customer-facing report inputs from runtime-owned data.
package reporting

import (
	"context"
	"fmt"
	"strings"
	"time"

	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

const defaultWeeklyWindow = 7 * 24 * time.Hour

// Provider is the connected reporting boundary. Implementations may call Gemini
// or another product-reporting backend, but must not execute actions or mutate runtime state.
type Provider interface {
	GenerateWeeklyReport(ctx context.Context, input WeeklyReportInput) (ProviderReport, error)
}

// ProviderReport is the provider's customer-facing result plus safe metadata.
type ProviderReport struct {
	Text      string
	Provider  string
	Model     string
	Tokens    int
	LatencyMS int64
	Metadata  map[string]any
}

// WeeklyReportGenerator builds report input from runtime exports and delegates language generation.
type WeeklyReportGenerator struct {
	runtime  runtimeclient.Client
	provider Provider
	now      func() time.Time
}

// WeeklyReportOption customizes WeeklyReportGenerator.
type WeeklyReportOption func(*WeeklyReportGenerator)

// WithNow overrides the generator clock for tests.
func WithNow(fn func() time.Time) WeeklyReportOption {
	return func(g *WeeklyReportGenerator) {
		if fn != nil {
			g.now = fn
		}
	}
}

// NewWeeklyReportGenerator constructs a weekly report generator.
func NewWeeklyReportGenerator(runtime runtimeclient.Client, provider Provider, opts ...WeeklyReportOption) (*WeeklyReportGenerator, error) {
	if runtime == nil {
		return nil, fmt.Errorf("reporting: runtime client must not be nil")
	}
	if provider == nil {
		return nil, fmt.Errorf("reporting: provider must not be nil")
	}
	g := &WeeklyReportGenerator{
		runtime:  runtime,
		provider: provider,
		now:      time.Now,
	}
	for _, opt := range opts {
		opt(g)
	}
	return g, nil
}

// WeeklyReportRequest identifies a customer-visible weekly report scope.
type WeeklyReportRequest struct {
	DeviceID     string
	CustomerName string
	SiteName     string
	Timezone     string
	SensorIDs    []string
	SinceMS      int64
	UntilMS      int64
}

// WeeklyReportInput is the complete provider input built from runtime exports.
type WeeklyReportInput struct {
	DeviceID       string
	CustomerName   string
	SiteName       string
	Timezone       string
	WindowStartMS  int64
	WindowEndMS    int64
	Health         CustomerHealthSummary
	SensorSeries   []SensorSeries
	Actions        []runtimeclient.ActionLogEntry
	TierCDecisions []runtimeclient.TierCDecisionEntry
	Warnings       []string
}

// CustomerHealthSummary is the reporting-safe projection of runtime health.
// It deliberately excludes operator identities and remote-command lockout details.
type CustomerHealthSummary struct {
	DeviceID             string
	Status               string
	LastReadingMS        int64
	GatewaySeen          bool
	PolicyStatus         string
	GatewayBrokerPosture *CustomerGatewayBrokerPosture
	StateStoreEncryption *CustomerStateStoreEncryptionPosture
	AlertOutbox          *CustomerAlertOutboxPosture
}

// CustomerGatewayBrokerPosture is the non-secret runtime MQTT broker posture for reports.
type CustomerGatewayBrokerPosture struct {
	Available             bool
	GatewayEnabled        bool
	DeploymentCheck       string
	AnonymousAccess       string
	ACLPolicy             string
	RequireCredentials    bool
	CredentialsConfigured bool
	RequiresACLHardening  bool
}

// CustomerStateStoreEncryptionPosture is the non-secret state-store encryption posture for reports.
type CustomerStateStoreEncryptionPosture struct {
	Available            bool
	Mode                 string
	Satisfied            bool
	MarkerConfigured     bool
	PathPrefixConfigured bool
}

// CustomerAlertOutboxPosture is the non-secret notification backlog posture for reports.
type CustomerAlertOutboxPosture struct {
	Available                     bool
	BacklogCount                  int
	OldestQueuedAgeMS             int64
	RetryIntervalMinutes          float64
	MaxNonTierDAttempts           int
	TierDCriticalWarningThreshold int
	BatchSize                     int
}

// SensorSeries groups bounded aggregates by sensor for provider prompt construction.
type SensorSeries struct {
	SensorID string
	Rows     []runtimeclient.SensorAggregate
}

// WeeklyReportArtifact is returned to callers for delivery or persistence by product/cloud code.
type WeeklyReportArtifact struct {
	DeviceID           string
	CustomerName       string
	SiteName           string
	WindowStartMS      int64
	WindowEndMS        int64
	GeneratedAtMS      int64
	Text               string
	Provider           string
	Model              string
	Tokens             int
	LatencyMS          int64
	RuntimeHealth      CustomerHealthSummary
	SensorSeriesCount  int
	SensorRowCount     int
	ActionCount        int
	TierCDecisionCount int
	Warnings           []string
	Metadata           map[string]any
}

// Generate builds input from runtime exports, calls the provider, and returns a report artifact.
func (g *WeeklyReportGenerator) Generate(ctx context.Context, req WeeklyReportRequest) (WeeklyReportArtifact, error) {
	now := g.now()
	normalized, err := g.normalizeRequest(req, now)
	if err != nil {
		return WeeklyReportArtifact{}, err
	}

	window := runtimeclient.BoundedWindow{
		DeviceID: normalized.DeviceID,
		SinceMS:  normalized.SinceMS,
		UntilMS:  normalized.UntilMS,
		Limit:    runtimeclient.MaxLimit,
	}
	health, err := g.runtime.Health(ctx, runtimeclient.HealthRequest{DeviceID: normalized.DeviceID})
	if err != nil {
		return WeeklyReportArtifact{}, fmt.Errorf("reporting: runtime health export failed: %w", err)
	}
	healthSummary := summarizeHealth(health)
	warnings := healthPostureWarnings(healthSummary)

	sensorSeries := make([]SensorSeries, 0, len(normalized.SensorIDs))
	sensorRowCount := 0
	for _, sensorID := range normalized.SensorIDs {
		rows, err := g.runtime.SensorHistory(ctx, runtimeclient.SensorHistoryRequest{
			BoundedWindow: window,
			SensorID:      sensorID,
			BucketMS:      runtimeclient.DefaultWeeklyReportBucketMS,
		})
		if err != nil {
			return WeeklyReportArtifact{}, fmt.Errorf("reporting: runtime sensor_history export failed for %s: %w", sensorID, err)
		}
		sensorRowCount += len(rows)
		sensorSeries = append(sensorSeries, SensorSeries{SensorID: sensorID, Rows: rows})
	}

	actions, err := g.runtime.ActionLog(ctx, runtimeclient.ActionLogRequest{BoundedWindow: window})
	if err != nil {
		return WeeklyReportArtifact{}, fmt.Errorf("reporting: runtime action_log export failed: %w", err)
	}
	if len(actions) == runtimeclient.MaxLimit {
		warnings = append(warnings, "action_log may be truncated at runtime export limit")
	}
	decisions, err := g.runtime.TierCDecisionLog(ctx, runtimeclient.TierCDecisionLogRequest{BoundedWindow: window})
	if err != nil {
		return WeeklyReportArtifact{}, fmt.Errorf("reporting: runtime tier_c_decision_log export failed: %w", err)
	}
	if len(decisions) == runtimeclient.MaxLimit {
		warnings = append(warnings, "tier_c_decision_log may be truncated at runtime export limit")
	}

	input := WeeklyReportInput{
		DeviceID:       normalized.DeviceID,
		CustomerName:   normalized.CustomerName,
		SiteName:       normalized.SiteName,
		Timezone:       normalized.Timezone,
		WindowStartMS:  normalized.SinceMS,
		WindowEndMS:    normalized.UntilMS,
		Health:         healthSummary,
		SensorSeries:   sensorSeries,
		Actions:        actions,
		TierCDecisions: decisions,
		Warnings:       append([]string(nil), warnings...),
	}
	report, err := g.provider.GenerateWeeklyReport(ctx, input)
	if err != nil {
		return WeeklyReportArtifact{}, fmt.Errorf("reporting: provider weekly report failed: %w", err)
	}

	generatedAtMS := now.UnixMilli()
	return WeeklyReportArtifact{
		DeviceID:           normalized.DeviceID,
		CustomerName:       normalized.CustomerName,
		SiteName:           normalized.SiteName,
		WindowStartMS:      normalized.SinceMS,
		WindowEndMS:        normalized.UntilMS,
		GeneratedAtMS:      generatedAtMS,
		Text:               report.Text,
		Provider:           report.Provider,
		Model:              report.Model,
		Tokens:             report.Tokens,
		LatencyMS:          report.LatencyMS,
		RuntimeHealth:      healthSummary,
		SensorSeriesCount:  len(sensorSeries),
		SensorRowCount:     sensorRowCount,
		ActionCount:        len(actions),
		TierCDecisionCount: len(decisions),
		Warnings:           append([]string(nil), warnings...),
		Metadata:           copyMetadata(report.Metadata),
	}, nil
}

func (g *WeeklyReportGenerator) normalizeRequest(req WeeklyReportRequest, now time.Time) (WeeklyReportRequest, error) {
	req.DeviceID = strings.TrimSpace(req.DeviceID)
	req.CustomerName = strings.TrimSpace(req.CustomerName)
	req.SiteName = strings.TrimSpace(req.SiteName)
	req.Timezone = strings.TrimSpace(req.Timezone)
	if req.DeviceID == "" {
		return WeeklyReportRequest{}, fmt.Errorf("reporting: device_id must not be empty")
	}
	if strings.ContainsAny(req.DeviceID, "/+#") {
		return WeeklyReportRequest{}, fmt.Errorf("reporting: device_id must not contain MQTT topic separators or wildcards")
	}
	if len(req.SensorIDs) == 0 {
		return WeeklyReportRequest{}, fmt.Errorf("reporting: at least one sensor_id is required")
	}
	if req.Timezone != "" {
		if _, err := time.LoadLocation(req.Timezone); err != nil {
			return WeeklyReportRequest{}, fmt.Errorf("reporting: timezone %q is invalid: %w", req.Timezone, err)
		}
	}

	seen := make(map[string]bool, len(req.SensorIDs))
	sensors := make([]string, 0, len(req.SensorIDs))
	for _, raw := range req.SensorIDs {
		sensorID := strings.TrimSpace(raw)
		if sensorID == "" {
			return WeeklyReportRequest{}, fmt.Errorf("reporting: sensor_id must not be empty")
		}
		if seen[sensorID] {
			continue
		}
		seen[sensorID] = true
		sensors = append(sensors, sensorID)
	}
	if len(sensors) == 0 {
		return WeeklyReportRequest{}, fmt.Errorf("reporting: at least one sensor_id is required")
	}
	req.SensorIDs = sensors

	if req.UntilMS == 0 {
		req.UntilMS = now.UnixMilli()
	}
	if req.SinceMS == 0 {
		req.SinceMS = req.UntilMS - int64(defaultWeeklyWindow/time.Millisecond)
	}
	if req.SinceMS <= 0 {
		return WeeklyReportRequest{}, fmt.Errorf("reporting: since_ms must be positive")
	}
	if req.UntilMS <= req.SinceMS {
		return WeeklyReportRequest{}, fmt.Errorf("reporting: until_ms must be greater than since_ms")
	}
	return req, nil
}

func summarizeHealth(health runtimeclient.HealthSnapshot) CustomerHealthSummary {
	return CustomerHealthSummary{
		DeviceID:             health.DeviceID,
		Status:               health.Status,
		LastReadingMS:        health.LastReadingMS,
		GatewaySeen:          health.GatewaySeen,
		PolicyStatus:         health.PolicyStatus,
		GatewayBrokerPosture: summarizeGatewayBrokerPosture(health.GatewayBrokerPosture),
		StateStoreEncryption: summarizeStateStoreEncryption(health.StateStoreEncryption),
		AlertOutbox:          summarizeAlertOutbox(health.AlertOutbox),
	}
}

func summarizeGatewayBrokerPosture(posture *runtimeclient.GatewayBrokerPosture) *CustomerGatewayBrokerPosture {
	if posture == nil {
		return nil
	}
	return &CustomerGatewayBrokerPosture{
		Available:             posture.Available,
		GatewayEnabled:        posture.GatewayEnabled,
		DeploymentCheck:       brokerDeploymentCheck(posture.DeploymentCheck),
		AnonymousAccess:       brokerAnonymousAccess(posture.AnonymousAccess),
		ACLPolicy:             brokerACLPolicy(posture.ACLPolicy),
		RequireCredentials:    posture.RequireCredentials,
		CredentialsConfigured: posture.CredentialsConfigured,
		RequiresACLHardening:  posture.RequiresACLHardening,
	}
}

func summarizeStateStoreEncryption(posture *runtimeclient.StateStoreEncryptionPosture) *CustomerStateStoreEncryptionPosture {
	if posture == nil {
		return nil
	}
	return &CustomerStateStoreEncryptionPosture{
		Available:            posture.Available,
		Mode:                 stateStoreEncryptionMode(posture.Mode),
		Satisfied:            posture.Satisfied,
		MarkerConfigured:     posture.MarkerConfigured,
		PathPrefixConfigured: posture.PathPrefixConfigured,
	}
}

func summarizeAlertOutbox(posture *runtimeclient.AlertOutboxPosture) *CustomerAlertOutboxPosture {
	if posture == nil {
		return nil
	}
	return &CustomerAlertOutboxPosture{
		Available:                     posture.Available,
		BacklogCount:                  posture.BacklogCount,
		OldestQueuedAgeMS:             posture.OldestQueuedAgeMS,
		RetryIntervalMinutes:          posture.RetryIntervalMinutes,
		MaxNonTierDAttempts:           posture.MaxNonTierDAttempts,
		TierDCriticalWarningThreshold: posture.TierDCriticalWarningThreshold,
		BatchSize:                     posture.BatchSize,
	}
}

func healthPostureWarnings(health CustomerHealthSummary) []string {
	warnings := make([]string, 0, 4)
	if health.GatewayBrokerPosture != nil {
		brokerPosture := health.GatewayBrokerPosture
		if brokerPosture.GatewayEnabled && brokerPosture.RequiresACLHardening {
			warnings = append(warnings, "runtime gateway broker posture requires ACL hardening")
		}
		if brokerPosture.GatewayEnabled && brokerPosture.RequireCredentials && !brokerPosture.CredentialsConfigured {
			warnings = append(warnings, "runtime gateway broker credentials are not configured")
		}
	}
	if health.StateStoreEncryption != nil && health.StateStoreEncryption.Available && health.StateStoreEncryption.Mode == "filesystem_required" && !health.StateStoreEncryption.Satisfied {
		warnings = append(warnings, "runtime state-store encryption posture is not satisfied")
	}
	if alertOutboxWarningRequired(health.AlertOutbox) {
		warnings = append(warnings, "runtime alert outbox has queued notifications")
	}
	return warnings
}

func brokerDeploymentCheck(value string) string {
	switch value {
	case "warning", "required":
		return value
	default:
		return "unknown"
	}
}

func brokerAnonymousAccess(value string) string {
	switch value {
	case "disabled":
		return value
	default:
		return "unknown"
	}
}

func brokerACLPolicy(value string) string {
	switch value {
	case "per_device_required":
		return value
	default:
		return "unknown"
	}
}

func stateStoreEncryptionMode(value string) string {
	switch value {
	case "none", "filesystem_required":
		return value
	default:
		return "unknown"
	}
}

func alertOutboxWarningRequired(posture *CustomerAlertOutboxPosture) bool {
	if posture == nil || !posture.Available || posture.BacklogCount <= 0 {
		return false
	}
	if posture.TierDCriticalWarningThreshold > 0 && posture.BacklogCount >= posture.TierDCriticalWarningThreshold {
		return true
	}
	retryIntervalMS := int64(posture.RetryIntervalMinutes * float64(time.Minute/time.Millisecond))
	return retryIntervalMS > 0 && posture.OldestQueuedAgeMS >= retryIntervalMS
}

func copyMetadata(in map[string]any) map[string]any {
	if in == nil {
		return nil
	}
	out := make(map[string]any, len(in))
	for k, v := range in {
		out[k] = deepCopyAny(v)
	}
	return out
}

func deepCopyAny(value any) any {
	switch v := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(v))
		for key, item := range v {
			out[key] = deepCopyAny(item)
		}
		return out
	case []any:
		out := make([]any, len(v))
		for i, item := range v {
			out[i] = deepCopyAny(item)
		}
		return out
	default:
		return v
	}
}
