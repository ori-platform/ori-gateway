// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"time"
)

// Deliverer delivers a completed weekly report artifact to one output channel.
// Delivery errors are advisory: the caller logs them and continues.
type Deliverer interface {
	Deliver(ctx context.Context, artifact WeeklyReportArtifact) error
}

// LogDeliverer records key report metadata to the structured logger.
type LogDeliverer struct {
	Logger *slog.Logger
}

// Deliver logs report metadata. It never returns an error.
func (d *LogDeliverer) Deliver(_ context.Context, artifact WeeklyReportArtifact) error {
	logger := d.Logger
	if logger == nil {
		logger = slog.Default()
	}
	logger.Info(
		"weekly report generated",
		"device_id", artifact.DeviceID,
		"provider", artifact.Provider,
		"model", artifact.Model,
		"sensor_rows", artifact.SensorRowCount,
		"actions", artifact.ActionCount,
		"tier_c_decisions", artifact.TierCDecisionCount,
		"tokens", artifact.Tokens,
		"latency_ms", artifact.LatencyMS,
		"warnings", len(artifact.Warnings),
	)
	return nil
}

// FileDeliverer writes a customer-safe JSON report to Dir.
// The filename is weekly-{site_slug}-{YYYY-MM-DD}.json where the date is
// derived from the artifact's WindowEndMS in UTC.
// Dir must be an absolute path to an existing directory; FileDeliverer never
// creates directories. Use NewFileDeliverer to validate at construction time.
type FileDeliverer struct {
	Dir string
}

// NewFileDeliverer validates dir and returns a FileDeliverer.
// Returns an error if dir is not absolute or does not exist.
func NewFileDeliverer(dir string) (*FileDeliverer, error) {
	if !filepath.IsAbs(dir) {
		return nil, fmt.Errorf("reporting: file deliverer directory %q must be an absolute path", dir)
	}
	if _, err := os.Stat(dir); err != nil {
		return nil, fmt.Errorf("reporting: file deliverer directory %q: %w", dir, err)
	}
	return &FileDeliverer{Dir: dir}, nil
}

// Deliver serialises a customer-safe payload to JSON and writes it to Dir.
// DeviceID, internal metadata, and any infrastructure identifiers are excluded.
func (d *FileDeliverer) Deliver(_ context.Context, artifact WeeklyReportArtifact) error {
	if !filepath.IsAbs(d.Dir) {
		return fmt.Errorf("reporting: file deliverer directory %q must be an absolute path", d.Dir)
	}
	date := time.UnixMilli(artifact.WindowEndMS).UTC().Format("2006-01-02")
	slug := siteSlug(artifact.SiteName)
	filename := fmt.Sprintf("weekly-%s-%s.json", slug, date)
	dest := filepath.Join(d.Dir, filename)

	data, err := json.Marshal(toFilePayload(artifact))
	if err != nil {
		return fmt.Errorf("reporting: marshal report artifact: %w", err)
	}
	if err := os.WriteFile(dest, data, 0o600); err != nil {
		return fmt.Errorf("reporting: write report file %q: %w", dest, err)
	}
	return nil
}

// weeklyReportFilePayload is the customer-safe shape written to disk.
// It deliberately excludes DeviceID, Metadata, and any field that could
// carry infrastructure identifiers, credentials, or internal routing details.
type weeklyReportFilePayload struct {
	CustomerName       string                `json:"customer_name,omitempty"`
	SiteName           string                `json:"site_name,omitempty"`
	WindowStartMS      int64                 `json:"window_start_ms"`
	WindowEndMS        int64                 `json:"window_end_ms"`
	GeneratedAtMS      int64                 `json:"generated_at_ms"`
	Text               string                `json:"text,omitempty"`
	Provider           string                `json:"provider,omitempty"`
	Model              string                `json:"model,omitempty"`
	Tokens             int                   `json:"tokens"`
	LatencyMS          int64                 `json:"latency_ms"`
	RuntimeHealth      customerHealthPayload `json:"runtime_health"`
	SensorSeriesCount  int                   `json:"sensor_series_count"`
	SensorRowCount     int                   `json:"sensor_row_count"`
	ActionCount        int                   `json:"action_count"`
	TierCDecisionCount int                   `json:"tier_c_decision_count"`
	Warnings           []string              `json:"warnings,omitempty"`
}

// customerHealthPayload is the customer-safe projection of CustomerHealthSummary.
// DeviceID is intentionally excluded.
type customerHealthPayload struct {
	Status               string                               `json:"status,omitempty"`
	LastReadingMS        int64                                `json:"last_reading_ms"`
	GatewaySeen          bool                                 `json:"gateway_seen"`
	PolicyStatus         string                               `json:"policy_status,omitempty"`
	GatewayBrokerPosture *CustomerGatewayBrokerPosture        `json:"gateway_broker_posture,omitempty"`
	StateStoreEncryption *CustomerStateStoreEncryptionPosture `json:"state_store_encryption,omitempty"`
	AlertOutbox          *CustomerAlertOutboxPosture          `json:"alert_outbox,omitempty"`
}

func toFilePayload(a WeeklyReportArtifact) weeklyReportFilePayload {
	h := customerHealthPayload{
		Status:               a.RuntimeHealth.Status,
		LastReadingMS:        a.RuntimeHealth.LastReadingMS,
		GatewaySeen:          a.RuntimeHealth.GatewaySeen,
		PolicyStatus:         a.RuntimeHealth.PolicyStatus,
		GatewayBrokerPosture: a.RuntimeHealth.GatewayBrokerPosture,
		StateStoreEncryption: a.RuntimeHealth.StateStoreEncryption,
		AlertOutbox:          a.RuntimeHealth.AlertOutbox,
	}
	warnings := a.Warnings
	if warnings == nil {
		warnings = []string{}
	}
	return weeklyReportFilePayload{
		CustomerName:       a.CustomerName,
		SiteName:           a.SiteName,
		WindowStartMS:      a.WindowStartMS,
		WindowEndMS:        a.WindowEndMS,
		GeneratedAtMS:      a.GeneratedAtMS,
		Text:               a.Text,
		Provider:           a.Provider,
		Model:              a.Model,
		Tokens:             a.Tokens,
		LatencyMS:          a.LatencyMS,
		RuntimeHealth:      h,
		SensorSeriesCount:  a.SensorSeriesCount,
		SensorRowCount:     a.SensorRowCount,
		ActionCount:        a.ActionCount,
		TierCDecisionCount: a.TierCDecisionCount,
		Warnings:           warnings,
	}
}

// siteSlug converts a site name to a safe filename component.
// Non-alphanumeric runes become underscores; consecutive underscores are collapsed.
func siteSlug(name string) string {
	if strings.TrimSpace(name) == "" {
		return "unknown"
	}
	var b strings.Builder
	for _, r := range strings.ToLower(name) {
		if (r >= 'a' && r <= 'z') || (r >= '0' && r <= '9') {
			b.WriteRune(r)
		} else {
			b.WriteByte('_')
		}
	}
	s := b.String()
	for strings.Contains(s, "__") {
		s = strings.ReplaceAll(s, "__", "_")
	}
	return strings.Trim(s, "_")
}
