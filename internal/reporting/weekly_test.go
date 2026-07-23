// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

type fakeProvider struct {
	calls  int
	input  WeeklyReportInput
	report ProviderReport
	err    error
}

func (f *fakeProvider) GenerateWeeklyReport(_ context.Context, input WeeklyReportInput) (ProviderReport, error) {
	f.calls++
	f.input = input
	if f.err != nil {
		return ProviderReport{}, f.err
	}
	return f.report, nil
}

func fixedNow() time.Time {
	return time.UnixMilli(1_800_000_000_000)
}

func newGenerator(t *testing.T, runtime *runtimeclient.FakeClient, provider *fakeProvider) *WeeklyReportGenerator {
	t.Helper()
	g, err := NewWeeklyReportGenerator(runtime, provider, WithNow(fixedNow))
	if err != nil {
		t.Fatal(err)
	}
	return g
}

func TestWeeklyReportBuildsInputFromRuntimeExports(t *testing.T) {
	runtime := &runtimeclient.FakeClient{
		HealthSnapshot: runtimeclient.HealthSnapshot{
			DeviceID:      "edge-1",
			Status:        "healthy",
			GatewaySeen:   true,
			LastReadingMS: 1_799_999_000_000,
			PolicyStatus:  "active",
			GatewayBrokerPosture: &runtimeclient.GatewayBrokerPosture{
				Available:             true,
				GatewayEnabled:        true,
				DeploymentCheck:       "required",
				AnonymousAccess:       "disabled",
				ACLPolicy:             "per_device_required",
				RequireCredentials:    true,
				CredentialsConfigured: true,
				RequiresACLHardening:  true,
			},
			StateStoreEncryption: &runtimeclient.StateStoreEncryptionPosture{
				Available:            true,
				Mode:                 "filesystem_required",
				Satisfied:            false,
				MarkerConfigured:     true,
				PathPrefixConfigured: false,
			},
			AlertOutbox: &runtimeclient.AlertOutboxPosture{
				Available:                     true,
				BacklogCount:                  2,
				OldestQueuedAgeMS:             900_000,
				RetryIntervalMinutes:          30,
				TierDCriticalWarningThreshold: 2,
				BatchSize:                     10,
			},
			LockoutRiskLevels: map[string]runtimeclient.LockoutRiskState{"sms:+2348012345678": {RiskLevel: "critical"}},
		},
		SensorHistoryRows: []runtimeclient.SensorAggregate{{
			DeviceID: "edge-1",
			SensorID: "current-main",
			AvgValue: 4.2,
			Samples:  12,
		}},
		ActionLogRows: []runtimeclient.ActionLogEntry{{
			DeviceID:              "edge-1",
			CreatedAtMS:           1_799_900_000_000,
			ActionName:            "alert_whatsapp",
			Tier:                  "A",
			Success:               true,
			AttestationStatus:     "signed",
			AttestationSeq:        int64Pointer(42),
			InputAttestationGrade: "attested",
			InputPosture:          "hardware_key",
		}},
		TierCDecisionLogRows: []runtimeclient.TierCDecisionEntry{{
			DeviceID:         "edge-1",
			ProposalID:       "p-123",
			OperatorDecision: "approved",
		}},
	}
	provider := &fakeProvider{report: ProviderReport{
		Text:      "Your weekly energy summary is ready.",
		Provider:  "gemini",
		Model:     "gemini-2.5-pro",
		Tokens:    320,
		LatencyMS: 1500,
		Metadata:  map[string]any{"prompt_version": "weekly-v1", "debug": map[string]any{"prompt_tokens": 320}},
	}}
	generator := newGenerator(t, runtime, provider)

	artifact, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:     "edge-1",
		CustomerName: "Site A Ltd",
		SiteName:     "Site A",
		Timezone:     "Africa/Lagos",
		SensorIDs:    []string{" current-main ", "current-main"},
	})
	if err != nil {
		t.Fatal(err)
	}

	wantUntil := fixedNow().UnixMilli()
	wantSince := wantUntil - int64(defaultWeeklyWindow/time.Millisecond)
	if artifact.WindowStartMS != wantSince || artifact.WindowEndMS != wantUntil {
		t.Fatalf("unexpected window: %#v", artifact)
	}
	if artifact.SensorSeriesCount != 1 || artifact.SensorRowCount != 1 || artifact.ActionCount != 1 || artifact.TierCDecisionCount != 1 {
		t.Fatalf("unexpected artifact counts: %#v", artifact)
	}
	if artifact.Text != "Your weekly energy summary is ready." || artifact.Provider != "gemini" || artifact.Model != "gemini-2.5-pro" {
		t.Fatalf("provider output not reflected: %#v", artifact)
	}
	if artifact.Metadata["prompt_version"] != "weekly-v1" {
		t.Fatalf("metadata not copied: %#v", artifact.Metadata)
	}
	artifact.Metadata["prompt_version"] = "tampered"
	if provider.report.Metadata["prompt_version"] != "weekly-v1" {
		t.Fatal("artifact metadata aliases provider metadata")
	}
	artifact.Metadata["debug"].(map[string]any)["prompt_tokens"] = 0
	if provider.report.Metadata["debug"].(map[string]any)["prompt_tokens"] != 320 {
		t.Fatal("artifact nested metadata aliases provider metadata")
	}
	artifactJSON, err := json.Marshal(artifact)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(artifactJSON), "+2348012345678") || strings.Contains(string(artifactJSON), "critical") {
		t.Fatalf("artifact leaked remote-command lockout details: %s", artifactJSON)
	}

	if runtime.LastHealthRequest().DeviceID != "edge-1" {
		t.Fatalf("health request not sent: %#v", runtime.LastHealthRequest())
	}
	historyReq := runtime.LastSensorHistoryRequest()
	if historyReq.DeviceID != "edge-1" || historyReq.SensorID != "current-main" || historyReq.BucketMS != runtimeclient.DefaultWeeklyReportBucketMS {
		t.Fatalf("unexpected sensor history request: %#v", historyReq)
	}
	if historyReq.SinceMS != wantSince || historyReq.UntilMS != wantUntil || historyReq.Limit != runtimeclient.MaxLimit {
		t.Fatalf("unexpected bounded history request: %#v", historyReq)
	}
	if runtime.LastActionLogRequest().SinceMS != wantSince || runtime.LastTierCDecisionLogRequest().UntilMS != wantUntil {
		t.Fatalf("bounded log requests not sent: action=%#v tierc=%#v", runtime.LastActionLogRequest(), runtime.LastTierCDecisionLogRequest())
	}

	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
	input := provider.input
	if input.CustomerName != "Site A Ltd" || input.SiteName != "Site A" || input.Timezone != "Africa/Lagos" {
		t.Fatalf("unexpected provider input identity: %#v", input)
	}
	if len(input.SensorSeries) != 1 || input.SensorSeries[0].SensorID != "current-main" || input.SensorSeries[0].Rows[0].AvgValue != 4.2 {
		t.Fatalf("sensor exports not transformed: %#v", input.SensorSeries)
	}
	if len(input.Actions) != 1 || len(input.TierCDecisions) != 1 || input.Health.Status != "healthy" {
		t.Fatalf("runtime exports not attached: %#v", input)
	}
	action := input.Actions[0]
	if action.AttestationStatus != "signed" || action.AttestationSeq == nil || *action.AttestationSeq != 42 {
		t.Fatalf("action attestation fields not preserved for reporting: %#v", action)
	}
	if action.InputAttestationGrade != "attested" || action.InputPosture != "hardware_key" {
		t.Fatalf("action input evidence fields not preserved for reporting: %#v", action)
	}
	if input.Health.LastReadingMS != 1_799_999_000_000 || input.Health.PolicyStatus != "active" {
		t.Fatalf("safe health summary not attached: %#v", input.Health)
	}
	if input.Health.GatewayBrokerPosture == nil || !input.Health.GatewayBrokerPosture.RequiresACLHardening {
		t.Fatalf("broker posture not attached: %#v", input.Health.GatewayBrokerPosture)
	}
	if input.Health.StateStoreEncryption == nil || input.Health.StateStoreEncryption.Satisfied {
		t.Fatalf("state encryption posture not attached: %#v", input.Health.StateStoreEncryption)
	}
	if input.Health.AlertOutbox == nil || input.Health.AlertOutbox.BacklogCount != 2 {
		t.Fatalf("alert outbox posture not attached: %#v", input.Health.AlertOutbox)
	}
	if len(input.Warnings) != 3 {
		t.Fatalf("posture warnings not attached: %#v", input.Warnings)
	}
	inputJSON, err := json.Marshal(input)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(inputJSON), "+2348012345678") || strings.Contains(string(inputJSON), "critical") {
		t.Fatalf("provider input leaked remote-command lockout details: %s", inputJSON)
	}
	if !strings.Contains(string(inputJSON), "RequiresACLHardening") || !strings.Contains(string(inputJSON), "BacklogCount") {
		t.Fatalf("provider input omitted safe posture fields: %s", inputJSON)
	}
}

func int64Pointer(value int64) *int64 {
	return &value
}

func TestWeeklyReportCallsReportingProvider(t *testing.T) {
	runtime := &runtimeclient.FakeClient{HealthSnapshot: runtimeclient.HealthSnapshot{DeviceID: "edge-1"}}
	provider := &fakeProvider{report: ProviderReport{Text: "report text", Provider: "fake", Model: "test-model"}}
	generator := newGenerator(t, runtime, provider)

	artifact, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
		SinceMS:   1,
		UntilMS:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
	if artifact.Text != "report text" || artifact.Provider != "fake" || artifact.Model != "test-model" {
		t.Fatalf("unexpected artifact: %#v", artifact)
	}
	if artifact.WindowStartMS != 1 || artifact.WindowEndMS != 2 {
		t.Fatalf("explicit window was not preserved: %#v", artifact)
	}
}

func TestWeeklyReportProviderFailureReturnsError(t *testing.T) {
	wantErr := errors.New("provider unavailable")
	runtime := &runtimeclient.FakeClient{HealthSnapshot: runtimeclient.HealthSnapshot{DeviceID: "edge-1"}}
	provider := &fakeProvider{err: wantErr}
	generator := newGenerator(t, runtime, provider)

	_, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
		SinceMS:   1,
		UntilMS:   2,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want wrapped provider error", err)
	}
	if provider.calls != 1 {
		t.Fatalf("provider calls = %d, want 1", provider.calls)
	}
}

func TestWeeklyReportRuntimeFailureReturnsError(t *testing.T) {
	wantErr := errors.New("runtime unavailable")
	runtime := &runtimeclient.FakeClient{ActionLogErr: wantErr}
	provider := &fakeProvider{report: ProviderReport{Text: "should not be called"}}
	generator := newGenerator(t, runtime, provider)

	_, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
		SinceMS:   1,
		UntilMS:   2,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want runtime error", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider should not be called after runtime failure, calls=%d", provider.calls)
	}
}

func TestWeeklyReportHealthFailureReturnsError(t *testing.T) {
	wantErr := errors.New("device offline")
	runtime := &runtimeclient.FakeClient{HealthErr: wantErr}
	provider := &fakeProvider{report: ProviderReport{Text: "should not be called"}}
	generator := newGenerator(t, runtime, provider)

	_, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
		SinceMS:   1,
		UntilMS:   2,
	})
	if !errors.Is(err, wantErr) {
		t.Fatalf("got %v, want health error", err)
	}
	if provider.calls != 0 {
		t.Fatalf("provider should not be called after health failure, calls=%d", provider.calls)
	}
}

func TestWeeklyReportSurfacesHealthPostureWarnings(t *testing.T) {
	runtime := &runtimeclient.FakeClient{
		HealthSnapshot: runtimeclient.HealthSnapshot{
			DeviceID: "edge-1",
			GatewayBrokerPosture: &runtimeclient.GatewayBrokerPosture{
				GatewayEnabled:        true,
				RequireCredentials:    true,
				CredentialsConfigured: false,
				RequiresACLHardening:  true,
			},
			StateStoreEncryption: &runtimeclient.StateStoreEncryptionPosture{Available: true, Mode: "filesystem_required", Satisfied: false},
			AlertOutbox: &runtimeclient.AlertOutboxPosture{
				Available:                     true,
				BacklogCount:                  1,
				TierDCriticalWarningThreshold: 1,
			},
		},
	}
	provider := &fakeProvider{report: ProviderReport{Text: "report"}}
	generator := newGenerator(t, runtime, provider)

	artifact, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
		SinceMS:   1,
		UntilMS:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.Warnings) != 4 {
		t.Fatalf("expected posture warnings, got %#v", artifact.Warnings)
	}
	if strings.Contains(strings.Join(artifact.Warnings, " "), "+234") {
		t.Fatalf("warnings leaked sender details: %#v", artifact.Warnings)
	}
}

func TestWeeklyReportSurfacesTruncationWarnings(t *testing.T) {
	actions := make([]runtimeclient.ActionLogEntry, runtimeclient.MaxLimit)
	decisions := make([]runtimeclient.TierCDecisionEntry, runtimeclient.MaxLimit)
	runtime := &runtimeclient.FakeClient{
		HealthSnapshot:       runtimeclient.HealthSnapshot{DeviceID: "edge-1"},
		ActionLogRows:        actions,
		TierCDecisionLogRows: decisions,
	}
	provider := &fakeProvider{report: ProviderReport{Text: "report"}}
	generator := newGenerator(t, runtime, provider)

	artifact, err := generator.Generate(context.Background(), WeeklyReportRequest{
		DeviceID:  "edge-1",
		SensorIDs: []string{"current-main"},
		SinceMS:   1,
		UntilMS:   2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(artifact.Warnings) != 2 || len(provider.input.Warnings) != 2 {
		t.Fatalf("expected truncation warnings in artifact and provider input, got artifact=%#v input=%#v", artifact.Warnings, provider.input.Warnings)
	}
}

func TestWeeklyReportRequestValidation(t *testing.T) {
	runtime := &runtimeclient.FakeClient{}
	provider := &fakeProvider{}
	generator := newGenerator(t, runtime, provider)

	cases := []struct {
		name string
		req  WeeklyReportRequest
		want string
	}{
		{name: "missing device", req: WeeklyReportRequest{SensorIDs: []string{"s1"}}, want: "device_id"},
		{name: "invalid device topic", req: WeeklyReportRequest{DeviceID: "edge/1", SensorIDs: []string{"s1"}}, want: "device_id"},
		{name: "empty sensors", req: WeeklyReportRequest{DeviceID: "edge-1"}, want: "sensor_id"},
		{name: "blank sensor", req: WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: []string{" "}}, want: "sensor_id"},
		{name: "invalid timezone", req: WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: []string{"s1"}, Timezone: "No/SuchZone"}, want: "timezone"},
		{name: "invalid window", req: WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: []string{"s1"}, SinceMS: 10, UntilMS: 9}, want: "until_ms"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			_, err := generator.Generate(context.Background(), tc.req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestNewWeeklyReportGeneratorValidation(t *testing.T) {
	provider := &fakeProvider{}
	if _, err := NewWeeklyReportGenerator(nil, provider); err == nil {
		t.Fatal("expected nil runtime client to fail")
	}
	if _, err := NewWeeklyReportGenerator(&runtimeclient.FakeClient{}, nil); err == nil {
		t.Fatal("expected nil provider to fail")
	}
}

func TestWeeklyReportSanitizesReportFacingHealthPostureEnums(t *testing.T) {
	health := summarizeHealth(runtimeclient.HealthSnapshot{
		GatewayBrokerPosture: &runtimeclient.GatewayBrokerPosture{
			DeploymentCheck: "mqtt://broker.internal:1883",
			AnonymousAccess: "/devices/+2348012345678/acl",
			ACLPolicy:       "../../etc/mosquitto/passwd",
		},
		StateStoreEncryption: &runtimeclient.StateStoreEncryptionPosture{Mode: "/mnt/customer-secrets"},
	})

	if health.GatewayBrokerPosture == nil {
		t.Fatal("broker posture missing")
	}
	if health.GatewayBrokerPosture.DeploymentCheck != "unknown" || health.GatewayBrokerPosture.AnonymousAccess != "unknown" || health.GatewayBrokerPosture.ACLPolicy != "unknown" {
		t.Fatalf("unsafe broker posture strings were not sanitized: %#v", health.GatewayBrokerPosture)
	}
	if health.StateStoreEncryption == nil || health.StateStoreEncryption.Mode != "unknown" {
		t.Fatalf("unsafe encryption mode was not sanitized: %#v", health.StateStoreEncryption)
	}
}

func TestWeeklyReportCredentialWarningRequiresCredentialPolicy(t *testing.T) {
	warnings := healthPostureWarnings(CustomerHealthSummary{
		GatewayBrokerPosture: &CustomerGatewayBrokerPosture{
			GatewayEnabled:        true,
			RequireCredentials:    false,
			CredentialsConfigured: false,
		},
	})
	if len(warnings) != 0 {
		t.Fatalf("unexpected warning without credential requirement: %#v", warnings)
	}

	warnings = healthPostureWarnings(CustomerHealthSummary{
		GatewayBrokerPosture: &CustomerGatewayBrokerPosture{
			GatewayEnabled:        true,
			RequireCredentials:    true,
			CredentialsConfigured: false,
		},
	})
	if len(warnings) != 1 || !strings.Contains(warnings[0], "credentials") {
		t.Fatalf("expected credential warning, got %#v", warnings)
	}
}

func TestWeeklyReportStateEncryptionWarningRequiresFilesystemMode(t *testing.T) {
	warnings := healthPostureWarnings(CustomerHealthSummary{
		StateStoreEncryption: &CustomerStateStoreEncryptionPosture{Available: true, Mode: "none", Satisfied: false},
	})
	if len(warnings) != 0 {
		t.Fatalf("unexpected warning for mode none: %#v", warnings)
	}

	warnings = healthPostureWarnings(CustomerHealthSummary{
		StateStoreEncryption: &CustomerStateStoreEncryptionPosture{Available: true, Mode: "filesystem_required", Satisfied: false},
	})
	if len(warnings) != 1 || !strings.Contains(warnings[0], "encryption") {
		t.Fatalf("expected encryption warning, got %#v", warnings)
	}
}

func TestWeeklyReportAlertOutboxWarningRequiresThresholdOrAge(t *testing.T) {
	warnings := healthPostureWarnings(CustomerHealthSummary{
		AlertOutbox: &CustomerAlertOutboxPosture{
			Available:                     true,
			BacklogCount:                  1,
			TierDCriticalWarningThreshold: 3,
			RetryIntervalMinutes:          30,
			OldestQueuedAgeMS:             int64(5 * time.Minute / time.Millisecond),
		},
	})
	if len(warnings) != 0 {
		t.Fatalf("unexpected transient outbox warning: %#v", warnings)
	}

	warnings = healthPostureWarnings(CustomerHealthSummary{
		AlertOutbox: &CustomerAlertOutboxPosture{
			Available:                     true,
			BacklogCount:                  3,
			TierDCriticalWarningThreshold: 3,
			RetryIntervalMinutes:          30,
		},
	})
	if len(warnings) != 1 || !strings.Contains(warnings[0], "outbox") {
		t.Fatalf("expected threshold outbox warning, got %#v", warnings)
	}

	warnings = healthPostureWarnings(CustomerHealthSummary{
		AlertOutbox: &CustomerAlertOutboxPosture{
			Available:            true,
			BacklogCount:         1,
			RetryIntervalMinutes: 30,
			OldestQueuedAgeMS:    int64(30 * time.Minute / time.Millisecond),
		},
	})
	if len(warnings) != 1 || !strings.Contains(warnings[0], "outbox") {
		t.Fatalf("expected aged outbox warning, got %#v", warnings)
	}
}
