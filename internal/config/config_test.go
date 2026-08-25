// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func writeConfig(t *testing.T, content string) string {
	t.Helper()
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.yaml")
	if err := os.WriteFile(path, []byte(content), 0o600); err != nil {
		t.Fatal(err)
	}
	return path
}

func TestLoadValidConfig(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  heartbeat_interval_s: 30
provider:
  name: echo
  timeout_ms: 10000
sim:
  enabled: false
fleet:
  enabled: false
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.BrokerURL != "tcp://localhost:1883" {
		t.Fatalf("unexpected broker_url: %q", cfg.Gateway.BrokerURL)
	}
	if cfg.Provider.Name != ProviderEcho {
		t.Fatalf("unexpected provider: %q", cfg.Provider.Name)
	}
}

func TestLoadMissingBrokerURL(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: ""
provider:
  name: echo
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for empty broker_url")
	}
	if !strings.Contains(err.Error(), "broker_url") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadUnknownProvider(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: bogus
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for unknown provider")
	}
	if !strings.Contains(err.Error(), "unknown") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEvidenceConfigRequiresSeparatedAbsoluteQueuesAndCredentialEnvNames(t *testing.T) {
	base := `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
evidence:
  enabled: true
  queue_directory: %q
  return_queue_directory: %q
  endpoint_env: ORI_EVIDENCE_ENDPOINT
  client_id_env: ORI_EVIDENCE_CLIENT_ID
  secret_env: ORI_EVIDENCE_INGEST_SECRET
`
	for _, tc := range []struct {
		name      string
		outbound  string
		returned  string
		wantError bool
	}{
		{"separate absolute", "/var/lib/ori/evidence-out", "/var/lib/ori/evidence-return", false},
		{"relative", "evidence-out", "/var/lib/ori/evidence-return", true},
		{"same", "/var/lib/ori/evidence", "/var/lib/ori/evidence", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, fmt.Sprintf(base, tc.outbound, tc.returned)))
			if tc.wantError {
				if err == nil {
					t.Fatal("unsafe evidence config was accepted")
				}
				return
			}
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Evidence.MaxItems != DefaultEvidenceMaxItems || cfg.Evidence.MaxBytes != DefaultEvidenceMaxBytes {
				t.Fatalf("evidence defaults = %#v", cfg.Evidence)
			}
		})
	}
}

func TestLlamaCppProviderRequiresURLWhenSelected(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: llama_cpp
  llama_cpp:
    url: ""
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected llama_cpp provider to require url")
	}
	if !strings.Contains(err.Error(), "provider.llama_cpp.url") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLlamaCppProviderLoadsWithURL(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: llama_cpp
  timeout_ms: 5000
  llama_cpp:
    url: "http://localhost:8080/completion"
    model: "local-model.gguf"
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider.Name != ProviderLlamaCpp {
		t.Fatalf("unexpected reasoning provider: %q", cfg.Provider.Name)
	}
	if cfg.Provider.LlamaCpp.URL != "http://localhost:8080/completion" {
		t.Fatalf("unexpected llama.cpp url: %q", cfg.Provider.LlamaCpp.URL)
	}
	if cfg.Provider.LlamaCpp.Model != "local-model.gguf" {
		t.Fatalf("unexpected llama.cpp model fallback: %q", cfg.Provider.LlamaCpp.Model)
	}
}

func TestDefaults(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.HeartbeatIntervalS != DefaultHeartbeatIntervalS {
		t.Fatalf("heartbeat default: got %d want %d", cfg.Gateway.HeartbeatIntervalS, DefaultHeartbeatIntervalS)
	}
	if cfg.Provider.TimeoutMS != DefaultProviderTimeoutMS {
		t.Fatalf("timeout default: got %d want %d", cfg.Provider.TimeoutMS, DefaultProviderTimeoutMS)
	}
	if cfg.Gateway.Auth.Enabled {
		t.Fatal("gateway auth should default disabled")
	}
	if cfg.Gateway.Auth.SharedSecretEnv != DefaultGatewayAuthSecretEnv {
		t.Fatalf("auth env default: got %q want %q", cfg.Gateway.Auth.SharedSecretEnv, DefaultGatewayAuthSecretEnv)
	}
}

func TestLoadMissingFile(t *testing.T) {
	_, err := Load(filepath.Join(t.TempDir(), "missing.yaml"))
	if err == nil {
		t.Fatal("expected error for missing file")
	}
	if !strings.Contains(err.Error(), "read config") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadMalformedYAML(t *testing.T) {
	path := writeConfig(t, "gateway:\n  broker_url: [\n")

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected parse error")
	}
	if !strings.Contains(err.Error(), "parse config") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayAuthConfigLoads(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  auth:
    enabled: true
    shared_secret_env: "CUSTOM_GATEWAY_SECRET"
provider:
  name: echo
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Gateway.Auth.Enabled {
		t.Fatal("expected gateway auth enabled")
	}
	if cfg.Gateway.Auth.SharedSecretEnv != "CUSTOM_GATEWAY_SECRET" {
		t.Fatalf("unexpected shared_secret_env: %q", cfg.Gateway.Auth.SharedSecretEnv)
	}
}

func TestGatewayAuthSecretEnvRejectsWhitespace(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  auth:
    enabled: true
    shared_secret_env: "BAD SECRET"
provider:
  name: echo
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected shared_secret_env validation error")
	}
	if !strings.Contains(err.Error(), "gateway.auth.shared_secret_env") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestLoadInvalidHeartbeatInterval(t *testing.T) {
	cases := []struct {
		name    string
		content string
	}{
		{
			name: "zero",
			content: `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  heartbeat_interval_s: 0
provider:
  name: echo
  timeout_ms: 10000
`,
		},
		{
			name: "negative",
			content: `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  heartbeat_interval_s: -1
provider:
  name: echo
  timeout_ms: 10000
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.content)
			_, err := Load(path)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), "heartbeat_interval_s") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestLoadInvalidProviderTimeout(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
  timeout_ms: -1
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected validation error")
	}
	if !strings.Contains(err.Error(), "timeout_ms") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestSIMDisabledNoModemRequired(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
sim:
  enabled: false
  modem_path: ""
`)

	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
}

func TestFleetDisabledNoCloudURL(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
fleet:
  enabled: false
  cloud_url: ""
`)

	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
}

func TestSIMEnabledRequiresModemPath(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
sim:
  enabled: true
  modem_path: ""
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for enabled SIM without modem_path")
	}
	if !strings.Contains(err.Error(), "modem_path") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestFleetEnabledRequiresCloudURL(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
fleet:
  enabled: true
  cloud_url: ""
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected error for enabled fleet without cloud_url")
	}
	if !strings.Contains(err.Error(), "cloud_url") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestUnknownTopLevelKeyIgnored(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
future_feature:
  enabled: true
`)

	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
}

func TestLoadExampleShape(t *testing.T) {
	path := filepath.Join("..", "..", "gateway.yaml.example")
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Gateway.BrokerURL == "" {
		t.Fatal("expected broker_url from example config")
	}
	if cfg.Provider.Name != ProviderEcho {
		t.Fatalf("unexpected example provider: %q", cfg.Provider.Name)
	}
}

func TestReportingConfigDefaultsDisabled(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  weekly_report:
    enabled: false
  tier_c_enrichment:
    enabled: false
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Reporting.WeeklyReport.Enabled {
		t.Fatal("expected weekly report disabled")
	}
	if cfg.Reporting.TierCEnrichment.Enabled {
		t.Fatal("expected tier C enrichment disabled")
	}
	if cfg.Reporting.Provider != "" {
		t.Fatalf("expected no reporting provider requirement when disabled, got %q", cfg.Reporting.Provider)
	}
}

func TestWeeklyReportRequiresProviderConfigWhenEnabled(t *testing.T) {
	base := `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  weekly_report:
    enabled: true
    day: monday
    time: "08:00"
    timezone: Africa/Lagos
`
	path := writeConfig(t, base)
	_, err := Load(path)
	if err == nil {
		t.Fatal("expected enabled weekly report to require provider")
	}
	if !strings.Contains(err.Error(), "reporting.provider") {
		t.Fatalf("unexpected error: %v", err)
	}

	path = writeConfig(t, base+`  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
`)
	_, err = Load(path)
	if err == nil {
		t.Fatal("expected enabled weekly report to require model")
	}
	if !strings.Contains(err.Error(), "model") {
		t.Fatalf("unexpected error: %v", err)
	}

	path = writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  weekly_report:
    enabled: true
    day: monday
    time: "08:00"
    timezone: Africa/Lagos
    device_id: dev-01
    sensor_ids: [current-main]
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Reporting.Provider != ReportingProviderGemini {
		t.Fatalf("unexpected reporting provider: %q", cfg.Reporting.Provider)
	}
}

func TestTierCEnrichmentRequiresProviderConfigWhenEnabled(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    model: gemini-2.5-flash
  tier_c_enrichment:
    enabled: true
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected enabled tier C enrichment to require api_key_env")
	}
	if !strings.Contains(err.Error(), "api_key_env") {
		t.Fatalf("unexpected error: %v", err)
	}

	path = writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  tier_c_enrichment:
    enabled: true
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Reporting.TierCEnrichment.Enabled {
		t.Fatal("expected tier C enrichment enabled")
	}
}

func TestReportingProviderDoesNotAffectReasoningProvider(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  weekly_report:
    enabled: true
    day: monday
    time: "08:00"
    timezone: Africa/Lagos
    device_id: dev-01
    sensor_ids: [current-main]
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider.Name != ProviderEcho {
		t.Fatalf("reasoning provider changed: %q", cfg.Provider.Name)
	}
	if cfg.Reporting.Provider != ReportingProviderGemini {
		t.Fatalf("reporting provider not loaded: %q", cfg.Reporting.Provider)
	}
}

func TestWeeklyReportRequiresReportScope(t *testing.T) {
	base := `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  weekly_report:
    enabled: true
    day: monday
    time: "08:00"
    timezone: Africa/Lagos
`

	cases := []struct {
		name    string
		content string
		want    string
	}{
		{name: "missing device", content: base + `    sensor_ids: [current-main]
`, want: "device_id"},
		{name: "invalid device", content: base + `    device_id: dev/01
    sensor_ids: [current-main]
`, want: "device_id"},
		{name: "missing sensors", content: base + `    device_id: dev-01
`, want: "sensor_ids"},
		{name: "blank sensor", content: base + `    device_id: dev-01
    sensor_ids: [""]
`, want: "sensor_ids"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.content)
			_, err := Load(path)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}

func TestWeeklyReportRejectsInvalidSchedule(t *testing.T) {
	cases := []struct {
		name    string
		field   string
		content string
	}{
		{
			name:  "missing day",
			field: "day",
			content: `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  weekly_report:
    enabled: true
    time: "08:00"
    timezone: Africa/Lagos
`,
		},
		{
			name:  "invalid time",
			field: "time",
			content: `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  weekly_report:
    enabled: true
    day: monday
    time: "8am"
    timezone: Africa/Lagos
`,
		},
		{
			name:  "invalid timezone",
			field: "timezone",
			content: `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  weekly_report:
    enabled: true
    day: monday
    time: "08:00"
    timezone: Not/AZone
`,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.content)
			_, err := Load(path)
			if err == nil {
				t.Fatal("expected schedule validation error")
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestReportingRejectsUnknownProviderEvenWhenDisabled(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: bogus
  weekly_report:
    enabled: false
  tier_c_enrichment:
    enabled: false
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected unknown reporting provider to fail")
	}
	if !strings.Contains(err.Error(), "reporting.provider") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloudLLMProviderRequiresProviderConfigWhenSelected(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: cloud_llm
  cloud_llm:
    vendor: claude
    model: claude-sonnet-4-5
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected cloud_llm provider to require api_key_env")
	}
	if !strings.Contains(err.Error(), "provider.cloud_llm.api_key_env") {
		t.Fatalf("unexpected error: %v", err)
	}

	path = writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: cloud_llm
  cloud_llm:
    vendor: claude
    api_key_env: ANTHROPIC_API_KEY
    model: claude-sonnet-4-5
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider.Name != ProviderCloudLLM {
		t.Fatalf("unexpected reasoning provider: %q", cfg.Provider.Name)
	}
	if cfg.Provider.CloudLLM.Vendor != CloudVendorClaude {
		t.Fatalf("unexpected cloud vendor: %q", cfg.Provider.CloudLLM.Vendor)
	}
}

func TestCloudLLMProviderSupportsSwappableVendors(t *testing.T) {
	vendors := []string{
		CloudVendorClaude,
		CloudVendorOpenAI,
		CloudVendorGemini,
		CloudVendorDeepSeek,
	}

	for _, vendor := range vendors {
		t.Run(vendor, func(t *testing.T) {
			path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: cloud_llm
  cloud_llm:
    vendor: `+vendor+`
    api_key_env: CLOUD_LLM_API_KEY
    model: default-model
`)
			cfg, err := Load(path)
			if err != nil {
				t.Fatal(err)
			}
			if cfg.Provider.CloudLLM.Vendor != vendor {
				t.Fatalf("unexpected vendor: %q", cfg.Provider.CloudLLM.Vendor)
			}
		})
	}
}

func TestCloudLLMOpenAICompatibleRequiresBaseURL(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: cloud_llm
  cloud_llm:
    vendor: openai_compatible
    api_key_env: CLOUD_LLM_API_KEY
    model: default-model
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected openai_compatible vendor to require base_url")
	}
	if !strings.Contains(err.Error(), "base_url") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloudLLMRejectsUnknownVendor(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: cloud_llm
  cloud_llm:
    vendor: bogus
    api_key_env: CLOUD_LLM_API_KEY
    model: default-model
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected unknown cloud_llm vendor to fail")
	}
	if !strings.Contains(err.Error(), "vendor") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestCloudLLMAPIKeyEnvMustBeEnvVarName(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: cloud_llm
  cloud_llm:
    vendor: claude
    api_key_env: "not a valid env var name"
    model: claude-sonnet-4-5
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected invalid cloud api_key_env to fail")
	}
	if !strings.Contains(err.Error(), "provider.cloud_llm.api_key_env") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReportingAPIKeyEnvMustBeEnvVarName(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
reporting:
  provider: gemini
  gemini:
    api_key_env: "not a valid env var name"
    model: gemini-2.5-flash
  tier_c_enrichment:
    enabled: true
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected invalid api_key_env to fail")
	}
	if !strings.Contains(err.Error(), "api_key_env") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayDeviceIDsRequired(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
provider:
  name: echo
`)

	_, err := Load(path)
	if err == nil {
		t.Fatal("expected gateway.device_ids validation error")
	}
	if !strings.Contains(err.Error(), "gateway.device_ids") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayDeviceIDsRejectMQTTAndAuthDelimiters(t *testing.T) {
	cases := []string{"dev/01", "dev+01", "dev#01", "dev|01", " dev-01"}
	for _, deviceID := range cases {
		t.Run(deviceID, func(t *testing.T) {
			path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["`+deviceID+`"]
provider:
  name: echo
`)

			_, err := Load(path)
			if err == nil {
				t.Fatal("expected gateway.device_ids validation error")
			}
			if !strings.Contains(err.Error(), "gateway.device_ids") {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestCloudLLMProviderIgnoresReportingConfig(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: cloud_llm
  timeout_ms: 5000
  cloud_llm:
    vendor: claude
    api_key_env: ANTHROPIC_API_KEY
    model: claude-sonnet-4-5
    base_url: "https://api.anthropic.com/v1/messages"
reporting:
  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
  weekly_report:
    enabled: true
    day: monday
    time: "08:00"
    timezone: Africa/Lagos
    device_id: dev-01
    sensor_ids: [current-main]
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Provider.Name != ProviderCloudLLM || cfg.Provider.CloudLLM.Vendor != CloudVendorClaude || cfg.Provider.CloudLLM.APIKeyEnv != "ANTHROPIC_API_KEY" {
		t.Fatalf("unexpected cloud provider config: %#v", cfg.Provider)
	}
	if cfg.Reporting.Provider != ReportingProviderGemini || cfg.Reporting.Gemini.APIKeyEnv != "GEMINI_API_KEY" {
		t.Fatalf("unexpected reporting config: %#v", cfg.Reporting)
	}
}

func TestWebhookBridgeDefaultsDisabled(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.WebhookBridge.Enabled {
		t.Fatal("webhook bridge should default disabled")
	}
	if cfg.WebhookBridge.ListenAddr != DefaultWebhookBridgeListenAddr {
		t.Fatalf("listen addr = %q", cfg.WebhookBridge.ListenAddr)
	}
	if cfg.WebhookBridge.Path != DefaultWebhookBridgePath {
		t.Fatalf("path = %q", cfg.WebhookBridge.Path)
	}
}

func TestWebhookBridgeLoadsWhenEnabled(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
webhook_bridge:
  enabled: true
  listen_addr: "127.0.0.1:8090"
  path: "/webhooks/sms/africastalking"
  target_url: "http://127.0.0.1:8080/webhooks/sms/africastalking"
  runtime_token_env: "ORI_SMS_WEBHOOK_TOKEN"
  hmac_secret_env: "ORI_SMS_WEBHOOK_HMAC_SECRET"
  request_timeout_ms: 3000
  max_body_bytes: 65536
`)
	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.WebhookBridge.Enabled {
		t.Fatal("expected webhook bridge enabled")
	}
	if cfg.WebhookBridge.TargetURL == "" || cfg.WebhookBridge.RuntimeTokenEnv == "" || cfg.WebhookBridge.HMACSecretEnv == "" {
		t.Fatalf("unexpected bridge config: %#v", cfg.WebhookBridge)
	}
}

func TestWebhookBridgeRejectsInvalidConfig(t *testing.T) {
	base := `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
webhook_bridge:
  enabled: true
  listen_addr: "127.0.0.1:8090"
  path: "/webhooks/sms/africastalking"
  target_url: "http://127.0.0.1:8080/webhooks/sms/africastalking"
  runtime_token_env: "ORI_SMS_WEBHOOK_TOKEN"
  hmac_secret_env: "ORI_SMS_WEBHOOK_HMAC_SECRET"
  request_timeout_ms: 3000
  max_body_bytes: 65536
`
	cases := []struct {
		name    string
		content string
		field   string
	}{
		{
			name: "missing target url",
			content: strings.Replace(base, `  target_url: "http://127.0.0.1:8080/webhooks/sms/africastalking"
`, `  target_url: ""
`, 1),
			field: "target_url",
		},
		{
			name: "bad path",
			content: strings.Replace(base, `  path: "/webhooks/sms/africastalking"
`, `  path: "webhooks/sms/africastalking"
`, 1),
			field: "path",
		},
		{
			name: "literal secret env",
			content: strings.Replace(base, `  hmac_secret_env: "ORI_SMS_WEBHOOK_HMAC_SECRET"
`, `  hmac_secret_env: "bad secret"
`, 1),
			field: "hmac_secret_env",
		},
		{
			name: "env name with equals rejected",
			content: strings.Replace(base, `  runtime_token_env: "ORI_SMS_WEBHOOK_TOKEN"
`, `  runtime_token_env: "ORI_SMS_WEBHOOK_TOKEN=value"
`, 1),
			field: "runtime_token_env",
		},
		{
			name: "non loopback requires source cidrs",
			content: strings.Replace(base, `  listen_addr: "127.0.0.1:8090"
`, `  listen_addr: "0.0.0.0:8090"
`, 1),
			field: "provider_source_cidrs",
		},
		{
			name: "catch all cidr rejected",
			content: base + `  provider_source_cidrs:
    - "0.0.0.0/0"
`,
			field: "catch-all",
		},
		{
			name: "too broad ipv4 cidr rejected",
			content: base + `  provider_source_cidrs:
    - "0.0.0.0/1"
`,
			field: "too broad",
		},
		{
			name: "oversized max body rejected",
			content: strings.Replace(base, `  max_body_bytes: 65536
`, `  max_body_bytes: 1048577
`, 1),
			field: "max_body_bytes",
		},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			path := writeConfig(t, tc.content)
			_, err := Load(path)
			if err == nil {
				t.Fatal("expected validation error")
			}
			if !strings.Contains(err.Error(), tc.field) {
				t.Fatalf("unexpected error: %v", err)
			}
		})
	}
}

func TestWebhookBridgeNonLoopbackAcceptsProviderCIDR(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
provider:
  name: echo
webhook_bridge:
  enabled: true
  listen_addr: "0.0.0.0:8090"
  path: "/webhooks/sms/africastalking"
  target_url: "http://127.0.0.1:8080/webhooks/sms/africastalking"
  provider_source_cidrs:
    - "127.0.0.1/32"
  runtime_token_env: "ORI_SMS_WEBHOOK_TOKEN"
  hmac_secret_env: "ORI_SMS_WEBHOOK_HMAC_SECRET"
`)
	if _, err := Load(path); err != nil {
		t.Fatal(err)
	}
}

func TestGatewayAuthPreviousSecretAndEncryptionLoad(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  auth:
    enabled: true
    shared_secret_env: "GATEWAY_SECRET_CURRENT"
    previous_shared_secret_env: "GATEWAY_SECRET_PREVIOUS"
  encryption:
    enabled: true
provider:
  name: echo
`)

	cfg, err := Load(path)
	if err != nil {
		t.Fatal(err)
	}
	if !cfg.Gateway.Auth.Enabled || cfg.Gateway.Auth.SharedSecretEnv != "GATEWAY_SECRET_CURRENT" {
		t.Fatalf("unexpected auth config: %#v", cfg.Gateway.Auth)
	}
	if cfg.Gateway.Auth.PreviousSharedSecretEnv != "GATEWAY_SECRET_PREVIOUS" {
		t.Fatalf("unexpected previous secret env: %#v", cfg.Gateway.Auth)
	}
	if !cfg.Gateway.Encryption.Enabled {
		t.Fatal("expected gateway encryption enabled")
	}
}

func TestGatewayEncryptionRequiresAuth(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  encryption:
    enabled: true
provider:
  name: echo
`)

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "gateway.encryption.enabled requires gateway.auth.enabled") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayPreviousSecretEnvRejectsWhitespace(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01"]
  auth:
    enabled: true
    previous_shared_secret_env: "BAD SECRET"
provider:
  name: echo
`)

	_, err := Load(path)
	if err == nil || !strings.Contains(err.Error(), "gateway.auth.previous_shared_secret_env") {
		t.Fatalf("unexpected error: %v", err)
	}
}
