// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
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

	path = writeConfig(t, base+`  provider: gemini
  gemini:
    api_key_env: GEMINI_API_KEY
    model: gemini-2.5-flash
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
