// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"net"
	"net/netip"
	"net/url"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultHeartbeatIntervalS   = 30
	DefaultProviderTimeoutMS    = 10000
	DefaultGatewayAuthSecretEnv = "GATEWAY_SHARED_SECRET"

	DefaultWebhookBridgeListenAddr       = "127.0.0.1:8090"
	DefaultWebhookBridgePath             = "/webhooks/sms/africastalking"
	DefaultWebhookBridgeRequestTimeoutMS = 3000
	DefaultWebhookBridgeMaxBodyBytes     = 65536

	ProviderEcho     = "echo"
	ProviderLlamaCpp = "llama_cpp"
	ProviderCloudLLM = "cloud_llm"

	CloudVendorClaude           = "claude"
	CloudVendorOpenAI           = "openai"
	CloudVendorGemini           = "gemini"
	CloudVendorDeepSeek         = "deepseek"
	CloudVendorOpenAICompatible = "openai_compatible"

	ReportingProviderGemini = "gemini"
)

// Config is the root gateway configuration loaded from gateway.yaml.
type Config struct {
	Gateway       GatewayConfig       `yaml:"gateway"`
	Provider      ProviderConfig      `yaml:"provider"`
	Reporting     ReportingConfig     `yaml:"reporting"`
	WebhookBridge WebhookBridgeConfig `yaml:"webhook_bridge"`
	SIM           SIMConfig           `yaml:"sim"`
	Fleet         FleetConfig         `yaml:"fleet"`
}

type GatewayConfig struct {
	BrokerURL          string                  `yaml:"broker_url"`
	DeviceIDs          []string                `yaml:"device_ids"`
	HeartbeatIntervalS int                     `yaml:"heartbeat_interval_s"`
	Auth               GatewayAuthConfig       `yaml:"auth"`
	Encryption         GatewayEncryptionConfig `yaml:"encryption"`
}

type GatewayAuthConfig struct {
	Enabled                 bool   `yaml:"enabled"`
	SharedSecretEnv         string `yaml:"shared_secret_env"`
	PreviousSharedSecretEnv string `yaml:"previous_shared_secret_env"`
}

type GatewayEncryptionConfig struct {
	Enabled bool `yaml:"enabled"`
}

type ProviderConfig struct {
	Name      string         `yaml:"name"`
	TimeoutMS int            `yaml:"timeout_ms"`
	LlamaCpp  LlamaCppConfig `yaml:"llama_cpp"`
	CloudLLM  CloudLLMConfig `yaml:"cloud_llm"`
}

type LlamaCppConfig struct {
	URL string `yaml:"url"`
	// Model is the fallback model name used when llama.cpp /props is unreachable or returns no model name.
	Model string `yaml:"model"`
}

type CloudLLMConfig struct {
	Vendor    string `yaml:"vendor"`
	APIKeyEnv string `yaml:"api_key_env"`
	Model     string `yaml:"model"`
	BaseURL   string `yaml:"base_url"`
}

// ReportingConfig configures customer-facing report and enrichment providers.
// It is intentionally separate from ProviderConfig, which handles Tier 3 reasoning.
type ReportingConfig struct {
	Provider        string                `yaml:"provider"`
	Gemini          ReportingGeminiConfig `yaml:"gemini"`
	WeeklyReport    WeeklyReportConfig    `yaml:"weekly_report"`
	TierCEnrichment TierCEnrichmentConfig `yaml:"tier_c_enrichment"`
}

type ReportingGeminiConfig struct {
	APIKeyEnv string `yaml:"api_key_env"`
	Model     string `yaml:"model"`
}

type WeeklyReportConfig struct {
	Enabled  bool   `yaml:"enabled"`
	Day      string `yaml:"day"`
	Time     string `yaml:"time"`
	Timezone string `yaml:"timezone"`
}

type TierCEnrichmentConfig struct {
	Enabled bool `yaml:"enabled"`
}

type SIMConfig struct {
	Enabled   bool   `yaml:"enabled"`
	ModemPath string `yaml:"modem_path"`
}

type FleetConfig struct {
	Enabled  bool   `yaml:"enabled"`
	CloudURL string `yaml:"cloud_url"`
}

// WebhookBridgeConfig configures the optional provider-ingress signing bridge.
// Secret values are resolved from environment variables at runtime.
type WebhookBridgeConfig struct {
	Enabled             bool     `yaml:"enabled"`
	ListenAddr          string   `yaml:"listen_addr"`
	Path                string   `yaml:"path"`
	TargetURL           string   `yaml:"target_url"`
	ProviderSourceCIDRs []string `yaml:"provider_source_cidrs"`
	RuntimeTokenEnv     string   `yaml:"runtime_token_env"`
	HMACSecretEnv       string   `yaml:"hmac_secret_env"`
	RequestTimeoutMS    int      `yaml:"request_timeout_ms"`
	MaxBodyBytes        int64    `yaml:"max_body_bytes"`
}

type fileConfig struct {
	Gateway       fileGatewayConfig       `yaml:"gateway"`
	Provider      fileProviderConfig      `yaml:"provider"`
	Reporting     ReportingConfig         `yaml:"reporting"`
	WebhookBridge fileWebhookBridgeConfig `yaml:"webhook_bridge"`
	SIM           SIMConfig               `yaml:"sim"`
	Fleet         FleetConfig             `yaml:"fleet"`
}

type fileGatewayConfig struct {
	BrokerURL          string                  `yaml:"broker_url"`
	DeviceIDs          []string                `yaml:"device_ids"`
	HeartbeatIntervalS *int                    `yaml:"heartbeat_interval_s"`
	Auth               GatewayAuthConfig       `yaml:"auth"`
	Encryption         GatewayEncryptionConfig `yaml:"encryption"`
}

type fileProviderConfig struct {
	Name      string         `yaml:"name"`
	TimeoutMS *int           `yaml:"timeout_ms"`
	LlamaCpp  LlamaCppConfig `yaml:"llama_cpp"`
	CloudLLM  CloudLLMConfig `yaml:"cloud_llm"`
}

type fileWebhookBridgeConfig struct {
	Enabled             bool     `yaml:"enabled"`
	ListenAddr          string   `yaml:"listen_addr"`
	Path                string   `yaml:"path"`
	TargetURL           string   `yaml:"target_url"`
	ProviderSourceCIDRs []string `yaml:"provider_source_cidrs"`
	RuntimeTokenEnv     string   `yaml:"runtime_token_env"`
	HMACSecretEnv       string   `yaml:"hmac_secret_env"`
	RequestTimeoutMS    *int     `yaml:"request_timeout_ms"`
	MaxBodyBytes        *int64   `yaml:"max_body_bytes"`
}

// Load reads and validates gateway configuration from path.
func Load(path string) (Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return Config{}, fmt.Errorf("read config %q: %w", path, err)
	}

	var raw fileConfig
	if err := yaml.Unmarshal(data, &raw); err != nil {
		return Config{}, fmt.Errorf("parse config %q: %w", path, err)
	}

	cfg, err := raw.normalize()
	if err != nil {
		return Config{}, fmt.Errorf("validate config %q: %w", path, err)
	}

	return cfg, nil
}

func (f *fileConfig) normalize() (Config, error) {
	cfg := Config{
		Gateway: GatewayConfig{
			BrokerURL: strings.TrimSpace(f.Gateway.BrokerURL),
			DeviceIDs: normalizeGatewayDeviceIDs(f.Gateway.DeviceIDs),
			Auth: GatewayAuthConfig{
				Enabled:                 f.Gateway.Auth.Enabled,
				SharedSecretEnv:         strings.TrimSpace(f.Gateway.Auth.SharedSecretEnv),
				PreviousSharedSecretEnv: strings.TrimSpace(f.Gateway.Auth.PreviousSharedSecretEnv),
			},
			Encryption: f.Gateway.Encryption,
		},
		Provider: normalizeProviderStrings(ProviderConfig{
			Name:     f.Provider.Name,
			LlamaCpp: f.Provider.LlamaCpp,
			CloudLLM: f.Provider.CloudLLM,
		}),
		Reporting:     normalizeReportingStrings(f.Reporting),
		WebhookBridge: normalizeWebhookBridge(f.WebhookBridge),
		SIM:           f.SIM,
		Fleet:         f.Fleet,
	}

	if cfg.Gateway.BrokerURL == "" {
		return Config{}, fmt.Errorf("gateway.broker_url must not be empty")
	}
	if len(cfg.Gateway.DeviceIDs) == 0 {
		return Config{}, fmt.Errorf("gateway.device_ids must include at least one runtime device")
	}
	for _, deviceID := range cfg.Gateway.DeviceIDs {
		if err := validateGatewayDeviceID(deviceID); err != nil {
			return Config{}, fmt.Errorf("gateway.device_ids: %w", err)
		}
	}
	if cfg.Gateway.Auth.SharedSecretEnv == "" {
		cfg.Gateway.Auth.SharedSecretEnv = DefaultGatewayAuthSecretEnv
	}
	if strings.ContainsAny(cfg.Gateway.Auth.SharedSecretEnv, " \t\r\n=") {
		return Config{}, fmt.Errorf("gateway.auth.shared_secret_env must be an environment variable name")
	}
	if cfg.Gateway.Auth.PreviousSharedSecretEnv != "" && strings.ContainsAny(cfg.Gateway.Auth.PreviousSharedSecretEnv, " \t\r\n=") {
		return Config{}, fmt.Errorf("gateway.auth.previous_shared_secret_env must be an environment variable name")
	}
	if cfg.Gateway.Encryption.Enabled && !cfg.Gateway.Auth.Enabled {
		return Config{}, fmt.Errorf("gateway.encryption.enabled requires gateway.auth.enabled")
	}

	if f.Gateway.HeartbeatIntervalS == nil {
		cfg.Gateway.HeartbeatIntervalS = DefaultHeartbeatIntervalS
	} else if *f.Gateway.HeartbeatIntervalS <= 0 {
		return Config{}, fmt.Errorf("gateway.heartbeat_interval_s must be positive")
	} else {
		cfg.Gateway.HeartbeatIntervalS = *f.Gateway.HeartbeatIntervalS
	}

	if f.Provider.TimeoutMS == nil {
		cfg.Provider.TimeoutMS = DefaultProviderTimeoutMS
	} else if *f.Provider.TimeoutMS <= 0 {
		return Config{}, fmt.Errorf("provider.timeout_ms must be positive")
	} else {
		cfg.Provider.TimeoutMS = *f.Provider.TimeoutMS
	}

	if cfg.Provider.Name == "" {
		return Config{}, fmt.Errorf("provider.name must not be empty")
	}
	if !isKnownProvider(cfg.Provider.Name) {
		return Config{}, fmt.Errorf(
			"provider.name %q is unknown (allowed: echo, llama_cpp, cloud_llm)",
			cfg.Provider.Name,
		)
	}

	if err := validateProvider(cfg.Provider); err != nil {
		return Config{}, err
	}

	if err := validateReporting(cfg.Reporting); err != nil {
		return Config{}, err
	}

	if err := validateWebhookBridge(cfg.WebhookBridge); err != nil {
		return Config{}, err
	}

	if cfg.SIM.Enabled && strings.TrimSpace(cfg.SIM.ModemPath) == "" {
		return Config{}, fmt.Errorf("sim.modem_path must not be empty when sim.enabled is true")
	}

	if cfg.Fleet.Enabled && strings.TrimSpace(cfg.Fleet.CloudURL) == "" {
		return Config{}, fmt.Errorf("fleet.cloud_url must not be empty when fleet.enabled is true")
	}

	return cfg, nil
}

func normalizeGatewayDeviceIDs(deviceIDs []string) []string {
	out := make([]string, 0, len(deviceIDs))
	seen := map[string]bool{}
	for _, deviceID := range deviceIDs {
		if seen[deviceID] {
			continue
		}
		seen[deviceID] = true
		out = append(out, deviceID)
	}
	return out
}

func validateGatewayDeviceID(deviceID string) error {
	if deviceID == "" {
		return fmt.Errorf("device_id must not be empty")
	}
	if strings.TrimSpace(deviceID) != deviceID {
		return fmt.Errorf("device_id %q must not contain leading or trailing whitespace", deviceID)
	}
	if strings.ContainsAny(deviceID, "/+#|") {
		return fmt.Errorf("device_id %q must not contain MQTT separators, wildcards, or auth delimiters", deviceID)
	}
	return nil
}

func isKnownProvider(name string) bool {
	switch name {
	case ProviderEcho, ProviderLlamaCpp, ProviderCloudLLM:
		return true
	default:
		return false
	}
}

func normalizeProviderStrings(provider ProviderConfig) ProviderConfig {
	provider.Name = strings.TrimSpace(provider.Name)
	provider.LlamaCpp.URL = strings.TrimSpace(provider.LlamaCpp.URL)
	provider.LlamaCpp.Model = strings.TrimSpace(provider.LlamaCpp.Model)
	provider.CloudLLM.Vendor = strings.TrimSpace(provider.CloudLLM.Vendor)
	provider.CloudLLM.APIKeyEnv = strings.TrimSpace(provider.CloudLLM.APIKeyEnv)
	provider.CloudLLM.Model = strings.TrimSpace(provider.CloudLLM.Model)
	provider.CloudLLM.BaseURL = strings.TrimSpace(provider.CloudLLM.BaseURL)
	return provider
}

func normalizeWebhookBridge(raw fileWebhookBridgeConfig) WebhookBridgeConfig {
	requestTimeoutMS := DefaultWebhookBridgeRequestTimeoutMS
	if raw.RequestTimeoutMS != nil {
		requestTimeoutMS = *raw.RequestTimeoutMS
	}
	maxBodyBytes := int64(DefaultWebhookBridgeMaxBodyBytes)
	if raw.MaxBodyBytes != nil {
		maxBodyBytes = *raw.MaxBodyBytes
	}
	cidrs := make([]string, 0, len(raw.ProviderSourceCIDRs))
	for _, cidr := range raw.ProviderSourceCIDRs {
		trimmed := strings.TrimSpace(cidr)
		if trimmed != "" {
			cidrs = append(cidrs, trimmed)
		}
	}
	return WebhookBridgeConfig{
		Enabled:             raw.Enabled,
		ListenAddr:          defaultIfBlank(raw.ListenAddr, DefaultWebhookBridgeListenAddr),
		Path:                defaultIfBlank(raw.Path, DefaultWebhookBridgePath),
		TargetURL:           strings.TrimSpace(raw.TargetURL),
		ProviderSourceCIDRs: cidrs,
		RuntimeTokenEnv:     strings.TrimSpace(raw.RuntimeTokenEnv),
		HMACSecretEnv:       strings.TrimSpace(raw.HMACSecretEnv),
		RequestTimeoutMS:    requestTimeoutMS,
		MaxBodyBytes:        maxBodyBytes,
	}
}

func defaultIfBlank(value string, fallback string) string {
	trimmed := strings.TrimSpace(value)
	if trimmed == "" {
		return fallback
	}
	return trimmed
}

func validateWebhookBridge(cfg WebhookBridgeConfig) error {
	if !cfg.Enabled {
		return nil
	}
	if _, _, err := net.SplitHostPort(cfg.ListenAddr); err != nil {
		return fmt.Errorf("webhook_bridge.listen_addr must be host:port: %w", err)
	}
	if !strings.HasPrefix(cfg.Path, "/") {
		return fmt.Errorf("webhook_bridge.path must start with /")
	}
	if cfg.TargetURL == "" {
		return fmt.Errorf("webhook_bridge.target_url must not be empty when webhook_bridge.enabled is true")
	}
	target, err := url.Parse(cfg.TargetURL)
	if err != nil || target.Scheme == "" || target.Host == "" {
		return fmt.Errorf("webhook_bridge.target_url must be an absolute http(s) URL")
	}
	if target.Scheme != "http" && target.Scheme != "https" {
		return fmt.Errorf("webhook_bridge.target_url must use http or https")
	}
	if err := validateEnvVarName("webhook_bridge.runtime_token_env", cfg.RuntimeTokenEnv); err != nil {
		return err
	}
	if err := validateEnvVarName("webhook_bridge.hmac_secret_env", cfg.HMACSecretEnv); err != nil {
		return err
	}
	if cfg.RequestTimeoutMS <= 0 {
		return fmt.Errorf("webhook_bridge.request_timeout_ms must be positive")
	}
	if cfg.MaxBodyBytes <= 0 || cfg.MaxBodyBytes > 1<<20 {
		return fmt.Errorf("webhook_bridge.max_body_bytes must be between 1 and 1048576")
	}
	for _, cidr := range cfg.ProviderSourceCIDRs {
		prefix, err := netip.ParsePrefix(cidr)
		if err != nil {
			return fmt.Errorf("webhook_bridge.provider_source_cidrs contains invalid CIDR %q: %w", cidr, err)
		}
		if err := validateProviderCIDR(prefix, cidr); err != nil {
			return err
		}
	}
	if !listenAddrIsLoopback(cfg.ListenAddr) && len(cfg.ProviderSourceCIDRs) == 0 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs must not be empty for non-loopback listen_addr")
	}
	return nil
}

func validateProviderCIDR(prefix netip.Prefix, raw string) error {
	bits := prefix.Bits()
	addr := prefix.Addr()
	if bits == 0 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs must not contain catch-all CIDR %q", raw)
	}
	if addr.Is4() && bits < 8 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs CIDR %q is too broad; IPv4 prefixes must be /8 or narrower", raw)
	}
	if addr.Is6() && bits < 32 {
		return fmt.Errorf("webhook_bridge.provider_source_cidrs CIDR %q is too broad; IPv6 prefixes must be /32 or narrower", raw)
	}
	return nil
}

func validateEnvVarName(field string, value string) error {
	if value == "" {
		return fmt.Errorf("%s must be an environment variable name", field)
	}
	if strings.ContainsAny(value, " \t\r\n=") {
		return fmt.Errorf("%s must be an environment variable name", field)
	}
	return nil
}

func listenAddrIsLoopback(listenAddr string) bool {
	host, _, err := net.SplitHostPort(listenAddr)
	if err != nil {
		return false
	}
	if host == "localhost" {
		return true
	}
	addr, err := netip.ParseAddr(host)
	if err != nil {
		return false
	}
	return addr.IsLoopback()
}

func validateProvider(provider ProviderConfig) error {
	if provider.Name == ProviderLlamaCpp {
		if provider.LlamaCpp.URL == "" {
			return fmt.Errorf("provider.llama_cpp.url must not be empty when provider.name is llama_cpp")
		}
		return nil
	}
	if provider.Name != ProviderCloudLLM {
		return nil
	}

	if provider.CloudLLM.Vendor == "" {
		return fmt.Errorf("provider.cloud_llm.vendor must not be empty when provider.name is cloud_llm")
	}
	if !isKnownCloudVendor(provider.CloudLLM.Vendor) {
		return fmt.Errorf(
			"provider.cloud_llm.vendor %q is unknown (allowed: claude, openai, gemini, deepseek, openai_compatible)",
			provider.CloudLLM.Vendor,
		)
	}
	if provider.CloudLLM.APIKeyEnv == "" {
		return fmt.Errorf("provider.cloud_llm.api_key_env must not be empty when provider.name is cloud_llm")
	}
	if strings.ContainsAny(provider.CloudLLM.APIKeyEnv, " \t\r\n") {
		return fmt.Errorf("provider.cloud_llm.api_key_env must be an environment variable name")
	}
	if provider.CloudLLM.Model == "" {
		return fmt.Errorf("provider.cloud_llm.model must not be empty when provider.name is cloud_llm")
	}
	if provider.CloudLLM.Vendor == CloudVendorOpenAICompatible && provider.CloudLLM.BaseURL == "" {
		return fmt.Errorf("provider.cloud_llm.base_url must not be empty when vendor is openai_compatible")
	}

	return nil
}

func isKnownCloudVendor(vendor string) bool {
	switch vendor {
	case CloudVendorClaude, CloudVendorOpenAI, CloudVendorGemini, CloudVendorDeepSeek, CloudVendorOpenAICompatible:
		return true
	default:
		return false
	}
}

func normalizeReportingStrings(reporting ReportingConfig) ReportingConfig {
	reporting.Provider = strings.TrimSpace(reporting.Provider)
	reporting.Gemini.APIKeyEnv = strings.TrimSpace(reporting.Gemini.APIKeyEnv)
	reporting.Gemini.Model = strings.TrimSpace(reporting.Gemini.Model)
	reporting.WeeklyReport.Day = strings.TrimSpace(reporting.WeeklyReport.Day)
	reporting.WeeklyReport.Time = strings.TrimSpace(reporting.WeeklyReport.Time)
	reporting.WeeklyReport.Timezone = strings.TrimSpace(reporting.WeeklyReport.Timezone)
	return reporting
}

func validateReporting(reporting ReportingConfig) error {
	if reporting.Provider != "" && !isKnownReportingProvider(reporting.Provider) {
		return fmt.Errorf(
			"reporting.provider %q is unknown (allowed: gemini)",
			reporting.Provider,
		)
	}

	if !reporting.WeeklyReport.Enabled && !reporting.TierCEnrichment.Enabled {
		return nil
	}

	if reporting.Provider == "" {
		return fmt.Errorf("reporting.provider must not be empty when reporting features are enabled")
	}
	if !isKnownReportingProvider(reporting.Provider) {
		return fmt.Errorf(
			"reporting.provider %q is unknown (allowed: gemini)",
			reporting.Provider,
		)
	}

	if reporting.Provider == ReportingProviderGemini {
		if reporting.Gemini.APIKeyEnv == "" {
			return fmt.Errorf("reporting.gemini.api_key_env must not be empty when reporting features are enabled")
		}
		if strings.ContainsAny(reporting.Gemini.APIKeyEnv, " \t\r\n") {
			return fmt.Errorf("reporting.gemini.api_key_env must be an environment variable name")
		}
		if reporting.Gemini.Model == "" {
			return fmt.Errorf("reporting.gemini.model must not be empty when reporting features are enabled")
		}
	}

	if reporting.WeeklyReport.Enabled {
		if !isValidWeekday(reporting.WeeklyReport.Day) {
			return fmt.Errorf("reporting.weekly_report.day must be a weekday name")
		}
		if _, err := time.Parse("15:04", reporting.WeeklyReport.Time); err != nil {
			return fmt.Errorf("reporting.weekly_report.time must use HH:MM 24-hour format")
		}
		if reporting.WeeklyReport.Timezone == "" {
			return fmt.Errorf("reporting.weekly_report.timezone must not be empty")
		}
		if _, err := time.LoadLocation(reporting.WeeklyReport.Timezone); err != nil {
			return fmt.Errorf("reporting.weekly_report.timezone %q is invalid: %w", reporting.WeeklyReport.Timezone, err)
		}
	}

	return nil
}

func isKnownReportingProvider(name string) bool {
	switch name {
	case ReportingProviderGemini:
		return true
	default:
		return false
	}
}

func isValidWeekday(day string) bool {
	switch strings.ToLower(day) {
	case "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday":
		return true
	default:
		return false
	}
}
