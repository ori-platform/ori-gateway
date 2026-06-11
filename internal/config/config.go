// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

const (
	DefaultHeartbeatIntervalS   = 30
	DefaultProviderTimeoutMS    = 10000
	DefaultGatewayAuthSecretEnv = "GATEWAY_SHARED_SECRET"

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
	Gateway   GatewayConfig   `yaml:"gateway"`
	Provider  ProviderConfig  `yaml:"provider"`
	Reporting ReportingConfig `yaml:"reporting"`
	SIM       SIMConfig       `yaml:"sim"`
	Fleet     FleetConfig     `yaml:"fleet"`
}

type GatewayConfig struct {
	BrokerURL          string            `yaml:"broker_url"`
	HeartbeatIntervalS int               `yaml:"heartbeat_interval_s"`
	Auth               GatewayAuthConfig `yaml:"auth"`
}

type GatewayAuthConfig struct {
	Enabled         bool   `yaml:"enabled"`
	SharedSecretEnv string `yaml:"shared_secret_env"`
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

type fileConfig struct {
	Gateway   fileGatewayConfig  `yaml:"gateway"`
	Provider  fileProviderConfig `yaml:"provider"`
	Reporting ReportingConfig    `yaml:"reporting"`
	SIM       SIMConfig          `yaml:"sim"`
	Fleet     FleetConfig        `yaml:"fleet"`
}

type fileGatewayConfig struct {
	BrokerURL          string            `yaml:"broker_url"`
	HeartbeatIntervalS *int              `yaml:"heartbeat_interval_s"`
	Auth               GatewayAuthConfig `yaml:"auth"`
}

type fileProviderConfig struct {
	Name      string         `yaml:"name"`
	TimeoutMS *int           `yaml:"timeout_ms"`
	LlamaCpp  LlamaCppConfig `yaml:"llama_cpp"`
	CloudLLM  CloudLLMConfig `yaml:"cloud_llm"`
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
			Auth: GatewayAuthConfig{
				Enabled:         f.Gateway.Auth.Enabled,
				SharedSecretEnv: strings.TrimSpace(f.Gateway.Auth.SharedSecretEnv),
			},
		},
		Provider: normalizeProviderStrings(ProviderConfig{
			Name:     f.Provider.Name,
			LlamaCpp: f.Provider.LlamaCpp,
			CloudLLM: f.Provider.CloudLLM,
		}),
		Reporting: normalizeReportingStrings(f.Reporting),
		SIM:       f.SIM,
		Fleet:     f.Fleet,
	}

	if cfg.Gateway.BrokerURL == "" {
		return Config{}, fmt.Errorf("gateway.broker_url must not be empty")
	}
	if cfg.Gateway.Auth.SharedSecretEnv == "" {
		cfg.Gateway.Auth.SharedSecretEnv = DefaultGatewayAuthSecretEnv
	}
	if strings.ContainsAny(cfg.Gateway.Auth.SharedSecretEnv, " \t\r\n") {
		return Config{}, fmt.Errorf("gateway.auth.shared_secret_env must be an environment variable name")
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

	if cfg.SIM.Enabled && strings.TrimSpace(cfg.SIM.ModemPath) == "" {
		return Config{}, fmt.Errorf("sim.modem_path must not be empty when sim.enabled is true")
	}

	if cfg.Fleet.Enabled && strings.TrimSpace(cfg.Fleet.CloudURL) == "" {
		return Config{}, fmt.Errorf("fleet.cloud_url must not be empty when fleet.enabled is true")
	}

	return cfg, nil
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
