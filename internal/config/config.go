// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"os"
	"strings"

	"gopkg.in/yaml.v3"
)

const (
	DefaultHeartbeatIntervalS = 30
	DefaultProviderTimeoutMS  = 10000

	ProviderEcho     = "echo"
	ProviderLlamaCpp = "llama_cpp"
	ProviderClaude   = "claude"
)

// Config is the root gateway configuration loaded from gateway.yaml.
type Config struct {
	Gateway  GatewayConfig  `yaml:"gateway"`
	Provider ProviderConfig `yaml:"provider"`
	SIM      SIMConfig      `yaml:"sim"`
	Fleet    FleetConfig    `yaml:"fleet"`
}

type GatewayConfig struct {
	BrokerURL          string `yaml:"broker_url"`
	HeartbeatIntervalS int    `yaml:"heartbeat_interval_s"`
}

type ProviderConfig struct {
	Name      string         `yaml:"name"`
	TimeoutMS int            `yaml:"timeout_ms"`
	LlamaCpp  LlamaCppConfig `yaml:"llama_cpp"`
	Claude    ClaudeConfig   `yaml:"claude"`
}

type LlamaCppConfig struct {
	URL string `yaml:"url"`
}

type ClaudeConfig struct {
	APIKeyEnv string `yaml:"api_key_env"`
	Model     string `yaml:"model"`
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
	Gateway  fileGatewayConfig  `yaml:"gateway"`
	Provider fileProviderConfig `yaml:"provider"`
	SIM      SIMConfig          `yaml:"sim"`
	Fleet    FleetConfig        `yaml:"fleet"`
}

type fileGatewayConfig struct {
	BrokerURL          string `yaml:"broker_url"`
	HeartbeatIntervalS *int   `yaml:"heartbeat_interval_s"`
}

type fileProviderConfig struct {
	Name      string         `yaml:"name"`
	TimeoutMS *int           `yaml:"timeout_ms"`
	LlamaCpp  LlamaCppConfig `yaml:"llama_cpp"`
	Claude    ClaudeConfig   `yaml:"claude"`
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
		},
		Provider: ProviderConfig{
			Name:     strings.TrimSpace(f.Provider.Name),
			LlamaCpp: f.Provider.LlamaCpp,
			Claude:   f.Provider.Claude,
		},
		SIM:   f.SIM,
		Fleet: f.Fleet,
	}

	if cfg.Gateway.BrokerURL == "" {
		return Config{}, fmt.Errorf("gateway.broker_url must not be empty")
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
			"provider.name %q is unknown (allowed: echo, llama_cpp, claude)",
			cfg.Provider.Name,
		)
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
	case ProviderEcho, ProviderLlamaCpp, ProviderClaude:
		return true
	default:
		return false
	}
}
