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

func TestDefaults(t *testing.T) {
	path := writeConfig(t, `
gateway:
  broker_url: "tcp://localhost:1883"
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
