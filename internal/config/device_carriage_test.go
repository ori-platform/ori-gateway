// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"fmt"
	"maps"
	"strings"
	"testing"
)

// TestDeviceCarriageActivatesTheVersionedRulesOnlyWhenDeclared loads, through
// Load, a site that breaks every rule the versioned carriage adds: a one-item
// queue shared by two devices, a back-off bound below its base, a store probe
// outside its range, and a device ID outside the evidence routing domain. It
// loads while no device declares gateway-evidence-carriage/v1, whatever shape
// the absence takes, and each rule refuses it once one does. With the courier
// enabled, a supplied map is validated even when it declares nothing, and an
// invalid one is refused rather than read as legacy.
func TestDeviceCarriageActivatesTheVersionedRulesOnlyWhenDeclared(t *testing.T) {
	const site = `
gateway:
  broker_url: "tcp://localhost:1883"
  device_ids: ["dev-01", "dev 02"]
provider:
  name: echo
evidence:
  enabled: %v
  queue_directory: /var/lib/ori/evidence-out
  return_queue_directory: /var/lib/ori/evidence-return
  endpoint_env: ORI_EVIDENCE_ENDPOINT
  client_id_env: ORI_EVIDENCE_CLIENT_ID
  secret_env: ORI_EVIDENCE_INGEST_SECRET
  max_items: %d
  retry_interval_s: 5
  backoff_base_s: %d
  backoff_max_s: 30
  store_probe_interval_s: %d
%s`
	legacy := map[string]string{"dev-01": CarriageGatewayAPIV1, "dev 02": CarriageGatewayAPIV1}
	for _, tc := range []struct {
		name     string
		enabled  bool
		carriage string
		// Each opted-in row fixes every other rule so the named one decides.
		maxItems, base, probe int
		refusal               string // "" means loads
		effective             map[string]string
	}{
		{"absent", true, "", 1, 60, 60, "", legacy},
		{"empty", true, "  device_carriage: {}\n", 1, 60, 60, "", legacy},
		{"null", true, "  device_carriage:\n", 1, 60, 60, "", legacy},
		{"legacy only, declared explicitly", true, "  device_carriage:\n    dev-01: gateway-api/v1\n    \"dev 02\": gateway-api/v1\n", 1, 60, 60, "", legacy},
		{"courier disabled with an invalid map", false, "  device_carriage:\n    dev-09: nonsense\n", 1, 60, 60, "", nil},
		{"invalid value", true, "  device_carriage:\n    dev-01: gateway-evidence-carriage/v2\n", 1, 60, 60, "evidence.device_carriage", nil},
		{"unconfigured device", true, "  device_carriage:\n    dev-09: gateway-api/v1\n", 1, 60, 60, "evidence.device_carriage", nil},
		{"a list, not a map", true, "  device_carriage: [dev-01]\n", 1, 60, 60, "parse config", nil},
		{"a nested value", true, "  device_carriage:\n    dev-01: {contract: gateway-api/v1}\n", 1, 60, 60, "parse config", nil},
		{"mixed: the legacy device keeps its ID, and the rules apply", true, "  device_carriage:\n    dev-01: gateway-evidence-carriage/v1\n", 4, 10, 300, "",
			map[string]string{"dev-01": CarriageEvidenceCarriageV1, "dev 02": CarriageGatewayAPIV1}},
		{"opted in: the declaring device's ID", true, "  device_carriage:\n    \"dev 02\": gateway-evidence-carriage/v1\n", 4, 10, 300, "gateway.device_ids", nil},
		{"opted in: the capacity share", true, "  device_carriage:\n    dev-01: gateway-evidence-carriage/v1\n", 1, 10, 300, "shared equally across", nil},
		{"opted in: the back-off ordering", true, "  device_carriage:\n    dev-01: gateway-evidence-carriage/v1\n", 4, 60, 300, "back-off must satisfy", nil},
		{"opted in: the store probe range", true, "  device_carriage:\n    dev-01: gateway-evidence-carriage/v1\n", 4, 10, 60, "store_probe_interval_s must be 300 through 900", nil},
	} {
		t.Run(tc.name, func(t *testing.T) {
			cfg, err := Load(writeConfig(t, fmt.Sprintf(site, tc.enabled, tc.maxItems, tc.base, tc.probe, tc.carriage)))
			if tc.refusal != "" {
				if err == nil || !strings.Contains(err.Error(), tc.refusal) {
					t.Fatalf("load = %v, want a refusal containing %q", err, tc.refusal)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if !maps.Equal(cfg.Evidence.DeviceCarriage, tc.effective) {
				t.Fatalf("device_carriage = %v, want %v", cfg.Evidence.DeviceCarriage, tc.effective)
			}
			want := false
			for _, carriage := range tc.effective {
				want = want || carriage == CarriageEvidenceCarriageV1
			}
			if got := cfg.Evidence.DeclaresVersionedCarriage(); got != want {
				t.Fatalf("DeclaresVersionedCarriage = %v, want %v", got, want)
			}
		})
	}
}
