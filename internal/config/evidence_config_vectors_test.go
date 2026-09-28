// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"encoding/json"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
	"maps"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// evidenceRuleMessages names the refusal each corpus rule must produce. A
// refusal for another rule's reason fails the case, so a vector cannot pass
// because an earlier, unrelated check refused it.
var evidenceRuleMessages = map[string]string{
	"directories": "director",
	"bounds":      "bounds and retry interval must be positive",
	"probe":       "evidence.store_probe_interval_s",
	"backoff":     "back-off must satisfy",
	"share":       "shared equally across",
	"env":         "must be an environment variable name",
	"device_id":   "gateway.device_ids",
	"ca_file":     "evidence.authority_ca_file",
	// The loader enforces neither rule yet; a refusal must name its key.
	"carriage": "evidence.device_carriage",
	"stopped":  "evidence.stopped",
}

// TestEvidenceConfigVectors loads every gateway-config/v2 evidence vector
// through Load: a valid case must produce exactly the corpus's effective
// values after defaults, and a refused case must be refused for its rule.
func TestEvidenceConfigVectors(t *testing.T) {
	raw, err := specvectors.Read("gateway-config/vectors/evidence-config-v2.json")
	if err != nil {
		t.Fatalf("the gateway-config evidence vectors are required at gateway-config/vectors/evidence-config-v2.json: %v", err)
	}
	var doc struct {
		Contract string `json:"contract"`
		Note     string `json:"note"`
		Cases    []struct {
			Name      string         `json:"name"`
			DeviceIDs []string       `json:"device_ids"`
			Evidence  map[string]any `json:"evidence"`
			Valid     bool           `json:"valid"`
			Rule      string         `json:"rule"`
			Effective *struct {
				MaxItems            int   `json:"max_items"`
				MaxBytes            int64 `json:"max_bytes"`
				RetryIntervalS      int   `json:"retry_interval_s"`
				BackoffBaseS        int   `json:"backoff_base_s"`
				BackoffMaxS         int   `json:"backoff_max_s"`
				StoreProbeIntervalS int   `json:"store_probe_interval_s"`
				// Its default is empty: the system trust store.
				AuthorityCAFile string `json:"authority_ca_file"`
				// gateway-config/v2's evidence-carriage and stopped-custody
				// keys, which the loader does not yet produce.
				DeviceCarriage        map[string]string `json:"device_carriage"`
				StoppedMaxItems       int               `json:"stopped_max_items"`
				StoppedMaxBytes       int64             `json:"stopped_max_bytes"`
				StoppedDeviceMaxItems int               `json:"stopped_device_max_items"`
				StoppedDeviceMaxBytes int64             `json:"stopped_device_max_bytes"`
			} `json:"effective"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("the corpus has no cases")
	}
	names := make([]string, 0, len(doc.Cases))
	for _, tc := range doc.Cases {
		names = append(names, tc.Name)
	}
	checkConfigPinsExist(t, names)
	for _, tc := range doc.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			report := func(format string, args ...any) {
				t.Helper()
				checkConfigPin(t, tc.Name, fmt.Sprintf(format, args...))
			}
			evidence := map[string]any{}
			for k, v := range tc.Evidence {
				if n, ok := v.(json.Number); ok {
					i, err := n.Int64()
					if err != nil {
						t.Fatalf("%s: non-integer %s", k, n)
					}
					v = i
				}
				evidence[k] = v
			}
			document, err := yaml.Marshal(map[string]any{
				"gateway":  map[string]any{"broker_url": "tcp://localhost:1883", "device_ids": tc.DeviceIDs},
				"provider": map[string]any{"name": "echo"},
				"evidence": evidence,
			})
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(writeConfig(t, string(document)))
			if !tc.Valid {
				want, ok := evidenceRuleMessages[tc.Rule]
				if !ok {
					t.Fatalf("rule %q has no expected refusal", tc.Rule)
				}
				switch {
				case err == nil:
					report("accepted, rule %s expects a refusal", tc.Rule)
				case !strings.Contains(err.Error(), want):
					report("refused for another rule: %v", err)
				default:
					report("")
				}
				return
			}
			if err != nil {
				report("refused: %v", err)
				return
			}
			if tc.Effective == nil {
				if cfg.Evidence.Enabled {
					report("a case with no effective values enabled the courier")
					return
				}
				report("")
				return
			}
			got := cfg.Evidence
			e := tc.Effective
			if got.MaxItems != e.MaxItems || got.MaxBytes != e.MaxBytes || got.RetryIntervalS != e.RetryIntervalS ||
				got.BackoffBaseS != e.BackoffBaseS || got.BackoffMaxS != e.BackoffMaxS ||
				got.StoreProbeIntervalS != e.StoreProbeIntervalS || got.AuthorityCAFile != e.AuthorityCAFile {
				report("effective = %+v, corpus = %+v", got, *e)
				return
			}
			if !maps.Equal(got.DeviceCarriage, e.DeviceCarriage) {
				report("device_carriage = %v, corpus = %v", got.DeviceCarriage, e.DeviceCarriage)
				return
			}
			if e.StoppedMaxItems != 0 || e.StoppedMaxBytes != 0 || e.StoppedDeviceMaxItems != 0 || e.StoppedDeviceMaxBytes != 0 {
				report("the loader produces no stopped-custody bounds")
				return
			}
			report("")
		})
	}
}
