// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"gopkg.in/yaml.v3"
)

// evidenceConfigVectorPath is the draft gateway-config evidence corpus in a
// sibling ori-specs checkout, read in place until the contract merges and it
// is vendored.
var evidenceConfigVectorPath = filepath.Join("..", "..", "..", "ori-specs", "gateway-config", "vectors", "evidence-config.json")

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
}

// TestEvidenceConfigVectors loads every gateway-config/v1 evidence vector
// through Load: a valid case must produce exactly the corpus's effective
// values after defaults, and a refused case must be refused for its rule.
func TestEvidenceConfigVectors(t *testing.T) {
	raw, err := os.ReadFile(evidenceConfigVectorPath)
	if err != nil {
		t.Fatalf("the gateway-config evidence vectors are required at %s: %v", evidenceConfigVectorPath, err)
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
	for _, tc := range doc.Cases {
		t.Run(tc.Name, func(t *testing.T) {
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
				if err == nil || !strings.Contains(err.Error(), want) {
					t.Fatalf("rule %s: got %v, want a refusal containing %q", tc.Rule, err, want)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if tc.Effective == nil {
				if cfg.Evidence.Enabled {
					t.Fatal("a case with no effective values enabled the courier")
				}
				return
			}
			got := cfg.Evidence
			e := tc.Effective
			if got.MaxItems != e.MaxItems || got.MaxBytes != e.MaxBytes || got.RetryIntervalS != e.RetryIntervalS ||
				got.BackoffBaseS != e.BackoffBaseS || got.BackoffMaxS != e.BackoffMaxS ||
				got.StoreProbeIntervalS != e.StoreProbeIntervalS {
				t.Fatalf("effective = %+v, corpus = %+v", got, *e)
			}
		})
	}
}
