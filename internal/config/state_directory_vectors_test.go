// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package config

import (
	"bytes"
	"encoding/json"
	"strings"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/specvectors"
	"gopkg.in/yaml.v3"
)

// stateDirectoryRuleMessages names the refusal each corpus rule must produce,
// so a vector cannot pass because another rule refused it.
var stateDirectoryRuleMessages = map[string]string{
	"required_under_auth":           "gateway.state_directory is required when gateway.auth.enabled is true",
	"absolute":                      "gateway.state_directory must be an absolute path",
	"separate_from_evidence_queues": "gateway.state_directory must be separate from the evidence queue directories",
}

// TestStateDirectoryVectors loads every gateway-config/v2 state-directory
// vector through Load: a valid case must yield exactly the corpus's effective
// state directory, and a refused case must be refused for its rule.
func TestStateDirectoryVectors(t *testing.T) {
	raw, err := specvectors.Read("gateway-config/vectors/state-directory-v2.json")
	if err != nil {
		t.Fatalf("the gateway-config state-directory vectors are required at gateway-config/vectors/state-directory-v2.json: %v", err)
	}
	var doc struct {
		Contract string `json:"contract"`
		Note     string `json:"note"`
		Cases    []struct {
			Name           string  `json:"name"`
			AuthEnabled    bool    `json:"auth_enabled"`
			StateDirectory *string `json:"state_directory"`
			Evidence       struct {
				Enabled              bool   `json:"enabled"`
				QueueDirectory       string `json:"queue_directory"`
				ReturnQueueDirectory string `json:"return_queue_directory"`
			} `json:"evidence"`
			Valid     bool    `json:"valid"`
			Effective *string `json:"effective"`
			Rule      string  `json:"rule"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatalf("decode corpus: %v", err)
	}
	if len(doc.Cases) == 0 {
		t.Fatal("the corpus has no cases")
	}
	seen := map[string]bool{}
	for _, tc := range doc.Cases {
		t.Run(tc.Name, func(t *testing.T) {
			gateway := map[string]any{
				"broker_url": "tcp://localhost:1883",
				"device_ids": []string{"site-a-edge-01"},
				"auth":       map[string]any{"enabled": tc.AuthEnabled},
			}
			if tc.StateDirectory != nil {
				gateway["state_directory"] = *tc.StateDirectory
			}
			document, err := yaml.Marshal(map[string]any{
				"gateway":  gateway,
				"provider": map[string]any{"name": "echo"},
				"evidence": map[string]any{
					"enabled":                tc.Evidence.Enabled,
					"queue_directory":        tc.Evidence.QueueDirectory,
					"return_queue_directory": tc.Evidence.ReturnQueueDirectory,
					"endpoint_env":           "ORI_EVIDENCE_ENDPOINT",
					"client_id_env":          "ORI_EVIDENCE_CLIENT_ID",
					"secret_env":             "ORI_EVIDENCE_INGEST_SECRET",
				},
			})
			if err != nil {
				t.Fatal(err)
			}
			cfg, err := Load(writeConfig(t, string(document)))
			if !tc.Valid {
				want, ok := stateDirectoryRuleMessages[tc.Rule]
				if !ok {
					t.Fatalf("rule %q has no expected refusal", tc.Rule)
				}
				seen[tc.Rule] = true
				switch {
				case err == nil:
					t.Fatalf("accepted with state directory %q, rule %s expects a refusal", cfg.Gateway.StateDirectory, tc.Rule)
				case !strings.Contains(err.Error(), want):
					t.Fatalf("refused for another rule: %v", err)
				}
				return
			}
			if err != nil {
				t.Fatalf("refused: %v", err)
			}
			if tc.Effective == nil {
				t.Fatal("a valid case names no effective state directory")
			}
			if cfg.Gateway.StateDirectory != *tc.Effective {
				t.Fatalf("state directory = %q, corpus = %q", cfg.Gateway.StateDirectory, *tc.Effective)
			}
		})
	}
	for rule := range stateDirectoryRuleMessages {
		if !seen[rule] {
			t.Errorf("no corpus case exercises rule %s", rule)
		}
	}
}
