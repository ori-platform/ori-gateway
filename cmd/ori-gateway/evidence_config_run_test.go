// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"bytes"
	"context"
	"encoding/json"
	"maps"
	"os"
	"path/filepath"
	"testing"
	"time"

	"gopkg.in/yaml.v3"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

// TestEveryAcceptedEvidenceConfigRuns carries each configuration the
// gateway-config/v2 evidence corpus accepts with the courier enabled through
// config.Load and into a running gateway: a configuration the loader accepts
// must not then refuse to start, whether or not any device declares
// gateway-evidence-carriage/v1.
func TestEveryAcceptedEvidenceConfigRuns(t *testing.T) {
	raw, err := specvectors.Read("gateway-config/vectors/evidence-config-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var doc struct {
		Cases []struct {
			Name      string         `json:"name"`
			DeviceIDs []string       `json:"device_ids"`
			Evidence  map[string]any `json:"evidence"`
			Valid     bool           `json:"valid"`
		} `json:"cases"`
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.UseNumber()
	if err := decoder.Decode(&doc); err != nil {
		t.Fatal(err)
	}
	// Legacy values the corpus does not carry and the loader keeps unvalidated
	// on a site that declares nothing.
	legacy := func(name string, deviceIDs []string, set map[string]any) {
		evidence := map[string]any{
			"enabled": true, "endpoint_env": "ORI_EVIDENCE_ENDPOINT",
			"client_id_env": "ORI_EVIDENCE_CLIENT_ID", "secret_env": "ORI_EVIDENCE_INGEST_SECRET",
		}
		maps.Copy(evidence, set)
		doc.Cases = append(doc.Cases, struct {
			Name      string         `json:"name"`
			DeviceIDs []string       `json:"device_ids"`
			Evidence  map[string]any `json:"evidence"`
			Valid     bool           `json:"valid"`
		}{Name: name, DeviceIDs: deviceIDs, Evidence: evidence, Valid: true})
	}
	legacy("a legacy store probe of zero", []string{"dev-01"}, map[string]any{"store_probe_interval_s": 0})
	legacy("a negative legacy store probe", []string{"dev-01"}, map[string]any{"store_probe_interval_s": -1})
	legacy("a legacy store probe past any duration", []string{"dev-01"}, map[string]any{"store_probe_interval_s": int64(1) << 40})
	legacy("a one-item legacy queue across two devices", []string{"dev-01", "dev-02"}, map[string]any{"max_items": 1})
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	// Unreachable, so nothing admitted is retired during the test.
	t.Setenv("ORI_EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("ORI_EVIDENCE_CLIENT_ID", "site-gateway-a")
	t.Setenv("ORI_EVIDENCE_INGEST_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	ran := 0
	for _, tc := range doc.Cases {
		if !tc.Valid || tc.Evidence["enabled"] != true {
			continue
		}
		ran++
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
			evidence["queue_directory"] = filepath.Join(t.TempDir(), "outbound")
			evidence["return_queue_directory"] = filepath.Join(t.TempDir(), "returned")
			document, err := yaml.Marshal(map[string]any{
				"gateway":  map[string]any{"broker_url": "tcp://localhost:1883", "device_ids": tc.DeviceIDs},
				"provider": map[string]any{"name": "echo"},
				"evidence": evidence,
			})
			if err != nil {
				t.Fatal(err)
			}
			path := filepath.Join(t.TempDir(), "gateway.yaml")
			if err := os.WriteFile(path, document, 0o600); err != nil {
				t.Fatal(err)
			}
			cfg, err := config.Load(path)
			if err != nil {
				t.Fatalf("the corpus accepts this configuration and the loader refused it: %v", err)
			}
			cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
			cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}

			mainBroker := newFakeBroker()
			evidenceBroker := newFakeBroker()
			deps := baseDeps(t, cfg, mainBroker, &fakeProvider{healthy: true}, newFakeHeartbeat())
			deps.newBroker = func(opts broker.Options) (brokerClient, error) {
				if opts.ClientID == defaultEvidenceClientID {
					return evidenceBroker, nil
				}
				return mainBroker, nil
			}
			ctx, cancel := context.WithCancel(context.Background())
			done := make(chan error, 1)
			go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
			deadline := time.After(2 * time.Second)
			for evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter) == nil {
				select {
				case err := <-done:
					cancel()
					t.Fatalf("the gateway stopped before carrying evidence: %v", err)
				case <-deadline:
					cancel()
					<-done
					t.Fatal("outbound evidence subscription never started")
				case <-time.After(5 * time.Millisecond):
				}
			}
			select {
			case err := <-done:
				cancel()
				t.Fatalf("the gateway stopped after starting: %v", err)
			case <-time.After(200 * time.Millisecond):
			}
			cancel()
			if err := <-done; err != nil {
				t.Fatalf("the gateway stopped with %v", err)
			}
		})
	}
	if ran == 0 {
		t.Fatal("the corpus holds no accepted configuration with the courier enabled")
	}
}
