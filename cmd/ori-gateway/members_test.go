// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"maps"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/enrichment"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

// withMember inserts a raw member at the front of a JSON object.
func withMember(payload []byte, member string) []byte {
	return []byte("{" + member + "," + string(payload[1:]))
}

// TestAPayloadTheMemberRuleRefusesChangesNothing runs the gateway on a real
// MQTT broker, unsigned and signed. For the reasoning and enrichment
// requests, a case variant and a duplicate of each binding member, and
// trailing bytes, are refused before any handler: no reply, no provider call,
// a log line with the reason. A runtime heartbeat with trailing bytes leaves
// the site view untouched. The same request with an unknown member, and the
// clean request, are answered.
func TestAPayloadTheMemberRuleRefusesChangesNothing(t *testing.T) {
	for _, signed := range []bool{false, true} {
		name := "unsigned"
		if signed {
			name = "signed"
		}
		t.Run(name, func(t *testing.T) { memberRuleThroughTheGateway(t, signed) })
	}
}

func memberRuleThroughTheGateway(t *testing.T, signed bool) {
	const secret = "runtime-gateway-envelope-secret"
	brokerURL := startEmbeddedBroker(t)
	siteAddr := loopbackAddr(t)
	cfg := validConfig()
	cfg.Gateway.BrokerURL = brokerURL
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: siteAddr}
	cfg.Reporting = config.ReportingConfig{
		Provider:        config.ReportingProviderGemini,
		Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
	}
	if signed {
		t.Setenv("GATEWAY_ENVELOPE", secret)
		cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
		cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	}
	var logs syncBuffer
	prov := &fakeProvider{healthy: true}
	enricher := &fakeTierCEnrichmentProvider{}
	deps := baseDeps(t, cfg, newFakeBroker(), prov, newFakeHeartbeat())
	deps.logger = slog.New(slog.NewTextHandler(&logs, nil))
	deps.newBroker = func(opts broker.Options) (brokerClient, error) { return broker.New(opts) }
	deps.newTierCEnrichmentProvider = func(config.ReportingConfig) (enrichment.Provider, error) { return enricher, nil }
	deps.newRuntimeClient = func(brokerClient, ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return &runtimeclient.FakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })
	waitUntil(t, "site health", func() bool { ingress, _ := siteIngress(t, siteAddr); return ingress != nil })
	peer := newRuntimePeer(t, brokerURL, "ori/dev-01/reasoning/response", "ori/dev-01/tier_c/enrichment/response")

	// sign adds extra members, then signs when the run is signed.
	sign := func(value any, messageType, requestID string, extra map[string]any) []byte {
		raw, err := json.Marshal(value)
		if err != nil {
			t.Fatal(err)
		}
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		maps.Copy(payload, extra)
		if !signed {
			return mustJSON(t, payload)
		}
		auth, err := mqttauth.Sign(payload, messageType, "dev-01", requestID, deps.now().UnixMilli(), secret)
		if err != nil {
			t.Fatal(err)
		}
		payload["auth"] = auth
		out, err := json.Marshal(payload)
		if err != nil {
			t.Fatal(err)
		}
		return out
	}
	reasoning := func(requestID string, extra map[string]any) []byte {
		return sign(contracts.ReasoningRequest{
			RequestID: requestID, DeviceID: "dev-01", SensorType: "current_clamp", TriggerName: "overcurrent",
			Prompt: "Explain anomaly.", ActionTierHint: contracts.ActionTierC, TimeoutMS: 1000,
		}, contracts.ReasoningRequestMessageType, requestID, extra)
	}
	enrichmentRequest := func(requestID string, extra map[string]any) []byte {
		var req contracts.TierCEnrichmentRequest
		if err := json.Unmarshal(validTierCEnrichmentRequestPayload(t), &req); err != nil {
			t.Fatal(err)
		}
		req.RequestID = requestID
		return sign(req, contracts.TierCEnrichmentRequestMessageType, requestID, extra)
	}

	type refusedCase struct {
		payload []byte
		reason  string
	}
	// A case variant is signed with the variant beside the exact member, by
	// the runtime's authenticator, so in the signed run only the member rule
	// can refuse it. A duplicate cannot be represented in what the
	// authenticator signs, so it is added to the signed bytes.
	var reasoningRefused, enrichmentRefused []refusedCase
	for _, member := range []struct {
		variant, exact string
		value          any
	}{
		{"Request_ID", "request_id", "forged"},
		{"DEVICE_ID", "device_id", "dev-02"},
		{"Timeout_MS", "timeout_ms", 1},
	} {
		value := string(mustJSON(t, member.value))
		reasoningRefused = append(reasoningRefused,
			refusedCase{reasoning("req-refused", map[string]any{member.variant: member.value}), contracts.ReasonCaseVariantMember},
			refusedCase{withMember(reasoning("req-refused", nil), `"`+member.exact+`":`+value), contracts.ReasonDuplicateMember})
	}
	reasoningRefused = append(reasoningRefused, refusedCase{append(reasoning("req-trailing", nil), " trailing"...), contracts.ReasonTrailingData})
	for _, member := range []struct {
		variant, exact string
		value          any
	}{
		{"REQUEST_ID", "request_id", "forged"},
		{"Proposal_Id", "proposal_id", "P0000001"},
		{"Device_ID", "device_id", "dev-02"},
	} {
		value := string(mustJSON(t, member.value))
		enrichmentRefused = append(enrichmentRefused,
			refusedCase{enrichmentRequest("enrich-refused", map[string]any{member.variant: member.value}), contracts.ReasonCaseVariantMember},
			refusedCase{withMember(enrichmentRequest("enrich-refused", nil), `"`+member.exact+`":`+value), contracts.ReasonDuplicateMember})
	}
	enrichmentRefused = append(enrichmentRefused, refusedCase{append(enrichmentRequest("enrich-trailing", nil), "\n{}"...), contracts.ReasonTrailingData})
	want := map[string]int{}
	for _, c := range append(slices.Clone(reasoningRefused), enrichmentRefused...) {
		want[c.reason]++
	}

	for _, c := range reasoningRefused {
		peer.publish(t, "ori/dev-01/reasoning/request", c.payload)
	}
	for _, c := range enrichmentRefused {
		peer.publish(t, "ori/dev-01/tier_c/enrichment/request", c.payload)
	}
	heartbeat := sign(contracts.RuntimeNodeHeartbeat{
		DeviceID: "dev-01", Status: "healthy", LastSeenMS: deps.now().UnixMilli(), ActiveTriggers: []string{},
	}, contracts.RuntimeHeartbeatMessageType, "", nil)
	peer.publish(t, "ori/dev-01/runtime/heartbeat", append(heartbeat, " trailing"...))
	want[contracts.ReasonTrailingData]++
	// Admitted after the refusals: the clean requests, and the same requests
	// with a member no section defines, which additive compatibility keeps.
	unknown := map[string]any{"x_note": "additive"}
	peer.publish(t, "ori/dev-01/reasoning/request", reasoning("req-clean", nil))
	peer.publish(t, "ori/dev-01/reasoning/request", reasoning("req-unknown", unknown))
	peer.publish(t, "ori/dev-01/tier_c/enrichment/request", enrichmentRequest("enrich-clean", nil))
	peer.publish(t, "ori/dev-01/tier_c/enrichment/request", enrichmentRequest("enrich-unknown", unknown))

	waitUntil(t, "the admitted requests to be answered", func() bool {
		return len(peer.received("ori/dev-01/reasoning/response")) >= 2 &&
			len(peer.received("ori/dev-01/tier_c/enrichment/response")) >= 2
	})
	// Each refusal is counted in the paced log; the lines after the first
	// for a family and reason come within the pacing interval.
	waitUntil(t, "every refusal to be logged with its reason", func() bool {
		got := map[string]int{}
		for line := range strings.SplitSeq(logs.String(), "\n") {
			_, rest, ok := strings.Cut(line, "reason=")
			if !ok || !strings.Contains(line, "payload member rule") {
				continue
			}
			reason, count, _ := strings.Cut(rest, " refused=")
			n, _ := strconv.Atoi(count)
			got[reason] += n
		}
		return maps.Equal(got, want)
	})

	answered := func(topic string) []string {
		var ids []string
		for _, raw := range peer.received(topic) {
			var reply struct {
				RequestID string `json:"request_id"`
			}
			if err := json.Unmarshal(raw, &reply); err != nil {
				t.Fatal(err)
			}
			ids = append(ids, reply.RequestID)
		}
		return ids
	}
	if got := answered("ori/dev-01/reasoning/response"); strings.Join(got, ",") != "req-clean,req-unknown" {
		t.Fatalf("reasoning answered %v, want only the clean and the unknown-member requests", got)
	}
	if got := answered("ori/dev-01/tier_c/enrichment/response"); strings.Join(got, ",") != "enrich-clean,enrich-unknown" {
		t.Fatalf("enrichment answered %v, want only the clean and the unknown-member requests", got)
	}
	prov.mu.Lock()
	reasoned := len(prov.requests)
	prov.mu.Unlock()
	enricher.mu.Lock()
	enriched := len(enricher.requests)
	enricher.mu.Unlock()
	if reasoned != 2 || enriched != 2 {
		t.Fatalf("providers saw %d reasoning and %d enrichment requests, want 2 each", reasoned, enriched)
	}
	if _, nodes := siteIngress(t, siteAddr); strings.Contains(string(mustJSON(t, nodes)), `"gateway_observed":true`) {
		t.Fatalf("a heartbeat with trailing bytes changed the site view: %s", mustJSON(t, nodes))
	}
	if strings.Contains(logs.String(), "forged") || strings.Contains(logs.String(), "dev-02") {
		t.Fatalf("a refusal record carries payload content:\n%s", logs.String())
	}
}
