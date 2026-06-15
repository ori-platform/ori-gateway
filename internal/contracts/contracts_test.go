// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import (
	"encoding/json"
	"os"
	"strings"
	"testing"
)

func TestTopicsMatchGatewaySpec(t *testing.T) {
	reqTopic, err := RequestTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if reqTopic != "ori/site-a/reasoning/request" {
		t.Fatalf("unexpected request topic: %s", reqTopic)
	}

	respTopic, err := ResponseTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if respTopic != "ori/site-a/reasoning/response" {
		t.Fatalf("unexpected response topic: %s", respTopic)
	}

	if GatewayHealthTopic != "ori/gateway/health" {
		t.Fatalf("unexpected heartbeat topic: %s", GatewayHealthTopic)
	}

	if GatewayReasoningRequestTopicFilter != "ori/+/reasoning/request" {
		t.Fatalf("unexpected request subscription topic: %s", GatewayReasoningRequestTopicFilter)
	}
	if HeartbeatMessageType != "gateway.heartbeat" {
		t.Fatalf("unexpected heartbeat message type: %s", HeartbeatMessageType)
	}
	if RuntimeHeartbeatMessageType != "runtime.heartbeat" {
		t.Fatalf("unexpected runtime heartbeat message type: %s", RuntimeHeartbeatMessageType)
	}
	if RuntimeNodeHeartbeatTopicFilter != "ori/+/runtime/heartbeat" {
		t.Fatalf("unexpected runtime heartbeat filter: %s", RuntimeNodeHeartbeatTopicFilter)
	}
	if TierCEnrichmentRequestTopicFilter != "ori/+/tier_c/enrichment/request" {
		t.Fatalf("unexpected tier c enrichment request filter: %s", TierCEnrichmentRequestTopicFilter)
	}
	if HeartbeatAuthScheme != "hmac-sha256" {
		t.Fatalf("unexpected heartbeat auth scheme: %s", HeartbeatAuthScheme)
	}

	runtimeBeatTopic, err := RuntimeNodeHeartbeatTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if runtimeBeatTopic != "ori/site-a/runtime/heartbeat" {
		t.Fatalf("unexpected runtime heartbeat topic: %s", runtimeBeatTopic)
	}
	deviceID, err := DeviceIDFromRuntimeNodeHeartbeatTopic(runtimeBeatTopic)
	if err != nil {
		t.Fatal(err)
	}
	if deviceID != "site-a" {
		t.Fatalf("unexpected runtime heartbeat topic device_id: %s", deviceID)
	}

	enrichReqTopic, err := TierCEnrichmentRequestTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if enrichReqTopic != "ori/site-a/tier_c/enrichment/request" {
		t.Fatalf("unexpected tier c enrichment request topic: %s", enrichReqTopic)
	}
	enrichRespTopic, err := TierCEnrichmentResponseTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if enrichRespTopic != "ori/site-a/tier_c/enrichment/response" {
		t.Fatalf("unexpected tier c enrichment response topic: %s", enrichRespTopic)
	}
	deviceID, err = DeviceIDFromTierCEnrichmentRequestTopic(enrichReqTopic)
	if err != nil {
		t.Fatal(err)
	}
	if deviceID != "site-a" {
		t.Fatalf("unexpected tier c enrichment topic device_id: %s", deviceID)
	}

	exportReqTopic, err := ExportRequestTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if exportReqTopic != "ori/site-a/export/request" {
		t.Fatalf("unexpected export request topic: %s", exportReqTopic)
	}

	exportRespTopic, err := ExportResponseTopic("site-a", "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if exportRespTopic != "ori/site-a/export/response/req-1" {
		t.Fatalf("unexpected export response topic: %s", exportRespTopic)
	}

	exportRespFilter, err := ExportResponseTopicFilter("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if exportRespFilter != "ori/site-a/export/response/+" {
		t.Fatalf("unexpected export response filter: %s", exportRespFilter)
	}
}

func TestTopicHelpersRejectInvalidDeviceIDs(t *testing.T) {
	invalid := []string{
		"",
		" site-a",
		"site-a ",
		"site/a",
		"site+a",
		"site#a",
		"site|a",
	}

	for _, deviceID := range invalid {
		if topic, err := RequestTopic(deviceID); err == nil {
			t.Fatalf("RequestTopic(%q) returned %q, expected error", deviceID, topic)
		}
		if topic, err := ResponseTopic(deviceID); err == nil {
			t.Fatalf("ResponseTopic(%q) returned %q, expected error", deviceID, topic)
		}
		if topic, err := ExportRequestTopic(deviceID); err == nil {
			t.Fatalf("ExportRequestTopic(%q) returned %q, expected error", deviceID, topic)
		}
		if topic, err := RuntimeNodeHeartbeatTopic(deviceID); err == nil {
			t.Fatalf("RuntimeNodeHeartbeatTopic(%q) returned %q, expected error", deviceID, topic)
		}
		if topic, err := TierCEnrichmentRequestTopic(deviceID); err == nil {
			t.Fatalf("TierCEnrichmentRequestTopic(%q) returned %q, expected error", deviceID, topic)
		}
		if topic, err := TierCEnrichmentResponseTopic(deviceID); err == nil {
			t.Fatalf("TierCEnrichmentResponseTopic(%q) returned %q, expected error", deviceID, topic)
		}
		if topic, err := ExportResponseTopicFilter(deviceID); err == nil {
			t.Fatalf("ExportResponseTopicFilter(%q) returned %q, expected error", deviceID, topic)
		}
	}

	invalidRequestIDs := []string{"req/1", "req 1", "req	1", "req.1"}
	for _, requestID := range invalidRequestIDs {
		if topic, err := ExportResponseTopic("site-a", requestID); err == nil {
			t.Fatalf("ExportResponseTopic returned %q, expected invalid request_id error", topic)
		}
	}
}

func TestTopicHelpersDoNotUseLegacyGatewayNamespace(t *testing.T) {
	reqTopic, err := RequestTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	respTopic, err := ResponseTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}

	legacyFragments := []string{
		"ori/gateway/site-a/reason/request",
		"ori/gateway/site-a/reason/response",
		"/reason/request",
		"/reason/response",
	}
	for _, fragment := range legacyFragments {
		if strings.Contains(reqTopic, fragment) {
			t.Fatalf("request topic %q contains legacy fragment %q", reqTopic, fragment)
		}
		if strings.Contains(respTopic, fragment) {
			t.Fatalf("response topic %q contains legacy fragment %q", respTopic, fragment)
		}
	}
}

func TestValidateResponseForRequestRequiresCorrelation(t *testing.T) {
	req := ReasoningRequest{RequestID: "req-1", DeviceID: "site-a", Prompt: "p", ActionTierHint: "A"}
	resp := ReasoningResponse{RequestID: "other", ActionTier: "A"}
	if err := ValidateResponseForRequest(req, resp); err == nil {
		t.Fatal("expected request_id mismatch error")
	}
}

func TestValidateRequestRejectsInvalidDeviceID(t *testing.T) {
	req := ReasoningRequest{RequestID: "req-1", DeviceID: "site/a", Prompt: "p", ActionTierHint: "A"}
	if err := ValidateRequest(req); err == nil {
		t.Fatal("expected invalid device_id error")
	}
}

func TestValidateRequestRejectsInvalidRequestID(t *testing.T) {
	req := ReasoningRequest{RequestID: "req/1", DeviceID: "site-a", Prompt: "p", ActionTierHint: "A"}
	if err := ValidateRequest(req); err == nil {
		t.Fatal("expected invalid request_id error")
	}
}

func TestValidateRequestRejectsInvalidTier(t *testing.T) {
	req := ReasoningRequest{RequestID: "req-1", DeviceID: "site-a", Prompt: "p", ActionTierHint: "cloud"}
	if err := ValidateRequest(req); err == nil {
		t.Fatal("expected invalid tier error")
	}
}

func TestNewErrorResponsePreservesRequestID(t *testing.T) {
	resp := NewErrorResponse("req-1", ActionTierC, "provider timeout")
	if resp.RequestID != "req-1" {
		t.Fatalf("unexpected request_id: %s", resp.RequestID)
	}
	if resp.Error == nil || *resp.Error != "provider timeout" {
		t.Fatalf("expected error response, got %#v", resp.Error)
	}
}

func TestGoldenFixturesRoundTrip(t *testing.T) {
	cases := []struct {
		path string
		dest any
	}{
		{"testdata/reasoning_request.json", &ReasoningRequest{}},
		{"testdata/reasoning_response.json", &ReasoningResponse{}},
		{"testdata/reasoning_error_response.json", &ReasoningResponse{}},
		{"testdata/heartbeat.json", &Heartbeat{}},
		{"testdata/runtime_node_heartbeat.json", &RuntimeNodeHeartbeat{}},
		{"testdata/tier_c_enrichment_request.json", &TierCEnrichmentRequest{}},
		{"testdata/tier_c_enrichment_response.json", &TierCEnrichmentResponse{}},
		{"testdata/tier_c_enrichment_error_response.json", &TierCEnrichmentResponse{}},
	}

	for _, tc := range cases {
		fixture, err := os.ReadFile(tc.path)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(fixture, tc.dest); err != nil {
			t.Fatalf("%s did not unmarshal: %v", tc.path, err)
		}
		encoded, err := json.Marshal(tc.dest)
		if err != nil {
			t.Fatalf("%s did not marshal: %v", tc.path, err)
		}
		want := strings.TrimSpace(string(fixture))
		if string(encoded) != want {
			t.Fatalf("%s round-trip drifted:\nwant %s\n got %s", tc.path, want, encoded)
		}
	}
}

func readContractFixture(t *testing.T, path string) []byte {
	t.Helper()
	fixture, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	return fixture
}

func assertFixtureRoundTrip(t *testing.T, path string, dest any) {
	t.Helper()
	fixture := readContractFixture(t, path)
	if err := json.Unmarshal(fixture, dest); err != nil {
		t.Fatalf("%s did not unmarshal: %v", path, err)
	}
	encoded, err := json.Marshal(dest)
	if err != nil {
		t.Fatalf("%s did not marshal: %v", path, err)
	}
	want := strings.TrimSpace(string(fixture))
	if string(encoded) != want {
		t.Fatalf("%s round-trip drifted:\nwant %s\n got %s", path, want, encoded)
	}
}

func TestReasoningRequestFixtureRoundTrip(t *testing.T) {
	var req ReasoningRequest
	assertFixtureRoundTrip(t, "testdata/reasoning_request.json", &req)
	if req.RequestID == "" || req.DeviceID != "site-a" || req.ActionTierHint != ActionTierD {
		t.Fatalf("unexpected request fixture: %#v", req)
	}
	if len(req.Context.History) != 1 || req.Context.History[0].Value != 8.1 {
		t.Fatalf("unexpected request history fixture: %#v", req.Context.History)
	}
}

func TestReasoningResponseFixtureRoundTrip(t *testing.T) {
	var resp ReasoningResponse
	assertFixtureRoundTrip(t, "testdata/reasoning_response.json", &resp)
	if resp.RequestID == "" || resp.ActionTier != ActionTierD || resp.Error != nil {
		t.Fatalf("unexpected response fixture: %#v", resp)
	}
}

func TestReasoningErrorResponseFixtureIncludesError(t *testing.T) {
	var resp ReasoningResponse
	assertFixtureRoundTrip(t, "testdata/reasoning_error_response.json", &resp)
	if resp.RequestID != "7d4bd5ee-7f7e-4f11-bdab-a4b3fb3ca7a3" {
		t.Fatalf("error response request_id drifted: %q", resp.RequestID)
	}
	if resp.Error == nil || *resp.Error == "" {
		t.Fatalf("error response fixture must include error: %#v", resp)
	}
}

func TestHeartbeatFixtureRoundTrip(t *testing.T) {
	var hb Heartbeat
	fixture := readContractFixture(t, "testdata/heartbeat.json")
	assertFixtureRoundTrip(t, "testdata/heartbeat.json", &hb)
	if hb.UptimeS != 12.5 {
		t.Fatalf("heartbeat uptime_s = %v, want 12.5", hb.UptimeS)
	}
	var raw map[string]any
	if err := json.Unmarshal(fixture, &raw); err != nil {
		t.Fatal(err)
	}
	if raw["uptime_s"] != 12.5 {
		t.Fatalf("heartbeat fixture must use float uptime_s=12.5, got %#v", raw["uptime_s"])
	}
}

func TestRuntimeNodeHeartbeatFixtureRoundTrip(t *testing.T) {
	var hb RuntimeNodeHeartbeat
	assertFixtureRoundTrip(t, "testdata/runtime_node_heartbeat.json", &hb)
	if hb.DeviceID != "dev-01" || hb.ActiveTriggers == nil {
		t.Fatalf("unexpected runtime heartbeat fixture: %#v", hb)
	}
}

func TestSDKFixtureAlignmentDocumented(t *testing.T) {
	doc := string(readContractFixture(t, "testdata/README.md"))
	if !strings.Contains(doc, "ori-sdk") || !strings.Contains(doc, "canonical gateway fixture source") {
		t.Fatalf("fixture README must document SDK alignment, got: %s", doc)
	}
}

func TestHeartbeatWebhookBridgePostureJSON(t *testing.T) {
	beat := Heartbeat{
		Status:        "healthy",
		UptimeS:       1.5,
		Provider:      "echo",
		TimestampMS:   123,
		WebhookBridge: &WebhookBridgePosture{Enabled: true, Ready: true, ProviderCIDRCount: 2},
	}
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(payload), "webhook_bridge") {
		t.Fatalf("webhook bridge posture omitted: %s", payload)
	}
	var decoded Heartbeat
	if err := json.Unmarshal(payload, &decoded); err != nil {
		t.Fatal(err)
	}
	if decoded.WebhookBridge == nil || !decoded.WebhookBridge.Ready || decoded.WebhookBridge.ProviderCIDRCount != 2 {
		t.Fatalf("posture did not round-trip: %#v", decoded.WebhookBridge)
	}
}
