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
