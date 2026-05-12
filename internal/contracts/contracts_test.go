// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import (
	"encoding/json"
	"os"
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
}

func TestValidateResponseForRequestRequiresCorrelation(t *testing.T) {
	req := ReasoningRequest{RequestID: "req-1", DeviceID: "site-a", Prompt: "p", ActionTierHint: "A"}
	resp := ReasoningResponse{RequestID: "other", ActionTier: "A"}
	if err := ValidateResponseForRequest(req, resp); err == nil {
		t.Fatal("expected request_id mismatch error")
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
		if string(encoded) != string(fixture) {
			t.Fatalf("%s round-trip drifted:\nwant %s\n got %s", tc.path, fixture, encoded)
		}
	}
}
