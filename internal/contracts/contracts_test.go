// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import "testing"

func TestTopicsMatchGatewaySpec(t *testing.T) {
	reqTopic, err := RequestTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if reqTopic != "ori/gateway/site-a/reason/request" {
		t.Fatalf("unexpected request topic: %s", reqTopic)
	}

	respTopic, err := ResponseTopic("site-a")
	if err != nil {
		t.Fatal(err)
	}
	if respTopic != "ori/gateway/site-a/reason/response" {
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
