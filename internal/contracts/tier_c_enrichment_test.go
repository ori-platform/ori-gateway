// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts

import (
	"encoding/json"
	"strings"
	"testing"
)

func validTierCEnrichmentRequest() TierCEnrichmentRequest {
	return TierCEnrichmentRequest{
		RequestID:         "enrich-req-1",
		ProposalID:        "prop-abc123",
		DeviceID:          "site-a",
		SkillName:         "energy-anomaly-detector",
		TriggerName:       "generator_overrun",
		SensorID:          "current-main",
		SensorType:        "current_clamp",
		ReadingValue:      14.2,
		Unit:              "A",
		HistoryWindow:     []TierCEnrichmentHistorySample{{SensorID: "current-main", SensorType: "current_clamp", Unit: "A", TimestampMS: 1234500000, Value: 8.1, Quality: 0.98}},
		ProposedAction:    "open safety circuit for protected load",
		SafeDefaultAction: "send escalation alert and keep circuit closed",
		OperatorMessage:   "Generator current is above normal for this site.",
		TimeoutMS:         10000,
	}
}

func validTierCEnrichmentResponse() TierCEnrichmentResponse {
	return TierCEnrichmentResponse{
		RequestID:                  "enrich-req-1",
		ProposalID:                 "prop-abc123",
		Explanation:                "The generator load has stayed above the normal weekly pattern.",
		EstimatedImpact:            "Likely extra diesel cost.",
		RecommendedOperatorContext: "Check whether grid power has returned.",
		Provider:                   "gemini",
		Model:                      "gemini-2.5-pro",
		TokensUsed:                 180,
		LatencyMS:                  900,
	}
}

func TestTierCEnrichmentContractRoundTrip(t *testing.T) {
	req := validTierCEnrichmentRequest()
	encodedReq, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	var decodedReq TierCEnrichmentRequest
	if err := json.Unmarshal(encodedReq, &decodedReq); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTierCEnrichmentRequest(decodedReq); err != nil {
		t.Fatal(err)
	}
	if decodedReq.ProposalID != req.ProposalID || decodedReq.HistoryWindow[0].Value != 8.1 {
		t.Fatalf("request round-trip drifted: %#v", decodedReq)
	}

	resp := validTierCEnrichmentResponse()
	encodedResp, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var decodedResp TierCEnrichmentResponse
	if err := json.Unmarshal(encodedResp, &decodedResp); err != nil {
		t.Fatal(err)
	}
	if err := ValidateTierCEnrichmentResponseForRequest(req, decodedResp); err != nil {
		t.Fatal(err)
	}
	if decodedResp.Explanation != resp.Explanation || decodedResp.EstimatedImpact == "" {
		t.Fatalf("response round-trip drifted: %#v", decodedResp)
	}
}

func TestTierCEnrichmentCannotChangeActionAuthority(t *testing.T) {
	resp := validTierCEnrichmentResponse()
	resp.Explanation = "The relay controlling the main bus may overheat if not approved."
	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	forbidden := []string{
		"action_tier",
		"action_name",
		"safe_default_action",
		"approval_required",
		"relay",
		"actuator",
	}
	for _, key := range forbidden {
		if _, ok := fields[key]; ok {
			t.Fatalf("enrichment response must not contain authority field %q: %s", key, encoded)
		}
	}
}

func TestTierCEnrichmentDropsInjectedAuthorityFields(t *testing.T) {
	payload := []byte(`{"request_id":"enrich-req-1","proposal_id":"prop-abc123","explanation":"ok","action_tier":"C","safe_default_action":"keep closed","approval_required":false}`)
	var resp TierCEnrichmentResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatal(err)
	}
	encoded, err := json.Marshal(resp)
	if err != nil {
		t.Fatal(err)
	}
	var fields map[string]any
	if err := json.Unmarshal(encoded, &fields); err != nil {
		t.Fatal(err)
	}
	for _, key := range []string{"action_tier", "safe_default_action", "approval_required"} {
		if _, ok := fields[key]; ok {
			t.Fatalf("injected authority field %q survived typed decode: %s", key, encoded)
		}
	}
}

func TestValidateTierCEnrichmentResponseForRequestRequiresCorrelation(t *testing.T) {
	req := validTierCEnrichmentRequest()
	resp := validTierCEnrichmentResponse()
	resp.RequestID = "other-request"
	if err := ValidateTierCEnrichmentResponseForRequest(req, resp); err == nil {
		t.Fatal("expected request_id mismatch")
	}

	resp = validTierCEnrichmentResponse()
	resp.ProposalID = "other-proposal"
	if err := ValidateTierCEnrichmentResponseForRequest(req, resp); err == nil {
		t.Fatal("expected proposal_id mismatch")
	}
}

func TestTierCEnrichmentErrorResponseValidation(t *testing.T) {
	req := validTierCEnrichmentRequest()
	resp := NewTierCEnrichmentErrorResponse(req.RequestID, req.ProposalID, "provider unavailable")
	if err := ValidateTierCEnrichmentResponseForRequest(req, resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || *resp.Error != "provider unavailable" {
		t.Fatalf("unexpected error response: %#v", resp)
	}

	blank := NewTierCEnrichmentErrorResponse(req.RequestID, req.ProposalID, "")
	if err := ValidateTierCEnrichmentResponseForRequest(req, blank); err != nil {
		t.Fatal(err)
	}
	if blank.Error == nil || *blank.Error != "enrichment unavailable" {
		t.Fatalf("blank error message was not normalized: %#v", blank)
	}
}

func TestValidateTierCEnrichmentRequestRejectsInvalidShape(t *testing.T) {
	cases := []struct {
		name string
		mut  func(*TierCEnrichmentRequest)
		want string
	}{
		{name: "bad request id", mut: func(r *TierCEnrichmentRequest) { r.RequestID = "req 1" }, want: "request_id"},
		{name: "missing proposal", mut: func(r *TierCEnrichmentRequest) { r.ProposalID = "" }, want: "proposal_id"},
		{name: "bad proposal", mut: func(r *TierCEnrichmentRequest) { r.ProposalID = "prop/evil" }, want: "proposal_id"},
		{name: "bad device", mut: func(r *TierCEnrichmentRequest) { r.DeviceID = "site/a" }, want: "device_id"},
		{name: "missing skill", mut: func(r *TierCEnrichmentRequest) { r.SkillName = "" }, want: "skill_name"},
		{name: "missing trigger", mut: func(r *TierCEnrichmentRequest) { r.TriggerName = "" }, want: "trigger_name"},
		{name: "missing sensor", mut: func(r *TierCEnrichmentRequest) { r.SensorID = "" }, want: "sensor_id"},
		{name: "missing sensor type", mut: func(r *TierCEnrichmentRequest) { r.SensorType = "" }, want: "sensor_type"},
		{name: "missing proposed action", mut: func(r *TierCEnrichmentRequest) { r.ProposedAction = "" }, want: "proposed_action"},
		{name: "missing safe default", mut: func(r *TierCEnrichmentRequest) { r.SafeDefaultAction = "" }, want: "safe_default_action"},
		{name: "missing operator message", mut: func(r *TierCEnrichmentRequest) { r.OperatorMessage = "" }, want: "operator_message"},
		{name: "zero timeout", mut: func(r *TierCEnrichmentRequest) { r.TimeoutMS = 0 }, want: "timeout_ms"},
		{name: "negative timeout", mut: func(r *TierCEnrichmentRequest) { r.TimeoutMS = -1 }, want: "timeout_ms"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			req := validTierCEnrichmentRequest()
			tc.mut(&req)
			err := ValidateTierCEnrichmentRequest(req)
			if err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want error containing %q", err, tc.want)
			}
		})
	}
}
