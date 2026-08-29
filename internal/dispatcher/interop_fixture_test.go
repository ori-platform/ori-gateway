// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package dispatcher

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// interopFixturePath holds the responses this dispatcher emits for a fixed
// clock and the published test secret, exactly as published. ori-runtime
// vendors the file and verifies every response with its own
// GatewayMessageAuthenticator, which is the inverse of the Unicode request
// fixture: runtime-signed bytes into Go, Go-signed bytes into the runtime.
// Regenerate with ORI_UPDATE_INTEROP_FIXTURE=1 when emission changes, and
// carry the new file to the runtime in the same change.
const interopFixturePath = "testdata/gateway_signed_reasoning_responses.json"

type interopFixture struct {
	Secret   string            `json:"secret"`
	SignedAt int64             `json:"signed_at_ms"`
	DeviceID string            `json:"device_id"`
	Cases    []interopResponse `json:"cases"`
}

type interopResponse struct {
	Name               string          `json:"name"`
	RequestID          string          `json:"request_id"`
	ProviderConfidence float64         `json:"provider_confidence"`
	EmittedConfidence  float64         `json:"emitted_confidence"`
	ProviderError      string          `json:"provider_error,omitempty"`
	Response           json.RawMessage `json:"response"`
}

func TestGatewaySignedResponsesMatchTheRuntimeInteropFixture(t *testing.T) {
	const secret = "published-test-reasoning-envelope-secret"
	fixture := interopFixture{Secret: secret, SignedAt: fixedNow().UnixMilli(), DeviceID: "site-a"}
	for _, tc := range []struct {
		name       string
		requestID  string
		confidence float64
		err        error
	}{
		{"below_agreement_zone_rounds_to_zero", "interop-1", 0.000042, nil},
		{"half_unit_rounds_up", "interop-2", 0.00005, nil},
		{"ordinary_confidence", "interop-3", 0.93, nil},
		{"five_decimals_round_to_four", "interop-4", 0.123456, nil},
		{"unit_confidence", "interop-5", 1, nil},
		{"error_response", "interop-6", 0, errors.New("provider unavailable")},
	} {
		pub := &fakePublisher{}
		prov := &fakeProvider{err: tc.err}
		if tc.err == nil {
			prov.response = contracts.ReasoningResponse{RequestID: tc.requestID, Text: "Ikẹjà ✓ 電力 — " + tc.name, Model: "fake-model", TokensUsed: 3, LatencyMS: 4, Confidence: tc.confidence, ActionTier: contracts.ActionTierC}
		}
		d := newTestDispatcher(t, pub, prov, Options{
			ProviderTimeoutMS: 1000,
			AuthVerifier:      newVerifier(t, secret, ""),
			SigningSecret:     secret,
			Now:               fixedNow,
		})
		payload := signedPayloadWithSecret(t, secret, tc.requestID)
		if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", payload); err != nil {
			t.Fatal(err)
		}
		emitted := pub.onlyPayload(t)
		var resp contracts.ReasoningResponse
		if err := json.Unmarshal(emitted, &resp); err != nil {
			t.Fatal(err)
		}
		fixture.Cases = append(fixture.Cases, interopResponse{
			Name: tc.name, RequestID: tc.requestID, ProviderConfidence: tc.confidence,
			EmittedConfidence: resp.Confidence, ProviderError: errText(tc.err), Response: json.RawMessage(emitted),
		})
	}
	encoded, err := json.MarshalIndent(fixture, "", "  ")
	if err != nil {
		t.Fatal(err)
	}
	encoded = append(encoded, '\n')
	if os.Getenv("ORI_UPDATE_INTEROP_FIXTURE") == "1" {
		if err := os.WriteFile(interopFixturePath, encoded, 0o644); err != nil {
			t.Fatal(err)
		}
	}
	stored, err := os.ReadFile(interopFixturePath)
	if err != nil {
		t.Fatalf("%v; regenerate with ORI_UPDATE_INTEROP_FIXTURE=1", err)
	}
	if !bytes.Equal(stored, encoded) {
		t.Fatal("the stored interop fixture no longer matches what the dispatcher emits; regenerate it and carry it to ori-runtime")
	}
}

func signedPayloadWithSecret(t *testing.T, secret, requestID string) []byte {
	t.Helper()
	return signedPayload(t, secret, func(r *contracts.ReasoningRequest) { r.RequestID = requestID })
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}
