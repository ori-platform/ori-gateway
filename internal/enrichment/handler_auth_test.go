// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package enrichment

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

const (
	currentSecret  = "current-shared-secret-for-enrichment-tests"
	previousSecret = "previous-shared-secret-for-enrichment-tests"
)

func fixedNow() time.Time { return time.UnixMilli(1_700_000_000_000) }

func newVerifier(t *testing.T, secret, previous string) *mqttauth.Verifier {
	t.Helper()
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: secret, PreviousSharedSecret: previous, Now: fixedNow})
	if err != nil {
		t.Fatal(err)
	}
	return verifier
}

func newAuthenticatedHandler(t *testing.T, pub *fakePublisher, provider *fakeEnrichmentProvider) *Handler {
	t.Helper()
	handler, err := NewHandler(pub, provider, Options{
		AuthVerifier:  newVerifier(t, currentSecret, previousSecret),
		SigningSecret: currentSecret,
		Now:           fixedNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	return handler
}

func signedRequestPayload(t *testing.T, req contracts.TierCEnrichmentRequest, secret string) []byte {
	t.Helper()
	req.Auth = nil
	auth, err := mqttauth.Sign(req, contracts.TierCEnrichmentRequestMessageType, req.DeviceID, req.RequestID, fixedNow().UnixMilli(), secret)
	if err != nil {
		t.Fatal(err)
	}
	req.Auth = &auth
	return requestPayload(t, req)
}

func (p *fakePublisher) onlyResponsePayload(t *testing.T) []byte {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(p.published))
	}
	return p.published[0].payload
}

func (p *fakePublisher) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.published)
}

func (p *fakeEnrichmentProvider) count() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.requests)
}

func assertDropped(t *testing.T, err error, pub *fakePublisher, provider *fakeEnrichmentProvider) {
	t.Helper()
	if !errors.Is(err, errUnauthenticated) {
		t.Fatalf("error = %v, want unauthenticated", err)
	}
	if n := pub.count(); n != 0 {
		t.Fatalf("published %d messages for an unauthenticated request, want none", n)
	}
	if n := provider.count(); n != 0 {
		t.Fatalf("provider invoked %d times for an unauthenticated request, want never", n)
	}
}

func TestNewHandlerRequiresVerifierAndSigningSecretTogether(t *testing.T) {
	verifier := newVerifier(t, currentSecret, "")
	for name, opts := range map[string]Options{
		"verifier only": {AuthVerifier: verifier},
		"secret only":   {SigningSecret: currentSecret},
		"blank secret":  {AuthVerifier: verifier, SigningSecret: "   "},
	} {
		if _, err := NewHandler(&fakePublisher{}, &fakeEnrichmentProvider{}, opts); err == nil {
			t.Fatalf("%s: NewHandler accepted a half-configured envelope", name)
		}
	}
}

func TestHandlerSignedRequestIsAnsweredWithSignedResponse(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)

	err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", signedRequestPayload(t, validRequest(), currentSecret))
	if err != nil {
		t.Fatal(err)
	}
	if provider.count() != 1 {
		t.Fatalf("provider invoked %d times, want 1", provider.count())
	}
	if provider.requests[0].Auth != nil {
		t.Fatal("the request envelope must not reach the provider")
	}

	payload := pub.onlyResponsePayload(t)
	verified, err := newVerifier(t, currentSecret, "").VerifyJSON(payload, contracts.TierCEnrichmentResponseMessageType, "dev-01", "req-1")
	if err != nil {
		t.Fatalf("response does not verify: %v", err)
	}
	if verified["explanation"] != "The proposed shutdown is advisory and approval-gated." {
		t.Fatalf("verified response = %v", verified)
	}
	var resp contracts.TierCEnrichmentResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Auth == nil || resp.Auth.SignedAtMS != fixedNow().UnixMilli() {
		t.Fatalf("response auth = %+v", resp.Auth)
	}
	if resp.DeviceID != "dev-01" {
		t.Fatalf("response device_id = %q, want the request's device", resp.DeviceID)
	}
}

func TestHandlerErrorResponseIsSigned(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{err: errors.New("provider unavailable")}
	handler := newAuthenticatedHandler(t, pub, provider)

	if err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", signedRequestPayload(t, validRequest(), currentSecret)); err != nil {
		t.Fatal(err)
	}
	verified, err := newVerifier(t, currentSecret, "").VerifyJSON(pub.onlyResponsePayload(t), contracts.TierCEnrichmentResponseMessageType, "dev-01", "req-1")
	if err != nil {
		t.Fatalf("error response does not verify: %v", err)
	}
	if verified["error"] == nil {
		t.Fatalf("verified payload lacks the error: %v", verified)
	}
}

func TestHandlerUnsignedRequestIsDroppedWhenAuthEnabled(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)

	err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, validRequest()))
	assertDropped(t, err, pub, provider)
}

func TestHandlerUnsignedInvalidRequestGetsNoErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)

	req := validRequest()
	req.ProposalID = ""
	err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req))
	assertDropped(t, err, pub, provider)
}

func TestHandlerTamperedRequestIsDropped(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)

	var envelope map[string]any
	if err := json.Unmarshal(signedRequestPayload(t, validRequest(), currentSecret), &envelope); err != nil {
		t.Fatal(err)
	}
	envelope["proposed_action"] = "open_main_breaker"
	payload, err := json.Marshal(envelope)
	if err != nil {
		t.Fatal(err)
	}
	assertDropped(t, handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", payload), pub, provider)
}

func TestHandlerWrongSecretRequestIsDropped(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)

	err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", signedRequestPayload(t, validRequest(), "some-other-secret"))
	assertDropped(t, err, pub, provider)
}

func TestHandlerReplayedRequestIsDropped(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)
	payload := signedRequestPayload(t, validRequest(), currentSecret)

	if err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", payload); err != nil {
		t.Fatal(err)
	}
	err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", payload)
	if !errors.Is(err, errUnauthenticated) {
		t.Fatalf("replay error = %v, want unauthenticated", err)
	}
	if pub.count() != 1 || provider.count() != 1 {
		t.Fatalf("replay reached the provider or the broker: published=%d provider=%d", pub.count(), provider.count())
	}
}

func TestHandlerTopicDeviceMismatchIsRefusedDespiteValidSignature(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)

	err := handler.HandleRequest(context.Background(), "ori/dev-02/tier_c/enrichment/request", signedRequestPayload(t, validRequest(), currentSecret))
	if err == nil || errors.Is(err, errUnauthenticated) {
		t.Fatalf("error = %v, want a device mismatch refusal independent of the signature", err)
	}
	if pub.count() != 0 || provider.count() != 0 {
		t.Fatalf("mismatched request had side effects: published=%d provider=%d", pub.count(), provider.count())
	}
}

func TestHandlerPreviousSecretVerifiesRequestButSignsResponseWithCurrent(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler := newAuthenticatedHandler(t, pub, provider)

	if err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", signedRequestPayload(t, validRequest(), previousSecret)); err != nil {
		t.Fatal(err)
	}
	payload := pub.onlyResponsePayload(t)
	if _, err := newVerifier(t, currentSecret, "").VerifyJSON(payload, contracts.TierCEnrichmentResponseMessageType, "dev-01", "req-1"); err != nil {
		t.Fatalf("response must verify under the current secret: %v", err)
	}
	if _, err := newVerifier(t, previousSecret, "").VerifyJSON(payload, contracts.TierCEnrichmentResponseMessageType, "dev-01", "req-1"); err == nil {
		t.Fatal("response must not be signed with the previous secret")
	}
}

func TestHandlerWithoutAuthAcceptsSignedRequestsAndAnswersUnsigned(t *testing.T) {
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler, err := NewHandler(pub, provider, Options{})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", signedRequestPayload(t, validRequest(), currentSecret)); err != nil {
		t.Fatal(err)
	}
	if resp := pub.onlyResponse(t); resp.Auth != nil {
		t.Fatalf("auth-disabled handler signed a response: %+v", resp.Auth)
	}
}

// The fixture was signed by the runtime's GatewayMessageAuthenticator under the
// same secret and signed_at_ms, so it proves the Go verifier and the runtime
// signer agree on canonical bytes for non-ASCII request content.
func TestHandlerAcceptsRuntimeSignedUnicodeRequest(t *testing.T) {
	payload, err := os.ReadFile("testdata/runtime_signed_request_unicode.json")
	if err != nil {
		t.Fatal(err)
	}
	pub := &fakePublisher{}
	provider := &fakeEnrichmentProvider{}
	handler, err := NewHandler(pub, provider, Options{
		AuthVerifier:  newVerifier(t, "published-test-enrichment-envelope-secret", ""),
		SigningSecret: "published-test-enrichment-envelope-secret",
		Now:           fixedNow,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", payload); err != nil {
		t.Fatal(err)
	}
	if provider.count() != 1 || provider.requests[0].OperatorMessage != "Approve HVAC scale-back at Ikẹjà? Ünïcode ✓ — 電力" {
		t.Fatalf("provider requests = %+v", provider.requests)
	}
	if _, err := newVerifier(t, "published-test-enrichment-envelope-secret", "").VerifyJSON(pub.onlyResponsePayload(t), contracts.TierCEnrichmentResponseMessageType, "dev-01", "req-unicode-1"); err != nil {
		t.Fatal(err)
	}
}
