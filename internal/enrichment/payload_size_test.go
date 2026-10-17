// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package enrichment

import (
	"context"
	"encoding/json"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

const topInt = 9007199254740991

// TestRequiredRepliesFitTheFloor builds the enrichment error response with
// the handler's own encoder at the vendored corpus's bounds.
func TestRequiredRepliesFitTheFloor(t *testing.T) {
	want := sizeReply(t, "enrichment_error")
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "site-local-secret"})
	if err != nil {
		t.Fatal(err)
	}
	h, err := NewHandler(&fakePublisher{}, &fakeEnrichmentProvider{}, Options{
		AuthVerifier: verifier, SigningSecret: "site-local-secret", Now: func() time.Time { return time.UnixMilli(topInt) },
	})
	if err != nil {
		t.Fatal(err)
	}
	routing := strings.Repeat("a", 128)
	req := contracts.TierCEnrichmentRequest{RequestID: routing, ProposalID: "AAAAAAAA", DeviceID: routing}
	payload, err := h.encodeResponse(req, contracts.NewTierCEnrichmentErrorResponse(routing, "AAAAAAAA", routing, strings.Repeat("\U0001F600", 1000)))
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > want.Length || want.Length > contracts.PayloadFloor || want.Verdict != "admitted" {
		t.Fatalf("enrichment error is %d bytes; the corpus bound is %d (%s) against the %d-byte floor", len(payload), want.Length, want.Verdict, contracts.PayloadFloor)
	}
	var resp contracts.TierCEnrichmentResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || utf8.RuneCountInString(*resp.Error) != contracts.MaxErrorScalars ||
		resp.Explanation != "" || resp.EstimatedImpact != "" || resp.RecommendedOperatorContext != "" || resp.Provider != "" || resp.Model != "" {
		t.Fatalf("error response carries free-form content beyond its bounded error: %+v", resp)
	}
}

func TestAnOversizeEnrichmentResponseIsReplacedByResponseTooLarge(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{response: contracts.TierCEnrichmentResponse{
		Explanation: strings.Repeat("x", contracts.PayloadMaxima[contracts.FamilyEnrichmentResponse]), Provider: "fake",
	}}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := validRequest()
	if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
		t.Fatal(err)
	}
	if len(pub.published) != 1 || len(pub.published[0].payload) > contracts.PayloadMaxima[contracts.FamilyEnrichmentResponse] {
		t.Fatal("a response over the topic maximum was published")
	}
	resp := pub.onlyResponse(t)
	if resp.Error == nil || *resp.Error != contracts.ErrorResponseTooLarge || resp.RequestID != req.RequestID || resp.ProposalID != req.ProposalID || resp.Explanation != "" {
		t.Fatalf("response = %+v, want a correlated response_too_large error", resp)
	}
}

// TestACorrelationOutsideItsFormIsRefusedUnanswered sends no error response
// for a request whose request_id or proposal_id an error reply could not echo
// within the floor.
func TestACorrelationOutsideItsFormIsRefusedUnanswered(t *testing.T) {
	for _, mutate := range []func(*contracts.TierCEnrichmentRequest){
		func(r *contracts.TierCEnrichmentRequest) { r.RequestID = strings.Repeat("a", 129) },
		func(r *contracts.TierCEnrichmentRequest) { r.ProposalID = strings.Repeat("p", 100000) },
		func(r *contracts.TierCEnrichmentRequest) { r.RequestID = "req 1" },
	} {
		pub, prov := &fakePublisher{}, &fakeEnrichmentProvider{}
		h, err := NewHandler(pub, prov, Options{})
		if err != nil {
			t.Fatal(err)
		}
		req := validRequest()
		mutate(&req)
		if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err == nil || len(pub.published) != 0 || len(prov.requests) != 0 {
			t.Fatalf("err=%v published=%d provider=%d; want refused unanswered", err, len(pub.published), len(prov.requests))
		}
	}
}

type sizeReplyCase struct {
	Name    string `json:"name"`
	Length  int    `json:"length"`
	Verdict string `json:"verdict"`
}

func sizeReply(t *testing.T, name string) sizeReplyCase {
	t.Helper()
	raw, err := specvectors.Read("gateway-api/vectors/payload-size-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Replies []sizeReplyCase `json:"replies"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, c := range corpus.Replies {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the corpus has no %s reply", name)
	return sizeReplyCase{}
}

// TestAProviderReturnedErrorIsSentAsTheBoundedErrorResponse drives a provider
// that fills the response's own error, short and long, with advisory text
// beside it, through HandleRequest.
func TestAProviderReturnedErrorIsSentAsTheBoundedErrorResponse(t *testing.T) {
	for _, message := range []string{"provider refused", strings.Repeat("E", 5000)} {
		pub := &fakePublisher{}
		prov := &fakeEnrichmentProvider{response: contracts.TierCEnrichmentResponse{
			Error: &message, Explanation: "retained text", Model: "retained model", Provider: "fake",
		}}
		h, err := NewHandler(pub, prov, Options{})
		if err != nil {
			t.Fatal(err)
		}
		req := validRequest()
		if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
			t.Fatal(err)
		}
		resp := pub.onlyResponse(t)
		if len(pub.published[0].payload) > contracts.PayloadFloor || resp.Error == nil ||
			utf8.RuneCountInString(*resp.Error) > contracts.MaxErrorScalars || !strings.HasPrefix(message, *resp.Error) ||
			resp.Explanation != "" || resp.Model != "" || resp.Provider != "" || resp.RequestID != req.RequestID || resp.ProposalID != req.ProposalID {
			t.Fatalf("%d bytes, response %+v; want the bounded, correlated error response", len(pub.published[0].payload), resp)
		}
	}
}
