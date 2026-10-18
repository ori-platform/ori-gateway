// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package dispatcher

import (
	"context"
	"encoding/json"
	"math"
	"strings"
	"testing"
	"time"
	"unicode/utf8"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

const topInt = 9007199254740991

// TestRequiredRepliesFitTheFloor builds the reasoning error response with the
// dispatcher's own encoder at the vendored corpus's bounds: routing members
// at 128 characters, an error far over 128 emoji, signed at the widest time.
func TestRequiredRepliesFitTheFloor(t *testing.T) {
	want := sizeReply(t, "reasoning_error")
	d := newTestDispatcher(t, &fakePublisher{}, &fakeProvider{}, Options{
		ProviderTimeoutMS: 1000, AuthVerifier: newVerifier(t, "site-local-secret", ""),
		SigningSecret: "site-local-secret", Now: func() time.Time { return time.UnixMilli(topInt) },
	})
	routing := strings.Repeat("a", 128)
	req := contracts.ReasoningRequest{RequestID: routing, DeviceID: routing, ActionTierHint: contracts.ActionTierA}
	payload, err := d.encodeResponse(req, contracts.NewErrorResponse(routing, routing, contracts.ActionTierA, strings.Repeat("\U0001F600", 1000)))
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > want.Length || want.Length > contracts.PayloadFloor || want.Verdict != "admitted" {
		t.Fatalf("reasoning error is %d bytes; the corpus bound is %d (%s) against the %d-byte floor", len(payload), want.Length, want.Verdict, contracts.PayloadFloor)
	}
	var resp contracts.ReasoningResponse
	if err := json.Unmarshal(payload, &resp); err != nil {
		t.Fatal(err)
	}
	if resp.Error == nil || utf8.RuneCountInString(*resp.Error) != contracts.MaxErrorScalars || resp.Text != "" || resp.Model != "" {
		t.Fatalf("error response carries free-form content beyond its bounded error: %+v", resp)
	}
}

// TestAnOversizeReasoningResponseIsReplacedByResponseTooLarge publishes the
// family's error response, still correlated, in place of a response over the
// topic maximum.
func TestAnOversizeReasoningResponseIsReplacedByResponseTooLarge(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{response: contracts.ReasoningResponse{
		RequestID: "req-1", Text: strings.Repeat("x", contracts.PayloadMaxima[contracts.FamilyReasoningResponse]),
		Model: "fake-model", Confidence: 0.5, ActionTier: contracts.ActionTierC,
	}}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})
	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
		t.Fatal(err)
	}
	payload := pub.onlyPayload(t)
	if len(payload) > contracts.PayloadMaxima[contracts.FamilyReasoningResponse] {
		t.Fatalf("published %d bytes over the topic maximum", len(payload))
	}
	resp := pub.publishedResponse(t)
	if resp.Error == nil || *resp.Error != contracts.ErrorResponseTooLarge || resp.RequestID != "req-1" || resp.Text != "" {
		t.Fatalf("response = %+v, want a correlated response_too_large error", resp)
	}
}

// TestARequestIDOutsideItsFormIsRefusedUnanswered refuses a request whose
// request_id is not 1 to 128 characters of [A-Za-z0-9._-]: no reply could
// echo it within the floor, so none is sent.
func TestARequestIDOutsideItsFormIsRefusedUnanswered(t *testing.T) {
	for _, requestID := range []string{strings.Repeat("a", 129), "req 1", "req:1", "réq"} {
		pub, prov := &fakePublisher{}, &fakeProvider{}
		d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})
		err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, func(r *contracts.ReasoningRequest) { r.RequestID = requestID }))
		if err == nil || pub.callCount() != 0 || prov.callCount() != 0 {
			t.Fatalf("request_id %q: err=%v, published %d, provider calls %d; want refused unanswered", requestID, err, pub.callCount(), prov.callCount())
		}
	}
	pub := &fakePublisher{}
	d := newTestDispatcher(t, pub, &fakeProvider{}, Options{ProviderTimeoutMS: 1000})
	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, func(r *contracts.ReasoningRequest) { r.RequestID = "req.1_" + strings.Repeat("a", 122) })); err != nil || pub.callCount() != 1 {
		t.Fatalf("a request_id in its form was not answered: %v", err)
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
// that fills the response's own error, short and long, with other free-form
// content beside it, through HandleRequest: the reply is the family's error
// response, correlated, within the floor.
func TestAProviderReturnedErrorIsSentAsTheBoundedErrorResponse(t *testing.T) {
	for _, message := range []string{"provider refused", strings.Repeat("E", 5000)} {
		pub := &fakePublisher{}
		prov := &fakeProvider{response: contracts.ReasoningResponse{
			RequestID: "req-1", ActionTier: contracts.ActionTierC, Error: &message,
			Text: "retained text", Model: "retained model", TokensUsed: 7,
		}}
		d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})
		if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
			t.Fatal(err)
		}
		payload := pub.onlyPayload(t)
		resp := pub.publishedResponse(t)
		if len(payload) > contracts.PayloadFloor || resp.Error == nil || utf8.RuneCountInString(*resp.Error) > contracts.MaxErrorScalars ||
			!strings.HasPrefix(message, *resp.Error) || resp.Text != "" || resp.Model != "" || resp.RequestID != "req-1" {
			t.Fatalf("%d bytes, response %+v; want the bounded, correlated error response", len(payload), resp)
		}
	}
}

// TestAResponseThatCannotBeEncodedIsAnsweredWithAnError has a provider
// return a NaN confidence, which JSON cannot carry: the request is still
// answered, with the family's correlated error response.
func TestAResponseThatCannotBeEncodedIsAnsweredWithAnError(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{response: contracts.ReasoningResponse{
		RequestID: "req-1", Text: "x", Model: "m", Confidence: math.NaN(), ActionTier: contracts.ActionTierC,
	}}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})
	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
		t.Fatalf("HandleRequest = %v, want the request answered", err)
	}
	resp := pub.publishedResponse(t)
	if resp.Error == nil || resp.RequestID != "req-1" {
		t.Fatalf("response = %+v, want a correlated error response", resp)
	}
}
