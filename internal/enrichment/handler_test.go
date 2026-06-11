// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package enrichment

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

type fakePublisher struct {
	mu        sync.Mutex
	published []publishedMessage
	err       error
}

type publishedMessage struct {
	topic   string
	qos     byte
	retain  bool
	payload []byte
}

func (p *fakePublisher) Publish(_ context.Context, topic string, qos byte, retain bool, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.err != nil {
		return p.err
	}
	p.published = append(p.published, publishedMessage{topic: topic, qos: qos, retain: retain, payload: append([]byte(nil), payload...)})
	return nil
}

func (p *fakePublisher) onlyResponse(t *testing.T) contracts.TierCEnrichmentResponse {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.published) != 1 {
		t.Fatalf("published messages = %d, want 1", len(p.published))
	}
	msg := p.published[0]
	if msg.topic != "ori/dev-01/tier_c/enrichment/response" {
		t.Fatalf("topic = %q", msg.topic)
	}
	if msg.qos != broker.QoSReasoning {
		t.Fatalf("qos = %d", msg.qos)
	}
	if msg.retain {
		t.Fatal("tier c enrichment responses must not be retained")
	}
	var resp contracts.TierCEnrichmentResponse
	if err := json.Unmarshal(msg.payload, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

type fakeEnrichmentProvider struct {
	mu       sync.Mutex
	requests []contracts.TierCEnrichmentRequest
	response contracts.TierCEnrichmentResponse
	err      error
	sleep    time.Duration
	panicSet bool
}

func (p *fakeEnrichmentProvider) EnrichTierC(ctx context.Context, req contracts.TierCEnrichmentRequest) (contracts.TierCEnrichmentResponse, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	if p.panicSet {
		panic("boom")
	}
	if p.sleep > 0 {
		select {
		case <-time.After(p.sleep):
		case <-ctx.Done():
			return contracts.TierCEnrichmentResponse{}, ctx.Err()
		}
	}
	if p.err != nil {
		return contracts.TierCEnrichmentResponse{}, p.err
	}
	if p.response.RequestID != "" || p.response.ProposalID != "" || p.response.Explanation != "" || p.response.Provider != "" || p.response.Error != nil {
		return p.response, nil
	}
	return contracts.TierCEnrichmentResponse{
		Explanation:                "The proposed shutdown is advisory and approval-gated.",
		EstimatedImpact:            "May reduce load by 2kW.",
		RecommendedOperatorContext: "Check whether staff are still on site before approving.",
		Provider:                   "fake",
		Model:                      "fake-model",
		TokensUsed:                 3,
		LatencyMS:                  4,
	}, nil
}

func validRequest() contracts.TierCEnrichmentRequest {
	return contracts.TierCEnrichmentRequest{
		RequestID:         "req-1",
		ProposalID:        "proposal-1",
		DeviceID:          "dev-01",
		SkillName:         "energy-anomaly-detector",
		TriggerName:       "sustained_high_load",
		SensorID:          "current-main",
		SensorType:        "current_clamp",
		ReadingValue:      18.4,
		Unit:              "A",
		HistoryWindow:     []contracts.TierCEnrichmentHistorySample{{SensorID: "current-main", SensorType: "current_clamp", Unit: "A", TimestampMS: 1234567890000, Value: 8.1, Quality: 0.99}},
		ProposedAction:    "open_hvac_contactor",
		SafeDefaultAction: "alert_operator",
		OperatorMessage:   "Approve HVAC scale-back?",
		TimeoutMS:         100,
	}
}

func requestPayload(t *testing.T, req contracts.TierCEnrichmentRequest) []byte {
	t.Helper()
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestHandlerPublishesAdvisoryResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := validRequest()
	if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
		t.Fatal(err)
	}
	resp := pub.onlyResponse(t)
	if resp.RequestID != req.RequestID || resp.ProposalID != req.ProposalID {
		t.Fatalf("correlation lost: %#v", resp)
	}
	if resp.Explanation == "" || resp.Provider != "fake" || resp.Model != "fake-model" {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if resp.Error != nil {
		t.Fatalf("unexpected error response: %#v", resp.Error)
	}
}

func TestHandlerProviderFailurePublishesErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{err: errors.New("secret provider detail")}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := validRequest()
	if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
		t.Fatal(err)
	}
	resp := pub.onlyResponse(t)
	if resp.Error == nil || *resp.Error != "provider error" {
		t.Fatalf("error = %#v", resp.Error)
	}
}

func TestHandlerTimeoutPublishesTimeoutErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{sleep: 50 * time.Millisecond}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := validRequest()
	req.TimeoutMS = 1
	if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
		t.Fatal(err)
	}
	resp := pub.onlyResponse(t)
	if resp.Error == nil || *resp.Error != "provider timeout" {
		t.Fatalf("error = %#v", resp.Error)
	}
}

func TestHandlerProviderPanicPublishesErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{panicSet: true}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := validRequest()
	if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
		t.Fatal(err)
	}
	resp := pub.onlyResponse(t)
	if resp.Error == nil || *resp.Error != "provider panic" {
		t.Fatalf("error = %#v", resp.Error)
	}
}

func TestHandlerMalformedJSONDoesNotPublish(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	err = h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", []byte("{"))
	if err == nil || !strings.Contains(err.Error(), "decode request") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.published) != 0 {
		t.Fatalf("unexpected publish: %#v", pub.published)
	}
}

func TestHandlerValidationFailurePublishesErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := validRequest()
	req.ProposedAction = ""
	if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
		t.Fatal(err)
	}
	resp := pub.onlyResponse(t)
	if resp.RequestID != req.RequestID || resp.ProposalID != req.ProposalID {
		t.Fatalf("correlation lost: %#v", resp)
	}
	if resp.Error == nil || *resp.Error != "invalid enrichment request" {
		t.Fatalf("error = %#v", resp.Error)
	}
	prov.mu.Lock()
	defer prov.mu.Unlock()
	if len(prov.requests) != 0 {
		t.Fatal("provider should not be called for invalid request")
	}
}

func TestHandlerRejectsTopicDeviceMismatchWithoutPublishing(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	err = h.HandleRequest(context.Background(), "ori/other/tier_c/enrichment/request", requestPayload(t, validRequest()))
	if err == nil || !strings.Contains(err.Error(), "does not match device_id") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(pub.published) != 0 {
		t.Fatalf("unexpected publish: %#v", pub.published)
	}
}

func TestHandlerProviderInvalidResponsePublishesErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeEnrichmentProvider{response: contracts.TierCEnrichmentResponse{Provider: "fake"}}
	h, err := NewHandler(pub, prov, Options{})
	if err != nil {
		t.Fatal(err)
	}
	req := validRequest()
	if err := h.HandleRequest(context.Background(), "ori/dev-01/tier_c/enrichment/request", requestPayload(t, req)); err != nil {
		t.Fatal(err)
	}
	resp := pub.onlyResponse(t)
	if resp.Error == nil || *resp.Error != "provider returned invalid response" {
		t.Fatalf("error = %#v", resp.Error)
	}
}
