// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package dispatcher

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

type publishedMessage struct {
	topic   string
	qos     byte
	retain  bool
	payload []byte
}

type fakePublisher struct {
	mu       sync.Mutex
	messages []publishedMessage
	err      error
	seenCtx  context.Context
}

func (p *fakePublisher) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.seenCtx = ctx
	p.messages = append(p.messages, publishedMessage{
		topic:   topic,
		qos:     qos,
		retain:  retain,
		payload: append([]byte(nil), payload...),
	})
	return p.err
}

func (p *fakePublisher) publishedResponse(t *testing.T) contracts.ReasoningResponse {
	t.Helper()
	return p.responseAt(t, 0)
}

func (p *fakePublisher) responseAt(t *testing.T, index int) contracts.ReasoningResponse {
	t.Helper()
	p.mu.Lock()
	defer p.mu.Unlock()
	if len(p.messages) <= index {
		t.Fatalf("publish messages = %d, want index %d", len(p.messages), index)
	}
	msg := p.messages[index]
	if msg.topic != "ori/site-a/reasoning/response" {
		t.Fatalf("topic = %q", msg.topic)
	}
	if msg.qos != broker.QoSReasoning || msg.retain {
		t.Fatalf("qos/retain = %d/%v", msg.qos, msg.retain)
	}
	var resp contracts.ReasoningResponse
	if err := json.Unmarshal(msg.payload, &resp); err != nil {
		t.Fatal(err)
	}
	return resp
}

func (p *fakePublisher) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return len(p.messages)
}

func (p *fakePublisher) publishDeadline() (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seenCtx == nil {
		return time.Time{}, false
	}
	return p.seenCtx.Deadline()
}

type fakeProvider struct {
	mu          sync.Mutex
	calls       int
	response    contracts.ReasoningResponse
	err         error
	panicValue  any
	panicSet    bool
	sleep       time.Duration
	block       chan struct{}
	seenCtx     context.Context
	seenRequest contracts.ReasoningRequest
}

func (p *fakeProvider) Name() string { return "fake" }

func (p *fakeProvider) Healthy(context.Context) bool { return true }

func (p *fakeProvider) Reason(ctx context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	p.mu.Lock()
	p.calls++
	p.seenCtx = ctx
	p.seenRequest = req
	p.mu.Unlock()
	if p.block != nil {
		<-p.block
	}
	if p.panicSet {
		panic(p.panicValue)
	}
	if p.sleep > 0 {
		select {
		case <-time.After(p.sleep):
		case <-ctx.Done():
			return contracts.ReasoningResponse{}, ctx.Err()
		}
	}
	if p.err != nil {
		return contracts.ReasoningResponse{}, p.err
	}
	resp := p.response
	if resp.RequestID == "" {
		resp = contracts.ReasoningResponse{
			RequestID:  req.RequestID,
			Text:       "reasoned response",
			Model:      "fake-model",
			TokensUsed: 3,
			LatencyMS:  4,
			Confidence: 0.8,
			ActionTier: req.ActionTierHint,
		}
	}
	return resp, nil
}

func (p *fakeProvider) callCount() int {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.calls
}

func (p *fakeProvider) providerDeadline() (time.Time, bool) {
	p.mu.Lock()
	defer p.mu.Unlock()
	if p.seenCtx == nil {
		return time.Time{}, false
	}
	return p.seenCtx.Deadline()
}

func validPayload(t *testing.T, mutate func(*contracts.ReasoningRequest)) []byte {
	t.Helper()
	req := contracts.ReasoningRequest{
		RequestID:      "req-1",
		DeviceID:       "site-a",
		SensorType:     "current_clamp",
		TriggerName:    "overcurrent",
		Prompt:         "Explain the current anomaly.",
		ActionTierHint: contracts.ActionTierC,
		TimeoutMS:      1000,
	}
	if mutate != nil {
		mutate(&req)
	}
	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func newTestDispatcher(t *testing.T, pub *fakePublisher, prov *fakeProvider, opts Options) *Dispatcher {
	t.Helper()
	d, err := New(pub, prov, opts)
	if err != nil {
		t.Fatal(err)
	}
	return d
}

func TestDispatcherSuccess(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
		t.Fatal(err)
	}
	if prov.callCount() != 1 {
		t.Fatalf("provider calls = %d", prov.callCount())
	}
	resp := pub.publishedResponse(t)
	if resp.RequestID != "req-1" || resp.Text != "reasoned response" || resp.ActionTier != contracts.ActionTierC || resp.Error != nil {
		t.Fatalf("unexpected response: %#v", resp)
	}
	if _, ok := prov.providerDeadline(); !ok {
		t.Fatal("provider did not receive a deadline-bound context")
	}
}

func TestDispatcherTimeoutErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{sleep: 50 * time.Millisecond}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})
	payload := validPayload(t, func(req *contracts.ReasoningRequest) { req.TimeoutMS = 1 })

	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", payload); err != nil {
		t.Fatal(err)
	}
	resp := pub.publishedResponse(t)
	if resp.RequestID != "req-1" || resp.ActionTier != contracts.ActionTierC || resp.Error == nil || *resp.Error != "provider timeout" {
		t.Fatalf("unexpected timeout response: %#v", resp)
	}
}

func TestDispatcherRequestTimeoutIsCappedByDispatcher(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 20})
	payload := validPayload(t, func(req *contracts.ReasoningRequest) { req.TimeoutMS = 60_000 })

	start := time.Now()
	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", payload); err != nil {
		t.Fatal(err)
	}
	deadline, ok := prov.providerDeadline()
	if !ok {
		t.Fatal("provider did not receive deadline")
	}
	remaining := deadline.Sub(start)
	if remaining > 100*time.Millisecond {
		t.Fatalf("provider deadline was not capped, remaining=%s", remaining)
	}
}

func TestDispatcherPublishUsesIndependentTimeout(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000, PublishTimeout: 200 * time.Millisecond})
	if err := d.publishResponse(contracts.ReasoningRequest{DeviceID: "site-a"}, contracts.ReasoningResponse{RequestID: "req-1", ActionTier: contracts.ActionTierC}); err != nil {
		t.Fatal(err)
	}
	deadline, ok := pub.publishDeadline()
	if !ok || time.Until(deadline) <= 0 {
		t.Fatalf("publish did not receive independent live deadline: %v %v", deadline, ok)
	}
}

func TestDispatcherProviderErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{err: errors.New("SECRET_INTERNAL_PROVIDER_DETAIL")}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
		t.Fatal(err)
	}
	resp := pub.publishedResponse(t)
	if resp.Error == nil || *resp.Error != "provider error" {
		t.Fatalf("unexpected provider error response: %#v", resp)
	}
	encoded := string(pub.messages[0].payload)
	if strings.Contains(encoded, "SECRET_INTERNAL_PROVIDER_DETAIL") {
		t.Fatalf("provider error leaked detail: %s", encoded)
	}
}

func TestDispatcherProviderPanicResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{panicSet: true, panicValue: "boom"}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
		t.Fatal(err)
	}
	resp := pub.publishedResponse(t)
	if resp.Error == nil || *resp.Error != "provider panic" {
		t.Fatalf("unexpected panic response: %#v", resp)
	}
}

func TestDispatcherProviderTypedNilPanicResponse(t *testing.T) {
	pub := &fakePublisher{}
	var typedNil *int
	prov := &fakeProvider{panicSet: true, panicValue: typedNil}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
		t.Fatal(err)
	}
	resp := pub.publishedResponse(t)
	if resp.Error == nil || *resp.Error != "provider panic" {
		t.Fatalf("unexpected typed-nil panic response: %#v", resp)
	}
}

func TestDispatcherInvalidPayloadsDoNotCallProvider(t *testing.T) {
	cases := []struct {
		name    string
		payload []byte
	}{
		{name: "invalid json", payload: []byte(`{`)},
		{name: "missing request id", payload: validPayload(t, func(req *contracts.ReasoningRequest) { req.RequestID = "" })},
		{name: "missing device id", payload: validPayload(t, func(req *contracts.ReasoningRequest) { req.DeviceID = "" })},
		{name: "invalid tier", payload: validPayload(t, func(req *contracts.ReasoningRequest) { req.ActionTierHint = "Z" })},
		{name: "missing prompt", payload: validPayload(t, func(req *contracts.ReasoningRequest) { req.Prompt = "" })},
		{name: "negative timeout", payload: validPayload(t, func(req *contracts.ReasoningRequest) { req.TimeoutMS = -1 })},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			pub := &fakePublisher{}
			prov := &fakeProvider{}
			d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

			err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", tc.payload)
			if err == nil {
				t.Fatal("expected local validation error")
			}
			if prov.callCount() != 0 {
				t.Fatalf("provider calls = %d", prov.callCount())
			}
			if pub.callCount() != 0 {
				t.Fatalf("publish calls = %d", pub.callCount())
			}
		})
	}
}

func TestDispatcherRejectsTopicDeviceMismatch(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	err := d.HandleRequest(context.Background(), "ori/other-site/reasoning/request", validPayload(t, nil))
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("unexpected error: %v", err)
	}
	if prov.callCount() != 0 || pub.callCount() != 0 {
		t.Fatalf("provider/publish calls = %d/%d", prov.callCount(), pub.callCount())
	}
}

func TestDispatcherAllowsEmptyTopicForInternalCallers(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	if err := d.HandleRequest(context.Background(), "", validPayload(t, nil)); err != nil {
		t.Fatal(err)
	}
	resp := pub.publishedResponse(t)
	if resp.RequestID != "req-1" {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestDispatcherProviderInvalidResponsePublishesErrorResponse(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{response: contracts.ReasoningResponse{
		RequestID:  "wrong-id",
		Text:       "bad",
		Model:      "fake",
		ActionTier: contracts.ActionTierC,
	}}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil)); err != nil {
		t.Fatal(err)
	}
	resp := pub.publishedResponse(t)
	if resp.RequestID != "req-1" || resp.Error == nil || *resp.Error != "provider returned invalid response" {
		t.Fatalf("unexpected invalid-provider response: %#v", resp)
	}
}

func TestDispatcherPublishFailureIsSurfaced(t *testing.T) {
	pub := &fakePublisher{err: errors.New("publish down")}
	prov := &fakeProvider{}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})

	err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", validPayload(t, nil))
	if err == nil || !strings.Contains(err.Error(), "publish response") {
		t.Fatalf("unexpected error: %v", err)
	}
	if prov.callCount() != 1 || pub.callCount() != 1 {
		t.Fatalf("provider/publish calls = %d/%d", prov.callCount(), pub.callCount())
	}
}

func TestDispatcherConcurrentDuplicateRequestIDPublishesOnce(t *testing.T) {
	pub := &fakePublisher{}
	block := make(chan struct{})
	prov := &fakeProvider{block: block}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})
	payload := validPayload(t, nil)

	done := make(chan error, 2)
	go func() {
		done <- d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", payload)
	}()
	for prov.callCount() == 0 {
		time.Sleep(time.Millisecond)
	}

	go func() {
		done <- d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", payload)
	}()
	time.Sleep(5 * time.Millisecond)
	close(block)
	for i := 0; i < 2; i++ {
		if err := <-done; err != nil {
			t.Fatal(err)
		}
	}
	if prov.callCount() != 1 || pub.callCount() != 1 {
		t.Fatalf("provider/publish calls = %d/%d", prov.callCount(), pub.callCount())
	}
	resp := pub.publishedResponse(t)
	if resp.RequestID != "req-1" || resp.ActionTier != contracts.ActionTierC {
		t.Fatalf("unexpected response: %#v", resp)
	}
}

func TestDispatcherSequentialDuplicateRequestIDResponsesAreCorrelated(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{}
	d := newTestDispatcher(t, pub, prov, Options{ProviderTimeoutMS: 1000})
	payload := validPayload(t, nil)

	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", payload); err != nil {
		t.Fatal(err)
	}
	if err := d.HandleRequest(context.Background(), "ori/site-a/reasoning/request", payload); err != nil {
		t.Fatal(err)
	}
	if prov.callCount() != 2 || pub.callCount() != 2 {
		t.Fatalf("provider/publish calls = %d/%d", prov.callCount(), pub.callCount())
	}
	for i := 0; i < 2; i++ {
		resp := pub.responseAt(t, i)
		if resp.RequestID != "req-1" || resp.ActionTier != contracts.ActionTierC {
			t.Fatalf("response %d not correlated: %#v", i, resp)
		}
	}
}

func TestNewRejectsInvalidDependencies(t *testing.T) {
	pub := &fakePublisher{}
	prov := &fakeProvider{}
	if _, err := New(nil, prov, Options{}); err == nil {
		t.Fatal("expected nil publisher error")
	}
	if _, err := New(pub, nil, Options{}); err == nil {
		t.Fatal("expected nil provider error")
	}
	if _, err := New(pub, prov, Options{ProviderTimeoutMS: -1}); err == nil {
		t.Fatal("expected negative provider timeout error")
	}
	if _, err := New(pub, prov, Options{PublishTimeout: -1}); err == nil {
		t.Fatal("expected negative publish timeout error")
	}
}
