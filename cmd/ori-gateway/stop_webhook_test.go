// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/provider"
	"github.com/ori-platform/ori-gateway/internal/webhookbridge"
)

// heldSubscribeBroker holds runGateway in its subscribe to one topic, so it
// cannot yet notice anything a callback reports. Once released, the held
// subscribe fails because its context has ended, as a subscribe started
// after the decision does, unless completes is set, as when an
// acknowledgement already awaited arrives anyway.
type heldSubscribeBroker struct {
	*fakeBroker
	topic            string
	completes        bool
	entered, release chan struct{}
}

func (b *heldSubscribeBroker) Subscribe(ctx context.Context, topic string, qos byte, h broker.MessageHandler) error {
	if topic == b.topic {
		close(b.entered)
		<-b.release
		if err := ctx.Err(); err != nil && !b.completes {
			return err
		}
	}
	return b.fakeBroker.Subscribe(ctx, topic, qos, h)
}

// TestTheFailureLimitStopsEveryAdmissionBeforeTheGatewayNoticesIt holds
// runGateway in a later startup subscribe, where it cannot read the request
// failure escalation, and fails the replies to authenticated reasoning
// requests up to the limit. The callback that reaches the limit decides the
// stop itself: the clock refuses to sign, a later reasoning request is not
// admitted, the run is cancelled so the webhook bridge stops listening, and
// no webhook request after the decision is signed or forwarded
// (gateway-config/v2 gateway.state_directory rule 13). The held subscribe
// then fails with the cancellation, and runGateway reports the escalation.
func TestTheFailureLimitStopsEveryAdmissionBeforeTheGatewayNoticesIt(t *testing.T) {
	if err := stopAtTheFailureLimit(t, false, false); err == nil || !strings.Contains(err.Error(), "reasoning request failure limit reached") {
		t.Fatalf("runGateway = %v, want the request failure escalation", err)
	}
}

// TestTheFailureLimitIsReportedWhenStartupCompletes runs the same stop with a
// held subscribe that completes anyway, so runGateway reaches its wait with
// both the escalation and the ended runner group ready, many times over: it
// always reports the escalation.
func TestTheFailureLimitIsReportedWhenStartupCompletes(t *testing.T) {
	for i := range 20 {
		if err := stopAtTheFailureLimit(t, true, false); err == nil || !strings.Contains(err.Error(), "reasoning request failure limit reached") {
			t.Fatalf("run %d: runGateway = %v, want the request failure escalation", i, err)
		}
	}
}

// TestTheFailureLimitIsReportedWhenTheCallerCancelsLater runs the same stop,
// then has the caller cancel the run before the held subscribe completes, so
// runGateway reaches its wait with both the escalation and the caller's
// cancellation ready, many times over: it always reports the escalation.
func TestTheFailureLimitIsReportedWhenTheCallerCancelsLater(t *testing.T) {
	for i := range 20 {
		if err := stopAtTheFailureLimit(t, true, true); err == nil || !strings.Contains(err.Error(), "reasoning request failure limit reached") {
			t.Fatalf("run %d: runGateway = %v, want the request failure escalation", i, err)
		}
	}
}

// stopAtTheFailureLimit runs the gateway to the request failure limit while
// it is held in a startup subscribe, checks that the decision stopped every
// admission, then releases the subscribe, after cancelling the run first if
// cancelFirst is set, and returns what runGateway returns.
func stopAtTheFailureLimit(t *testing.T, heldCompletes, cancelFirst bool) error {
	t.Helper()
	var forwards atomic.Int32
	target := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		forwards.Add(1)
		w.WriteHeader(http.StatusOK)
	}))
	defer target.Close()
	t.Setenv("STOP_WEBHOOK_TOKEN", "runtime-token")
	t.Setenv("STOP_WEBHOOK_HMAC", "bridge-secret")
	g := newStallingGateway(t, filepath.Join(t.TempDir(), "gateway-state"), false)
	g.cfg.WebhookBridge = config.WebhookBridgeConfig{
		Enabled: true, ListenAddr: loopbackAddr(t), Path: "/sms", TargetURL: target.URL,
		ProviderSourceCIDRs: []string{"127.0.0.1/32"}, RuntimeTokenEnv: "STOP_WEBHOOK_TOKEN", HMACSecretEnv: "STOP_WEBHOOK_HMAC",
		RequestTimeoutMS: 1000, MaxBodyBytes: 1024,
		SenderPerMinute: config.DefaultWebhookBridgeSenderPerMinute, SenderBurst: config.DefaultWebhookBridgeSenderBurst,
		GlobalPerMinute: config.DefaultWebhookBridgeGlobalPerMinute, GlobalBurst: config.DefaultWebhookBridgeGlobalBurst,
		MaxSenders: config.DefaultWebhookBridgeMaxSenders,
	}
	g.deps.loadConfig = func(string) (config.Config, error) { return g.cfg, nil }
	g.deps.newWebhookBridge = func(cfg config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error) {
		return webhookbridge.New(cfg, opts)
	}
	fp := &fakeProvider{healthy: true}
	g.deps.newProvider = func(config.ProviderConfig) (provider.Provider, error) { return fp, nil }
	b := &heldSubscribeBroker{fakeBroker: newFakeBroker(), topic: "ori/dev-01/runtime/heartbeat", completes: heldCompletes,
		entered: make(chan struct{}), release: make(chan struct{})}
	b.publishErr = errors.New("reply not delivered")
	g.deps.newBroker = func(broker.Options) (brokerClient, error) { return b, nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	var once sync.Once
	releaseHeld := func() { once.Do(func() { close(b.release) }) }
	defer func() {
		releaseHeld()
		cancel()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("the gateway did not stop")
		}
	}()
	select {
	case <-b.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the gateway never reached the held subscribe")
	}
	url := "http://" + g.cfg.WebhookBridge.ListenAddr + "/sms"
	waitUntil(t, "the webhook listener", func() bool {
		resp, err := http.Get(url)
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		return resp.StatusCode == http.StatusMethodNotAllowed
	})

	handler := b.handlerFor(contracts.GatewayReasoningRequestTopicFilter)
	if handler == nil {
		t.Fatal("missing reasoning subscription")
	}
	request := func(i int) []byte {
		req := map[string]any{"request_id": fmt.Sprintf("stop-%d", i), "device_id": "dev-01", "sensor_type": "current_clamp",
			"trigger_name": "overcurrent", "prompt": "Explain anomaly.", "action_tier_hint": "C", "timeout_ms": 1000}
		auth, err := mqttauth.Sign(req, contracts.ReasoningRequestMessageType, "dev-01", req["request_id"].(string), g.wallMS.Load(), "runtime-gateway-envelope-secret")
		if err != nil {
			t.Fatal(err)
		}
		req["auth"] = auth
		raw, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for i := range defaultRequestFailureLimit {
		handler("ori/dev-01/reasoning/request", request(i))
	}
	// Requests run off the broker callback, so the decision is taken once
	// the last failed reply returns, not when the handler does.
	signer := <-g.signers
	waitUntil(t, "the failure limit to stop the signing clock", func() bool {
		_, err := signer.Next()
		return errors.Is(err, mqttauth.ErrSigningClockClosed)
	})
	fp.mu.Lock()
	answered := len(fp.requests)
	fp.mu.Unlock()
	if answered != defaultRequestFailureLimit {
		t.Fatalf("%d requests reached the provider before the limit, want %d", answered, defaultRequestFailureLimit)
	}
	// Checked once runGateway returns, which joins every request admitted.
	handler("ori/dev-01/reasoning/request", request(defaultRequestFailureLimit))

	// The decision cancelled the run, so the bridge stops listening while
	// runGateway is still held; until it does, a request is answered 503.
	// Either way nothing is signed or forwarded.
	waitUntil(t, "the webhook listener to close", func() bool {
		resp, err := http.Post(url, "application/x-www-form-urlencoded", strings.NewReader("from=%2B2348000000000&text=YES-test"))
		if err != nil {
			return true
		}
		_, _ = io.Copy(io.Discard, resp.Body)
		_ = resp.Body.Close()
		if resp.StatusCode != http.StatusServiceUnavailable {
			t.Errorf("a webhook request after the decision to stop = %d, want 503", resp.StatusCode)
		}
		return false
	})
	if n := forwards.Load(); n != 0 {
		t.Fatalf("a webhook request after the decision to stop was forwarded %d times, want none", n)
	}
	// The run ends on its own decision, unless the caller cancels it first.
	if cancelFirst {
		cancel()
	}
	releaseHeld()
	select {
	case err := <-done:
		done <- err
		fp.mu.Lock()
		after := len(fp.requests)
		fp.mu.Unlock()
		if after != answered {
			t.Fatal("a reasoning request was admitted after the failure limit decided the stop")
		}
		return err
	case <-time.After(5 * time.Second):
		t.Fatal("the gateway did not stop on its own decision")
		return nil
	}
}

// contextBridge is a webhook bridge runner that returns as soon as its
// context ends, as the real one does once its server has shut down, and
// hands its admission over to the test.
type contextBridge struct{}

func (contextBridge) Run(ctx context.Context) error {
	<-ctx.Done()
	return ctx.Err()
}

// TestShutdownJoinsAnAdmittedWebhookRequest admits a webhook request, then
// stops the gateway while that request is still being answered:
// runGateway does not return until the request is released, so no admitted
// request outlives the release of the state directory.
func TestShutdownJoinsAnAdmittedWebhookRequest(t *testing.T) {
	cfg := validConfig()
	cfg.WebhookBridge.Enabled = true
	fb := newFakeBroker()
	deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	admits := make(chan func() (func(), bool), 1)
	deps.newWebhookBridge = func(_ config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error) {
		admits <- opts.Admit
		return contextBridge{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("the gateway did not subscribe")
	}
	admit := <-admits
	release, admitted := admit()
	if !admitted {
		t.Fatal("a webhook request was refused while the gateway runs")
	}
	cancel()
	select {
	case err := <-done:
		release()
		t.Fatalf("runGateway returned %v while an admitted webhook request was still being answered", err)
	case <-time.After(300 * time.Millisecond):
	}
	if _, admitted := admit(); admitted {
		t.Fatal("a webhook request was admitted after the decision to stop")
	}
	release()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runGateway did not return once the admitted request was released")
	}
}

// cancelAwareProvider answers every request at once, except one it holds
// until the run cancels it.
type cancelAwareProvider struct {
	fakeProvider
	hold, held chan struct{}
}

func (p *cancelAwareProvider) Reason(ctx context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	select {
	case <-p.hold:
		close(p.held)
		<-ctx.Done()
		return contracts.ReasoningResponse{}, ctx.Err()
	default:
	}
	return p.fakeProvider.Reason(ctx, req)
}

// joinedDisconnectBroker waits, before it disconnects, until a callback the
// test names has returned, so runGateway decides what to report only after
// that callback has counted, or not, its reply.
type joinedDisconnectBroker struct {
	*fakeBroker
	joined <-chan struct{}
}

func (b *joinedDisconnectBroker) Disconnect(ctx context.Context) error {
	select {
	case <-b.joined:
	case <-time.After(5 * time.Second):
	}
	return b.fakeBroker.Disconnect(ctx)
}

// TestAReplyTheStopRefusesIsNotADeliveryFailure fails one reply short of the
// request failure limit, holds a later request in the provider, and stops
// the gateway: the held request's reply is refused by the stop, its context
// cancelled and its signing clock closed, and that is not counted, so the
// clean stop returns nil rather than the escalation (GW-6).
func TestAReplyTheStopRefusesIsNotADeliveryFailure(t *testing.T) {
	g := newStallingGateway(t, filepath.Join(t.TempDir(), "gateway-state"), false)
	g.deps.loadConfig = func(string) (config.Config, error) { return g.cfg, nil }
	fp := &cancelAwareProvider{fakeProvider: fakeProvider{healthy: true}, hold: make(chan struct{}), held: make(chan struct{})}
	g.deps.newProvider = func(config.ProviderConfig) (provider.Provider, error) { return fp, nil }
	handled := make(chan struct{})
	b := &joinedDisconnectBroker{fakeBroker: newFakeBroker(), joined: handled}
	b.publishErr = errors.New("reply not delivered")
	g.deps.newBroker = func(broker.Options) (brokerClient, error) { return b, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	waitUntil(t, "every subscription", func() bool { return b.handlerFor("ori/dev-01/runtime/heartbeat") != nil })
	handler := b.handlerFor(contracts.GatewayReasoningRequestTopicFilter)
	request := func(i int) []byte {
		req := map[string]any{"request_id": fmt.Sprintf("held-%d", i), "device_id": "dev-01", "sensor_type": "current_clamp",
			"trigger_name": "overcurrent", "prompt": "Explain anomaly.", "action_tier_hint": "C", "timeout_ms": 1000}
		auth, err := mqttauth.Sign(req, contracts.ReasoningRequestMessageType, "dev-01", req["request_id"].(string), g.wallMS.Load(), "runtime-gateway-envelope-secret")
		if err != nil {
			t.Fatal(err)
		}
		req["auth"] = auth
		raw, err := json.Marshal(req)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	for i := range defaultRequestFailureLimit - 1 {
		handler("ori/dev-01/reasoning/request", request(i))
	}
	b.mu.Lock()
	b.publishErr = nil
	b.mu.Unlock()
	close(fp.hold)
	go func() {
		defer close(handled)
		handler("ori/dev-01/reasoning/request", request(defaultRequestFailureLimit))
	}()
	<-fp.held
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a clean stop with a request in flight = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the gateway did not stop")
	}
}

// reasoningRequest is reasoning request i for dev-01, signed at signedAtMS
// under the stalling gateway's envelope secret, or unsigned when sign is
// false.
func reasoningRequest(t *testing.T, i int, sign bool, signedAtMS int64) []byte {
	t.Helper()
	req := map[string]any{"request_id": fmt.Sprintf("stop-refused-%d", i), "device_id": "dev-01", "sensor_type": "current_clamp",
		"trigger_name": "overcurrent", "prompt": "Explain anomaly.", "action_tier_hint": "C", "timeout_ms": 1000}
	if sign {
		auth, err := mqttauth.Sign(req, contracts.ReasoningRequestMessageType, "dev-01", req["request_id"].(string), signedAtMS, "runtime-gateway-envelope-secret")
		if err != nil {
			t.Fatal(err)
		}
		req["auth"] = auth
	}
	raw, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

// TestAReplyFailingAfterTheCallerCancelsIsNotCounted runs without
// gateway.auth, so no signing clock is involved, with every reply failing:
// one short of the limit, then a held request whose reply fails only after
// the caller has cancelled the run. That failure is the run's own stop, not
// a delivery failure, and the clean stop returns nil.
func TestAReplyFailingAfterTheCallerCancelsIsNotCounted(t *testing.T) {
	g := newStallingGateway(t, filepath.Join(t.TempDir(), "gateway-state"), false)
	g.cfg.Gateway.Auth = config.GatewayAuthConfig{}
	g.deps.loadConfig = func(string) (config.Config, error) { return g.cfg, nil }
	fp := &cancelAwareProvider{fakeProvider: fakeProvider{healthy: true}, hold: make(chan struct{}), held: make(chan struct{})}
	g.deps.newProvider = func(config.ProviderConfig) (provider.Provider, error) { return fp, nil }
	handled := make(chan struct{})
	b := &joinedDisconnectBroker{fakeBroker: newFakeBroker(), joined: handled}
	b.publishErr = errors.New("reply not delivered")
	g.deps.newBroker = func(broker.Options) (brokerClient, error) { return b, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	waitUntil(t, "every subscription", func() bool { return b.handlerFor("ori/dev-01/runtime/heartbeat") != nil })
	handler := b.handlerFor(contracts.GatewayReasoningRequestTopicFilter)
	for i := range defaultRequestFailureLimit - 1 {
		handler("ori/dev-01/reasoning/request", reasoningRequest(t, i, false, 0))
	}
	close(fp.hold)
	go func() {
		defer close(handled)
		handler("ori/dev-01/reasoning/request", reasoningRequest(t, defaultRequestFailureLimit, false, 0))
	}()
	<-fp.held
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("a clean stop with a request in flight = %v, want nil", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("the gateway did not stop")
	}
}

// TestARefusalByAClosedClockWhileTheRunIsLiveIsNotCounted closes the signing
// clock while the run's context is still live, as between the two steps of
// a decision to stop, and sends more requests than the limit: every reply is
// refused by the closed clock, none counts, and the gateway does not stop on
// its own.
func TestARefusalByAClosedClockWhileTheRunIsLiveIsNotCounted(t *testing.T) {
	g := newStallingGateway(t, filepath.Join(t.TempDir(), "gateway-state"), false)
	g.deps.loadConfig = func(string) (config.Config, error) { return g.cfg, nil }
	b := newFakeBroker()
	g.deps.newBroker = func(broker.Options) (brokerClient, error) { return b, nil }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	waitUntil(t, "every subscription", func() bool { return b.handlerFor("ori/dev-01/runtime/heartbeat") != nil })
	clock := <-g.signers
	clock.Close()
	handler := b.handlerFor(contracts.GatewayReasoningRequestTopicFilter)
	for i := range defaultRequestFailureLimit + 1 {
		handler("ori/dev-01/reasoning/request", reasoningRequest(t, i, true, g.wallMS.Load()))
	}
	select {
	case err := <-done:
		t.Fatalf("the gateway stopped on its own after replies the closed clock refused: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
	cancel()
	select {
	case <-done:
	case <-time.After(10 * time.Second):
		t.Fatal("the gateway did not stop")
	}
}
