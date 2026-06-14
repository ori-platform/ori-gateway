// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/enrichment"
	"github.com/ori-platform/ori-gateway/internal/fleet"
	"github.com/ori-platform/ori-gateway/internal/heartbeat"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/provider"
	"github.com/ori-platform/ori-gateway/internal/reporting"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
	"github.com/ori-platform/ori-gateway/internal/sim"
	"github.com/ori-platform/ori-gateway/internal/site"
	"github.com/ori-platform/ori-gateway/internal/webhookbridge"
)

type fakeBroker struct {
	mu sync.Mutex

	connectErr          error
	subscribeErr        error
	subscribeErrByTopic map[string]error
	disconnectErr       error
	publishErr          error

	connected    bool
	disconnected bool

	subscribeTopic      string
	subscribeQoS        byte
	handler             broker.MessageHandler
	subscribeTopics     []string
	subscribeQoSByTopic map[string]byte
	handlers            map[string]broker.MessageHandler

	published []publishedMessage

	subscribedOnce   sync.Once
	disconnectedOnce sync.Once
	subscribed       chan struct{}
	disconnectedCh   chan struct{}
}

type publishedMessage struct {
	topic   string
	qos     byte
	retain  bool
	payload []byte
}

type subscribeEventBroker struct {
	*fakeBroker
	onSubscribe func()
}

func (b *subscribeEventBroker) Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error {
	if b.onSubscribe != nil {
		b.onSubscribe()
	}
	return b.fakeBroker.Subscribe(ctx, topic, qos, handler)
}

func newFakeBroker() *fakeBroker {
	return &fakeBroker{
		subscribeQoSByTopic: map[string]byte{},
		handlers:            map[string]broker.MessageHandler{},
		subscribed:          make(chan struct{}),
		disconnectedCh:      make(chan struct{}),
	}
}

func (b *fakeBroker) Connect(context.Context) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.connectErr != nil {
		return b.connectErr
	}
	b.connected = true
	return nil
}

func (b *fakeBroker) Disconnect(context.Context) error {
	b.mu.Lock()
	b.disconnected = true
	err := b.disconnectErr
	b.mu.Unlock()
	b.disconnectedOnce.Do(func() { close(b.disconnectedCh) })
	return err
}

func (b *fakeBroker) Subscribe(_ context.Context, topic string, qos byte, handler broker.MessageHandler) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.subscribeErr != nil {
		return b.subscribeErr
	}
	if err := b.subscribeErrByTopic[topic]; err != nil {
		return err
	}
	b.subscribeTopic = topic
	b.subscribeQoS = qos
	b.handler = handler
	b.subscribeTopics = append(b.subscribeTopics, topic)
	b.subscribeQoSByTopic[topic] = qos
	b.handlers[topic] = handler
	b.subscribedOnce.Do(func() { close(b.subscribed) })
	return nil
}

func (b *fakeBroker) Publish(_ context.Context, topic string, qos byte, retain bool, payload []byte) error {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.publishErr != nil {
		return b.publishErr
	}
	b.published = append(b.published, publishedMessage{
		topic:   topic,
		qos:     qos,
		retain:  retain,
		payload: append([]byte(nil), payload...),
	})
	return nil
}

func (b *fakeBroker) currentHandler() broker.MessageHandler {
	return b.handlerFor(contracts.GatewayReasoningRequestTopicFilter)
}

func (b *fakeBroker) handlerFor(topic string) broker.MessageHandler {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.handlers[topic]
}

func (b *fakeBroker) publishedResponse(t *testing.T) contracts.ReasoningResponse {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		var payload []byte
		b.mu.Lock()
		for _, msg := range b.published {
			if msg.topic == "ori/site-a/reasoning/response" {
				payload = append([]byte(nil), msg.payload...)
				break
			}
		}
		b.mu.Unlock()
		if payload != nil {
			var resp contracts.ReasoningResponse
			if err := json.Unmarshal(payload, &resp); err != nil {
				t.Fatal(err)
			}
			return resp
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for reasoning response")
	return contracts.ReasoningResponse{}
}

type fakeProvider struct {
	mu       sync.Mutex
	healthy  bool
	requests []contracts.ReasoningRequest
}

func (p *fakeProvider) Name() string { return "fake-provider" }

func (p *fakeProvider) Healthy(context.Context) bool { return p.healthy }

func (p *fakeProvider) Reason(_ context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	return contracts.ReasoningResponse{
		RequestID:  req.RequestID,
		Text:       "reasoned",
		Model:      "fake-provider",
		TokensUsed: 1,
		LatencyMS:  2,
		Confidence: 0.8,
		ActionTier: req.ActionTierHint,
	}, nil
}

type fakeTierCEnrichmentProvider struct {
	mu       sync.Mutex
	requests []contracts.TierCEnrichmentRequest
	response contracts.TierCEnrichmentResponse
	err      error
}

func (p *fakeTierCEnrichmentProvider) EnrichTierC(_ context.Context, req contracts.TierCEnrichmentRequest) (contracts.TierCEnrichmentResponse, error) {
	p.mu.Lock()
	p.requests = append(p.requests, req)
	p.mu.Unlock()
	if p.err != nil {
		return contracts.TierCEnrichmentResponse{}, p.err
	}
	if p.response.Explanation != "" || p.response.Error != nil {
		return p.response, nil
	}
	return contracts.TierCEnrichmentResponse{
		Explanation:                "This action needs operator approval.",
		EstimatedImpact:            "Could reduce peak load.",
		RecommendedOperatorContext: "Confirm whether occupants are still present.",
		Provider:                   "fake-reporting",
		Model:                      "fake-model",
	}, nil
}

type fakeReportingProvider struct {
	mu     sync.Mutex
	inputs []reporting.WeeklyReportInput
}

func (p *fakeReportingProvider) GenerateWeeklyReport(_ context.Context, input reporting.WeeklyReportInput) (reporting.ProviderReport, error) {
	p.mu.Lock()
	p.inputs = append(p.inputs, input)
	p.mu.Unlock()
	return reporting.ProviderReport{Text: "report", Provider: "fake", Model: "fake-model"}, nil
}

type fakeWeeklyReportRunner struct {
	started chan struct{}
	once    sync.Once
	err     error
}

func newFakeWeeklyReportRunner() *fakeWeeklyReportRunner {
	return &fakeWeeklyReportRunner{started: make(chan struct{})}
}

func (r *fakeWeeklyReportRunner) Run(ctx context.Context) error {
	r.once.Do(func() { close(r.started) })
	if r.err != nil {
		return r.err
	}
	<-ctx.Done()
	return ctx.Err()
}

type fakeHeartbeat struct {
	started   chan struct{}
	once      sync.Once
	err       error
	returnNil bool
}

type fakeWebhookBridge struct {
	started   chan struct{}
	once      sync.Once
	err       error
	returnNil bool
}

func newFakeHeartbeat() *fakeHeartbeat {
	return &fakeHeartbeat{started: make(chan struct{})}
}

func newFakeWebhookBridge() *fakeWebhookBridge {
	return &fakeWebhookBridge{started: make(chan struct{})}
}

func (h *fakeHeartbeat) Run(ctx context.Context) error {
	h.once.Do(func() { close(h.started) })
	if h.err != nil {
		return h.err
	}
	if h.returnNil {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func (b *fakeWebhookBridge) Run(ctx context.Context) error {
	b.once.Do(func() { close(b.started) })
	if b.err != nil {
		return b.err
	}
	if b.returnNil {
		return nil
	}
	<-ctx.Done()
	return ctx.Err()
}

func validConfig() config.Config {
	return config.Config{
		Gateway: config.GatewayConfig{
			BrokerURL:          "tcp://localhost:1883",
			DeviceIDs:          []string{"dev-01"},
			HeartbeatIntervalS: 30,
		},
		Provider: config.ProviderConfig{
			Name:      config.ProviderEcho,
			TimeoutMS: 1000,
		},
		SIM:   config.SIMConfig{Enabled: false},
		Fleet: config.FleetConfig{Enabled: false},
	}
}

func validRequestPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := json.Marshal(contracts.ReasoningRequest{
		RequestID:      "req-1",
		DeviceID:       "site-a",
		SensorType:     "current_clamp",
		TriggerName:    "overcurrent",
		Prompt:         "Explain anomaly.",
		ActionTierHint: contracts.ActionTierC,
		TimeoutMS:      1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func baseDeps(t *testing.T, cfg config.Config, fb *fakeBroker, fp *fakeProvider, hb *fakeHeartbeat) appDependencies {
	t.Helper()
	return appDependencies{
		loadConfig: func(path string) (config.Config, error) {
			if path != "gateway.yaml" {
				return config.Config{}, fmt.Errorf("config path = %q", path)
			}
			return cfg, nil
		},
		newProvider: func(config.ProviderConfig) (provider.Provider, error) {
			return fp, nil
		},
		newTierCEnrichmentProvider: func(config.ReportingConfig) (enrichment.Provider, error) {
			return &fakeTierCEnrichmentProvider{}, nil
		},
		newReportingProvider: func(config.ReportingConfig) (reporting.Provider, error) {
			return &fakeReportingProvider{}, nil
		},
		newBroker: func(broker.Options) (brokerClient, error) {
			return fb, nil
		},
		newSIM: func(cfg config.SIMConfig, opts sim.Options) (*sim.Client, error) {
			if opts.Probe != nil {
				return nil, errors.New("main wiring must not inject SIM probe in disabled path")
			}
			return sim.New(cfg, opts)
		},
		newFleet: func(cfg config.FleetConfig, opts fleet.Options) (*fleet.Client, error) {
			if opts.Credential != nil || opts.Health != nil {
				return nil, errors.New("main wiring must not inject fleet network/auth hooks in disabled path")
			}
			return fleet.New(cfg, opts)
		},
		newWebhookBridge: func(cfg config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error) {
			if !cfg.Enabled {
				return nil, errors.New("webhook bridge should not be constructed when disabled")
			}
			return newFakeWebhookBridge(), nil
		},
		newWeeklyReportRunner: func(*reporting.WeeklyReportGenerator, reporting.WeeklyReportRequest, reporting.Schedule, reporting.RunnerOptions) (weeklyReportRunner, error) {
			return newFakeWeeklyReportRunner(), nil
		},
		newSiteRegistry: site.NewRegistry,
		newHeartbeat: func(
			heartbeat.PublishFunc,
			heartbeat.ProviderStatus,
			heartbeat.SIMStatus,
			heartbeat.Options,
		) (heartbeatRunner, error) {
			return hb, nil
		},
		logger: slog.Default(),
		now: func() time.Time {
			return time.Unix(1_700_000_000, 0)
		},
	}
}

func TestHeartbeatAuthFromConfigReadsEnvSecret(t *testing.T) {
	t.Setenv("CUSTOM_GATEWAY_SECRET", "site-local-secret")

	auth, err := heartbeatAuthFromConfig(config.GatewayAuthConfig{
		Enabled:         true,
		SharedSecretEnv: "CUSTOM_GATEWAY_SECRET",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !auth.Enabled || auth.SharedSecret != "site-local-secret" {
		t.Fatalf("unexpected auth config: %#v", auth)
	}
}

func TestHeartbeatAuthFromConfigFailsWhenEnvSecretMissing(t *testing.T) {
	t.Setenv("MISSING_GATEWAY_SECRET", "")

	_, err := heartbeatAuthFromConfig(config.GatewayAuthConfig{
		Enabled:         true,
		SharedSecretEnv: "MISSING_GATEWAY_SECRET",
	})
	if err == nil {
		t.Fatal("expected missing env secret error")
	}
	if !strings.Contains(err.Error(), "MISSING_GATEWAY_SECRET") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayAuthSecretPassesToHeartbeatOptions(t *testing.T) {
	t.Setenv("CUSTOM_GATEWAY_SECRET", "site-local-secret")
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{
		Enabled:         true,
		SharedSecretEnv: "CUSTOM_GATEWAY_SECRET",
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	deps := baseDeps(t, cfg, fb, fp, hb)
	var captured heartbeat.AuthConfig
	deps.newHeartbeat = func(
		_ heartbeat.PublishFunc,
		_ heartbeat.ProviderStatus,
		_ heartbeat.SIMStatus,
		opts heartbeat.Options,
	) (heartbeatRunner, error) {
		captured = opts.Auth
		return hb, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
	if !captured.Enabled || captured.SharedSecret != "site-local-secret" {
		t.Fatalf("heartbeat auth = %#v", captured)
	}
}

func TestGatewayAuthMissingSecretStopsBeforeHeartbeat(t *testing.T) {
	t.Setenv("CUSTOM_GATEWAY_SECRET", "")
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{
		Enabled:         true,
		SharedSecretEnv: "CUSTOM_GATEWAY_SECRET",
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	deps := baseDeps(t, cfg, fb, fp, hb)
	heartbeatCalled := false
	deps.newHeartbeat = func(
		heartbeat.PublishFunc,
		heartbeat.ProviderStatus,
		heartbeat.SIMStatus,
		heartbeat.Options,
	) (heartbeatRunner, error) {
		heartbeatCalled = true
		return hb, nil
	}

	err := runGateway(context.Background(), "gateway.yaml", deps)
	if err == nil || !strings.Contains(err.Error(), "CUSTOM_GATEWAY_SECRET") {
		t.Fatalf("unexpected error: %v", err)
	}
	if heartbeatCalled {
		t.Fatal("heartbeat should not be constructed when auth secret is missing")
	}
}

func TestMainStartupMissingConfig(t *testing.T) {
	providerCalled := false
	err := runGateway(context.Background(), "missing.yaml", appDependencies{
		loadConfig: func(string) (config.Config, error) {
			return config.Config{}, errors.New("missing config")
		},
		newProvider: func(config.ProviderConfig) (provider.Provider, error) {
			providerCalled = true
			return nil, nil
		},
	})
	if err == nil {
		t.Fatal("expected missing config error")
	}
	if providerCalled {
		t.Fatal("provider should not be constructed after config load failure")
	}
}

func TestGatewayStartupProviderFailureStopsBeforeBroker(t *testing.T) {
	brokerCalled := false
	err := runGateway(context.Background(), "gateway.yaml", appDependencies{
		loadConfig: func(string) (config.Config, error) {
			return validConfig(), nil
		},
		newProvider: func(config.ProviderConfig) (provider.Provider, error) {
			return nil, errors.New("provider failed")
		},
		newBroker: func(broker.Options) (brokerClient, error) {
			brokerCalled = true
			return nil, nil
		},
	})
	if err == nil || !strings.Contains(err.Error(), "construct provider") {
		t.Fatalf("unexpected error: %v", err)
	}
	if brokerCalled {
		t.Fatal("broker should not be constructed after provider failure")
	}
}

func TestGatewayStartupConnectFailureStopsBeforeOptionalModules(t *testing.T) {
	fb := newFakeBroker()
	fb.connectErr = errors.New("connect failed")
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	simCalled := false
	fleetCalled := false
	deps := baseDeps(t, validConfig(), fb, fp, hb)
	deps.newSIM = func(config.SIMConfig, sim.Options) (*sim.Client, error) {
		simCalled = true
		return nil, nil
	}
	deps.newFleet = func(config.FleetConfig, fleet.Options) (*fleet.Client, error) {
		fleetCalled = true
		return nil, nil
	}

	err := runGateway(context.Background(), "gateway.yaml", deps)
	if err == nil || !strings.Contains(err.Error(), "connect broker") {
		t.Fatalf("unexpected error: %v", err)
	}
	if simCalled {
		t.Fatal("SIM should not be constructed after broker connect failure")
	}
	if fleetCalled {
		t.Fatal("fleet should not be constructed after broker connect failure")
	}
}

func TestGatewayStartupSubscribeFailureCancelsHeartbeatAndDisconnects(t *testing.T) {
	fb := newFakeBroker()
	fb.subscribeErr = errors.New("subscribe failed")
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()

	err := runGateway(context.Background(), "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	if err == nil || !strings.Contains(err.Error(), "subscribe reasoning requests") {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case <-fb.disconnectedCh:
	case <-time.After(time.Second):
		t.Fatal("broker should be disconnected after subscribe failure")
	}
}

func TestGatewayStartupRuntimeHeartbeatSubscribeFailureCancelsHeartbeatAndDisconnects(t *testing.T) {
	fb := newFakeBroker()
	fb.subscribeErrByTopic = map[string]error{
		"ori/dev-01/runtime/heartbeat": errors.New("runtime heartbeat subscribe failed"),
	}
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()

	err := runGateway(context.Background(), "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	if err == nil || !strings.Contains(err.Error(), "subscribe runtime node heartbeats") {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case <-fb.disconnectedCh:
	case <-time.After(time.Second):
		t.Fatal("broker should be disconnected after runtime heartbeat subscribe failure")
	}
}

func TestGatewayHeartbeatFailureReturnsError(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := &fakeHeartbeat{
		started: make(chan struct{}),
		err:     errors.New("heartbeat failed"),
	}

	err := runGateway(context.Background(), "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	if err == nil || !strings.Contains(err.Error(), "heartbeat stopped") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGracefulShutdown(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	}()

	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop after cancellation")
	}
	select {
	case <-fb.disconnectedCh:
	default:
		t.Fatal("broker was not disconnected")
	}
}

func TestMainWiresDispatcher(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	}()

	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	fb.mu.Lock()
	if _, ok := fb.handlers[contracts.GatewayReasoningRequestTopicFilter]; !ok {
		t.Fatalf("missing reasoning subscription, got topics %v", fb.subscribeTopics)
	}
	if fb.subscribeQoSByTopic[contracts.GatewayReasoningRequestTopicFilter] != broker.QoSReasoning {
		t.Fatalf("reasoning subscribe qos = %d", fb.subscribeQoSByTopic[contracts.GatewayReasoningRequestTopicFilter])
	}
	fb.mu.Unlock()

	handler := fb.currentHandler()
	if handler == nil {
		t.Fatal("missing subscription handler")
	}
	handler("ori/site-a/reasoning/request", validRequestPayload(t))
	resp := fb.publishedResponse(t)
	if resp.RequestID != "req-1" || resp.ActionTier != contracts.ActionTierC {
		t.Fatalf("unexpected response: %#v", resp)
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
}

func TestMainEscalatesRepeatedRequestHandlerFailures(t *testing.T) {
	fb := newFakeBroker()
	fb.publishErr = errors.New("broker disconnected")
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	}()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	handler := fb.currentHandler()
	if handler == nil {
		t.Fatal("missing subscription handler")
	}
	for i := 0; i < defaultRequestFailureLimit; i++ {
		handler("ori/site-a/reasoning/request", validRequestPayload(t))
	}

	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "reasoning request failure limit reached") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not escalate repeated request failures")
	}
}

func TestMainStartsHeartbeatBeforeSubscribe(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	var mu sync.Mutex
	var events []string
	deps := baseDeps(t, validConfig(), fb, fp, hb)
	deps.newHeartbeat = func(
		heartbeat.PublishFunc,
		heartbeat.ProviderStatus,
		heartbeat.SIMStatus,
		heartbeat.Options,
	) (heartbeatRunner, error) {
		mu.Lock()
		events = append(events, "heartbeat")
		mu.Unlock()
		return hb, nil
	}
	deps.newBroker = func(broker.Options) (brokerClient, error) {
		return &subscribeEventBroker{
			fakeBroker: fb,
			onSubscribe: func() {
				mu.Lock()
				events = append(events, "subscribe")
				mu.Unlock()
			},
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", deps)
	}()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}

	mu.Lock()
	defer mu.Unlock()
	if len(events) < 2 || events[0] != "heartbeat" || events[1] != "subscribe" {
		t.Fatalf("startup events = %v, want heartbeat before subscribe", events)
	}
}

func TestMainDisabledOptionalModulesDoNotFailStartup(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	}()

	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not start with disabled optional modules")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
}

func TestMainSubscribesToRuntimeNodeHeartbeats(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	}()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fb.mu.Lock()
		_, ok := fb.handlers["ori/dev-01/runtime/heartbeat"]
		qos := fb.subscribeQoSByTopic["ori/dev-01/runtime/heartbeat"]
		fb.mu.Unlock()
		if ok {
			if qos != broker.QoSHeartbeat {
				t.Fatalf("runtime heartbeat qos = %d", qos)
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("runGateway returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("gateway did not stop")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("missing runtime heartbeat subscription, got topics %v", fb.subscribeTopics)
}

func TestMainRuntimeHeartbeatUpdatesInjectedRegistry(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	registry := site.NewRegistry()
	deps := baseDeps(t, validConfig(), fb, fp, hb)
	deps.newSiteRegistry = func() *site.Registry { return registry }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", deps)
	}()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	var handler broker.MessageHandler
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		handler = fb.handlerFor("ori/dev-01/runtime/heartbeat")
		if handler != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if handler == nil {
		t.Fatalf("missing runtime heartbeat handler, got topics %v", fb.subscribeTopics)
	}
	payload, err := json.Marshal(contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         site.NodeStatusHealthy,
		LastSeenMS:     1234567890000,
		GatewaySeenMS:  0,
		ActiveTriggers: []string{"grid_low"},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler("ori/dev-01/runtime/heartbeat", payload)

	snapshot := registry.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one runtime node, got %d", len(snapshot))
	}
	if snapshot[0].DeviceID != "dev-01" || snapshot[0].GatewaySeen != 1_700_000_000_000 {
		t.Fatalf("unexpected registry snapshot: %#v", snapshot[0])
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
}

func TestMainRuntimeHeartbeatVerifiesSignedPayloadWhenGatewayAuthEnabled(t *testing.T) {
	t.Setenv("CUSTOM_GATEWAY_SECRET", "site-local-secret")
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{
		Enabled:         true,
		SharedSecretEnv: "CUSTOM_GATEWAY_SECRET",
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	registry := site.NewRegistry()
	deps := baseDeps(t, cfg, fb, fp, hb)
	deps.newSiteRegistry = func() *site.Registry { return registry }
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", deps)
	}()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	var handler broker.MessageHandler
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		handler = fb.handlerFor("ori/dev-01/runtime/heartbeat")
		if handler != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if handler == nil {
		t.Fatalf("missing runtime heartbeat handler, got topics %v", fb.subscribeTopics)
	}
	signedAt := int64(1_700_000_000_000)
	beat := contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         site.NodeStatusHealthy,
		LastSeenMS:     signedAt,
		GatewaySeenMS:  0,
		ActiveTriggers: []string{},
	}
	auth, err := mqttauth.Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", signedAt, "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	handler("ori/dev-01/runtime/heartbeat", payload)
	if len(registry.Snapshot()) != 1 {
		t.Fatalf("signed runtime heartbeat should update registry, got %#v", registry.Snapshot())
	}

	unsigned, err := json.Marshal(contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-02",
		Status:         site.NodeStatusHealthy,
		LastSeenMS:     signedAt,
		ActiveTriggers: []string{},
	})
	if err != nil {
		t.Fatal(err)
	}
	handler("ori/dev-02/runtime/heartbeat", unsigned)
	if len(registry.Snapshot()) != 1 {
		t.Fatalf("unsigned runtime heartbeat should be rejected when auth enabled, got %#v", registry.Snapshot())
	}

	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
}

func TestEvictStaleRuntimeNodesRemovesStaleAndFutureDatedNodes(t *testing.T) {
	registry := site.NewRegistry()
	registry.Upsert(site.NodeHeartbeat{DeviceID: "stale", Status: site.NodeStatusHealthy, LastSeenMS: 1000})
	registry.Upsert(site.NodeHeartbeat{DeviceID: "future", Status: site.NodeStatusHealthy, LastSeenMS: 100_000})
	registry.Upsert(site.NodeHeartbeat{DeviceID: "fresh", Status: site.NodeStatusHealthy, LastSeenMS: 9999})
	now := time.UnixMilli(10_000)
	calls := 0
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	go evictStaleRuntimeNodes(ctx, registry, time.Millisecond, func() time.Time {
		calls++
		return now
	}, slog.Default())
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		snapshot := registry.Snapshot()
		if len(snapshot) == 1 && snapshot[0].DeviceID == "fresh" && calls > 0 {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("stale runtime nodes were not evicted: %#v", registry.Snapshot())
}

func validTierCEnrichmentRequestPayload(t *testing.T) []byte {
	t.Helper()
	payload, err := json.Marshal(contracts.TierCEnrichmentRequest{
		RequestID:         "enrich-1",
		ProposalID:        "proposal-1",
		DeviceID:          "dev-01",
		SkillName:         "energy-anomaly-detector",
		TriggerName:       "sustained_high_load",
		SensorID:          "current-main",
		SensorType:        "current_clamp",
		ReadingValue:      18.4,
		Unit:              "A",
		ProposedAction:    "open_hvac_contactor",
		SafeDefaultAction: "alert_operator",
		OperatorMessage:   "Approve HVAC scale-back?",
		TimeoutMS:         1000,
	})
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestMainDoesNotSubscribeTierCEnrichmentWhenDisabled(t *testing.T) {
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", baseDeps(t, validConfig(), fb, fp, hb))
	}()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
	if _, ok := fb.handlers["ori/dev-01/tier_c/enrichment/request"]; ok {
		t.Fatalf("tier c enrichment subscription should be disabled, topics %v", fb.subscribeTopics)
	}
}

func TestMainSubscribesAndHandlesTierCEnrichmentWhenEnabled(t *testing.T) {
	cfg := validConfig()
	cfg.Reporting = config.ReportingConfig{
		Provider:        config.ReportingProviderGemini,
		Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	enrichmentProvider := &fakeTierCEnrichmentProvider{}
	deps := baseDeps(t, cfg, fb, fp, hb)
	deps.newTierCEnrichmentProvider = func(config.ReportingConfig) (enrichment.Provider, error) {
		return enrichmentProvider, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)

	go func() {
		done <- runGateway(ctx, "gateway.yaml", deps)
	}()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	var handler broker.MessageHandler
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		handler = fb.handlerFor("ori/dev-01/tier_c/enrichment/request")
		if handler != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if handler == nil {
		t.Fatalf("missing tier c enrichment handler, got topics %v", fb.subscribeTopics)
	}
	if fb.subscribeQoSByTopic["ori/dev-01/tier_c/enrichment/request"] != broker.QoSReasoning {
		t.Fatalf("tier c enrichment qos = %d", fb.subscribeQoSByTopic["ori/dev-01/tier_c/enrichment/request"])
	}
	handler("ori/dev-01/tier_c/enrichment/request", validTierCEnrichmentRequestPayload(t))

	deadline = time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		fb.mu.Lock()
		var payload []byte
		var retain bool
		var qos byte
		for _, msg := range fb.published {
			if msg.topic == "ori/dev-01/tier_c/enrichment/response" {
				payload = append([]byte(nil), msg.payload...)
				retain = msg.retain
				qos = msg.qos
				break
			}
		}
		fb.mu.Unlock()
		if payload != nil {
			if retain {
				t.Fatal("tier c enrichment response must not be retained")
			}
			if qos != broker.QoSReasoning {
				t.Fatalf("tier c enrichment response qos = %d", qos)
			}
			var resp contracts.TierCEnrichmentResponse
			if err := json.Unmarshal(payload, &resp); err != nil {
				t.Fatal(err)
			}
			if resp.RequestID != "enrich-1" || resp.ProposalID != "proposal-1" || resp.Explanation == "" {
				t.Fatalf("unexpected enrichment response: %#v", resp)
			}
			cancel()
			select {
			case err := <-done:
				if err != nil {
					t.Fatalf("runGateway returned %v", err)
				}
			case <-time.After(time.Second):
				t.Fatal("gateway did not stop")
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("timed out waiting for tier c enrichment response")
}

func TestMainTierCEnrichmentProviderFailureStopsStartupWhenEnabled(t *testing.T) {
	cfg := validConfig()
	cfg.Reporting = config.ReportingConfig{
		Provider:        config.ReportingProviderGemini,
		Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	deps := baseDeps(t, cfg, fb, fp, hb)
	deps.newTierCEnrichmentProvider = func(config.ReportingConfig) (enrichment.Provider, error) {
		return nil, errors.New("reporting provider unavailable")
	}

	err := runGateway(context.Background(), "gateway.yaml", deps)
	if err == nil || !strings.Contains(err.Error(), "construct tier c enrichment provider") {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case <-fb.disconnectedCh:
	case <-time.After(time.Second):
		t.Fatal("broker should be disconnected after enrichment provider startup failure")
	}
}

func TestMainTierCEnrichmentSubscribeFailureCancelsHeartbeatAndDisconnects(t *testing.T) {
	cfg := validConfig()
	cfg.Reporting = config.ReportingConfig{
		Provider:        config.ReportingProviderGemini,
		Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
	}
	fb := newFakeBroker()
	fb.subscribeErrByTopic = map[string]error{
		"ori/dev-01/tier_c/enrichment/request": errors.New("tier c subscribe failed"),
	}
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()

	err := runGateway(context.Background(), "gateway.yaml", baseDeps(t, cfg, fb, fp, hb))
	if err == nil || !strings.Contains(err.Error(), "subscribe tier c enrichment requests") {
		t.Fatalf("unexpected error: %v", err)
	}
	select {
	case <-fb.disconnectedCh:
	case <-time.After(time.Second):
		t.Fatal("broker should be disconnected after tier c enrichment subscribe failure")
	}
}

func TestGatewayPassesWebhookBridgePostureToHeartbeat(t *testing.T) {
	cfg := validConfig()
	cfg.WebhookBridge = config.WebhookBridgeConfig{
		Enabled:             true,
		ListenAddr:          "0.0.0.0:8090",
		Path:                "/webhooks/sms/africastalking",
		TargetURL:           "http://127.0.0.1:8080/webhooks/sms/africastalking",
		ProviderSourceCIDRs: []string{"102.89.0.0/16", "197.210.0.0/16"},
		RuntimeTokenEnv:     "ORI_SMS_WEBHOOK_TOKEN",
		HMACSecretEnv:       "ORI_SMS_WEBHOOK_HMAC_SECRET",
		RequestTimeoutMS:    3000,
		MaxBodyBytes:        65536,
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	bridge := newFakeWebhookBridge()
	deps := baseDeps(t, cfg, fb, fp, hb)
	deps.newWebhookBridge = func(config.WebhookBridgeConfig, webhookbridge.Options) (webhookBridgeRunner, error) {
		return bridge, nil
	}
	var captured func() *contracts.WebhookBridgePosture
	deps.newHeartbeat = func(
		_ heartbeat.PublishFunc,
		_ heartbeat.ProviderStatus,
		_ heartbeat.SIMStatus,
		opts heartbeat.Options,
	) (heartbeatRunner, error) {
		captured = opts.WebhookBridge
		return hb, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	select {
	case <-bridge.started:
	case <-time.After(time.Second):
		t.Fatal("webhook bridge did not start")
	}
	if captured == nil {
		t.Fatal("webhook bridge posture probe was not passed to heartbeat")
	}
	posture := captured()
	if posture == nil || !posture.Enabled || !posture.Ready {
		t.Fatalf("webhook bridge posture missing: %#v", posture)
	}
	if posture.LoopbackOnly || !posture.SourceCIDRsConfigured || posture.ProviderCIDRCount != 2 {
		t.Fatalf("webhook bridge CIDR posture wrong: %#v", posture)
	}
	if posture.BodyLimitBytes != 65536 || posture.RequestTimeoutMS != 3000 {
		t.Fatalf("webhook bridge limits wrong: %#v", posture)
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
}

func TestWebhookBridgePostureOmittedForDisabledBridge(t *testing.T) {
	probe := webhookBridgePostureProbe(config.WebhookBridgeConfig{
		Enabled:          false,
		ListenAddr:       "127.0.0.1:8090",
		Path:             "/webhooks/sms/africastalking",
		RequestTimeoutMS: config.DefaultWebhookBridgeRequestTimeoutMS,
		MaxBodyBytes:     config.DefaultWebhookBridgeMaxBodyBytes,
	}, func() bool { return true })
	if probe != nil {
		t.Fatalf("disabled bridge posture should be omitted, got %#v", probe())
	}
}

func TestWebhookBridgePostureReadyTracksProbe(t *testing.T) {
	ready := false
	probe := webhookBridgePostureProbe(config.WebhookBridgeConfig{
		Enabled:             true,
		TargetURL:           "http://127.0.0.1:8080/webhook",
		ProviderSourceCIDRs: []string{"127.0.0.1/32"},
		RequestTimeoutMS:    3000,
		MaxBodyBytes:        65536,
	}, func() bool { return ready })
	if probe == nil {
		t.Fatal("expected enabled bridge probe")
	}
	if probe().Ready {
		t.Fatal("bridge should not be ready before readiness probe is true")
	}
	ready = true
	if !probe().Ready {
		t.Fatal("bridge should be ready after readiness probe is true")
	}
}

func TestGatewayStartsWebhookBridgeWhenEnabled(t *testing.T) {
	cfg := validConfig()
	cfg.WebhookBridge = config.WebhookBridgeConfig{
		Enabled:          true,
		ListenAddr:       "127.0.0.1:8090",
		Path:             "/webhooks/sms/africastalking",
		TargetURL:        "http://127.0.0.1:8080/webhooks/sms/africastalking",
		RuntimeTokenEnv:  "RUNTIME_TOKEN",
		HMACSecretEnv:    "WEBHOOK_HMAC_SECRET",
		RequestTimeoutMS: 1000,
		MaxBodyBytes:     65536,
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	bridge := newFakeWebhookBridge()
	deps := baseDeps(t, cfg, fb, fp, hb)
	constructed := false
	deps.newWebhookBridge = func(got config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error) {
		constructed = true
		if !got.Enabled || got.TargetURL != cfg.WebhookBridge.TargetURL {
			t.Fatalf("unexpected bridge config: %#v", got)
		}
		if opts.Now == nil || opts.Logger == nil {
			t.Fatal("expected bridge options to include clock and logger")
		}
		return bridge, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-bridge.started:
	case <-time.After(time.Second):
		t.Fatal("webhook bridge did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
	if !constructed {
		t.Fatal("webhook bridge was not constructed")
	}
}

func TestGatewayWebhookBridgeRuntimeErrorReturnsError(t *testing.T) {
	cfg := validConfig()
	cfg.WebhookBridge = config.WebhookBridgeConfig{Enabled: true}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	bridge := &fakeWebhookBridge{started: make(chan struct{}), err: errors.New("listener failed")}
	deps := baseDeps(t, cfg, fb, fp, hb)
	deps.newWebhookBridge = func(config.WebhookBridgeConfig, webhookbridge.Options) (webhookBridgeRunner, error) {
		return bridge, nil
	}

	err := runGateway(context.Background(), "gateway.yaml", deps)
	if err == nil || !strings.Contains(err.Error(), "webhook bridge stopped") || !strings.Contains(err.Error(), "listener failed") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayWebhookBridgeUnexpectedNilReturnIsError(t *testing.T) {
	cfg := validConfig()
	cfg.WebhookBridge = config.WebhookBridgeConfig{Enabled: true}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	bridge := &fakeWebhookBridge{started: make(chan struct{}), returnNil: true}
	deps := baseDeps(t, cfg, fb, fp, hb)
	deps.newWebhookBridge = func(config.WebhookBridgeConfig, webhookbridge.Options) (webhookBridgeRunner, error) {
		return bridge, nil
	}

	err := runGateway(context.Background(), "gateway.yaml", deps)
	if err == nil || !strings.Contains(err.Error(), "webhook bridge stopped unexpectedly") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayWebhookBridgeFailureReturnsErrorBeforeBrokerConnect(t *testing.T) {
	cfg := validConfig()
	cfg.WebhookBridge = config.WebhookBridgeConfig{Enabled: true}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	deps := baseDeps(t, cfg, fb, fp, hb)
	brokerCalled := false
	deps.newBroker = func(broker.Options) (brokerClient, error) {
		brokerCalled = true
		return fb, nil
	}
	deps.newWebhookBridge = func(config.WebhookBridgeConfig, webhookbridge.Options) (webhookBridgeRunner, error) {
		return nil, errors.New("bridge config invalid")
	}

	err := runGateway(context.Background(), "gateway.yaml", deps)
	if err == nil || !strings.Contains(err.Error(), "construct webhook bridge") {
		t.Fatalf("unexpected error: %v", err)
	}
	if brokerCalled {
		t.Fatal("broker should not be constructed after webhook bridge config failure")
	}
}

func TestGatewayAuthSecretsFromConfigReadsPreviousSecret(t *testing.T) {
	t.Setenv("GATEWAY_SECRET_CURRENT", "current-secret")
	t.Setenv("GATEWAY_SECRET_PREVIOUS", "previous-secret")

	secrets, err := gatewayAuthSecretsFromConfig(config.GatewayAuthConfig{
		Enabled:                 true,
		SharedSecretEnv:         "GATEWAY_SECRET_CURRENT",
		PreviousSharedSecretEnv: "GATEWAY_SECRET_PREVIOUS",
	})
	if err != nil {
		t.Fatal(err)
	}
	if !secrets.Enabled || secrets.CurrentSecret != "current-secret" || secrets.PreviousSecret != "previous-secret" {
		t.Fatalf("unexpected secrets: %#v", secrets)
	}
}

func TestGatewayAuthSecretsFromConfigRejectsMissingPreviousSecret(t *testing.T) {
	t.Setenv("GATEWAY_SECRET_CURRENT", "current-secret")
	t.Setenv("GATEWAY_SECRET_PREVIOUS", "")

	_, err := gatewayAuthSecretsFromConfig(config.GatewayAuthConfig{
		Enabled:                 true,
		SharedSecretEnv:         "GATEWAY_SECRET_CURRENT",
		PreviousSharedSecretEnv: "GATEWAY_SECRET_PREVIOUS",
	})
	if err == nil || !strings.Contains(err.Error(), "GATEWAY_SECRET_PREVIOUS") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestGatewayAuthSecretsFromConfigRejectsMatchingPreviousSecret(t *testing.T) {
	t.Setenv("GATEWAY_SECRET_CURRENT", "same-secret")
	t.Setenv("GATEWAY_SECRET_PREVIOUS", "same-secret")

	_, err := gatewayAuthSecretsFromConfig(config.GatewayAuthConfig{
		Enabled:                 true,
		SharedSecretEnv:         "GATEWAY_SECRET_CURRENT",
		PreviousSharedSecretEnv: "GATEWAY_SECRET_PREVIOUS",
	})
	if err == nil || !strings.Contains(err.Error(), "must reference a secret different") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRuntimeClientOptionsFromSecretsBuildsSecureOptions(t *testing.T) {
	secrets := gatewayAuthSecrets{Enabled: true, CurrentSecret: "current-secret", PreviousSecret: "previous-secret"}
	encryption := config.GatewayEncryptionConfig{Enabled: true}

	opts, err := runtimeClientOptionsFromSecrets(encryption, secrets, func() time.Time { return time.UnixMilli(1234567890000) })
	if err != nil {
		t.Fatal(err)
	}
	if len(opts) != 2 {
		t.Fatalf("expected auth and encryption options, got %d", len(opts))
	}
}

func TestGatewayCleanShutdownWhenHeartbeatReturnsNilAfterCancel(t *testing.T) {
	cfg := validConfig()
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := &fakeHeartbeat{started: make(chan struct{}), returnNil: true}
	deps := baseDeps(t, cfg, fb, fp, hb)

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-hb.started:
	case <-time.After(time.Second):
		t.Fatal("heartbeat did not start")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
}

func TestGatewayConstructsSecureRuntimeClientForWeeklyReports(t *testing.T) {
	t.Setenv("GATEWAY_SECRET_CURRENT", "current-secret")
	t.Setenv("GEMINI_API_KEY", "gemini-key")
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_SECRET_CURRENT"}
	cfg.Gateway.Encryption.Enabled = true
	cfg.Reporting = config.ReportingConfig{
		Provider:     config.ReportingProviderGemini,
		Gemini:       config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		WeeklyReport: config.WeeklyReportConfig{Enabled: true, Day: "monday", Time: "08:00", Timezone: "Africa/Lagos", DeviceID: "site-a", SensorIDs: []string{"current-main"}},
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	deps := baseDeps(t, cfg, fb, fp, hb)
	runtimeClientCalled := false
	reportingProviderCalled := false
	weeklyRunnerCalled := false
	weeklyRunner := newFakeWeeklyReportRunner()
	deps.newRuntimeClient = func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		runtimeClientCalled = true
		if b == nil {
			t.Fatal("runtime client broker is nil")
		}
		if len(opts) != 2 {
			t.Fatalf("expected auth and encryption options, got %d", len(opts))
		}
		return runtimeclient.NewMQTTClient(b, opts...)
	}
	deps.newReportingProvider = func(got config.ReportingConfig) (reporting.Provider, error) {
		reportingProviderCalled = true
		if got.Provider != config.ReportingProviderGemini {
			t.Fatalf("unexpected reporting provider config: %#v", got)
		}
		return &fakeReportingProvider{}, nil
	}
	deps.newWeeklyReportRunner = func(generator *reporting.WeeklyReportGenerator, req reporting.WeeklyReportRequest, schedule reporting.Schedule, opts reporting.RunnerOptions) (weeklyReportRunner, error) {
		weeklyRunnerCalled = true
		if generator == nil || opts.Logger == nil || opts.Now == nil {
			t.Fatal("weekly runner missing generator/options")
		}
		if req.DeviceID != "site-a" || len(req.SensorIDs) != 1 || req.SensorIDs[0] != "current-main" {
			t.Fatalf("unexpected weekly request: %#v", req)
		}
		if schedule.Location == nil || schedule.Weekday != time.Monday {
			t.Fatalf("unexpected schedule: %#v", schedule)
		}
		return weeklyRunner, nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop")
	}
	if !runtimeClientCalled {
		t.Fatal("weekly reporting did not construct runtime export client")
	}
	if !reportingProviderCalled {
		t.Fatal("weekly reporting did not construct reporting provider")
	}
	if !weeklyRunnerCalled {
		t.Fatal("weekly reporting did not construct runner")
	}
}

func TestGatewayWeeklyReportRuntimeClientFailureStopsStartup(t *testing.T) {
	t.Setenv("GEMINI_API_KEY", "gemini-key")
	cfg := validConfig()
	cfg.Reporting = config.ReportingConfig{
		Provider:     config.ReportingProviderGemini,
		Gemini:       config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		WeeklyReport: config.WeeklyReportConfig{Enabled: true, Day: "monday", Time: "08:00", Timezone: "Africa/Lagos", DeviceID: "site-a", SensorIDs: []string{"current-main"}},
	}
	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	deps := baseDeps(t, cfg, fb, fp, hb)
	deps.newRuntimeClient = func(brokerClient, ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return nil, errors.New("runtime export unavailable")
	}

	err := runGateway(context.Background(), "gateway.yaml", deps)
	if err == nil || !strings.Contains(err.Error(), "construct runtime export client") {
		t.Fatalf("unexpected error: %v", err)
	}
}
