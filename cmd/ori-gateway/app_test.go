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
	"github.com/ori-platform/ori-gateway/internal/fleet"
	"github.com/ori-platform/ori-gateway/internal/heartbeat"
	"github.com/ori-platform/ori-gateway/internal/provider"
	"github.com/ori-platform/ori-gateway/internal/sim"
)

type fakeBroker struct {
	mu sync.Mutex

	connectErr    error
	subscribeErr  error
	disconnectErr error
	publishErr    error

	connected    bool
	disconnected bool

	subscribeTopic string
	subscribeQoS   byte
	handler        broker.MessageHandler

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
		subscribed:     make(chan struct{}),
		disconnectedCh: make(chan struct{}),
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
	b.subscribeTopic = topic
	b.subscribeQoS = qos
	b.handler = handler
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
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.handler
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

type fakeHeartbeat struct {
	started chan struct{}
	once    sync.Once
	err     error
}

func newFakeHeartbeat() *fakeHeartbeat {
	return &fakeHeartbeat{started: make(chan struct{})}
}

func (h *fakeHeartbeat) Run(ctx context.Context) error {
	h.once.Do(func() { close(h.started) })
	if h.err != nil {
		return h.err
	}
	<-ctx.Done()
	return ctx.Err()
}

func validConfig() config.Config {
	return config.Config{
		Gateway: config.GatewayConfig{
			BrokerURL:          "tcp://localhost:1883",
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
	if fb.subscribeTopic != contracts.GatewayReasoningRequestTopicFilter {
		t.Fatalf("subscribe topic = %q", fb.subscribeTopic)
	}
	if fb.subscribeQoS != broker.QoSReasoning {
		t.Fatalf("subscribe qos = %d", fb.subscribeQoS)
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
	if len(events) != 2 || events[0] != "heartbeat" || events[1] != "subscribe" {
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
