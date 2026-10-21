// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"log/slog"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/provider"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

// startGatewayOnABroker runs the gateway on a real MQTT broker with site
// health, and returns the broker URL, the site health address and the
// gateway's clock.
func startGatewayOnABroker(t *testing.T, prov provider.Provider) (string, string, func() time.Time) {
	t.Helper()
	brokerURL := startEmbeddedBroker(t)
	siteAddr := loopbackAddr(t)
	cfg := validConfig()
	cfg.Gateway.BrokerURL = brokerURL
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: siteAddr}
	logs := &syncBuffer{}
	deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.newProvider = func(config.ProviderConfig) (provider.Provider, error) { return prov, nil }
	deps.logger = slog.New(slog.NewTextHandler(logs, nil))
	deps.newBroker = func(opts broker.Options) (brokerClient, error) { return broker.New(opts) }
	deps.newRuntimeClient = func(brokerClient, ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return &runtimeclient.FakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(logs.String())
		}
	})
	// The gateway's subscriptions are made before site health starts serving.
	waitUntil(t, "site health", func() bool { ingress, _ := siteIngress(t, siteAddr); return ingress != nil })
	return brokerURL, siteAddr, deps.now
}

// TestABurstOfReasoningRequestsIsAnsweredWithoutStalling sends a burst of
// requests within the ingress rate through a real broker. Each reply waits
// for its PUBACK; were a request handled on the client's ordered delivery,
// the next request would arrive ahead of that PUBACK and the reply would wait
// out the publish timeout. Every request is answered well within it.
func TestABurstOfReasoningRequestsIsAnsweredWithoutStalling(t *testing.T) {
	prov := &fakeProvider{healthy: true}
	brokerURL, _, _ := startGatewayOnABroker(t, prov)
	peer := newRuntimePeer(t, brokerURL, "ori/dev-01/reasoning/response")

	const burst = 10
	started := time.Now()
	for i := range burst {
		peer.publish(t, "ori/dev-01/reasoning/request", reasoningRequestFrom(t, "dev-01", fmt.Sprintf("burst-%d", i), 300))
	}
	waitUntil(t, "every request in the burst to be answered", func() bool {
		return len(peer.received("ori/dev-01/reasoning/response")) == burst
	})
	if elapsed := time.Since(started); elapsed >= 4*time.Second {
		t.Fatalf("the burst took %v to answer, want well within the 5s publish timeout", elapsed)
	}
	answered := map[string]bool{}
	for _, raw := range peer.received("ori/dev-01/reasoning/response") {
		var reply contracts.ReasoningResponse
		if err := json.Unmarshal(raw, &reply); err != nil {
			t.Fatal(err)
		}
		if reply.Error != nil {
			t.Fatalf("request %s answered with an error: %s", reply.RequestID, *reply.Error)
		}
		answered[reply.RequestID] = true
	}
	for i := range burst {
		if id := fmt.Sprintf("burst-%d", i); !answered[id] {
			t.Fatalf("request %s was not answered; answered %v", id, answered)
		}
	}
}

// TestAHeartbeatIsNotDelayedByAReasoningCall holds a reasoning call in the
// provider and sends a runtime heartbeat behind it: the heartbeat reaches site
// health while the call is still held.
func TestAHeartbeatIsNotDelayedByAReasoningCall(t *testing.T) {
	prov := &heldProvider{fakeProvider: fakeProvider{healthy: true}, entered: make(chan struct{}), release: make(chan struct{})}
	brokerURL, siteAddr, now := startGatewayOnABroker(t, prov)
	var releaseOnce sync.Once
	release := func() { releaseOnce.Do(func() { close(prov.release) }) }
	t.Cleanup(release)
	peer := newRuntimePeer(t, brokerURL, "ori/dev-01/reasoning/response")

	peer.publish(t, "ori/dev-01/reasoning/request", reasoningRequestFrom(t, "dev-01", "held", 300))
	select {
	case <-prov.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the reasoning call never reached the provider")
	}
	heartbeat := mustJSON(t, contracts.RuntimeNodeHeartbeat{
		DeviceID: "dev-01", Status: "healthy", LastSeenMS: now().UnixMilli(), ActiveTriggers: []string{},
	})
	peer.publish(t, "ori/dev-01/runtime/heartbeat", heartbeat)
	waitUntil(t, "the heartbeat to reach site health while the call is held", func() bool {
		_, nodes := siteIngress(t, siteAddr)
		return strings.Contains(string(mustJSON(t, nodes)), `"gateway_observed":true`)
	})
	if got := peer.received("ori/dev-01/reasoning/response"); len(got) != 0 {
		t.Fatalf("the held call was answered before release: %q", got)
	}
	release()
	waitUntil(t, "the held call to be answered once released", func() bool {
		return len(peer.received("ori/dev-01/reasoning/response")) == 1
	})
}

// TestEachDeviceHasItsOwnAllowanceOfRequestsInFlight fills one configured
// device's allowance: its next request is dropped and counted as a volume
// drop, while another configured device's request still runs, and every
// unconfigured device segment shares one allowance. A finished request
// returns its place.
func TestEachDeviceHasItsOwnAllowanceOfRequestsInFlight(t *testing.T) {
	gate := newCallbackGate()
	ingress := broker.NewIngressStats(slog.New(slog.DiscardHandler))
	pool := newRequestPool(contracts.FamilyReasoningRequest, []string{"dev-01", "dev-02"}, gate, ingress, slog.New(slog.DiscardHandler))
	limit := broker.IngressRates[contracts.FamilyReasoningRequest].Burst
	if pool.limit != limit || limit < 1 {
		t.Fatalf("allowance = %d, want the family burst %d", pool.limit, limit)
	}
	release := make(chan struct{})
	var mu sync.Mutex
	ran := map[string]int{}
	handler := pool.handler(func(topic string, _ []byte) {
		mu.Lock()
		ran[topic]++
		mu.Unlock()
		<-release
	})
	running := func(topic string) int {
		mu.Lock()
		defer mu.Unlock()
		return ran[topic]
	}
	dropped := func() uint64 {
		return ingress.Snapshot()[contracts.FamilyReasoningRequest].VolumeDropped
	}

	for range limit {
		handler("ori/dev-01/reasoning/request", []byte(`{}`))
	}
	waitUntil(t, "a device's allowance to be in flight", func() bool { return running("ori/dev-01/reasoning/request") == limit })
	handler("ori/dev-01/reasoning/request", []byte(`{}`))
	if got := dropped(); got != 1 {
		t.Fatalf("volume drops = %d after a request over the allowance, want 1", got)
	}
	handler("ori/dev-02/reasoning/request", []byte(`{}`))
	waitUntil(t, "another configured device's request to run", func() bool { return running("ori/dev-02/reasoning/request") == 1 })

	for i := range limit {
		handler(fmt.Sprintf("ori/stranger-%d/reasoning/request", i), []byte(`{}`))
	}
	handler("ori/stranger-x/reasoning/request", []byte(`{}`))
	if got := dropped(); got != 2 {
		t.Fatalf("volume drops = %d, want unconfigured device segments to share one allowance", got)
	}

	close(release)
	gate.close()
	if got := pool.held; len(got) != 0 {
		t.Fatalf("requests still counted in flight after every one finished: %v", got)
	}
}

// TestNoRequestStartsOnceTheStopIsDecided refuses the gate: a request is not
// run and holds no place in the allowance.
func TestNoRequestStartsOnceTheStopIsDecided(t *testing.T) {
	gate := newCallbackGate()
	ingress := broker.NewIngressStats(slog.New(slog.DiscardHandler))
	pool := newRequestPool(contracts.FamilyReasoningRequest, []string{"dev-01"}, gate, ingress, slog.New(slog.DiscardHandler))
	gate.refuse()
	ran := make(chan struct{}, 1)
	pool.handler(func(string, []byte) { ran <- struct{}{} })("ori/dev-01/reasoning/request", []byte(`{}`))
	gate.close()
	select {
	case <-ran:
		t.Fatal("a request ran after the stop was decided")
	default:
	}
	if len(pool.held) != 0 || ingress.Snapshot()[contracts.FamilyReasoningRequest].VolumeDropped != 0 {
		t.Fatalf("a refused request kept state: in flight %v, counts %v", pool.held, ingress.Snapshot())
	}
}

// TestShutdownJoinsARequestRunningOffTheCallback starts a request whose
// handler ignores cancellation: closing the gate does not return until it
// does, so no request outlives the run (GW-15).
func TestShutdownJoinsARequestRunningOffTheCallback(t *testing.T) {
	gate := newCallbackGate()
	pool := newRequestPool(contracts.FamilyReasoningRequest, []string{"dev-01"}, gate,
		broker.NewIngressStats(slog.New(slog.DiscardHandler)), slog.New(slog.DiscardHandler))
	entered, release := make(chan struct{}), make(chan struct{})
	pool.handler(func(string, []byte) {
		close(entered)
		<-release
	})("ori/dev-01/reasoning/request", []byte(`{}`))
	<-entered
	closed := make(chan struct{})
	go func() {
		gate.close()
		close(closed)
	}()
	select {
	case <-closed:
		t.Fatal("the gate closed while an admitted request was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	select {
	case <-closed:
	case <-time.After(5 * time.Second):
		t.Fatal("the gate did not close once the request returned")
	}
}

// TestAPanickingRequestIsRecoveredAndReleased has a handler panic off the
// callback: the process survives, the panic is logged, and the request's
// place and admission are released.
func TestAPanickingRequestIsRecoveredAndReleased(t *testing.T) {
	gate := newCallbackGate()
	var logs syncBuffer
	pool := newRequestPool(contracts.FamilyReasoningRequest, []string{"dev-01"}, gate,
		broker.NewIngressStats(slog.New(slog.DiscardHandler)), slog.New(slog.NewTextHandler(&logs, nil)))
	pool.handler(func(string, []byte) { panic("provider exploded") })("ori/dev-01/reasoning/request", []byte(`{}`))
	gate.close()
	if len(pool.held) != 0 {
		t.Fatalf("a panicking request kept its place: %v", pool.held)
	}
	if !strings.Contains(logs.String(), "mqtt message handler panic") {
		t.Fatalf("the panic was not logged:\n%s", logs.String())
	}
}

func TestRequestDeviceSegment(t *testing.T) {
	for topic, want := range map[string]string{
		"ori/dev-01/reasoning/request":         "dev-01",
		"ori/dev-01/tier_c/enrichment/request": "dev-01",
		"ori//reasoning/request":               "",
		"other/dev-01/reasoning/request":       "",
		"ori/dev-01":                           "dev-01",
	} {
		if got := requestDeviceSegment(topic); got != want {
			t.Errorf("requestDeviceSegment(%q) = %q, want %q", topic, got, want)
		}
	}
}

// TestABurstOfOutboundCarriagesIsAcknowledgedWithoutStalling sends a burst of
// outbound evidence carriages from one device through a real broker. Each
// acknowledgement waits for its PUBACK; were a carriage handled on the
// evidence client's ordered delivery, the next carriage would arrive ahead of
// that PUBACK and the acknowledgement would wait out the 30s publish bound.
// Every carriage is acknowledged well within it, in the order it arrived.
func TestABurstOfOutboundCarriagesIsAcknowledgedWithoutStalling(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	brokerURL := startEmbeddedBroker(t)
	siteAddr := loopbackAddr(t)
	cfg := validConfig()
	cfg.Gateway.BrokerURL = brokerURL
	cfg.Gateway.DeviceIDs = []string{"dev-01", "dev-02"}
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: siteAddr}
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: filepath.Join(t.TempDir(), "outbound"),
		ReturnQueueDirectory: filepath.Join(t.TempDir(), "returned"), MaxItems: 64,
		MaxBytes: 16 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
	}
	var logs syncBuffer
	deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(slog.NewTextHandler(&logs, nil))
	deps.newBroker = func(opts broker.Options) (brokerClient, error) { return broker.New(opts) }
	deps.newRuntimeClient = func(brokerClient, ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return &runtimeclient.FakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })
	t.Cleanup(func() {
		if t.Failed() {
			t.Log(logs.String())
		}
	})
	waitUntil(t, "site health", func() bool { ingress, _ := siteIngress(t, siteAddr); return ingress != nil })
	peer := newRuntimePeer(t, brokerURL, "ori/dev-01/evidence/outbound/ack")

	const burst = 10
	started := time.Now()
	for i := range burst {
		artifact := mustJSON(t, map[string]any{"v": 1, "device_id": "dev-01", "high_water_seq": i + 1, "signature": "ed25519:opaque"})
		peer.publish(t, "ori/dev-01/evidence/outbound", mustJSON(t, map[string]string{
			"device_id": "dev-01", "artifact_type": "checkpoint", "artifact_b64": base64.StdEncoding.EncodeToString(artifact),
		}))
	}
	waitUntil(t, "every carriage in the burst to be acknowledged", func() bool {
		return len(peer.received("ori/dev-01/evidence/outbound/ack")) == burst
	})
	if elapsed := time.Since(started); elapsed >= 4*time.Second {
		t.Fatalf("the burst took %v to acknowledge, want well within the publish bound", elapsed)
	}
	for _, raw := range peer.received("ori/dev-01/evidence/outbound/ack") {
		var ack struct {
			Outcome string `json:"outcome"`
		}
		if err := json.Unmarshal(raw, &ack); err != nil {
			t.Fatal(err)
		}
		if ack.Outcome != "queued" {
			t.Fatalf("a carriage was acknowledged %q, want queued: %s", ack.Outcome, raw)
		}
	}
}

// TestAnOrderedPoolKeepsEachDevicesArrivalOrder holds one device's first
// request: its later requests wait behind it and then run in arrival order,
// while another device's requests run meanwhile.
func TestAnOrderedPoolKeepsEachDevicesArrivalOrder(t *testing.T) {
	gate := newCallbackGate()
	ingress := broker.NewIngressStats(slog.New(slog.DiscardHandler))
	pool := newOrderedRequestPool(contracts.FamilyEvidenceOutbound, []string{"dev-01", "dev-02"}, gate, ingress, slog.New(slog.DiscardHandler))
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	handler := pool.handler(func(topic string, payload []byte) {
		if string(payload) == "dev-01:0" {
			<-release
		}
		mu.Lock()
		order = append(order, string(payload))
		mu.Unlock()
	})
	seen := func() []string {
		mu.Lock()
		defer mu.Unlock()
		return slices.Clone(order)
	}
	for i := range 5 {
		handler("ori/dev-01/evidence/outbound", fmt.Appendf(nil, "dev-01:%d", i))
	}
	for i := range 3 {
		handler("ori/dev-02/evidence/outbound", fmt.Appendf(nil, "dev-02:%d", i))
	}
	waitUntil(t, "another device's requests to run past the held one", func() bool { return len(seen()) == 3 })
	if got := strings.Join(seen(), ","); got != "dev-02:0,dev-02:1,dev-02:2" {
		t.Fatalf("ran %s while dev-01 was held, want only dev-02's, in order", got)
	}
	close(release)
	gate.close()
	var dev01 []string
	for _, item := range seen() {
		if strings.HasPrefix(item, "dev-01:") {
			dev01 = append(dev01, item)
		}
	}
	if got := strings.Join(dev01, ","); got != "dev-01:0,dev-01:1,dev-01:2,dev-01:3,dev-01:4" {
		t.Fatalf("dev-01 ran %s, want arrival order", got)
	}
	if len(pool.held) != 0 || len(pool.queued) != 0 || len(pool.draining) != 0 {
		t.Fatalf("state left after every request finished: held %v queued %v draining %v", pool.held, pool.queued, pool.draining)
	}
}

// TestAnOrderedPoolBoundsWhatADeviceHolds fills one device's allowance with
// one request running and the rest queued: the next is dropped and counted,
// and a refused gate accepts nothing.
func TestAnOrderedPoolBoundsWhatADeviceHolds(t *testing.T) {
	gate := newCallbackGate()
	ingress := broker.NewIngressStats(slog.New(slog.DiscardHandler))
	pool := newOrderedRequestPool(contracts.FamilyEvidenceOutbound, []string{"dev-01"}, gate, ingress, slog.New(slog.DiscardHandler))
	limit := broker.IngressRates[contracts.FamilyEvidenceOutbound].Burst
	release := make(chan struct{})
	var ran sync.WaitGroup
	ran.Add(1)
	var once sync.Once
	handler := pool.handler(func(string, []byte) {
		once.Do(ran.Done)
		<-release
	})
	for range limit {
		handler("ori/dev-01/evidence/outbound", []byte(`{}`))
	}
	ran.Wait()
	handler("ori/dev-01/evidence/outbound", []byte(`{}`))
	if got := ingress.Snapshot()[contracts.FamilyEvidenceOutbound].VolumeDropped; got != 1 {
		t.Fatalf("volume drops = %d with the allowance held, want 1", got)
	}
	gate.refuse()
	close(release)
	handler("ori/dev-01/evidence/outbound", []byte(`{}`))
	gate.close()
	if len(pool.held) != 0 || len(pool.queued) != 0 {
		t.Fatalf("state left after the gate closed: held %v queued %v", pool.held, pool.queued)
	}
}
