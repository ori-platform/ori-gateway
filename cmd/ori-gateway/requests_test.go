// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
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
	"github.com/ori-platform/ori-gateway/internal/enrichment"
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
	pool := newRequestPool([]string{"dev-01", "dev-02"}, gate, nil, ingress, slog.New(slog.DiscardHandler), contracts.FamilyReasoningRequest)
	limit := broker.IngressRates[contracts.FamilyReasoningRequest].Burst
	if pool.limits[contracts.FamilyReasoningRequest] != limit || limit < 1 {
		t.Fatalf("allowance = %d, want the family burst %d", pool.limits[contracts.FamilyReasoningRequest], limit)
	}
	release := make(chan struct{})
	var mu sync.Mutex
	ran := map[string]int{}
	handler := pool.handler(contracts.FamilyReasoningRequest, func(topic string, _ []byte) {
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
	// After the stop nothing is accepted, so nothing is counted either.
	gate.refuse()
	handler("ori/dev-01/reasoning/request", []byte(`{}`))
	if got := dropped(); got != 2 {
		t.Fatalf("volume drops = %d after the stop, want a refused message uncounted", got)
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
	pool := newRequestPool([]string{"dev-01"}, gate, nil, ingress, slog.New(slog.DiscardHandler), contracts.FamilyReasoningRequest)
	gate.refuse()
	ran := make(chan struct{}, 1)
	pool.handler(contracts.FamilyReasoningRequest, func(string, []byte) { ran <- struct{}{} })("ori/dev-01/reasoning/request", []byte(`{}`))
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
	pool := newRequestPool([]string{"dev-01"}, gate, nil,
		broker.NewIngressStats(slog.New(slog.DiscardHandler)), slog.New(slog.DiscardHandler), contracts.FamilyReasoningRequest)
	entered, release := make(chan struct{}), make(chan struct{})
	pool.handler(contracts.FamilyReasoningRequest, func(string, []byte) {
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
	pool := newRequestPool([]string{"dev-01"}, gate, nil,
		broker.NewIngressStats(slog.New(slog.DiscardHandler)), slog.New(slog.NewTextHandler(&logs, nil)), contracts.FamilyReasoningRequest)
	pool.handler(contracts.FamilyReasoningRequest, func(string, []byte) { panic("provider exploded") })("ori/dev-01/reasoning/request", []byte(`{}`))
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

	const burst = 20
	var want []string
	started := time.Now()
	for i := range burst {
		artifact := mustJSON(t, map[string]any{"v": 1, "device_id": "dev-01", "high_water_seq": i + 1, "signature": "ed25519:opaque"})
		digest := sha256.Sum256(artifact)
		want = append(want, "sha256:"+hex.EncodeToString(digest[:]))
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
	// Admission order is the courier's delivery order, so each carriage is
	// admitted, and acknowledged, in the order it arrived.
	var got []string
	for _, raw := range peer.received("ori/dev-01/evidence/outbound/ack") {
		var ack struct {
			Outcome        string `json:"outcome"`
			ArtifactDigest string `json:"artifact_digest"`
		}
		if err := json.Unmarshal(raw, &ack); err != nil {
			t.Fatal(err)
		}
		if ack.Outcome != "queued" {
			t.Fatalf("a carriage was acknowledged %q, want queued: %s", ack.Outcome, raw)
		}
		got = append(got, ack.ArtifactDigest)
	}
	if !slices.Equal(got, want) {
		t.Fatalf("carriages were acknowledged in the order\n%v\nwant arrival order\n%v", got, want)
	}
}

// TestAnOrderedPoolKeepsEachDevicesArrivalOrder holds one device's first
// request: its later requests wait behind it and then run in arrival order,
// while another device's requests run meanwhile.
func TestAnOrderedPoolKeepsEachDevicesArrivalOrder(t *testing.T) {
	gate := newCallbackGate()
	ingress := broker.NewIngressStats(slog.New(slog.DiscardHandler))
	pool := newOrderedRequestPool([]string{"dev-01", "dev-02"}, gate, nil, ingress, slog.New(slog.DiscardHandler), contracts.FamilyEvidenceOutbound)
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	handler := pool.handler(contracts.FamilyEvidenceOutbound, func(topic string, payload []byte) {
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
	pool := newOrderedRequestPool([]string{"dev-01"}, gate, nil, ingress, slog.New(slog.DiscardHandler), contracts.FamilyEvidenceOutbound)
	limit := broker.IngressRates[contracts.FamilyEvidenceOutbound].Burst
	release := make(chan struct{})
	var ran sync.WaitGroup
	ran.Add(1)
	var once sync.Once
	handler := pool.handler(contracts.FamilyEvidenceOutbound, func(string, []byte) {
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

// TestABurstOfEnrichmentRequestsIsAnsweredWithoutStalling is the enrichment
// family's burst through a real broker: every request is answered well
// within the 5s publish timeout.
func TestABurstOfEnrichmentRequestsIsAnsweredWithoutStalling(t *testing.T) {
	brokerURL := startEmbeddedBroker(t)
	siteAddr := loopbackAddr(t)
	cfg := validConfig()
	cfg.Gateway.BrokerURL = brokerURL
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: siteAddr}
	cfg.Reporting = config.ReportingConfig{
		Provider:        config.ReportingProviderGemini,
		Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
	}
	deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.newBroker = func(opts broker.Options) (brokerClient, error) { return broker.New(opts) }
	deps.newTierCEnrichmentProvider = func(config.ReportingConfig) (enrichment.Provider, error) {
		return &fakeTierCEnrichmentProvider{}, nil
	}
	deps.newRuntimeClient = func(brokerClient, ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return &runtimeclient.FakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })
	waitUntil(t, "site health", func() bool { ingress, _ := siteIngress(t, siteAddr); return ingress != nil })
	peer := newRuntimePeer(t, brokerURL, "ori/dev-01/tier_c/enrichment/response")

	const burst = 8
	started := time.Now()
	for i := range burst {
		var req contracts.TierCEnrichmentRequest
		if err := json.Unmarshal(validTierCEnrichmentRequestPayload(t), &req); err != nil {
			t.Fatal(err)
		}
		req.RequestID = fmt.Sprintf("enrich-%d", i)
		peer.publish(t, "ori/dev-01/tier_c/enrichment/request", mustJSON(t, req))
	}
	waitUntil(t, "every enrichment request in the burst to be answered", func() bool {
		return len(peer.received("ori/dev-01/tier_c/enrichment/response")) == burst
	})
	if elapsed := time.Since(started); elapsed >= 4*time.Second {
		t.Fatalf("the burst took %v to answer, want well within the 5s publish timeout", elapsed)
	}
}

// TestADuplicateInFlightRequestNeitherCountsNorResetsTheFailureCount fails
// every reply, holds one request in the provider and sends its request_id
// again after four failures: the refused repeat leaves the count alone, so
// the fifth failure still reaches the limit (GW-6).
func TestADuplicateInFlightRequestNeitherCountsNorResetsTheFailureCount(t *testing.T) {
	fb := newFakeBroker()
	fb.publishErr = errors.New("broker refuses replies")
	prov := &heldProvider{fakeProvider: fakeProvider{healthy: true}, entered: make(chan struct{}), release: make(chan struct{})}
	var logs syncBuffer
	deps := baseDeps(t, validConfig(), fb, &prov.fakeProvider, newFakeHeartbeat())
	deps.newProvider = func(config.ProviderConfig) (provider.Provider, error) { return selectiveHold{prov}, nil }
	deps.logger = slog.New(slog.NewTextHandler(&logs, nil))
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(5 * time.Second):
		t.Fatal("the gateway did not subscribe")
	}
	handler := fb.currentHandler()
	request := func(id string) []byte {
		return mustJSON(t, contracts.ReasoningRequest{
			RequestID: id, DeviceID: "site-a", SensorType: "current_clamp", TriggerName: "overcurrent",
			Prompt: "Explain anomaly.", ActionTierHint: contracts.ActionTierC, TimeoutMS: 1000,
		})
	}
	lines := func(text string) int { return strings.Count(logs.String(), text) }

	handler("ori/site-a/reasoning/request", request("held"))
	<-prov.entered
	for i := range defaultRequestFailureLimit - 1 {
		handler("ori/site-a/reasoning/request", request(fmt.Sprintf("fail-%d", i)))
	}
	waitUntil(t, "four delivery failures", func() bool { return lines("publish response") == defaultRequestFailureLimit-1 })
	handler("ori/site-a/reasoning/request", request("held"))
	waitUntil(t, "the repeat to be refused", func() bool { return lines("already in flight") == 1 })
	handler("ori/site-a/reasoning/request", request("fail-last"))
	select {
	case err := <-done:
		t.Fatalf("the gateway returned %v while the held request was still running", err)
	case <-time.After(50 * time.Millisecond):
	}
	close(prov.release)
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "reasoning request failure limit reached") {
			t.Fatalf("runGateway = %v, want the request failure escalation", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatalf("five delivery failures with a refused repeat between them did not escalate:\n%s", logs.String())
	}
}

// selectiveHold holds only the request named "held" in its provider.
type selectiveHold struct{ *heldProvider }

func (p selectiveHold) Reason(ctx context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	if req.RequestID == "held" {
		return p.heldProvider.Reason(ctx, req)
	}
	return p.heldProvider.fakeProvider.Reason(ctx, req)
}

// TestAPoolRefusesATopicOutsideItsFamilies subscribes a pool to a topic of a
// family it does not serve: the subscription is refused, so a pool wired to
// the wrong family stops the start rather than bounding with another
// family's allowance.
func TestAPoolRefusesATopicOutsideItsFamilies(t *testing.T) {
	pool := newOrderedRequestPool([]string{"dev-01"}, newCallbackGate(), nil,
		broker.NewIngressStats(slog.New(slog.DiscardHandler)), slog.New(slog.DiscardHandler), contracts.FamilyEvidenceOutbound)
	fb := newFakeBroker()
	if err := pool.subscribe(context.Background(), fb, contracts.GatewayReasoningRequestTopicFilter, 1, func(string, []byte) {}); err == nil {
		t.Fatal("a pool for outbound evidence subscribed reasoning requests")
	}
	if err := pool.subscribe(context.Background(), fb, "not/a/family", 1, func(string, []byte) {}); err == nil {
		t.Fatal("a pool subscribed a topic of no family")
	}
	if err := pool.subscribe(context.Background(), fb, contracts.EvidenceOutboundTopicFilter, 1, func(string, []byte) {}); err != nil {
		t.Fatalf("a pool refused its own family: %v", err)
	}
}

// TestAMessageWaitingWhenTheRunStopsIsReleasedUnhandled queues messages
// behind a held one and stops the run: the waiting messages are not handled,
// and every admission is still released, so nothing durable is done after the
// stop and shutdown is not held up by a queue.
func TestAMessageWaitingWhenTheRunStopsIsReleasedUnhandled(t *testing.T) {
	gate := newCallbackGate()
	stopped := make(chan struct{})
	pool := newOrderedRequestPool([]string{"dev-01"}, gate, stopped,
		broker.NewIngressStats(slog.New(slog.DiscardHandler)), slog.New(slog.DiscardHandler), contracts.FamilyEvidenceOutbound)
	entered, release := make(chan struct{}), make(chan struct{})
	var mu sync.Mutex
	var handled []string
	handler := pool.handler(contracts.FamilyEvidenceOutbound, func(_ string, payload []byte) {
		if string(payload) == "first" {
			close(entered)
			<-release
		}
		mu.Lock()
		handled = append(handled, string(payload))
		mu.Unlock()
	})
	handler("ori/dev-01/evidence/outbound", []byte("first"))
	<-entered
	handler("ori/dev-01/evidence/outbound", []byte("second"))
	handler("ori/dev-01/evidence/outbound", []byte("third"))
	close(stopped)
	gate.refuse()
	close(release)
	gate.close()
	if strings.Join(handled, ",") != "first" {
		t.Fatalf("handled %v after the stop, want only the one already running", handled)
	}
	if len(pool.held) != 0 || len(pool.queued) != 0 || len(pool.draining) != 0 {
		t.Fatalf("state left after the stop: held %v queued %v draining %v", pool.held, pool.queued, pool.draining)
	}
}

// TestCarriagesAndAcknowledgementsShareEachDevicesOrder interleaves two
// families in one ordered pool: a device's messages run in arrival order
// across both, so an acknowledgement never overtakes a carriage that arrived
// before it, and each family keeps its own allowance.
func TestCarriagesAndAcknowledgementsShareEachDevicesOrder(t *testing.T) {
	gate := newCallbackGate()
	ingress := broker.NewIngressStats(slog.New(slog.DiscardHandler))
	pool := newEvidencePool([]string{"dev-01"}, gate, nil, ingress, slog.New(slog.DiscardHandler))
	if !pool.ordered {
		t.Fatal("the evidence pool is not ordered")
	}
	if pool.limits[contracts.FamilyEvidenceOutbound] != broker.IngressRates[contracts.FamilyEvidenceOutbound].Burst ||
		pool.limits[contracts.FamilyEvidenceInboundAck] != broker.IngressRates[contracts.FamilyEvidenceInboundAck].Burst {
		t.Fatalf("allowances = %v, want each family's burst", pool.limits)
	}
	release := make(chan struct{})
	var mu sync.Mutex
	var order []string
	record := func(_ string, payload []byte) {
		if string(payload) == "carriage-0" {
			<-release
		}
		mu.Lock()
		order = append(order, string(payload))
		mu.Unlock()
	}
	carriage := pool.handler(contracts.FamilyEvidenceOutbound, record)
	ack := pool.handler(contracts.FamilyEvidenceInboundAck, record)
	carriage("ori/dev-01/evidence/outbound", []byte("carriage-0"))
	ack("ori/dev-01/evidence/inbound/ack", []byte("ack-0"))
	carriage("ori/dev-01/evidence/outbound", []byte("carriage-1"))
	ack("ori/dev-01/evidence/inbound/ack", []byte("ack-1"))
	close(release)
	gate.close()
	if got := strings.Join(order, ","); got != "carriage-0,ack-0,carriage-1,ack-1" {
		t.Fatalf("ran %s, want arrival order across both families", got)
	}

	// Each family keeps its own allowance: a device holding its full
	// allowance of carriages still has its acknowledgements accepted.
	gate = newCallbackGate()
	pool = newEvidencePool([]string{"dev-01"}, gate, nil, ingress, slog.New(slog.DiscardHandler))
	hold := make(chan struct{})
	var acked sync.WaitGroup
	acked.Add(1)
	carriage = pool.handler(contracts.FamilyEvidenceOutbound, func(string, []byte) { <-hold })
	ack = pool.handler(contracts.FamilyEvidenceInboundAck, func(string, []byte) { acked.Done() })
	for range pool.limits[contracts.FamilyEvidenceOutbound] {
		carriage("ori/dev-01/evidence/outbound", []byte(`{}`))
	}
	before := ingress.Snapshot()[contracts.FamilyEvidenceInboundAck].VolumeDropped
	ack("ori/dev-01/evidence/inbound/ack", []byte(`{}`))
	if got := ingress.Snapshot()[contracts.FamilyEvidenceInboundAck].VolumeDropped; got != before {
		t.Fatalf("an acknowledgement was dropped while only the carriage allowance was full")
	}
	close(hold)
	acked.Wait()
	gate.close()
}

// orderProbe is a slog handler that, when the gateway logs its refusal of an
// inbound evidence acknowledgement, records whether the carriage sent before
// it had already been acknowledged.
type orderProbe struct {
	slog.Handler
	acked func() bool
	mu    *sync.Mutex
	seen  *[]bool
}

func (h orderProbe) Handle(ctx context.Context, r slog.Record) error {
	if strings.Contains(r.Message, "evidence inbound acknowledgement refused") {
		h.mu.Lock()
		*h.seen = append(*h.seen, h.acked())
		h.mu.Unlock()
	}
	return h.Handler.Handle(ctx, r)
}

// TestTheRunningGatewayKeepsEachDevicesEvidenceOrder sends a device's
// carriage and then an inbound acknowledgement through the gateway's real
// evidence subscriptions: the acknowledgement is handled only once the
// carriage before it has been admitted and acknowledged, as the evidence
// client delivered them.
func TestTheRunningGatewayKeepsEachDevicesEvidenceOrder(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	cfg := validConfig()
	cfg.Gateway.DeviceIDs = []string{"dev-01", "dev-02"}
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: filepath.Join(t.TempDir(), "outbound"),
		ReturnQueueDirectory: filepath.Join(t.TempDir(), "returned"), MaxItems: 64,
		MaxBytes: 16 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
	}
	mainBroker, evidenceBroker := newFakeBroker(), newFakeBroker()
	deps := baseDeps(t, cfg, mainBroker, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.newBroker = func(opts broker.Options) (brokerClient, error) {
		if opts.ClientID == defaultEvidenceClientID {
			return evidenceBroker, nil
		}
		return mainBroker, nil
	}
	carriageAcked := func(n int) func() bool {
		return func() bool {
			evidenceBroker.mu.Lock()
			defer evidenceBroker.mu.Unlock()
			count := 0
			for _, msg := range evidenceBroker.published {
				if msg.topic == "ori/dev-01/evidence/outbound/ack" {
					count++
				}
			}
			return count >= n
		}
	}
	var mu sync.Mutex
	var seen []bool
	acked := 0
	deps.logger = slog.New(orderProbe{Handler: slog.NewTextHandler(io.Discard, nil), acked: func() bool { return carriageAcked(acked)() }, mu: &mu, seen: &seen})
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })
	waitUntil(t, "the evidence subscriptions", func() bool {
		return evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter) != nil &&
			evidenceBroker.handlerFor("ori/dev-01/evidence/inbound/ack") != nil
	})
	carriage := evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter)
	ack := evidenceBroker.handlerFor("ori/dev-01/evidence/inbound/ack")

	const rounds = 10
	for i := range rounds {
		mu.Lock()
		acked = i + 1
		mu.Unlock()
		artifact := mustJSON(t, map[string]any{"v": 1, "device_id": "dev-01", "high_water_seq": i + 1, "signature": "ed25519:opaque"})
		carriage("ori/dev-01/evidence/outbound", mustJSON(t, map[string]string{
			"device_id": "dev-01", "artifact_type": "checkpoint", "artifact_b64": base64.StdEncoding.EncodeToString(artifact),
		}))
		ack("ori/dev-01/evidence/inbound/ack", []byte(`{}`))
		waitUntil(t, "the acknowledgement to be handled", func() bool {
			mu.Lock()
			defer mu.Unlock()
			return len(seen) == i+1
		})
	}
	mu.Lock()
	defer mu.Unlock()
	for i, ok := range seen {
		if !ok {
			t.Fatalf("round %d: an inbound acknowledgement was handled before the carriage that arrived ahead of it", i)
		}
	}
}
