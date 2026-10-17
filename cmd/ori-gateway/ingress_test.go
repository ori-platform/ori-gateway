// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"testing"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
	mqttsrv "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
	"github.com/ori-platform/ori-gateway/internal/site"
)

func waitUntil(t *testing.T, what string, ok func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for !ok() {
		if time.Now().After(deadline) {
			t.Fatalf("timed out waiting for %s", what)
		}
		time.Sleep(5 * time.Millisecond)
	}
}

func loopbackAddr(t *testing.T) string {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	return addr
}

// TestEverySubscriptionHasAPayloadMaximum runs the gateway with every module
// that subscribes, drives the lazy export subscription, and requires every
// topic it subscribed to have a payload maximum, covering every family the
// gateway receives.
func TestEverySubscriptionHasAPayloadMaximum(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")

	cfg := validConfig()
	cfg.Gateway.DeviceIDs = []string{"dev-01", "dev-02"}
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: filepath.Join(t.TempDir(), "outbound"),
		ReturnQueueDirectory: filepath.Join(t.TempDir(), "returned"), MaxItems: 10,
		MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
	}
	cfg.Reporting = config.ReportingConfig{
		Provider:        config.ReportingProviderGemini,
		Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
	}
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: loopbackAddr(t)}

	mainBroker, evidenceBroker := newFakeBroker(), newFakeBroker()
	deps := baseDeps(t, cfg, mainBroker, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	var optsMu sync.Mutex
	var brokerOpts []broker.Options
	deps.newBroker = func(opts broker.Options) (brokerClient, error) {
		optsMu.Lock()
		brokerOpts = append(brokerOpts, opts)
		optsMu.Unlock()
		if opts.ClientID == defaultEvidenceClientID {
			return evidenceBroker, nil
		}
		return mainBroker, nil
	}
	deps.newRuntimeClient = func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return runtimeclient.NewMQTTClient(b, opts...)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })

	waitUntil(t, "the heartbeat subscription", func() bool {
		return mainBroker.handlerFor("ori/dev-01/runtime/heartbeat") != nil
	})
	// A heartbeat starts the posture fetch, which subscribes export responses.
	signedAt := deps.now().UnixMilli()
	beat := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: site.NodeStatusHealthy, LastSeenMS: signedAt, ActiveTriggers: []string{}}
	beatAuth, err := mqttauth.Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", signedAt, "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &beatAuth
	mainBroker.handlerFor("ori/dev-01/runtime/heartbeat")("ori/dev-01/runtime/heartbeat", mustJSON(t, beat))
	waitUntil(t, "the export response subscription", func() bool {
		return mainBroker.handlerFor("ori/dev-01/export/response/+") != nil
	})
	waitUntil(t, "the evidence subscriptions", func() bool {
		return evidenceBroker.handlerFor("ori/dev-02/evidence/inbound/ack") != nil
	})

	// Both clients rate each configured device on its own and share one
	// ingress count.
	optsMu.Lock()
	if len(brokerOpts) != 2 || brokerOpts[0].Ingress == nil || brokerOpts[0].Ingress != brokerOpts[1].Ingress {
		t.Fatalf("broker clients do not share one ingress count: %+v", brokerOpts)
	}
	for _, opts := range brokerOpts {
		if !slices.Equal(opts.Devices, cfg.Gateway.DeviceIDs) {
			t.Fatalf("%s client rates devices %v, want the configured %v", opts.ClientID, opts.Devices, cfg.Gateway.DeviceIDs)
		}
	}
	optsMu.Unlock()

	families := map[string]bool{}
	for _, b := range []*fakeBroker{mainBroker, evidenceBroker} {
		b.mu.Lock()
		topics := append([]string(nil), b.subscribeTopics...)
		b.mu.Unlock()
		for _, topic := range topics {
			if err := broker.CheckSubscription(topic); err != nil {
				t.Errorf("subscription %q: %v", topic, err)
			}
			family, _ := contracts.PayloadFamily(topic)
			families[family] = true
		}
	}
	for _, family := range contracts.ReceivedFamilies {
		if !families[family] {
			t.Errorf("no subscription exercised %s", family)
		}
	}
	if len(families) != len(contracts.ReceivedFamilies) {
		t.Errorf("subscribed families %v, want exactly %v", families, contracts.ReceivedFamilies)
	}
}

func startEmbeddedBroker(t *testing.T) string {
	t.Helper()
	addr := loopbackAddr(t)
	srv := mqttsrv.New(&mqttsrv.Options{Logger: slog.New(slog.NewTextHandler(io.Discard, nil))})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	if err := srv.AddListener(listeners.NewTCP(listeners.Config{ID: "ingress", Address: addr})); err != nil {
		t.Fatal(err)
	}
	if err := srv.Serve(); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = srv.Close() })
	return "tcp://" + addr
}

// runtimePeer is a raw MQTT client standing in for a runtime.
type runtimePeer struct {
	client mqtt.Client
	mu     sync.Mutex
	got    map[string][][]byte
}

func newRuntimePeer(t *testing.T, brokerURL string, topics ...string) *runtimePeer {
	t.Helper()
	p := &runtimePeer{got: map[string][][]byte{}}
	opts := mqtt.NewClientOptions().AddBroker(brokerURL).SetClientID("runtime-peer").SetOrderMatters(true)
	p.client = mqtt.NewClient(opts)
	if token := p.client.Connect(); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("runtime peer connect: %v", token.Error())
	}
	t.Cleanup(func() { p.client.Disconnect(100) })
	for _, topic := range topics {
		token := p.client.Subscribe(topic, 1, func(_ mqtt.Client, msg mqtt.Message) {
			p.mu.Lock()
			p.got[msg.Topic()] = append(p.got[msg.Topic()], append([]byte(nil), msg.Payload()...))
			p.mu.Unlock()
		})
		if !token.WaitTimeout(5*time.Second) || token.Error() != nil {
			t.Fatalf("runtime peer subscribe: %v", token.Error())
		}
	}
	return p
}

func (p *runtimePeer) publish(t *testing.T, topic string, payload []byte) {
	t.Helper()
	if token := p.client.Publish(topic, 1, false, payload); !token.WaitTimeout(5*time.Second) || token.Error() != nil {
		t.Fatalf("runtime peer publish: %v", token.Error())
	}
}

func (p *runtimePeer) received(topic string) [][]byte {
	p.mu.Lock()
	defer p.mu.Unlock()
	return append([][]byte(nil), p.got[topic]...)
}

// reasoningRequestOfLength is a valid reasoning request exactly length bytes
// long, its prompt padded.
func reasoningRequestOfLength(t *testing.T, requestID string, length int) []byte {
	t.Helper()
	return reasoningRequestFrom(t, "dev-01", requestID, length)
}

func reasoningRequestFrom(t *testing.T, deviceID, requestID string, length int) []byte {
	t.Helper()
	req := contracts.ReasoningRequest{
		RequestID: requestID, DeviceID: deviceID, SensorType: "current_clamp", TriggerName: "overcurrent",
		Prompt: "p", ActionTierHint: contracts.ActionTierC, TimeoutMS: 1000,
	}
	base, err := json.Marshal(req)
	if err != nil {
		t.Fatal(err)
	}
	req.Prompt += strings.Repeat("p", length-len(base))
	payload, err := json.Marshal(req)
	if err != nil || len(payload) != length {
		t.Fatalf("built %d bytes, want %d: %v", len(payload), length, err)
	}
	return payload
}

func siteIngress(t *testing.T, addr string) (map[string]map[string]any, []any) {
	t.Helper()
	resp, err := http.Get("http://" + addr + "/health") //nolint:noctx
	if err != nil {
		return nil, nil
	}
	defer resp.Body.Close()
	var health struct {
		Nodes   []any `json:"nodes"`
		Gateway struct {
			MQTTIngress map[string]map[string]any `json:"mqtt_ingress"`
		} `json:"gateway"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&health); err != nil {
		t.Fatal(err)
	}
	return health.Gateway.MQTTIngress, health.Nodes
}

// TestARefusedOversizePayloadChangesNothingAndIsNotAnswered runs the gateway
// on a real MQTT broker. A reasoning request one byte over its maximum is not
// answered and the provider never sees it; one at the maximum, sent after it,
// is answered; a runtime heartbeat over its maximum leaves the registry
// untouched; and site health counts each refusal by family, with no content.
func TestARefusedOversizePayloadChangesNothingAndIsNotAnswered(t *testing.T) {
	brokerURL := startEmbeddedBroker(t)
	siteAddr := loopbackAddr(t)
	cfg := validConfig()
	cfg.Gateway.BrokerURL = brokerURL
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: siteAddr}

	var logs syncBuffer
	prov := &fakeProvider{healthy: true}
	deps := baseDeps(t, cfg, newFakeBroker(), prov, newFakeHeartbeat())
	deps.logger = slog.New(slog.NewTextHandler(&logs, nil))
	deps.newBroker = func(opts broker.Options) (brokerClient, error) { return broker.New(opts) }
	deps.newRuntimeClient = func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return &runtimeclient.FakeClient{}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })

	waitUntil(t, "site health", func() bool { ingress, _ := siteIngress(t, siteAddr); return ingress != nil })
	peer := newRuntimePeer(t, brokerURL, "ori/dev-01/reasoning/response")
	// The gateway's subscriptions are made before site health starts serving.
	maximum := contracts.PayloadMaxima[contracts.FamilyReasoningRequest]
	peer.publish(t, "ori/dev-01/reasoning/request", reasoningRequestOfLength(t, "req-over", maximum+1))
	peer.publish(t, "ori/dev-01/reasoning/request", reasoningRequestOfLength(t, "req-at", maximum))
	oversizeHeartbeat := `{"device_id":"dev-01","status":"healthy","last_seen_ms":1,"active_triggers":["` +
		strings.Repeat("t", contracts.PayloadMaxima[contracts.FamilyRuntimeHeartbeat]) + `"]}`
	peer.publish(t, "ori/dev-01/runtime/heartbeat", []byte(oversizeHeartbeat))

	defer func() {
		if t.Failed() {
			t.Log(logs.String())
		}
	}()
	waitUntil(t, "the request at the maximum to be answered", func() bool {
		return len(peer.received("ori/dev-01/reasoning/response")) > 0
	})
	waitUntil(t, "both refusals to be counted", func() bool {
		ingress, _ := siteIngress(t, siteAddr)
		return ingress[contracts.FamilyReasoningRequest]["payload_too_large"] == float64(1) &&
			ingress[contracts.FamilyRuntimeHeartbeat]["payload_too_large"] == float64(1)
	})

	responses := peer.received("ori/dev-01/reasoning/response")
	if len(responses) != 1 || !strings.Contains(string(responses[0]), `"request_id":"req-at"`) {
		t.Fatalf("responses = %q, want only the request at the maximum answered", responses)
	}
	prov.mu.Lock()
	seen := len(prov.requests)
	prov.mu.Unlock()
	if seen != 1 {
		t.Fatalf("the provider saw %d requests, want only the one at the maximum", seen)
	}
	ingress, nodes := siteIngress(t, siteAddr)
	if len(nodes) != 1 || strings.Contains(string(mustJSON(t, nodes)), `"gateway_observed":true`) {
		t.Fatalf("an oversize heartbeat changed the registry: %s", string(mustJSON(t, nodes)))
	}
	if len(ingress) != len(contracts.ReceivedFamilies) {
		t.Fatalf("mqtt_ingress = %v, want one member per received family", ingress)
	}
	if text := logs.String(); strings.Contains(text, "req-over") || strings.Contains(text, "ppppp") ||
		strings.Contains(text, "dev-01/reasoning") || strings.Contains(text, "ttttt") {
		t.Fatalf("a refusal record carries payload content or a device identifier:\n%s", text)
	}
}

func mustJSON(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}
