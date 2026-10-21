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
// cannot yet notice anything a callback reports.
type heldSubscribeBroker struct {
	*fakeBroker
	topic            string
	entered, release chan struct{}
}

func (b *heldSubscribeBroker) Subscribe(ctx context.Context, topic string, qos byte, h broker.MessageHandler) error {
	if topic == b.topic {
		close(b.entered)
		<-b.release
	}
	return b.fakeBroker.Subscribe(ctx, topic, qos, h)
}

// TestTheFailureLimitStopsEveryAdmissionBeforeTheGatewayNoticesIt holds
// runGateway in a later startup subscribe, where it cannot read the request
// failure escalation, and fails the replies to authenticated reasoning
// requests up to the limit. The callback that reaches the limit decides the
// stop itself: the clock refuses to sign, a later reasoning request is not
// admitted, and a webhook request through the production listener is
// answered 503 and never signed or forwarded (gateway-config/v2
// gateway.state_directory rule 13).
func TestTheFailureLimitStopsEveryAdmissionBeforeTheGatewayNoticesIt(t *testing.T) {
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
	b := &heldSubscribeBroker{fakeBroker: newFakeBroker(), topic: "ori/dev-01/runtime/heartbeat",
		entered: make(chan struct{}), release: make(chan struct{})}
	b.publishErr = errors.New("reply not delivered")
	g.deps.newBroker = func(broker.Options) (brokerClient, error) { return b, nil }

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	defer func() {
		close(b.release)
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
	signer := <-g.signers
	if _, err := signer.Next(); !errors.Is(err, mqttauth.ErrSigningClockClosed) {
		t.Fatalf("the clock issued %v after the failure limit, want ErrSigningClockClosed", err)
	}
	fp.mu.Lock()
	answered := len(fp.requests)
	fp.mu.Unlock()
	if answered != defaultRequestFailureLimit {
		t.Fatalf("%d requests reached the provider before the limit, want %d", answered, defaultRequestFailureLimit)
	}
	handler("ori/dev-01/reasoning/request", request(defaultRequestFailureLimit))
	fp.mu.Lock()
	after := len(fp.requests)
	fp.mu.Unlock()
	if after != answered {
		t.Fatal("a reasoning request was admitted after the failure limit decided the stop")
	}

	resp, err := http.Post(url, "application/x-www-form-urlencoded", strings.NewReader("from=%2B2348000000000&text=YES-test"))
	if err != nil {
		t.Fatal(err)
	}
	_, _ = io.Copy(io.Discard, resp.Body)
	_ = resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable || forwards.Load() != 0 {
		t.Fatalf("a webhook request after the decision to stop = %d, forwarded %d times; want 503 and none", resp.StatusCode, forwards.Load())
	}
}
