// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/webhookbridge"
)

// TestSiteHealthCountsTheBridgesRefusals runs the gateway with the real SMS
// bridge and site health, floods the bridge from one number, and reads the
// refusal count from site health: a count only, with no number in it.
func TestSiteHealthCountsTheBridgesRefusals(t *testing.T) {
	runtime := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) }))
	t.Cleanup(runtime.Close)
	t.Setenv("ORI_SMS_WEBHOOK_TOKEN", "runtime-token")
	t.Setenv("ORI_SMS_WEBHOOK_HMAC_SECRET", "bridge-secret")
	bridgeAddr, siteAddr := loopbackAddr(t), loopbackAddr(t)
	cfg := validConfig()
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: siteAddr}
	cfg.WebhookBridge = config.WebhookBridgeConfig{
		Enabled: true, ListenAddr: bridgeAddr, Path: "/webhooks/sms/africastalking", TargetURL: runtime.URL,
		RuntimeTokenEnv: "ORI_SMS_WEBHOOK_TOKEN", HMACSecretEnv: "ORI_SMS_WEBHOOK_HMAC_SECRET",
		RequestTimeoutMS: 3000, MaxBodyBytes: 65536,
		SenderPerMinute: config.DefaultWebhookBridgeSenderPerMinute, SenderBurst: config.DefaultWebhookBridgeSenderBurst,
		GlobalPerMinute: config.DefaultWebhookBridgeGlobalPerMinute, GlobalBurst: config.DefaultWebhookBridgeGlobalBurst,
		MaxSenders: config.DefaultWebhookBridgeMaxSenders,
	}
	deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(slog.DiscardHandler)
	deps.newWebhookBridge = func(cfg config.WebhookBridgeConfig, opts webhookbridge.Options) (webhookBridgeRunner, error) {
		return webhookbridge.New(cfg, opts)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })

	post := func() int {
		resp, err := http.Post("http://"+bridgeAddr+"/webhooks/sms/africastalking", "application/x-www-form-urlencoded",
			strings.NewReader("from=%2B254700000001&text=YES")) //nolint:noctx
		if err != nil {
			return 0
		}
		resp.Body.Close()
		return resp.StatusCode
	}
	waitUntil(t, "the bridge to serve", func() bool { return post() == http.StatusOK })
	for range 2 * config.DefaultWebhookBridgeSenderBurst {
		post()
	}
	var raw []byte
	waitUntil(t, "site health to count the refusals", func() bool {
		resp, err := http.Get("http://" + siteAddr + "/health") //nolint:noctx
		if err != nil {
			return false
		}
		defer resp.Body.Close()
		var health struct {
			Gateway struct {
				Refused json.RawMessage `json:"webhook_bridge_refused"`
			} `json:"gateway"`
		}
		if json.NewDecoder(resp.Body).Decode(&health) != nil {
			return false
		}
		raw = health.Gateway.Refused
		var counts struct{ Sender, Global uint64 }
		return json.Unmarshal(raw, &counts) == nil && counts.Sender > 0
	})
	if want := fmt.Sprintf(`{"sender":%d,"global":0}`, config.DefaultWebhookBridgeSenderBurst+1); string(raw) != want {
		t.Fatalf("webhook_bridge_refused = %s, want %s", raw, want)
	}
	if strings.Contains(string(raw), "254700000001") {
		t.Fatal("site health carries the sender")
	}
}
