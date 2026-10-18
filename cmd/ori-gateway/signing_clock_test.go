// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"github.com/ori-platform/ori-gateway/internal/enrichment"
	"github.com/ori-platform/ori-gateway/internal/provider"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
	"github.com/ori-platform/ori-gateway/internal/heartbeat"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

// TestASignedAtNeverRepeatsAcrossARestart runs the gateway twice on one
// evidence queue directory, the second time with its clock set back, and
// takes signing times from the clock its signers share: every time the
// second run issues is later than any the first run issued.
func TestASignedAtNeverRepeatsAcrossARestart(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	dir := t.TempDir()
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: filepath.Join(dir, "outbound"),
		ReturnQueueDirectory: filepath.Join(dir, "returned"), MaxItems: 10,
		MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
	}
	run := func(clock time.Time, issue int) (first, last int64) {
		t.Helper()
		deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
		deps.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
		deps.now = func() time.Time { return clock }
		deps.newBroker = func(broker.Options) (brokerClient, error) { return newFakeBroker(), nil }
		signers := make(chan *mqttauth.SigningClock, 1)
		hb := newFakeHeartbeat()
		deps.newHeartbeat = func(_ heartbeat.PublishFunc, _ heartbeat.ProviderStatus, _ heartbeat.SIMStatus, opts heartbeat.Options) (heartbeatRunner, error) {
			signers <- opts.SigningClock
			return hb, nil
		}
		ctx, cancel := context.WithCancel(context.Background())
		done := make(chan error, 1)
		go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
		var signer *mqttauth.SigningClock
		select {
		case signer = <-signers:
		case <-time.After(5 * time.Second):
			t.Fatal("the heartbeat was not built")
		}
		for i := range issue {
			at := signer.Next()
			if i == 0 {
				first = at
			}
			last = at
		}
		cancel()
		<-done
		return first, last
	}
	start := time.UnixMilli(1_787_000_000_000)
	_, lastBefore := run(start, 50)
	firstAfter, _ := run(start.Add(-time.Hour), 1)
	if firstAfter <= lastBefore {
		t.Fatalf("after a restart with the clock set back the gateway signed at %d, not after %d", firstAfter, lastBefore)
	}
}

// TestMalformedReasoningRequestsNeverStopTheGateway sends many more
// malformed, unsigned and stale reasoning requests than the failure limit:
// the gateway keeps running, because only a reply it could not deliver counts
// toward the escalation.
func TestMalformedReasoningRequestsNeverStopTheGateway(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	fb := newFakeBroker()
	deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	ctx := t.Context()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	waitUntil(t, "the reasoning subscription", func() bool {
		return fb.handlerFor(contracts.GatewayReasoningRequestTopicFilter) != nil
	})
	handler := fb.handlerFor(contracts.GatewayReasoningRequestTopicFilter)
	stale, err := json.Marshal(contracts.ReasoningRequest{
		RequestID: "req-stale", DeviceID: "dev-01", SensorType: "current_clamp", TriggerName: "overcurrent",
		Prompt: "p", ActionTierHint: contracts.ActionTierC, TimeoutMS: 1000,
		Auth: &contracts.HeartbeatAuth{Scheme: contracts.HeartbeatAuthScheme, SignedAtMS: 1, Signature: "hmac-sha256:" + strings.Repeat("0", 64)},
	})
	if err != nil {
		t.Fatal(err)
	}
	for range 4 * defaultRequestFailureLimit {
		handler("ori/dev-01/reasoning/request", []byte("{"))
		handler("ori/dev-01/reasoning/request", validRequestPayload(t))
		handler("ori/dev-01/reasoning/request", stale)
	}
	select {
	case err := <-done:
		t.Fatalf("refused requests stopped the gateway: %v", err)
	case <-time.After(200 * time.Millisecond):
	}
}

// TestEverySignerSharesTheGatewaysSigningClock freezes the gateway's clock,
// takes signing times from the heartbeat's clock, and then has the gateway
// sign a reasoning response, an enrichment response and an export request:
// each is signed above every time already issued, which only one shared
// clock gives under a frozen wall clock.
func TestEverySignerSharesTheGatewaysSigningClock(t *testing.T) {
	const secret = "runtime-gateway-envelope-secret"
	t.Setenv("GATEWAY_ENVELOPE", secret)
	frozen := time.UnixMilli(1_787_000_000_000)
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: loopbackAddr(t)}
	cfg.Reporting = config.ReportingConfig{
		Provider:        config.ReportingProviderGemini,
		Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
		TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
	}
	fb := newFakeBroker()
	deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	deps.now = func() time.Time { return frozen }
	deps.newRuntimeClient = func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		return runtimeclient.NewMQTTClient(b, opts...)
	}
	signers := make(chan *mqttauth.SigningClock, 1)
	hb := newFakeHeartbeat()
	deps.newHeartbeat = func(_ heartbeat.PublishFunc, _ heartbeat.ProviderStatus, _ heartbeat.SIMStatus, opts heartbeat.Options) (heartbeatRunner, error) {
		signers <- opts.SigningClock
		return hb, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })
	signer := <-signers
	var issued int64
	for range 10 {
		issued = signer.Next()
	}
	waitUntil(t, "the subscriptions", func() bool {
		return fb.handlerFor("ori/dev-01/tier_c/enrichment/request") != nil && fb.handlerFor("ori/dev-01/runtime/heartbeat") != nil
	})
	sign := func(value any, messageType, requestID string) []byte {
		raw := mustJSON(t, value)
		var payload map[string]any
		if err := json.Unmarshal(raw, &payload); err != nil {
			t.Fatal(err)
		}
		auth, err := mqttauth.Sign(payload, messageType, "dev-01", requestID, frozen.UnixMilli(), secret)
		if err != nil {
			t.Fatal(err)
		}
		payload["auth"] = auth
		return mustJSON(t, payload)
	}
	fb.handlerFor(contracts.GatewayReasoningRequestTopicFilter)("ori/dev-01/reasoning/request", sign(contracts.ReasoningRequest{
		RequestID: "req-1", DeviceID: "dev-01", SensorType: "current_clamp", TriggerName: "overcurrent",
		Prompt: "p", ActionTierHint: contracts.ActionTierC, TimeoutMS: 1000,
	}, contracts.ReasoningRequestMessageType, "req-1"))
	var enrichReq contracts.TierCEnrichmentRequest
	if err := json.Unmarshal(validTierCEnrichmentRequestPayload(t), &enrichReq); err != nil {
		t.Fatal(err)
	}
	fb.handlerFor("ori/dev-01/tier_c/enrichment/request")("ori/dev-01/tier_c/enrichment/request",
		sign(enrichReq, contracts.TierCEnrichmentRequestMessageType, enrichReq.RequestID))
	fb.handlerFor("ori/dev-01/runtime/heartbeat")("ori/dev-01/runtime/heartbeat", sign(contracts.RuntimeNodeHeartbeat{
		DeviceID: "dev-01", Status: "healthy", LastSeenMS: frozen.UnixMilli(), ActiveTriggers: []string{},
	}, contracts.RuntimeHeartbeatMessageType, ""))

	signedAt := func(topic string) int64 {
		var at int64
		waitUntil(t, topic, func() bool {
			fb.mu.Lock()
			defer fb.mu.Unlock()
			for _, msg := range fb.published {
				if msg.topic == topic {
					var body struct {
						Auth *contracts.HeartbeatAuth `json:"auth"`
					}
					if json.Unmarshal(msg.payload, &body) == nil && body.Auth != nil {
						at = body.Auth.SignedAtMS
						return true
					}
				}
			}
			return false
		})
		return at
	}
	for _, topic := range []string{"ori/dev-01/reasoning/response", "ori/dev-01/tier_c/enrichment/response", "ori/dev-01/export/request"} {
		if at := signedAt(topic); at <= issued {
			t.Errorf("%s signed at %d, not above %d already issued: its signer does not share the gateway's clock", topic, at, issued)
		}
	}
}

// pausedDurable holds the durable signing clock's write until released, as a
// slow disk would.
type pausedDurable struct {
	inner                   mqttauth.DurableClock
	armed                   atomic.Bool
	entered, release, wrote chan struct{}
}

func (d *pausedDurable) Last() int64 { return d.inner.Last() }

func (d *pausedDurable) NextAtLeast(least int64) (int64, error) {
	if !d.armed.CompareAndSwap(true, false) {
		return d.inner.NextAtLeast(least)
	}
	close(d.entered)
	<-d.release
	at, err := d.inner.NextAtLeast(least)
	close(d.wrote)
	return at, err
}

// TestShutdownWaitsForAPostureFetchesSigningWrite moves the clock past the
// signing reservation, so a posture fetch's export request must persist a new
// one before it is signed, and holds that write; then it stops the gateway:
// runGateway does not return until that write is done, so no durable state
// changes after the gateway has stopped.
func TestShutdownWaitsForAPostureFetchesSigningWrite(t *testing.T) {
	const secret = "runtime-gateway-envelope-secret"
	t.Setenv("GATEWAY_ENVELOPE", secret)
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	var wallMS atomic.Int64
	wallMS.Store(1_787_000_000_000)
	now := func() time.Time { return time.UnixMilli(wallMS.Load()) }
	dir := t.TempDir()
	outbound := filepath.Join(dir, "outbound")
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: loopbackAddr(t)}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: outbound, ReturnQueueDirectory: filepath.Join(dir, "returned"), MaxItems: 10,
		MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
	}
	real, err := signingclock.OpenMessages(outbound, now)
	if err != nil {
		t.Fatal(err)
	}
	paused := &pausedDurable{inner: real, entered: make(chan struct{}), release: make(chan struct{}), wrote: make(chan struct{})}
	fb := newFakeBroker()
	deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.now = now
	deps.logger = slog.New(slog.DiscardHandler)
	deps.newRuntimeClient = func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		opts = append(opts, runtimeclient.WithSigningClock(mqttauth.NewSigningClock(now, paused, deps.logger)))
		return runtimeclient.NewMQTTClient(b, opts...)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	waitUntil(t, "the heartbeat subscription", func() bool { return fb.handlerFor("ori/dev-01/runtime/heartbeat") != nil })
	// Past the reservation persisted at startup, and the write is held.
	wallMS.Add(10_000)
	wall := now()
	paused.armed.Store(true)
	beat := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: "healthy", LastSeenMS: wall.UnixMilli(), ActiveTriggers: []string{}}
	auth, err := mqttauth.Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", wall.UnixMilli(), secret)
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	fb.handlerFor("ori/dev-01/runtime/heartbeat")("ori/dev-01/runtime/heartbeat", mustJSON(t, beat))
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the posture fetch never signed")
	}
	cancel()
	select {
	case err := <-done:
		t.Fatalf("runGateway returned (%v) while a posture fetch was still writing the signing clock", err)
	case <-time.After(200 * time.Millisecond):
	}
	close(paused.release)
	<-paused.wrote
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runGateway did not return once the write was done")
	}
}

// TestAClosedPostureDebouncerStartsNothing closes the debouncer, as shutdown
// does: a heartbeat callback arriving after that starts no fetch.
func TestAClosedPostureDebouncerStartsNothing(t *testing.T) {
	d := newPostureDebouncer()
	if !d.tryStart("dev-01") {
		t.Fatal("an open debouncer refused a fetch")
	}
	d.done("dev-01")
	d.close()
	if d.tryStart("dev-02") {
		t.Fatal("a closed debouncer started a fetch")
	}
	d.close() // closing again is harmless
}

// heldProvider holds a reasoning or enrichment request inside the provider
// until released, as a slow model would.
type heldProvider struct {
	fakeProvider
	fakeTierCEnrichmentProvider
	entered, release chan struct{}
}

func (p *heldProvider) Reason(ctx context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	close(p.entered)
	<-p.release
	return p.fakeProvider.Reason(ctx, req)
}

func (p *heldProvider) EnrichTierC(ctx context.Context, req contracts.TierCEnrichmentRequest) (contracts.TierCEnrichmentResponse, error) {
	close(p.entered)
	<-p.release
	return p.fakeTierCEnrichmentProvider.EnrichTierC(ctx, req)
}

// TestShutdownJoinsEveryBrokerCallback holds a signed reasoning request, then
// an enrichment request, inside its provider and stops the gateway:
// runGateway does not return until the callback has signed its reply, with
// the clock moved past its reservation so that reply writes the durable mark,
// and the mark never changes after runGateway has returned.
func TestShutdownJoinsEveryBrokerCallback(t *testing.T) {
	for _, family := range []string{"reasoning", "enrichment"} {
		t.Run(family, func(t *testing.T) {
			const secret = "runtime-gateway-envelope-secret"
			t.Setenv("GATEWAY_ENVELOPE", secret)
			var wallMS atomic.Int64
			wallMS.Store(1_787_000_000_000)
			cfg := validConfig()
			cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
			cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
			cfg.Reporting = config.ReportingConfig{
				Provider:        config.ReportingProviderGemini,
				Gemini:          config.ReportingGeminiConfig{APIKeyEnv: "GEMINI_API_KEY", Model: "gemini-2.5-flash"},
				TierCEnrichment: config.TierCEnrichmentConfig{Enabled: true},
			}
			held := &heldProvider{fakeProvider: fakeProvider{healthy: true}, entered: make(chan struct{}), release: make(chan struct{})}
			fb := newFakeBroker()
			deps := baseDeps(t, cfg, fb, &held.fakeProvider, newFakeHeartbeat())
			deps.logger = slog.New(slog.DiscardHandler)
			deps.now = func() time.Time { return time.UnixMilli(wallMS.Load()) }
			deps.newProvider = func(config.ProviderConfig) (provider.Provider, error) { return held, nil }
			deps.newTierCEnrichmentProvider = func(config.ReportingConfig) (enrichment.Provider, error) { return held, nil }
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			done := make(chan error, 1)
			go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
			topic, filter := "ori/dev-01/reasoning/request", contracts.GatewayReasoningRequestTopicFilter
			var payload []byte
			if family == "reasoning" {
				req := contracts.ReasoningRequest{RequestID: "held", DeviceID: "dev-01", SensorType: "s", TriggerName: "t",
					Prompt: "p", ActionTierHint: contracts.ActionTierA, TimeoutMS: 60000}
				auth, err := mqttauth.Sign(req, contracts.ReasoningRequestMessageType, "dev-01", req.RequestID, wallMS.Load(), secret)
				if err != nil {
					t.Fatal(err)
				}
				req.Auth = &auth
				payload = mustJSON(t, req)
			} else {
				topic, filter = "ori/dev-01/tier_c/enrichment/request", "ori/dev-01/tier_c/enrichment/request"
				var req contracts.TierCEnrichmentRequest
				if err := json.Unmarshal(validTierCEnrichmentRequestPayload(t), &req); err != nil {
					t.Fatal(err)
				}
				req.TimeoutMS = 60000
				auth, err := mqttauth.Sign(req, contracts.TierCEnrichmentRequestMessageType, "dev-01", req.RequestID, wallMS.Load(), secret)
				if err != nil {
					t.Fatal(err)
				}
				req.Auth = &auth
				payload = mustJSON(t, req)
			}
			waitUntil(t, "the subscription", func() bool { return fb.handlerFor(filter) != nil })
			handled := make(chan struct{})
			go func() { defer close(handled); fb.handlerFor(filter)(topic, payload) }()
			select {
			case <-held.entered:
			case <-time.After(5 * time.Second):
				t.Fatal("the provider never received the request")
			}
			cancel()
			select {
			case err := <-done:
				close(held.release)
				t.Fatalf("runGateway returned (%v) while a %s callback was still running", err, family)
			case <-time.After(200 * time.Millisecond):
			}
			wallMS.Add(10_000) // past the reservation: the reply writes the mark
			close(held.release)
			<-handled
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Fatal("runGateway did not return once the callback finished")
			}
			mark := signingclock.MessagesPath(cfg.Gateway.StateDirectory)
			before, err := os.ReadFile(mark)
			if err != nil {
				t.Fatal(err)
			}
			time.Sleep(50 * time.Millisecond)
			after, err := os.ReadFile(mark)
			if err != nil || string(before) != string(after) {
				t.Fatalf("the durable mark changed after runGateway returned: %s -> %s (%v)", before, after, err)
			}
		})
	}
}
