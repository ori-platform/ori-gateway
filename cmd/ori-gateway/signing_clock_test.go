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
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/gatewaystate"
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
			at, err := signer.Next()
			if err != nil {
				t.Fatal(err)
			}
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

// TestAMarkInTheEvidenceQueueDirectoryIsNeverRead leaves a message clock mark
// in the outbound evidence queue directory, where no build ever kept one:
// corrupt, the start is not refused, and far ahead, the clock does not move
// to it. The file is left as it was.
func TestAMarkInTheEvidenceQueueDirectoryIsNeverRead(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	wall := time.UnixMilli(1_787_000_000_000)
	for _, body := range []string{"corrupt", "9000000000000000"} {
		t.Run(body, func(t *testing.T) {
			dir := t.TempDir()
			outbound := filepath.Join(dir, "outbound")
			cfg := validConfig()
			cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
			cfg.Gateway.StateDirectory = filepath.Join(dir, "gateway-state")
			cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
			cfg.Evidence = config.EvidenceConfig{
				Enabled: true, QueueDirectory: outbound,
				ReturnQueueDirectory: filepath.Join(dir, "returned"), MaxItems: 10,
				MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
				EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
			}
			// The first run creates the queue directory as the courier owns it.
			run := func() int64 {
				t.Helper()
				deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
				deps.logger = slog.New(slog.DiscardHandler)
				deps.now = func() time.Time { return wall }
				deps.newBroker = func(broker.Options) (brokerClient, error) { return newFakeBroker(), nil }
				signers := make(chan *mqttauth.SigningClock, 1)
				deps.newHeartbeat = func(_ heartbeat.PublishFunc, _ heartbeat.ProviderStatus, _ heartbeat.SIMStatus, opts heartbeat.Options) (heartbeatRunner, error) {
					signers <- opts.SigningClock
					return newFakeHeartbeat(), nil
				}
				ctx, cancel := context.WithCancel(context.Background())
				done := make(chan error, 1)
				go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
				defer func() { cancel(); <-done }()
				var signer *mqttauth.SigningClock
				select {
				case signer = <-signers:
				case err := <-done:
					done <- err
					t.Fatalf("the start was refused: %v", err)
				case <-time.After(5 * time.Second):
					t.Fatal("the heartbeat was not built")
				}
				at, err := signer.Next()
				if err != nil {
					t.Fatal(err)
				}
				return at
			}
			run()
			stray := filepath.Join(outbound, ".ori-signed-at-messages")
			if err := os.WriteFile(stray, []byte(body), 0o600); err != nil {
				t.Fatal(err)
			}
			// The first run left the state directory's mark one reservation
			// past the frozen wall clock; the second resumes one past it.
			if at, want := run(), wall.UnixMilli()+1001; at != want {
				t.Fatalf("signed at %d after the stray mark, want %d, one past the state directory's mark", at, want)
			}
			if got, err := os.ReadFile(stray); err != nil || string(got) != body {
				t.Fatalf("the stray mark was touched: %q, %v", got, err)
			}
		})
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
		at, err := signer.Next()
		if err != nil {
			t.Fatal(err)
		}
		issued = at
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
	inner                   mqttauth.DurableMark
	armed                   atomic.Bool
	entered, release, wrote chan struct{}
}

func (d *pausedDurable) Found() (int64, bool) { return d.inner.Found() }

func (d *pausedDurable) Persist(mark int64) error {
	if !d.armed.CompareAndSwap(true, false) {
		return d.inner.Persist(mark)
	}
	close(d.entered)
	<-d.release
	err := d.inner.Persist(mark)
	close(d.wrote)
	return err
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
	owner, err := gatewaystate.Acquire(filepath.Join(t.TempDir(), "posture-state"))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = owner.Close() })
	paused := &pausedDurable{inner: owner.Mark(), entered: make(chan struct{}), release: make(chan struct{}), wrote: make(chan struct{})}
	fb := newFakeBroker()
	deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.now = now
	deps.logger = slog.New(slog.DiscardHandler)
	deps.newRuntimeClient = func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		clock, err := mqttauth.NewDurableSigningClock(now, paused, deps.logger)
		if err != nil {
			return nil, err
		}
		opts = append(opts, runtimeclient.WithSigningClock(clock))
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
			mark := filepath.Join(cfg.Gateway.StateDirectory, ".ori-signed-at-messages")
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

// TestAStateDirectoryThatRefusesStopsTheStartBeforeTheBroker starts the
// gateway on state directories gateway-config/v2 refuses: each start fails,
// and no broker client is ever built (GW-15).
func TestAStateDirectoryThatRefusesStopsTheStartBeforeTheBroker(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	state := func(t *testing.T, mark string) string {
		t.Helper()
		dir := filepath.Join(t.TempDir(), "gateway-state")
		owner, err := gatewaystate.Acquire(dir)
		if err != nil {
			t.Fatal(err)
		}
		if err := owner.Close(); err != nil {
			t.Fatal(err)
		}
		if mark != "" {
			if err := os.WriteFile(filepath.Join(dir, ".ori-signed-at-messages"), []byte(mark), 0o600); err != nil {
				t.Fatal(err)
			}
		}
		return dir
	}
	cases := map[string]func(t *testing.T) string{
		"a corrupt mark": func(t *testing.T) string { return state(t, "-1") },
		"a mark at the ceiling": func(t *testing.T) string {
			return state(t, strconv.FormatInt(mqttauth.SigningCeiling, 10))
		},
		"a mark whose next value reaches the ceiling": func(t *testing.T) string {
			return state(t, strconv.FormatInt(mqttauth.SigningCeiling-1, 10))
		},
		"a group-readable directory": func(t *testing.T) string {
			dir := state(t, "")
			if err := os.Chmod(dir, 0o750); err != nil {
				t.Fatal(err)
			}
			return dir
		},
		"a directory that is not the gateway's": func(t *testing.T) string {
			dir := filepath.Join(t.TempDir(), "gateway-state")
			if err := os.Mkdir(dir, 0o700); err != nil {
				t.Fatal(err)
			}
			if err := os.WriteFile(filepath.Join(dir, "other"), nil, 0o600); err != nil {
				t.Fatal(err)
			}
			return dir
		},
		"a directory another gateway holds": func(t *testing.T) string {
			dir := state(t, "")
			owner, err := gatewaystate.Acquire(dir)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = owner.Close() })
			return dir
		},
		"a directory that cannot be written": func(t *testing.T) string {
			if os.Geteuid() == 0 {
				t.Skip("root writes through directory permissions")
			}
			dir := state(t, "")
			if err := os.Chmod(dir, 0o500); err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
			return dir
		},
	}
	for name, setup := range cases {
		t.Run(name, func(t *testing.T) {
			cfg := validConfig()
			cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
			cfg.Gateway.StateDirectory = setup(t)
			deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
			deps.logger = slog.New(slog.DiscardHandler)
			var built atomic.Bool
			deps.newBroker = func(broker.Options) (brokerClient, error) {
				built.Store(true)
				return newFakeBroker(), nil
			}
			// A start that is wrongly accepted runs until this bound.
			ctx, cancel := context.WithTimeout(t.Context(), 5*time.Second)
			defer cancel()
			err := runGateway(ctx, "gateway.yaml", deps)
			if err == nil || !strings.Contains(err.Error(), "gateway state directory") && !strings.Contains(err.Error(), "signing clock") {
				t.Fatalf("runGateway = %v, want the state directory refused", err)
			}
			if built.Load() {
				t.Fatal("a broker client was built after the state directory refused")
			}
		})
	}
}

// TestShutdownWaitsForTheGatewaysOwnExtension uses more than half of the
// gateway's own signing reservation, so its clock extends it in the
// background, and holds that write; then it stops the gateway: runGateway
// does not return, and the state directory stays held, until the write is
// done.
func TestShutdownWaitsForTheGatewaysOwnExtension(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	frozen := time.UnixMilli(1_787_000_000_000)
	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.StateDirectory = filepath.Join(t.TempDir(), "gateway-state")
	deps := baseDeps(t, cfg, newFakeBroker(), &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(slog.DiscardHandler)
	deps.now = func() time.Time { return frozen }
	paused := &pausedDurable{entered: make(chan struct{}), release: make(chan struct{}), wrote: make(chan struct{})}
	deps.signingMark = func(mark mqttauth.DurableMark) mqttauth.DurableMark {
		paused.inner = mark
		return paused
	}
	signers := make(chan *mqttauth.SigningClock, 1)
	deps.newHeartbeat = func(_ heartbeat.PublishFunc, _ heartbeat.ProviderStatus, _ heartbeat.SIMStatus, opts heartbeat.Options) (heartbeatRunner, error) {
		signers <- opts.SigningClock
		return newFakeHeartbeat(), nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	var signer *mqttauth.SigningClock
	select {
	case signer = <-signers:
	case <-time.After(5 * time.Second):
		t.Fatal("the heartbeat was not built")
	}
	paused.armed.Store(true)
	// Under a frozen clock the reservation is 1,000 values; more than half
	// of it starts the extension.
	for range 502 {
		if _, err := signer.Next(); err != nil {
			t.Fatal(err)
		}
	}
	select {
	case <-paused.entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the clock never extended its reservation")
	}
	cancel()
	select {
	case err := <-done:
		close(paused.release)
		t.Fatalf("runGateway returned (%v) while its clock was still writing the mark", err)
	case <-time.After(200 * time.Millisecond):
	}
	if owner, err := gatewaystate.Acquire(cfg.Gateway.StateDirectory); err == nil {
		_ = owner.Close()
		close(paused.release)
		t.Fatal("the state directory was released while its clock was still writing the mark")
	}
	close(paused.release)
	<-paused.wrote
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("runGateway did not return once the write was done")
	}
	owner, err := gatewaystate.Acquire(cfg.Gateway.StateDirectory)
	if err != nil {
		t.Fatalf("after shutdown the state directory is still held: %v", err)
	}
	_ = owner.Close()
}
