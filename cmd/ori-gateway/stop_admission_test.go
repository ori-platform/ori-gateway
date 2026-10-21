// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/heartbeat"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/site"
)

// stopHeldRunner stands in for a supervised runner that takes its time to
// return once cancelled, holding the stop between its decision and the join.
type stopHeldRunner struct{ stopping, release chan struct{} }

func (h *stopHeldRunner) Run(ctx context.Context) error {
	<-ctx.Done()
	close(h.stopping)
	<-h.release
	return ctx.Err()
}

// TestNoCallbackIsAdmittedAfterTheDecisionToStop stops the gateway while a
// supervised runner is still being joined, then delivers, through the
// production subscriptions of both broker clients, a newly signed runtime
// heartbeat and an outbound evidence carriage: neither is admitted, so the
// site registry, the evidence queue and the brokers' publishes are unchanged
// (gateway-config/v2 gateway.state_directory rule 13: the decision to stop
// stops admitting work).
func TestNoCallbackIsAdmittedAfterTheDecisionToStop(t *testing.T) {
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")
	g := newStallingGateway(t, filepath.Join(t.TempDir(), "gateway-state"), false)
	outbound := filepath.Join(t.TempDir(), "outbound")
	g.cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	g.cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: outbound,
		ReturnQueueDirectory: filepath.Join(t.TempDir(), "returned"), MaxItems: 10,
		MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
	}
	g.deps.loadConfig = func(string) (config.Config, error) { return g.cfg, nil }
	mainBroker, evidenceBroker := newFakeBroker(), newFakeBroker()
	g.deps.newBroker = func(opts broker.Options) (brokerClient, error) {
		if opts.ClientID == defaultEvidenceClientID {
			return evidenceBroker, nil
		}
		return mainBroker, nil
	}
	registry := site.NewRegistry()
	g.deps.newSiteRegistry = func() *site.Registry { return registry }
	held := &stopHeldRunner{stopping: make(chan struct{}), release: make(chan struct{})}
	g.deps.newHeartbeat = func(_ heartbeat.PublishFunc, _ heartbeat.ProviderStatus, _ heartbeat.SIMStatus, opts heartbeat.Options) (heartbeatRunner, error) {
		g.signers <- opts.SigningClock
		return held, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", g.deps) }()
	signer := <-g.signers
	const heartbeatTopic = "ori/dev-01/runtime/heartbeat"
	waitUntil(t, "both brokers' subscriptions", func() bool {
		return mainBroker.handlerFor(heartbeatTopic) != nil && evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter) != nil
	})

	cancel()
	select {
	case <-held.stopping:
	case <-time.After(5 * time.Second):
		t.Fatal("the stop was never decided")
	}
	defer func() {
		close(held.release)
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Error("runGateway did not return once the runner was released")
		}
	}()
	// The runner sees the cancellation as runGateway does; the decision itself
	// is observable as the clock refusing to sign.
	waitUntil(t, "the decision to stop", func() bool {
		_, err := signer.Next()
		return err != nil
	})
	queued := queueEntries(t, outbound)
	mainBroker.mu.Lock()
	mainPublished := len(mainBroker.published)
	mainBroker.mu.Unlock()
	evidenceBroker.mu.Lock()
	evidencePublished := len(evidenceBroker.published)
	evidenceBroker.mu.Unlock()

	beat := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: site.NodeStatusHealthy, LastSeenMS: g.wallMS.Load(), ActiveTriggers: []string{}}
	auth, err := mqttauth.Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", g.wallMS.Load(), "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	mainBroker.handlerFor(heartbeatTopic)(heartbeatTopic, mustJSON(t, beat))
	artifact := `{"v":1,"device_id":"dev-01","high_water_seq":1,"signature":"ed25519:opaque"}`
	carriage, err := json.Marshal(map[string]string{
		"device_id": "dev-01", "artifact_type": "checkpoint",
		"artifact_b64": base64.StdEncoding.EncodeToString([]byte(artifact)),
	})
	if err != nil {
		t.Fatal(err)
	}
	evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter)("ori/dev-01/evidence/outbound", carriage)

	if n := len(registry.Snapshot()); n != 0 {
		t.Fatalf("a runtime heartbeat was admitted after the decision to stop: the registry holds %d node(s)", n)
	}
	if got := queueEntries(t, outbound); got != queued {
		t.Fatalf("an evidence carriage was admitted after the decision to stop: %d queue entries, was %d", got, queued)
	}
	mainBroker.mu.Lock()
	mainAfter := len(mainBroker.published)
	mainBroker.mu.Unlock()
	evidenceBroker.mu.Lock()
	evidenceAfter := len(evidenceBroker.published)
	evidenceBroker.mu.Unlock()
	if mainAfter != mainPublished || evidenceAfter != evidencePublished {
		t.Fatalf("a callback published after the decision to stop: main %d→%d, evidence %d→%d", mainPublished, mainAfter, evidencePublished, evidenceAfter)
	}
}

func queueEntries(t *testing.T, dir string) int {
	t.Helper()
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	return len(entries)
}

// TestARefusedGateStillJoinsWhatItAdmitted holds a callback admitted before
// the gate refuses: a later callback is refused at once, and close returns
// only once the admitted one has.
func TestARefusedGateStillJoinsWhatItAdmitted(t *testing.T) {
	gate := newCallbackGate()
	fb := newFakeBroker()
	gated := gate.wrap(fb)
	const topic = "ori/dev-01/runtime/heartbeat"
	entered, release := make(chan struct{}), make(chan struct{})
	var ran atomic.Int32
	if err := gated.Subscribe(context.Background(), topic, 1, func(string, []byte) {
		if ran.Add(1) == 1 {
			close(entered)
			<-release
		}
	}); err != nil {
		t.Fatal(err)
	}
	go fb.handlerFor(topic)(topic, nil)
	<-entered
	gate.refuse()
	fb.handlerFor(topic)(topic, nil)
	if n := ran.Load(); n != 1 {
		t.Fatalf("%d callbacks ran, want the refused one never run", n)
	}
	closed := make(chan struct{})
	go func() { gate.close(); close(closed) }()
	select {
	case <-closed:
		t.Fatal("close returned while an admitted callback was still running")
	case <-time.After(50 * time.Millisecond):
	}
	close(release)
	<-closed
}
