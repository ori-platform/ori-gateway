// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

// TestRunningGatewayReleasesAHoldOnlyOnItsDevicesHandoff drives the gateway's
// real construction and subscriptions. dev-01's custody return is carried and
// refused unknown_key, which holds it. dev-02's admitted handoff does not carry
// it again; dev-01's admitted handoff does.
func TestRunningGatewayReleasesAHoldOnlyOnItsDevicesHandoff(t *testing.T) {
	const secret = "runtime-gateway-envelope-secret"
	t.Setenv("GATEWAY_ENVELOPE", secret)
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
	mainBroker := newFakeBroker()
	evidenceBroker := newFakeBroker()
	deps := baseDeps(t, cfg, mainBroker, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.newBroker = func(opts broker.Options) (brokerClient, error) {
		if opts.ClientID == defaultEvidenceClientID {
			return evidenceBroker, nil
		}
		return mainBroker, nil
	}
	// The gateway's clock. Each step advances it past the retry interval, so
	// only a hold, never the recently-carried window, keeps the head back.
	var clockMu sync.Mutex
	clock := time.UnixMilli(1787000000000)
	deps.now = func() time.Time {
		clockMu.Lock()
		defer clockMu.Unlock()
		return clock
	}
	advance := func() {
		clockMu.Lock()
		clock = clock.Add(2 * time.Minute)
		clockMu.Unlock()
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })

	waitFor := func(what string, ok func() bool) {
		t.Helper()
		deadline := time.Now().Add(3 * time.Second)
		for !ok() {
			if time.Now().After(deadline) {
				t.Fatalf("timed out waiting for %s", what)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	waitFor("the outbound subscription", func() bool {
		return evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter) != nil
	})
	handoff := evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter)
	carriage := func(device, kind, artifact string) []byte {
		raw, _ := json.Marshal(map[string]string{
			"device_id": device, "artifact_type": kind,
			"artifact_b64": base64.StdEncoding.EncodeToString([]byte(artifact)),
		})
		return raw
	}
	checkpoint := func(device string) []byte {
		return carriage(device, "checkpoint", `{"v":1,"device_id":"`+device+`","high_water_seq":1,"signature":"ed25519:opaque"}`)
	}
	inbound := func() [][]byte {
		evidenceBroker.mu.Lock()
		defer evidenceBroker.mu.Unlock()
		var carried [][]byte
		for _, msg := range evidenceBroker.published {
			if msg.topic == "ori/dev-01/evidence/inbound" {
				carried = append(carried, msg.payload)
			}
		}
		return carried
	}

	handoff("ori/dev-01/evidence/outbound",
		carriage("dev-01", "delivery_envelope", `{"v":1,"device_id":"dev-01","local_seq":1,"signature":"ed25519:opaque"}`))
	waitFor("dev-01's custody return to be carried", func() bool { return len(inbound()) == 1 })

	var envelope struct {
		Artifact json.RawMessage `json:"artifact"`
	}
	if err := json.Unmarshal(inbound()[0], &envelope); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(envelope.Artifact)
	ack := map[string]any{
		"device_id": "dev-01", "artifact_type": "custody_acknowledgement",
		"artifact_digest": "sha256:" + hex.EncodeToString(digest[:]),
		"outcome":         "refused", "reason": "unknown_key", "acknowledged_at_ms": deps.now().UnixMilli(),
	}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceInboundAckMessageType, "dev-01", "", deps.now().UnixMilli(), secret)
	if err != nil {
		t.Fatal(err)
	}
	ack["auth"] = auth
	payload, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	ackHandler := evidenceBroker.handlerFor("ori/dev-01/evidence/inbound/ack")
	if ackHandler == nil {
		t.Fatal("dev-01's inbound acknowledgement subscription never started")
	}
	ackHandler("ori/dev-01/evidence/inbound/ack", payload)

	// Another device's admitted handoff. A release would carry the head at
	// once; the window only bounds how long the test waits to see one.
	advance()
	handoff("ori/dev-02/evidence/outbound", checkpoint("dev-02"))
	time.Sleep(300 * time.Millisecond)
	if n := len(inbound()); n != 1 {
		t.Fatalf("dev-02's handoff released dev-01's hold: carried %d times", n)
	}

	advance()
	handoff("ori/dev-01/evidence/outbound", checkpoint("dev-01"))
	waitFor("dev-01's handoff to carry its held return again", func() bool { return len(inbound()) == 2 })
}
