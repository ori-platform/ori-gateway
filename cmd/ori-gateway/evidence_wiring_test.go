// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"io"
	"net"
	"net/http"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/site"
)

// TestOutboundHandoffWakesTheCarriageDevicesLane drives the gateway's real
// outbound subscription: a carriage on one device's topic must wake that
// device's lanes and no other device's, and a carriage for an unconfigured device must be
// refused before admission: no acknowledgement, no custody, no queue entry, and
// device_unconfigured raised without the device's identifier.
func TestOutboundHandoffWakesTheCarriageDevicesLane(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	// Unreachable: an attempt is observable as a backing-off device.
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")

	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()

	cfg := validConfig()
	cfg.Gateway.DeviceIDs = []string{"dev-01", "dev-02"}
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: addr}
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(2 * time.Second)
	for evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter) == nil {
		if time.Now().After(deadline) {
			t.Fatal("outbound evidence subscription never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	handle := evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter)
	carriage := func(device string) []byte {
		artifact := `{"v":1,"device_id":"` + device + `","high_water_seq":1,"signature":"ed25519:opaque"}`
		raw, _ := json.Marshal(map[string]string{
			"device_id": device, "artifact_type": "checkpoint",
			"artifact_b64": base64.StdEncoding.EncodeToString([]byte(artifact)),
		})
		return raw
	}

	handle("ori/dev-02/evidence/outbound", carriage("dev-02"))
	handle("ori/dev-99/evidence/outbound", carriage("dev-99"))

	var delivery *site.GatewayEvidenceDeliveryView
	var body []byte
	deadline = time.Now().Add(3 * time.Second)
	for {
		if resp, err := http.Get("http://" + addr + "/health"); err == nil { //nolint:noctx
			body, _ = io.ReadAll(resp.Body)
			resp.Body.Close()
			var health site.SiteHealth
			_ = json.Unmarshal(body, &health)
			delivery = health.Gateway.EvidenceDelivery
			if delivery != nil && len(delivery.Devices) > 0 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("the handed-off device's lanes were never woken: %#v", delivery)
		}
		time.Sleep(10 * time.Millisecond)
	}
	if len(delivery.Devices) != 1 || delivery.Devices[0].DeviceID != "dev-02" || delivery.Devices[0].State != "backing_off" {
		t.Fatalf("devices = %#v, want dev-02 backing off alone", delivery.Devices)
	}
	if delivery.Pending != 1 {
		t.Fatalf("pending = %d: the unconfigured device's artifact was admitted", delivery.Pending)
	}
	if !slices.Contains(delivery.Faults, "device_unconfigured") {
		t.Fatalf("faults = %v, want device_unconfigured", delivery.Faults)
	}
	if strings.Contains(string(body), "dev-99") {
		t.Fatalf("site health names the unconfigured device: %s", body)
	}

	evidenceBroker.mu.Lock()
	defer evidenceBroker.mu.Unlock()
	for _, msg := range evidenceBroker.published {
		if strings.HasPrefix(msg.topic, "ori/dev-99/") {
			t.Fatalf("published %s for an unconfigured device", msg.topic)
		}
	}
	mainBroker.mu.Lock()
	defer mainBroker.mu.Unlock()
	for _, msg := range mainBroker.published {
		if strings.HasPrefix(msg.topic, "ori/dev-99/") {
			t.Fatalf("published %s for an unconfigured device", msg.topic)
		}
	}
}

// TestRunningGatewayReservesARegistrationSlotPerDevice drives the gateway's
// real construction and outbound subscription: its outbound queue is opened
// with the configured devices, so each device's share (here two items) keeps
// one slot for a registration. A second artifact of evidence is refused
// queue_full while the registration that would confirm its epoch is still
// admitted, and another device's share is untouched.
func TestRunningGatewayReservesARegistrationSlotPerDevice(t *testing.T) {
	t.Setenv("GATEWAY_ENVELOPE", "runtime-gateway-envelope-secret")
	t.Setenv("GATEWAY_CUSTODY", "gateway-custody-secret-with-at-least-32-bytes")
	// Unreachable, so nothing admitted is retired during the test.
	t.Setenv("EVIDENCE_ENDPOINT", "https://127.0.0.1:1/v1/evidence/artifacts")
	t.Setenv("EVIDENCE_CLIENT", "site-gateway-a")
	t.Setenv("EVIDENCE_SECRET", "evidence-ingest-secret-with-at-least-32-bytes")

	cfg := validConfig()
	cfg.Gateway.DeviceIDs = []string{"dev-01", "dev-02"}
	cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: "GATEWAY_ENVELOPE"}
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "GATEWAY_CUSTODY"}
	cfg.Evidence = config.EvidenceConfig{
		Enabled: true, QueueDirectory: filepath.Join(t.TempDir(), "outbound"),
		ReturnQueueDirectory: filepath.Join(t.TempDir(), "returned"), MaxItems: 4,
		MaxBytes: 8 << 20, RetryIntervalS: 60, BackoffBaseS: 60, BackoffMaxS: 60, StoreProbeIntervalS: 900,
		EndpointEnv: "EVIDENCE_ENDPOINT", ClientIDEnv: "EVIDENCE_CLIENT", SecretEnv: "EVIDENCE_SECRET",
		DeviceCarriage: map[string]string{"dev-01": config.CarriageEvidenceCarriageV1, "dev-02": config.CarriageGatewayAPIV1},
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
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	t.Cleanup(func() { cancel(); <-done })

	deadline := time.Now().Add(2 * time.Second)
	for evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter) == nil {
		if time.Now().After(deadline) {
			t.Fatal("outbound evidence subscription never started")
		}
		time.Sleep(5 * time.Millisecond)
	}
	handle := evidenceBroker.handlerFor(contracts.EvidenceOutboundTopicFilter)
	// handOff publishes one carriage and returns the acknowledgement's
	// outcome and reason.
	handOff := func(device, kind, artifact string) (string, string) {
		t.Helper()
		raw, _ := json.Marshal(map[string]string{
			"device_id": device, "artifact_type": kind,
			"artifact_b64": base64.StdEncoding.EncodeToString([]byte(artifact)),
		})
		evidenceBroker.mu.Lock()
		before := len(evidenceBroker.published)
		evidenceBroker.mu.Unlock()
		handle("ori/"+device+"/evidence/outbound", raw)
		deadline := time.Now().Add(2 * time.Second)
		for {
			evidenceBroker.mu.Lock()
			for _, msg := range evidenceBroker.published[before:] {
				if msg.topic == "ori/"+device+"/evidence/outbound/ack" {
					var ack struct {
						Outcome string `json:"outcome"`
						Reason  string `json:"reason"`
					}
					_ = json.Unmarshal(msg.payload, &ack)
					evidenceBroker.mu.Unlock()
					return ack.Outcome, ack.Reason
				}
			}
			evidenceBroker.mu.Unlock()
			if time.Now().After(deadline) {
				t.Fatalf("no acknowledgement for %s %s", device, kind)
			}
			time.Sleep(5 * time.Millisecond)
		}
	}
	checkpoint := func(device string, n int) string {
		return `{"v":1,"device_id":"` + device + `","high_water_seq":` + string(rune('0'+n)) + `,"signature":"ed25519:opaque"}`
	}
	registration := `{"v":1,"device_id":"dev-01","anchor_epoch_id":"epoch-1","signature":"ed25519:opaque"}`

	if outcome, reason := handOff("dev-01", "checkpoint", checkpoint("dev-01", 1)); outcome != "queued" {
		t.Fatalf("first checkpoint = %s %s", outcome, reason)
	}
	if outcome, reason := handOff("dev-01", "checkpoint", checkpoint("dev-01", 2)); outcome != "refused" || reason != "queue_full" {
		t.Fatalf("evidence into the registration reserve = %s %s, want refused queue_full", outcome, reason)
	}
	if outcome, reason := handOff("dev-01", "anchor_registration", registration); outcome != "queued" {
		t.Fatalf("the reserved registration = %s %s", outcome, reason)
	}
	if outcome, reason := handOff("dev-02", "checkpoint", checkpoint("dev-02", 1)); outcome != "queued" {
		t.Fatalf("another device's share = %s %s", outcome, reason)
	}
}
