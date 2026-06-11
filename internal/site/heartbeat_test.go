// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

func TestRuntimeHeartbeatHandlerUpdatesRegistry(t *testing.T) {
	reg := NewRegistry()
	now := time.UnixMilli(1234567895000)
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	payload := runtimeHeartbeatPayload(t, contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         NodeStatusHealthy,
		LastSeenMS:     1234567890000,
		GatewaySeenMS:  999,
		ActiveTriggers: []string{"high_current"},
	})

	if err := handler.Handle("ori/dev-01/runtime/heartbeat", payload); err != nil {
		t.Fatal(err)
	}
	snapshot := reg.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one node, got %d", len(snapshot))
	}
	got := snapshot[0]
	if got.DeviceID != "dev-01" || got.Status != NodeStatusHealthy || got.LastSeenMS != 1234567890000 {
		t.Fatalf("unexpected heartbeat: %#v", got)
	}
	if got.GatewaySeen != now.UnixMilli() {
		t.Fatalf("gateway_seen_ms = %d, want %d", got.GatewaySeen, now.UnixMilli())
	}
	if len(got.ActiveTriggers) != 1 || got.ActiveTriggers[0] != "high_current" {
		t.Fatalf("unexpected active triggers: %#v", got.ActiveTriggers)
	}
}

func TestRuntimeHeartbeatHandlerRejectsTopicPayloadDeviceMismatch(t *testing.T) {
	reg := NewRegistry()
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	payload := runtimeHeartbeatPayload(t, contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-02",
		Status:         NodeStatusHealthy,
		LastSeenMS:     1234567890000,
		ActiveTriggers: []string{},
	})

	err = handler.Handle("ori/dev-01/runtime/heartbeat", payload)
	if err == nil || !strings.Contains(err.Error(), "does not match") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reg.Snapshot()) != 0 {
		t.Fatal("registry should not update after rejected heartbeat")
	}
}

func TestRuntimeHeartbeatHandlerRejectsInvalidStatus(t *testing.T) {
	reg := NewRegistry()
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	payload := runtimeHeartbeatPayload(t, contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         "offline",
		LastSeenMS:     1234567890000,
		ActiveTriggers: []string{},
	})

	err = handler.Handle("ori/dev-01/runtime/heartbeat", payload)
	if err == nil || !strings.Contains(err.Error(), "not supported") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRuntimeHeartbeatHandlerVerifiesSignedHeartbeat(t *testing.T) {
	reg := NewRegistry()
	now := time.UnixMilli(1234567890000)
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "site-local-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{
		AuthVerifier: verifier,
		Now:          func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	beat := contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         NodeStatusDegraded,
		LastSeenMS:     now.UnixMilli(),
		ActiveTriggers: []string{},
	}
	auth, err := mqttauth.Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth

	if err := handler.Handle("ori/dev-01/runtime/heartbeat", runtimeHeartbeatPayload(t, beat)); err != nil {
		t.Fatal(err)
	}
	if got := reg.Snapshot()[0]; got.Status != NodeStatusDegraded {
		t.Fatalf("status = %q", got.Status)
	}
}

func TestRuntimeHeartbeatHandlerRejectsUnsignedWhenAuthEnabled(t *testing.T) {
	reg := NewRegistry()
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "site-local-secret"})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{AuthVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	payload := runtimeHeartbeatPayload(t, contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         NodeStatusHealthy,
		LastSeenMS:     1234567890000,
		ActiveTriggers: []string{},
	})

	err = handler.Handle("ori/dev-01/runtime/heartbeat", payload)
	if err == nil || !strings.Contains(err.Error(), "missing auth") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestRuntimeHeartbeatHandlerRejectsReplayedHeartbeat(t *testing.T) {
	reg := NewRegistry()
	now := time.UnixMilli(1234567890000)
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "site-local-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{AuthVerifier: verifier})
	if err != nil {
		t.Fatal(err)
	}
	beat := contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         NodeStatusHealthy,
		LastSeenMS:     now.UnixMilli(),
		ActiveTriggers: []string{},
	}
	auth, err := mqttauth.Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	payload := runtimeHeartbeatPayload(t, beat)
	if err := handler.Handle("ori/dev-01/runtime/heartbeat", payload); err != nil {
		t.Fatal(err)
	}
	err = handler.Handle("ori/dev-01/runtime/heartbeat", payload)
	if err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func runtimeHeartbeatPayload(t *testing.T, beat contracts.RuntimeNodeHeartbeat) []byte {
	t.Helper()
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestRuntimeHeartbeatHandlerRejectsFarFutureLastSeen(t *testing.T) {
	reg := NewRegistry()
	now := time.UnixMilli(1234567890000)
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	payload := runtimeHeartbeatPayload(t, contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         NodeStatusHealthy,
		LastSeenMS:     now.Add(10 * time.Minute).UnixMilli(),
		ActiveTriggers: []string{},
	})

	err = handler.Handle("ori/dev-01/runtime/heartbeat", payload)
	if err == nil || !strings.Contains(err.Error(), "future") {
		t.Fatalf("unexpected error: %v", err)
	}
	if len(reg.Snapshot()) != 0 {
		t.Fatal("registry should not update after future-dated heartbeat")
	}
}

func TestRuntimeHeartbeatHandlerIgnoresUnknownFields(t *testing.T) {
	reg := NewRegistry()
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"device_id":"dev-01","status":"healthy","last_seen_ms":1234567890000,"gateway_seen_ms":0,"active_triggers":[],"node_version":"future"}`)
	if err := handler.Handle("ori/dev-01/runtime/heartbeat", payload); err != nil {
		t.Fatal(err)
	}
	if len(reg.Snapshot()) != 1 {
		t.Fatalf("expected registry update, got %#v", reg.Snapshot())
	}
}
