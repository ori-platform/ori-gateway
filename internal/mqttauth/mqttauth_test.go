// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"encoding/json"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func TestVerifierAcceptsSignedRuntimeHeartbeat(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	beat := contracts.RuntimeNodeHeartbeat{
		DeviceID:       "dev-01",
		Status:         "healthy",
		LastSeenMS:     now.UnixMilli(),
		GatewaySeenMS:  0,
		ActiveTriggers: []string{},
	}
	auth, err := Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(Config{SharedSecret: "site-local-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}

	unsigned, err := verifier.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "")
	if err != nil {
		t.Fatal(err)
	}
	if _, ok := unsigned["auth"]; ok {
		t.Fatal("auth field should be removed from verified payload")
	}
}

func TestVerifierRejectsWrongSecret(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	beat := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: "healthy", LastSeenMS: now.UnixMilli(), ActiveTriggers: []string{}}
	auth, err := Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(Config{SharedSecret: "other-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}

	_, err = verifier.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "")
	if err == nil || !strings.Contains(err.Error(), "signature mismatch") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVerifierRejectsStalePayload(t *testing.T) {
	signedAt := time.UnixMilli(1234567890000)
	beat := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: "healthy", LastSeenMS: signedAt.UnixMilli(), ActiveTriggers: []string{}}
	auth, err := Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", signedAt.UnixMilli(), "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(Config{
		SharedSecret: "site-local-secret",
		MaxSkew:      time.Minute,
		Now: func() time.Time {
			return signedAt.Add(2 * time.Minute)
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	_, err = verifier.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "")
	if err == nil || !strings.Contains(err.Error(), "skew") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestVerifierRejectsReplay(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	beat := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: "healthy", LastSeenMS: now.UnixMilli(), ActiveTriggers: []string{}}
	auth, err := Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(Config{SharedSecret: "site-local-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", ""); err != nil {
		t.Fatal(err)
	}
	_, err = verifier.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "")
	if err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestReplayKeyIsLengthPrefixed(t *testing.T) {
	left := replayKey("a|b", "c", "", 1, "sig")
	right := replayKey("a", "b|c", "", 1, "sig")
	if left == right {
		t.Fatal("replay keys should not collide when fields contain delimiters")
	}
}
