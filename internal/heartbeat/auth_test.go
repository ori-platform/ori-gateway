// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"strings"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func TestSignHeartbeatUsesRuntimeCompatibleCanonicalString(t *testing.T) {
	beat := contracts.Heartbeat{
		Status:       StatusHealthy,
		UptimeS:      12.5,
		Provider:     "llama_cpp",
		SIMAvailable: false,
		TimestampMS:  1234567890000,
	}

	signed, err := SignHeartbeat(beat, "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	if signed.Auth == nil {
		t.Fatal("expected auth envelope")
	}
	if signed.Auth.Scheme != contracts.HeartbeatAuthScheme {
		t.Fatalf("scheme = %q", signed.Auth.Scheme)
	}
	if signed.Auth.SignedAtMS != beat.TimestampMS {
		t.Fatalf("signed_at_ms = %d, want timestamp_ms %d", signed.Auth.SignedAtMS, beat.TimestampMS)
	}

	canonical := `{"provider":"llama_cpp","sim_available":false,"status":"healthy","timestamp_ms":1234567890000,"uptime_s":12.5}`
	gotCanonical, err := canonicalHeartbeatJSON(beat)
	if err != nil {
		t.Fatal(err)
	}
	if string(gotCanonical) != canonical {
		t.Fatalf("canonical json drifted:\nwant %s\n got %s", canonical, gotCanonical)
	}

	mac := hmac.New(sha256.New, []byte("site-local-secret"))
	_, _ = mac.Write([]byte(strings.Join([]string{
		contracts.HeartbeatMessageType,
		"",
		"",
		"1234567890000",
		canonical,
	}, "\n")))
	wantSignature := "hmac-sha256:" + hex.EncodeToString(mac.Sum(nil))
	if signed.Auth.Signature != wantSignature {
		t.Fatalf("signature mismatch:\nwant %s\n got %s", wantSignature, signed.Auth.Signature)
	}
}

func TestSignHeartbeatRejectsEmptySecret(t *testing.T) {
	_, err := SignHeartbeat(contracts.Heartbeat{TimestampMS: 1}, "  ")
	if err == nil {
		t.Fatal("expected empty secret error")
	}
}

func TestSignHeartbeatRejectsMissingTimestamp(t *testing.T) {
	_, err := SignHeartbeat(contracts.Heartbeat{}, "secret")
	if err == nil {
		t.Fatal("expected timestamp error")
	}
}
