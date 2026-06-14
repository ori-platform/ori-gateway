// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"encoding/hex"
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

func TestVerifierAcceptsPreviousSecret(t *testing.T) {
	now := time.UnixMilli(1234567890000)
	beat := contracts.RuntimeNodeHeartbeat{DeviceID: "dev-01", Status: "healthy", LastSeenMS: now.UnixMilli(), ActiveTriggers: []string{}}
	auth, err := Sign(beat, contracts.RuntimeHeartbeatMessageType, "dev-01", "", now.UnixMilli(), "previous-secret")
	if err != nil {
		t.Fatal(err)
	}
	beat.Auth = &auth
	payload, err := json.Marshal(beat)
	if err != nil {
		t.Fatal(err)
	}
	verifier, err := NewVerifier(Config{SharedSecret: "current-secret", PreviousSharedSecret: "previous-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", ""); err != nil {
		t.Fatal(err)
	}
}

func TestVerifierRejectsMatchingPreviousSecret(t *testing.T) {
	_, err := NewVerifier(Config{SharedSecret: "same", PreviousSharedSecret: "same"})
	if err == nil || !strings.Contains(err.Error(), "previous") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEncryptorRoundTrip(t *testing.T) {
	encryptor, err := NewEncryptor("site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain := map[string]any{
		"request_id":  "req-1",
		"device_id":   "dev-01",
		"export_type": "sensor_history",
		"items":       []any{map[string]any{"value": 4.2}},
		"complete":    true,
	}
	encrypted, err := encryptor.Encrypt(plain, contracts.ExportResponseMessageType, []byte("123456789012"))
	if err != nil {
		t.Fatal(err)
	}
	if encrypted["encrypted"] != true || encrypted["items"] != nil {
		t.Fatalf("unexpected encrypted envelope: %#v", encrypted)
	}
	decoded, err := encryptor.Decrypt(encrypted, contracts.ExportResponseMessageType, "dev-01", "req-1")
	if err != nil {
		t.Fatal(err)
	}
	if decoded["export_type"] != "sensor_history" || decoded["request_id"] != "req-1" {
		t.Fatalf("unexpected decoded payload: %#v", decoded)
	}
}

func TestEncryptorRejectsTamperedMetadata(t *testing.T) {
	encryptor, err := NewEncryptor("site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	plain := map[string]any{"request_id": "req-1", "device_id": "dev-01", "export_type": "sensor_history", "items": []any{}}
	encrypted, err := encryptor.Encrypt(plain, contracts.ExportResponseMessageType, []byte("123456789012"))
	if err != nil {
		t.Fatal(err)
	}
	encrypted["export_type"] = "action_log"
	_, err = encryptor.Decrypt(encrypted, contracts.ExportResponseMessageType, "dev-01", "req-1")
	if err == nil || !strings.Contains(err.Error(), "decrypt") {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestEncryptionAADMatchesRuntimeCanonicalJSON(t *testing.T) {
	metadata := map[string]string{"message_type": "export_response", "request_id": "req-1", "device_id": "dev-01", "export_type": "sensor_history"}
	got := string(encryptionAAD(metadata, contracts.ExportResponseMessageType))
	want := `{"device_id":"dev-01","export_type":"sensor_history","message_type":"export_response","request_id":"req-1"}`
	if got != want {
		t.Fatalf("AAD mismatch:\nwant %s\n got %s", want, got)
	}
}

func TestHKDFSHA256RFC5869Vector(t *testing.T) {
	ikm := []byte{0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b, 0x0b}
	salt := []byte{0x00, 0x01, 0x02, 0x03, 0x04, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c}
	info := []byte{0xf0, 0xf1, 0xf2, 0xf3, 0xf4, 0xf5, 0xf6, 0xf7, 0xf8, 0xf9}
	got := hex.EncodeToString(hkdfSHA256(ikm, salt, info, 42))
	want := "3cb25f25faacd57a90434f64d0362f2a2d2d0a90cf1a5a4c5db02d56ecc4c5bf34007208d5b887185865"
	if got != want {
		t.Fatalf("HKDF mismatch:\nwant %s\n got %s", want, got)
	}
}
