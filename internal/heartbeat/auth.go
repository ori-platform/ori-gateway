// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"crypto/hmac"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

const (
	signaturePrefix             = "hmac-sha256:"
	heartbeatBroadcastDeviceID  = ""
	heartbeatBroadcastRequestID = ""
)

// AuthConfig configures optional HMAC signing for gateway heartbeat payloads.
type AuthConfig struct {
	Enabled      bool
	SharedSecret string
}

// SignHeartbeat attaches the runtime-compatible broadcast HMAC envelope.
func SignHeartbeat(beat contracts.Heartbeat, sharedSecret string) (contracts.Heartbeat, error) {
	secret := strings.TrimSpace(sharedSecret)
	if secret == "" {
		return contracts.Heartbeat{}, fmt.Errorf("heartbeat auth shared secret must not be empty")
	}
	if beat.TimestampMS <= 0 {
		return contracts.Heartbeat{}, fmt.Errorf("heartbeat timestamp_ms must be positive")
	}

	canonical, err := canonicalHeartbeatJSON(beat)
	if err != nil {
		return contracts.Heartbeat{}, err
	}
	signingInput := heartbeatSigningInput(canonical, beat.TimestampMS)

	mac := hmac.New(sha256.New, []byte(secret))
	_, _ = mac.Write([]byte(signingInput))
	beat.Auth = &contracts.HeartbeatAuth{
		Scheme:     contracts.HeartbeatAuthScheme,
		SignedAtMS: beat.TimestampMS,
		Signature:  signaturePrefix + hex.EncodeToString(mac.Sum(nil)),
	}
	return beat, nil
}

func heartbeatSigningInput(canonicalPayload []byte, signedAtMS int64) string {
	// Runtime verify_broadcast uses the same five-slot envelope as per-device
	// gateway MQTT messages, but broadcast heartbeat has no device_id or request_id.
	return strings.Join([]string{
		contracts.HeartbeatMessageType,
		heartbeatBroadcastDeviceID,
		heartbeatBroadcastRequestID,
		fmt.Sprintf("%d", signedAtMS),
		string(canonicalPayload),
	}, "\n")
}

func canonicalHeartbeatJSON(beat contracts.Heartbeat) ([]byte, error) {
	// Sign the full heartbeat payload that the runtime will parse, excluding only
	// the auth envelope. Runtime verification canonicalizes the received JSON
	// object after removing auth, so any future heartbeat field must be covered
	// by HMAC on both sides rather than silently unsigned.
	unsigned := beat
	unsigned.Auth = nil
	raw, err := json.Marshal(unsigned)
	if err != nil {
		return nil, err
	}
	var payload map[string]any
	if err := json.Unmarshal(raw, &payload); err != nil {
		return nil, err
	}
	return json.Marshal(payload)
}
