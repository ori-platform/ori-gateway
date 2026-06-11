// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"fmt"
	"strings"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

const (
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

	auth, err := mqttauth.Sign(
		beat,
		contracts.HeartbeatMessageType,
		heartbeatBroadcastDeviceID,
		heartbeatBroadcastRequestID,
		beat.TimestampMS,
		secret,
	)
	if err != nil {
		return contracts.Heartbeat{}, err
	}
	beat.Auth = &auth
	return beat, nil
}
