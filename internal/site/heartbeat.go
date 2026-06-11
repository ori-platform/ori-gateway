// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"bytes"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

const (
	NodeStatusHealthy  = "healthy"
	NodeStatusDegraded = "degraded"

	defaultMaxActiveTriggers = 64
	maxActiveTriggerLength   = 128
	defaultMaxFutureSkew     = 5 * time.Minute
)

// RuntimeHeartbeatHandler validates runtime node heartbeats and updates the site registry.
type RuntimeHeartbeatHandler struct {
	registry          *Registry
	authVerifier      *mqttauth.Verifier
	now               func() time.Time
	maxActiveTriggers int
	maxFutureSkew     time.Duration
}

// RuntimeHeartbeatHandlerOptions configures runtime node heartbeat consumption.
type RuntimeHeartbeatHandlerOptions struct {
	AuthVerifier      *mqttauth.Verifier
	Now               func() time.Time
	MaxActiveTriggers int
	MaxFutureSkew     time.Duration
}

func NewRuntimeHeartbeatHandler(registry *Registry, opts RuntimeHeartbeatHandlerOptions) (*RuntimeHeartbeatHandler, error) {
	if registry == nil {
		return nil, fmt.Errorf("site registry must not be nil")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	maxActiveTriggers := opts.MaxActiveTriggers
	if maxActiveTriggers <= 0 {
		maxActiveTriggers = defaultMaxActiveTriggers
	}
	maxFutureSkew := opts.MaxFutureSkew
	if maxFutureSkew <= 0 {
		maxFutureSkew = defaultMaxFutureSkew
	}
	return &RuntimeHeartbeatHandler{
		registry:          registry,
		authVerifier:      opts.AuthVerifier,
		now:               now,
		maxActiveTriggers: maxActiveTriggers,
		maxFutureSkew:     maxFutureSkew,
	}, nil
}

func (h *RuntimeHeartbeatHandler) Handle(topic string, payload []byte) error {
	deviceID, err := contracts.DeviceIDFromRuntimeNodeHeartbeatTopic(topic)
	if err != nil {
		return err
	}
	unsignedPayload, err := h.unsignedPayload(deviceID, payload)
	if err != nil {
		return err
	}
	var beat contracts.RuntimeNodeHeartbeat
	dec := json.NewDecoder(bytes.NewReader(unsignedPayload))
	if err := dec.Decode(&beat); err != nil {
		return fmt.Errorf("decode runtime node heartbeat: %w", err)
	}
	if err := h.validate(deviceID, beat); err != nil {
		return err
	}
	h.registry.Upsert(NodeHeartbeat{
		DeviceID:       beat.DeviceID,
		Status:         beat.Status,
		LastSeenMS:     beat.LastSeenMS,
		GatewaySeen:    h.now().UnixMilli(),
		ActiveTriggers: append([]string(nil), beat.ActiveTriggers...),
	})
	return nil
}

func (h *RuntimeHeartbeatHandler) unsignedPayload(deviceID string, payload []byte) ([]byte, error) {
	if h.authVerifier != nil {
		unsigned, err := h.authVerifier.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, deviceID, "")
		if err != nil {
			return nil, err
		}
		return json.Marshal(unsigned)
	}
	var unsigned map[string]any
	dec := json.NewDecoder(bytes.NewReader(payload))
	dec.UseNumber()
	if err := dec.Decode(&unsigned); err != nil {
		return nil, fmt.Errorf("decode runtime node heartbeat: %w", err)
	}
	delete(unsigned, "auth")
	return json.Marshal(unsigned)
}

func (h *RuntimeHeartbeatHandler) validate(topicDeviceID string, beat contracts.RuntimeNodeHeartbeat) error {
	if beat.DeviceID != topicDeviceID {
		return fmt.Errorf("device_id %q does not match topic device_id %q", beat.DeviceID, topicDeviceID)
	}
	switch beat.Status {
	case NodeStatusHealthy, NodeStatusDegraded:
	default:
		return fmt.Errorf("runtime heartbeat status %q is not supported", beat.Status)
	}
	if beat.LastSeenMS <= 0 {
		return fmt.Errorf("runtime heartbeat last_seen_ms must be positive")
	}
	if time.UnixMilli(beat.LastSeenMS).After(h.now().Add(h.maxFutureSkew)) {
		return fmt.Errorf("runtime heartbeat last_seen_ms is too far in the future")
	}
	if len(beat.ActiveTriggers) > h.maxActiveTriggers {
		return fmt.Errorf("runtime heartbeat active_triggers exceeds maximum of %d", h.maxActiveTriggers)
	}
	for _, trigger := range beat.ActiveTriggers {
		if strings.TrimSpace(trigger) != trigger || trigger == "" {
			return fmt.Errorf("runtime heartbeat active trigger names must be non-empty and trimmed")
		}
		if len(trigger) > maxActiveTriggerLength {
			return fmt.Errorf("runtime heartbeat active trigger name exceeds %d bytes", maxActiveTriggerLength)
		}
	}
	return nil
}
