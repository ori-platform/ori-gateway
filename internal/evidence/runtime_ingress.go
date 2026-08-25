// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

type EvidencePublishFunc func(context.Context, string, byte, bool, []byte) error

type RuntimeIngress struct {
	courier        *Courier
	publish        EvidencePublishFunc
	envelopeSecret string
	now            func() time.Time
}

func NewRuntimeIngress(courier *Courier, publish EvidencePublishFunc, envelopeSecret string, now func() time.Time) (*RuntimeIngress, error) {
	if courier == nil || publish == nil {
		return nil, fmt.Errorf("evidence: runtime ingress requires courier and publisher")
	}
	if envelopeSecret == "" {
		return nil, fmt.Errorf("evidence: runtime ingress requires the MQTT envelope secret")
	}
	if now == nil {
		now = time.Now
	}
	return &RuntimeIngress{courier: courier, publish: publish, envelopeSecret: envelopeSecret, now: now}, nil
}

func (h *RuntimeIngress) Handle(ctx context.Context, topic string, payload []byte) error {
	deviceID, err := contracts.DeviceIDFromEvidenceOutboundTopic(topic)
	if err != nil {
		return err
	}
	var carriage struct {
		DeviceID     string `json:"device_id"`
		ArtifactType string `json:"artifact_type"`
		ArtifactB64  string `json:"artifact_b64"`
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&carriage); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("evidence: malformed outbound carriage")
	}
	artifactBytes, err := base64.StdEncoding.Strict().DecodeString(carriage.ArtifactB64)
	if err != nil || len(artifactBytes) == 0 {
		return fmt.Errorf("evidence: malformed outbound artifact bytes")
	}
	if carriage.DeviceID != deviceID {
		return h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "refused", "binding_mismatch", h.now().UnixMilli())
	}
	var artifactRouting struct {
		DeviceID string `json:"device_id"`
	}
	if err := json.Unmarshal(artifactBytes, &artifactRouting); err != nil || artifactRouting.DeviceID == "" {
		return h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "refused", "malformed", h.now().UnixMilli())
	}
	if artifactRouting.DeviceID != deviceID {
		return h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "refused", "binding_mismatch", h.now().UnixMilli())
	}
	kind := ArtifactType(carriage.ArtifactType)
	if err := validateArtifactRoutingFields(kind, artifactBytes); err != nil {
		return h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "refused", "malformed", h.now().UnixMilli())
	}
	admission, err := h.courier.Admit(kind, artifactBytes)
	if err != nil {
		if errors.Is(err, ErrQueueFull) {
			return h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "refused", "queue_full", h.now().UnixMilli())
		}
		return err
	}
	if err := h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "queued", "", admission.Queued.EnqueuedAtMS); err != nil {
		return err
	}
	if admission.Custody == nil {
		return nil
	}
	artifact, err := json.Marshal(admission.Custody)
	if err != nil {
		return fmt.Errorf("evidence: encode custody artifact: %w", err)
	}
	inbound := inboundEnvelope{
		DeviceID: deviceID, ArtifactType: "custody_acknowledgement", Artifact: artifact,
	}
	signedAt := h.now().UnixMilli()
	auth, err := mqttauth.Sign(inbound, contracts.EvidenceInboundMessageType, deviceID, "", signedAt, h.envelopeSecret)
	if err != nil {
		return fmt.Errorf("evidence: sign custody delivery: %w", err)
	}
	inbound.Auth = &auth
	encodedInbound, err := json.Marshal(inbound)
	if err != nil {
		return fmt.Errorf("evidence: encode custody delivery: %w", err)
	}
	inboundTopic, err := contracts.EvidenceInboundTopic(deviceID)
	if err != nil {
		return err
	}
	if err := h.publish(ctx, inboundTopic, 1, false, encodedInbound); err != nil {
		return fmt.Errorf("evidence: publish custody delivery: %w", err)
	}
	return nil
}

func (h *RuntimeIngress) publishOutboundAck(ctx context.Context, deviceID, artifactType string, artifactBytes []byte, outcome, reason string, acknowledgedAtMS int64) error {
	digest := sha256.Sum256(artifactBytes)
	ack := outboundAck{
		DeviceID:         deviceID,
		ArtifactType:     artifactType,
		ArtifactDigest:   "sha256:" + hex.EncodeToString(digest[:]),
		Outcome:          outcome,
		Reason:           reason,
		AcknowledgedAtMS: acknowledgedAtMS,
	}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceOutboundAckMessageType, deviceID, "", ack.AcknowledgedAtMS, h.envelopeSecret)
	if err != nil {
		return fmt.Errorf("evidence: sign outbound acknowledgement: %w", err)
	}
	ack.Auth = &auth
	encodedAck, err := json.Marshal(ack)
	if err != nil {
		return fmt.Errorf("evidence: encode outbound acknowledgement: %w", err)
	}
	ackTopic, err := contracts.EvidenceOutboundAckTopic(deviceID)
	if err != nil {
		return err
	}
	if err := h.publish(ctx, ackTopic, 1, false, encodedAck); err != nil {
		return fmt.Errorf("evidence: publish outbound acknowledgement: %w", err)
	}
	return nil
}

type outboundAck struct {
	DeviceID         string                   `json:"device_id"`
	ArtifactType     string                   `json:"artifact_type"`
	ArtifactDigest   string                   `json:"artifact_digest"`
	Outcome          string                   `json:"outcome"`
	Reason           string                   `json:"reason"`
	AcknowledgedAtMS int64                    `json:"acknowledged_at_ms"`
	Auth             *contracts.HeartbeatAuth `json:"auth,omitempty"`
}

type inboundEnvelope struct {
	DeviceID     string                   `json:"device_id"`
	ArtifactType string                   `json:"artifact_type"`
	Artifact     json.RawMessage          `json:"artifact"`
	Auth         *contracts.HeartbeatAuth `json:"auth,omitempty"`
}
