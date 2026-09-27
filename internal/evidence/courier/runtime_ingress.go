// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
	"io"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

type EvidencePublishFunc func(context.Context, string, byte, bool, []byte) error

type RuntimeIngress struct {
	courier        *Courier
	returns        *DurableAuthoritySink
	publish        EvidencePublishFunc
	envelopeSecret string
	now            func() time.Time
	signingClock   *signingclock.Clock
	notify         func()
	faults         *faults.Recorder
}

// The two forms of admission a fault is tracked for (gateway-evidence-carriage/v1):
// durable outbound queueing, and for a delivery envelope, durable staging of its
// required custody return. Each clears only when a later admission of the same
// form completes; a store probe clears neither.
const (
	admissionQueueSource   = "admission.queue"
	admissionCustodySource = "admission.custody"
)

// SetFaultRecorder names where admission failures are recorded: raised when
// evidence the courier must retain could not be admitted, cleared when an
// admission next completes.
func (h *RuntimeIngress) SetFaultRecorder(faults *faults.Recorder) {
	h.faults = faults
}

// NewRuntimeIngress builds the runtime-facing side of the courier. Custody
// acknowledgements it issues are queued in returns, the same durable queue the
// authority's artifacts travel back through, so the runtime's acknowledgement
// retires them and a lost one is redelivered rather than reissued.
func NewRuntimeIngress(courier *Courier, returns *DurableAuthoritySink, signingClock *signingclock.Clock, publish EvidencePublishFunc, envelopeSecret string, now func() time.Time) (*RuntimeIngress, error) {
	if courier == nil || returns == nil || signingClock == nil || publish == nil {
		return nil, fmt.Errorf("evidence: runtime ingress requires courier, return queue, signing clock and publisher")
	}
	if envelopeSecret == "" {
		return nil, fmt.Errorf("evidence: runtime ingress requires the MQTT envelope secret")
	}
	if now == nil {
		now = time.Now
	}
	return &RuntimeIngress{courier: courier, returns: returns, signingClock: signingClock, publish: publish, envelopeSecret: envelopeSecret, now: now}, nil
}

// SetReturnNotifier names what to wake once a custody acknowledgement is queued.
func (h *RuntimeIngress) SetReturnNotifier(notify func()) {
	h.notify = notify
}

func (h *RuntimeIngress) Handle(ctx context.Context, topic string, payload []byte) error {
	deviceID, err := contracts.DeviceIDFromEvidenceOutboundTopic(topic)
	if err != nil {
		return err
	}
	// A topic whose device is not configured is rejected before anything
	// else: no admission, no custody and no acknowledgement of any outcome.
	if !h.courier.queue.Configured(deviceID) {
		_ = h.courier.queue.NoteUnconfiguredDevice(deviceID)
		return faults.ErrUnconfiguredDevice
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
	if err := decodeRouting(artifactBytes, &artifactRouting); err != nil || artifactRouting.DeviceID == "" {
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
		if errors.Is(err, faults.ErrUnconfiguredDevice) {
			// The artifact names an unconfigured device; refused before
			// admission, with no acknowledgement.
			_ = h.courier.queue.NoteUnconfiguredDevice(deviceID)
			return err
		}
		if errors.Is(err, ErrQueueFull) {
			// A defined, retriable refusal of a full share, not a failure.
			return h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "refused", "queue_full", h.now().UnixMilli())
		}
		if errors.Is(err, errCustodyConstruction) {
			// Queued durably, but the envelope's custody return could not be
			// built: the custody form failed, the queueing form completed.
			h.faults.Note(faults.AdmissionFailed, admissionQueueSource, nil)
			h.faults.Note(faults.AdmissionFailed, admissionCustodySource, err)
			return err
		}
		h.faults.Note(faults.AdmissionFailed, admissionQueueSource, err)
		return err
	}
	// Durable outbound queueing completed. What follows — publishing the
	// acknowledgement — is not admission, and its failure is not
	// admission_failed: the runtime republishes and the idempotent queue
	// recovers the same entry.
	h.faults.Note(faults.AdmissionFailed, admissionQueueSource, nil)
	if err := h.publishOutboundAck(ctx, deviceID, carriage.ArtifactType, artifactBytes, "queued", "", admission.Queued.EnqueuedAtMS); err != nil {
		return err
	}
	if admission.Custody == nil {
		h.courier.queue.NoteAdmittedDevice(deviceID)
		return nil
	}
	// Canonical bytes, because the runtime acknowledges by a digest over the
	// artifact's canonical form and the queue retires by the bytes it holds.
	artifact, err := mqttauth.CanonicalJSONWithoutAuth(admission.Custody)
	if err != nil {
		h.faults.Note(faults.AdmissionFailed, admissionCustodySource, err)
		return fmt.Errorf("evidence: encode custody artifact: %w", err)
	}
	if err := h.returns.Store(ctx, AuthorityArtifact{
		Type: InboundCustodyAcknowledgement, DeviceID: deviceID, Payload: artifact,
	}); err != nil {
		h.faults.Note(faults.AdmissionFailed, admissionCustodySource, err)
		return fmt.Errorf("evidence: queue custody acknowledgement: %w", err)
	}
	h.faults.Note(faults.AdmissionFailed, admissionCustodySource, nil)
	h.courier.queue.NoteAdmittedDevice(deviceID)
	if h.notify != nil {
		h.notify()
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
	// acknowledged_at_ms is the queue decision and repeats on re-admission;
	// signed_at_ms comes from the durable clock so it never repeats, even
	// across a restart under an unchanged or regressed system clock.
	signedAt, err := h.signingClock.Next()
	if err != nil {
		return err
	}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceOutboundAckMessageType, deviceID, "", signedAt, h.envelopeSecret)
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
