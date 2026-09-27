// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/custody"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

const (
	defaultDeliveryRetry   = 5 * time.Second
	defaultBlockedReminder = 15 * time.Minute
	// maxArtifactBytes is the evidence-transport/v1 artifact size limit. A
	// larger artifact is refused at admission, before custody.
	maxArtifactBytes = 1 << 20
	// maxQueueRecordBytes is the exact encoded size of the largest outbound
	// queue record: a maximum-size artifact under the longest outbound type
	// (anchor_registration), with the largest queue_seq and enqueued_at_ms a
	// positive int64 can hold. It is the maximum encoded registration record,
	// and no evidence record is larger.
	maxQueueRecordBytes = maxRecordOverhead + int64(len(ArtifactAnchorRegistration))
	// maxEvidenceRecordBytes is the exact encoded size of the largest
	// non-registration record: the longer of delivery_envelope and checkpoint,
	// which is delivery_envelope.
	maxEvidenceRecordBytes = maxRecordOverhead + int64(len(ArtifactDeliveryEnvelope))
	// maxRecordOverhead is every byte of a maximum queue record but its type
	// name: the fixed fields, 19-digit queue_seq and enqueued_at_ms, and the
	// Base64 of a maximum-size artifact.
	maxRecordOverhead = int64(len(`{"v":1,"id":"`) + 64 +
		len(`","artifact_type":"`) +
		len(`","queue_seq":`) + 19 +
		len(`,"enqueued_at_ms":`) + 19 +
		len(`,"payload":"`) + 4*((maxArtifactBytes+2)/3) + len(`"}`))

	// MinDeviceShareItems and MinDeviceShareBytes are the least one device's
	// share may hold: the registration reserve (one anchor_registration slot
	// and one maximum encoded registration record) plus one maximum encoded
	// evidence record (evidence-transport/v1, gateway-config/v1).
	MinDeviceShareItems = 2
	MinDeviceShareBytes = maxQueueRecordBytes + maxEvidenceRecordBytes
)

var (
	errChannelUnavailable      = errors.New("evidence channel unavailable")
	errChannelRefused          = errors.New("evidence channel explicitly refused artifact")
	errChannelPermanentRefusal = errors.New("evidence channel permanently refused artifact")
	errChannelReceiverState    = errors.New("evidence channel refused artifact pending receiver state")
	errHeadHeld                = errors.New("queue head is held after a permanent refusal")
	errRuntimeSink             = errors.New("runtime authority-artifact sink unavailable")
	errMalformedResponse       = errors.New("evidence channel returned a malformed artifact")
	errQueueRetirement         = errors.New("durable queue retirement failed")
	errCustodyConstruction     = errors.New("evidence: construct custody acknowledgement")
	// ErrUnrecognisedOutcome is a response outside the refusal policy: a status
	// outside the table, or a response whose media type, body, fields, digest
	// binding, outcome or retry metadata is invalid or absent. It is retained,
	// recorded as unrecognised, and backed off; never retired or held.
	ErrUnrecognisedOutcome = errors.New("evidence: unrecognised transport outcome")
)

// Admission is the result of accepting one runtime artifact into durable
// custody. Custody is present only for delivery envelopes; the other outbound
// artifacts are carried but do not represent a runtime ledger row whose local
// queue can be released.
type Admission struct {
	Queued  QueuedArtifact
	Custody *custody.Acknowledgement
}

// Courier owns the only boundary allowed to issue custody: a successful
// DurableQueue commit followed by acknowledgement construction from the exact
// bytes committed.
type Courier struct {
	queue  *DurableQueue
	signer *custody.Signer
}

func NewCourier(queue *DurableQueue, signer *custody.Signer) (*Courier, error) {
	if queue == nil {
		return nil, fmt.Errorf("evidence: courier requires a durable queue")
	}
	return &Courier{queue: queue, signer: signer}, nil
}

// Admit stores payload byte-for-byte before returning custody. It deliberately
// performs no signature verification: the gateway holds no evidence-device or
// authority key and must not acquire either merely to be a courier.
func (c *Courier) Admit(kind ArtifactType, payload []byte) (Admission, error) {
	if c == nil || c.queue == nil {
		return Admission{}, fmt.Errorf("evidence: courier is not configured")
	}
	if err := validateArtifactRoutingFields(kind, payload); err != nil {
		return Admission{}, err
	}
	queued, err := c.queue.Enqueue(kind, payload)
	if err != nil {
		return Admission{}, err
	}
	admission := Admission{Queued: queued}
	if kind != ArtifactDeliveryEnvelope || c.signer == nil {
		return admission, nil
	}

	var envelope struct {
		DeviceID string `json:"device_id"`
		LocalSeq int64  `json:"local_seq"`
	}
	// validateArtifactRoutingFields already decoded these exact bytes. This
	// second decode is local routing only and never changes queued.Payload.
	if err := json.Unmarshal(payload, &envelope); err != nil {
		return Admission{}, fmt.Errorf("evidence: decode queued envelope routing fields: %w", err)
	}
	digest := sha256.Sum256(payload)
	ack, _, err := c.signer.Acknowledge(
		envelope.DeviceID,
		envelope.LocalSeq,
		"sha256:"+hex.EncodeToString(digest[:]),
		queued.EnqueuedAtMS,
	)
	if err != nil {
		// The artifact is already durable. Returning an error is intentional: the
		// runtime must retry and the idempotent enqueue will recover the same entry
		// and timestamp rather than losing it or claiming custody prematurely.
		return Admission{}, fmt.Errorf("%w: %w", errCustodyConstruction, err)
	}
	admission.Custody = &ack
	return admission, nil
}

func validateArtifactRoutingFields(kind ArtifactType, payload []byte) error {
	if !validOutboundArtifactType(kind) {
		return fmt.Errorf("evidence: unsupported outbound artifact type %q", kind)
	}
	// Courier opacity (evidence-transport/v1): only the stable routing
	// projection is read — device_id, and local_seq for an envelope. The
	// artifact's declared v is never read, so an artifact version the courier
	// does not know is admitted and delivered; the evidence authority alone
	// decides version support.
	var routing struct {
		DeviceID string `json:"device_id"`
		LocalSeq int64  `json:"local_seq"`
	}
	if err := json.Unmarshal(payload, &routing); err != nil {
		return fmt.Errorf("evidence: artifact routing fields are missing or unparseable: %w", err)
	}
	if !validRoutingDeviceID(routing.DeviceID) {
		return fmt.Errorf("evidence: artifact device_id is outside its routing domain")
	}
	if len(payload) > maxArtifactBytes {
		return fmt.Errorf("evidence: artifact exceeds the evidence-transport maximum of %d bytes", maxArtifactBytes)
	}
	if kind == ArtifactDeliveryEnvelope && routing.LocalSeq <= 0 {
		return fmt.Errorf("evidence: delivery envelope local_seq must be positive")
	}
	if kind == ArtifactDeliveryEnvelope && routing.LocalSeq > custody.MaxSafeInteger {
		return fmt.Errorf("evidence: delivery envelope local_seq is outside the D-011 integer zone")
	}
	return nil
}

// validRoutingDeviceID applies the v1 routing domain of device_id, as a
// carriage topic segment requires (evidence-exchange/v1).
func validRoutingDeviceID(device string) bool {
	return contracts.ValidEvidenceRoutingDeviceID(device)
}

// authorityRouting is the stable routing projection of an authority artifact.
type authorityRouting struct {
	DeviceID string `json:"device_id"`
	FromSeq  *int64 `json:"from_seq"`
	ToSeq    *int64 `json:"to_seq"`
}

// authorityRoutingProjection reads only the routing projection of an authority
// artifact (evidence-exchange/v1): device_id, and from_seq and to_seq on a
// delivery receipt, each present with its v1 type. The version is never read.
func authorityRoutingProjection(kind AuthorityArtifactType, payload []byte) (authorityRouting, error) {
	var routing authorityRouting
	if err := json.Unmarshal(payload, &routing); err != nil || !validRoutingDeviceID(routing.DeviceID) {
		return authorityRouting{}, fmt.Errorf("invalid authority routing fields")
	}
	if kind != AuthorityDeliveryReceipt {
		return routing, nil
	}
	if routing.FromSeq == nil || routing.ToSeq == nil ||
		*routing.FromSeq <= 0 || *routing.ToSeq < *routing.FromSeq || *routing.ToSeq > custody.MaxSafeInteger {
		return authorityRouting{}, fmt.Errorf("invalid receipt range")
	}
	return routing, nil
}

// AuthorityArtifactType is deliberately disjoint from ArtifactType and from
// custody.Acknowledgement. Only authority-signed artifacts may travel back to a
// runtime through this path.
type AuthorityArtifactType string

const (
	AuthorityDeliveryReceipt   AuthorityArtifactType = "delivery_receipt"
	AuthorityEpochConfirmation AuthorityArtifactType = "epoch_confirmation"
	// InboundCustodyAcknowledgement is gateway-issued, not authority-issued: it
	// rides the same durable return queue so the runtime's acknowledgement
	// retires it, but the authority channel never returns one.
	InboundCustodyAcknowledgement AuthorityArtifactType = "custody_acknowledgement"
)

// AuthorityArtifact is an authority-signed artifact returned by the evidence
// channel. Payload remains opaque to the gateway and is forwarded unchanged;
// DeviceID is routing metadata parsed from those bytes, not authenticated or
// rewritten by the gateway.
type AuthorityArtifact struct {
	Type     AuthorityArtifactType
	DeviceID string
	Payload  []byte
}

// DeliveryResult is the independent evidence channel's response. Accepted is
// affirmative application by the evidence authority, not broker receipt and
// not gateway custody.
type DeliveryResult struct {
	Accepted   bool
	Retriable  bool
	RetryAfter time.Duration
	// ReceiverState marks a non-retriable refusal that names receiver or
	// credential state rather than the artifact bytes. It is retried on the
	// next runtime handoff and never holds the queue. A non-retriable refusal
	// without it is terminal for these bytes: attempted once, then held.
	ReceiverState bool
	// RefusalStatus is the HTTP status of a refusal, or 0 when the channel has
	// none.
	RefusalStatus      int
	RefusalReason      string
	AuthorityArtifacts []AuthorityArtifact
}

// EvidenceChannel is deliberately not the fleet client. Implementations own
// independent authentication, credentials, transport and failure state.
type EvidenceChannel interface {
	Deliver(ctx context.Context, artifact QueuedArtifact) (DeliveryResult, error)
}

// AuthoritySink durably and idempotently accepts authority artifacts for
// forwarding to the originating runtime. Returning nil means a later process
// restart cannot lose the artifact; an MQTT PUBACK alone is not sufficient for
// this interface. Idempotence is required because a crash after Store and before
// outbound queue retirement causes safe redelivery.
type AuthoritySink interface {
	Store(ctx context.Context, artifact AuthorityArtifact) error
}

func validAuthorityArtifactType(kind AuthorityArtifactType) bool {
	return kind == AuthorityDeliveryReceipt || kind == AuthorityEpochConfirmation
}

func validateAuthorityRouting(queued QueuedArtifact, artifact AuthorityArtifact) (string, bool, error) {
	// Only the routing projection is read, never v: device_id and local_seq
	// of the queued artifact, and device_id with from_seq and to_seq of a
	// returned receipt.
	var outbound struct {
		DeviceID string `json:"device_id"`
		LocalSeq int64  `json:"local_seq"`
	}
	if err := json.Unmarshal(queued.Payload, &outbound); err != nil || outbound.DeviceID == "" {
		return "", false, fmt.Errorf("invalid queued routing fields")
	}
	authority, err := authorityRoutingProjection(artifact.Type, artifact.Payload)
	if err != nil {
		return "", false, err
	}
	if authority.DeviceID != outbound.DeviceID {
		return "", false, fmt.Errorf("authority artifact names a different device")
	}
	if artifact.Type != AuthorityDeliveryReceipt {
		return authority.DeviceID, false, nil
	}
	covers := queued.Type == ArtifactDeliveryEnvelope &&
		outbound.LocalSeq >= *authority.FromSeq && outbound.LocalSeq <= *authority.ToSeq
	return authority.DeviceID, covers, nil
}

func payloadDigest(payload []byte) string {
	digest := sha256.Sum256(payload)
	return "sha256:" + hex.EncodeToString(digest[:])
}

func artifactDeviceID(payload []byte) string {
	var routing struct {
		DeviceID string `json:"device_id"`
	}
	// The device is the one the admission path bound to its topic and the
	// configured device list; the log handler escapes it.
	if err := json.Unmarshal(payload, &routing); err != nil || routing.DeviceID == "" {
		return "unrecognised"
	}
	return routing.DeviceID
}

func safeFailureReason(err error) string {
	switch {
	case errors.Is(err, errChannelUnavailable):
		return "channel_unavailable"
	case errors.Is(err, errChannelRefused):
		return "channel_refused"
	case errors.Is(err, errChannelPermanentRefusal):
		return "channel_permanent_refusal"
	case errors.Is(err, errChannelReceiverState):
		return "channel_receiver_state_refusal"
	case errors.Is(err, errRuntimeSink):
		return "runtime_sink_unavailable"
	case errors.Is(err, errMalformedResponse):
		return "malformed_channel_response"
	case errors.Is(err, errQueueRetirement):
		return "queue_retirement_failed"
	default:
		return "delivery_pending"
	}
}
