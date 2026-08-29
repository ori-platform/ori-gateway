// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"
)

const defaultDeliveryRetry = 5 * time.Second

var (
	errChannelUnavailable      = errors.New("evidence channel unavailable")
	errChannelRefused          = errors.New("evidence channel explicitly refused artifact")
	errChannelPermanentRefusal = errors.New("evidence channel permanently refused artifact")
	errRuntimeSink             = errors.New("runtime authority-artifact sink unavailable")
	errMalformedResponse       = errors.New("evidence channel returned a malformed artifact")
	errQueueRetirement         = errors.New("durable queue retirement failed")
)

// Admission is the result of accepting one runtime artifact into durable
// custody. Custody is present only for delivery envelopes; the other outbound
// artifacts are carried but do not represent a runtime ledger row whose local
// queue can be released.
type Admission struct {
	Queued  QueuedArtifact
	Custody *CustodyAcknowledgement
}

// Courier owns the only boundary allowed to issue custody: a successful
// DurableQueue commit followed by acknowledgement construction from the exact
// bytes committed.
type Courier struct {
	queue  *DurableQueue
	signer *CustodySigner
}

func NewCourier(queue *DurableQueue, signer *CustodySigner) (*Courier, error) {
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
		return Admission{}, fmt.Errorf("evidence: construct custody acknowledgement: %w", err)
	}
	admission.Custody = &ack
	return admission, nil
}

func validateArtifactRoutingFields(kind ArtifactType, payload []byte) error {
	if !validOutboundArtifactType(kind) {
		return fmt.Errorf("evidence: unsupported outbound artifact type %q", kind)
	}
	var routing struct {
		V        int    `json:"v"`
		DeviceID string `json:"device_id"`
		LocalSeq int64  `json:"local_seq"`
	}
	if err := json.Unmarshal(payload, &routing); err != nil {
		return fmt.Errorf("evidence: artifact is not a JSON object: %w", err)
	}
	if routing.V != 1 {
		return fmt.Errorf("evidence: artifact version must be 1")
	}
	if routing.DeviceID == "" {
		return fmt.Errorf("evidence: artifact device_id must not be empty")
	}
	if kind == ArtifactDeliveryEnvelope && routing.LocalSeq <= 0 {
		return fmt.Errorf("evidence: delivery envelope local_seq must be positive")
	}
	if kind == ArtifactDeliveryEnvelope && routing.LocalSeq > maxSafeInteger {
		return fmt.Errorf("evidence: delivery envelope local_seq is outside the D-011 integer zone")
	}
	return nil
}

// AuthorityArtifactType is deliberately disjoint from ArtifactType and from
// CustodyAcknowledgement. Only authority-signed artifacts may travel back to a
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
	Accepted           bool
	Retriable          bool
	RetryAfter         time.Duration
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

// DeliveryWorker drains the durable queue through the independent evidence
// channel. Failure leaves the head entry intact and retries later.
type DeliveryWorker struct {
	queue   *DurableQueue
	channel EvidenceChannel
	sink    AuthoritySink
	retry   time.Duration
	wake    chan struct{}

	deliveryMu    sync.Mutex
	mu            sync.Mutex
	lastFailureAt time.Time
	lastError     string
	blocked       bool
}

type DeliveryWorkerOptions struct {
	RetryInterval time.Duration
}

type channelRefusalError struct {
	retryAfter time.Duration
}

func (e *channelRefusalError) Error() string { return errChannelRefused.Error() }
func (e *channelRefusalError) Unwrap() error { return errChannelRefused }

func NewDeliveryWorker(queue *DurableQueue, channel EvidenceChannel, sink AuthoritySink, opts DeliveryWorkerOptions) (*DeliveryWorker, error) {
	if queue == nil {
		return nil, fmt.Errorf("evidence: delivery worker requires a durable queue")
	}
	if channel == nil {
		return nil, fmt.Errorf("evidence: delivery worker requires an independent evidence channel")
	}
	retry := opts.RetryInterval
	if retry <= 0 {
		retry = defaultDeliveryRetry
	}
	return &DeliveryWorker{
		queue: queue, channel: channel, sink: sink, retry: retry, wake: make(chan struct{}, 1),
	}, nil
}

// Notify wakes a worker after a new durable admission. It is edge-triggered and
// non-blocking; a full signal buffer already means the worker will look again.
func (w *DeliveryWorker) Notify() {
	if w == nil {
		return
	}
	w.mu.Lock()
	w.blocked = false
	w.mu.Unlock()
	select {
	case w.wake <- struct{}{}:
	default:
	}
}

func (w *DeliveryWorker) Run(ctx context.Context) error {
	if w == nil {
		return fmt.Errorf("evidence: nil delivery worker")
	}
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-w.wake:
		case <-timer.C:
		}

		delivered, err := w.deliverHead(ctx)
		if err != nil {
			if errors.Is(err, context.Canceled) && ctx.Err() != nil {
				return nil
			}
			w.recordFailure(err)
			if errors.Is(err, errChannelPermanentRefusal) {
				w.mu.Lock()
				w.blocked = true
				w.mu.Unlock()
				if !timer.Stop() {
					select {
					case <-timer.C:
					default:
					}
				}
				continue
			}
			delay := w.retry
			var refusal *channelRefusalError
			if errors.As(err, &refusal) && refusal.retryAfter > delay {
				delay = refusal.retryAfter
			}
			resetTimer(timer, delay)
			continue
		}
		w.clearFailure()
		if delivered {
			// Drain immediately while work exists. No timer is needed to make
			// progress, and each successful removal is durably committed first.
			resetTimer(timer, 0)
			continue
		}
		resetTimer(timer, w.retry)
	}
}

func resetTimer(timer *time.Timer, delay time.Duration) {
	if !timer.Stop() {
		select {
		case <-timer.C:
		default:
		}
	}
	timer.Reset(delay)
}

func (w *DeliveryWorker) deliverHead(ctx context.Context) (bool, error) {
	w.deliveryMu.Lock()
	defer w.deliveryMu.Unlock()

	queued, ok := w.queue.Peek()
	if !ok {
		return false, nil
	}
	result, err := w.channel.Deliver(ctx, queued)
	if err != nil {
		return false, errChannelUnavailable
	}
	if !result.Accepted {
		if !result.Retriable {
			return false, errChannelPermanentRefusal
		}
		return false, &channelRefusalError{retryAfter: result.RetryAfter}
	}
	if len(result.AuthorityArtifacts) > 0 && w.sink == nil {
		return false, errRuntimeSink
	}
	routed := make([]AuthorityArtifact, 0, len(result.AuthorityArtifacts))
	hasCoveringReceipt := false
	for _, artifact := range result.AuthorityArtifacts {
		if len(artifact.Payload) == 0 || !validAuthorityArtifactType(artifact.Type) {
			return false, errMalformedResponse
		}
		deviceID, coversEnvelope, err := validateAuthorityRouting(queued, artifact)
		if err != nil {
			return false, errMalformedResponse
		}
		hasCoveringReceipt = hasCoveringReceipt || coversEnvelope
		routed = append(routed, AuthorityArtifact{
			Type: artifact.Type, DeviceID: deviceID, Payload: append([]byte(nil), artifact.Payload...),
		})
	}
	if queued.Type == ArtifactDeliveryEnvelope && !hasCoveringReceipt {
		// An authority accepting an envelope without returning a receipt covering
		// that device and sequence leaves the runtime unable to distinguish
		// recorded evidence from a stalled or substituted hop. Keep the envelope
		// so an idempotent retry can recover the correct receipt.
		return false, errMalformedResponse
	}
	for _, artifact := range routed {
		if err := w.sink.Store(ctx, artifact); err != nil {
			return false, errRuntimeSink
		}
	}
	if err := w.queue.Remove(queued.ID); err != nil {
		return false, errQueueRetirement
	}
	return true, nil
}

func validAuthorityArtifactType(kind AuthorityArtifactType) bool {
	return kind == AuthorityDeliveryReceipt || kind == AuthorityEpochConfirmation
}

func validateAuthorityRouting(queued QueuedArtifact, artifact AuthorityArtifact) (string, bool, error) {
	var outbound struct {
		V        int    `json:"v"`
		DeviceID string `json:"device_id"`
		LocalSeq int64  `json:"local_seq"`
	}
	if err := json.Unmarshal(queued.Payload, &outbound); err != nil || outbound.V != 1 || outbound.DeviceID == "" {
		return "", false, fmt.Errorf("invalid queued routing fields")
	}
	var authority struct {
		V        int    `json:"v"`
		DeviceID string `json:"device_id"`
		FromSeq  int64  `json:"from_seq"`
		ToSeq    int64  `json:"to_seq"`
	}
	if err := json.Unmarshal(artifact.Payload, &authority); err != nil || authority.V != 1 || authority.DeviceID == "" {
		return "", false, fmt.Errorf("invalid authority routing fields")
	}
	if authority.DeviceID != outbound.DeviceID {
		return "", false, fmt.Errorf("authority artifact names a different device")
	}
	if artifact.Type != AuthorityDeliveryReceipt {
		return authority.DeviceID, false, nil
	}
	if authority.FromSeq <= 0 || authority.ToSeq < authority.FromSeq || authority.ToSeq > maxSafeInteger {
		return "", false, fmt.Errorf("invalid receipt range")
	}
	covers := queued.Type == ArtifactDeliveryEnvelope &&
		outbound.LocalSeq >= authority.FromSeq && outbound.LocalSeq <= authority.ToSeq
	return authority.DeviceID, covers, nil
}

// Status exposes queue/failure state without channel endpoint, credentials,
// implementation identity, or authority details.
type DeliveryStatus struct {
	Pending       int
	Degraded      bool
	Blocked       bool
	LastFailureAt time.Time
	LastError     string
}

func (w *DeliveryWorker) Status() DeliveryStatus {
	if w == nil {
		return DeliveryStatus{}
	}
	w.mu.Lock()
	defer w.mu.Unlock()
	return DeliveryStatus{
		Pending: w.queue.Len(), Degraded: w.lastError != "", Blocked: w.blocked,
		LastFailureAt: w.lastFailureAt, LastError: w.lastError,
	}
}

func (w *DeliveryWorker) recordFailure(err error) {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastFailureAt = time.Now()
	// Errors are deliberately reduced to a closed, implementation-neutral
	// vocabulary. Raw transport errors may contain hostnames or URLs.
	w.lastError = safeFailureReason(err)
}

func safeFailureReason(err error) string {
	switch {
	case errors.Is(err, errChannelUnavailable):
		return "channel_unavailable"
	case errors.Is(err, errChannelRefused):
		return "channel_refused"
	case errors.Is(err, errChannelPermanentRefusal):
		return "channel_permanent_refusal"
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

func (w *DeliveryWorker) clearFailure() {
	w.mu.Lock()
	defer w.mu.Unlock()
	w.lastError = ""
	w.blocked = false
}
