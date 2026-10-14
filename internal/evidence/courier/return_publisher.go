// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
	"io"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

type ReturnPublisher struct {
	sink           *DurableAuthoritySink
	signingClock   *signingclock.Clock
	publish        EvidencePublishFunc
	envelopeSecret string
	verifier       *mqttauth.Verifier
	now            func() time.Time
	retry          time.Duration
	wake           chan struct{}

	mu      sync.Mutex
	blocked bool
	// The head last carried and when, so a wake arriving right after a
	// publish — the initial timer and a Notify for the same entry — does not
	// put the same bytes on the wire twice within one retry interval. The
	// runtime would apply the first and refuse the second as a replay, then
	// acknowledge both and find the queue empty for the second.
	lastID string
	lastAt time.Time
}

func NewReturnPublisher(
	sink *DurableAuthoritySink,
	signingClock *signingclock.Clock,
	publish EvidencePublishFunc,
	envelopeSecret string,
	previousEnvelopeSecret string,
	now func() time.Time,
	retry time.Duration,
) (*ReturnPublisher, error) {
	if sink == nil || signingClock == nil || publish == nil || envelopeSecret == "" {
		return nil, fmt.Errorf("evidence: return publisher requires sink, signing clock, publisher, and envelope secret")
	}
	if now == nil {
		now = time.Now
	}
	if retry <= 0 {
		retry = 5 * time.Second
	}
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{
		SharedSecret: envelopeSecret, PreviousSharedSecret: previousEnvelopeSecret, Now: now,
	})
	if err != nil {
		return nil, err
	}
	return &ReturnPublisher{
		sink: sink, signingClock: signingClock, publish: publish, envelopeSecret: envelopeSecret,
		verifier: verifier, now: now, retry: retry, wake: make(chan struct{}, 1),
	}, nil
}

func (p *ReturnPublisher) Notify() {
	if p == nil {
		return
	}
	p.mu.Lock()
	p.blocked = false
	p.mu.Unlock()
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// ReceiverStateChanged retries a retained authority artifact after an event
// that can repair a receiver-state refusal. It is deliberately event-driven:
// unknown keys, versions, and sequences do not become valid merely because a
// short retry timer elapsed.
func (p *ReturnPublisher) ReceiverStateChanged() {
	p.Notify()
}

func (p *ReturnPublisher) Run(ctx context.Context) error {
	timer := time.NewTimer(0)
	defer timer.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.wake:
		case <-timer.C:
		}
		p.mu.Lock()
		blocked := p.blocked
		p.mu.Unlock()
		if blocked {
			continue
		}
		if err := p.publishHead(ctx); err != nil {
			resetTimer(timer, p.retry)
			continue
		}
		resetTimer(timer, p.retry)
	}
}

func (p *ReturnPublisher) publishHead(ctx context.Context) error {
	queued, ok := p.sink.queue.Peek()
	if !ok {
		return nil
	}
	deviceID, kind, err := authorityQueueRouting(queued)
	if err != nil {
		return err
	}
	p.mu.Lock()
	recentlyCarried := queued.ID == p.lastID && p.now().Sub(p.lastAt) < p.retry
	p.mu.Unlock()
	if recentlyCarried {
		return nil
	}
	envelope := inboundEnvelope{
		DeviceID: deviceID, ArtifactType: string(kind), Artifact: json.RawMessage(queued.Payload),
	}
	// Every delivery attempt is a newly signed envelope, across restarts too:
	// the durable clock guarantees a later signed_at_ms than any earlier
	// attempt, so a retained entry re-emitted under a regressed clock cannot
	// be byte-identical to one the runtime has already refused or applied.
	signedAt, err := p.signingClock.Next()
	if err != nil {
		return err
	}
	auth, err := mqttauth.Sign(envelope, contracts.EvidenceInboundMessageType, deviceID, "", signedAt, p.envelopeSecret)
	if err != nil {
		return err
	}
	envelope.Auth = &auth
	payload, err := json.Marshal(envelope)
	if err != nil {
		return err
	}
	topic, err := contracts.EvidenceInboundTopic(deviceID)
	if err != nil {
		return err
	}
	if err := p.publish(ctx, topic, 1, false, payload); err != nil {
		return err
	}
	p.mu.Lock()
	p.lastID, p.lastAt = queued.ID, p.now()
	p.mu.Unlock()
	return nil
}

func (p *ReturnPublisher) HandleAck(topic string, payload []byte) error {
	deviceID, err := contracts.DeviceIDFromEvidenceInboundAckTopic(topic)
	if err != nil {
		return err
	}
	if _, err := p.verifier.VerifyJSON(payload, contracts.EvidenceInboundAckMessageType, deviceID, ""); err != nil {
		return err
	}
	var ack inboundAck
	if err := refuseAmbiguousMembers(payload, &ack); err != nil {
		return fmt.Errorf("evidence: malformed inbound acknowledgement")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ack); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return fmt.Errorf("evidence: malformed inbound acknowledgement")
	}
	if ack.DeviceID != deviceID || ack.AcknowledgedAtMS <= 0 {
		return fmt.Errorf("evidence: inbound acknowledgement binding mismatch")
	}
	queued, ok := p.sink.queue.Peek()
	if !ok {
		return fmt.Errorf("evidence: acknowledgement names an empty return queue")
	}
	queuedDevice, queuedType, err := authorityQueueRouting(queued)
	if err != nil {
		return err
	}
	digest := sha256.Sum256(queued.Payload)
	if queuedDevice != deviceID || string(queuedType) != ack.ArtifactType || ack.ArtifactDigest != "sha256:"+hex.EncodeToString(digest[:]) {
		return fmt.Errorf("evidence: acknowledgement does not name the queued authority artifact")
	}
	retire, recognised, err := inboundDecision(ack.Outcome, ack.Reason)
	if err != nil {
		return err
	}
	if !retire {
		p.mu.Lock()
		p.blocked = true
		p.mu.Unlock()
		if !recognised {
			return ErrUnrecognisedInboundReason
		}
		return nil
	}
	if err := p.sink.queue.Remove(queued.ID); err != nil {
		return err
	}
	p.Notify()
	return nil
}

// ErrUnrecognisedInboundReason reports a refusal whose reason is outside the
// receiver rejection vocabulary. The artifact is held, not retired, and is
// carried again only on a receiver state change.
var ErrUnrecognisedInboundReason = errors.New("evidence: inbound refusal reason unrecognised; authority artifact held")

// inboundRefusalRetires is the evidence-exchange/v3 receiver rejection
// vocabulary and, per gateway-evidence-carriage/v1, whether each retires.
var inboundRefusalRetires = map[string]bool{
	"malformed":            true,
	"bad_authenticator":    true,
	"binding_mismatch":     true,
	"non_contiguous_range": true,
	"unrecognised_version": false,
	"wrong_purpose":        false,
	"unknown_key":          false,
	"retired_key":          false,
	"unknown_sequence":     false,
	"superseded":           true,
	"restatement_conflict": true,
}

// inboundDecision classifies an authenticated decision. Any outcome other than
// applied with no reason, or refused, is not a decision and is an error. A
// refusal whose reason is outside the vocabulary is held, never retired.
func inboundDecision(outcome, reason string) (retire, recognised bool, err error) {
	switch outcome {
	case "applied":
		if reason != "" {
			return false, false, fmt.Errorf("evidence: invalid inbound acknowledgement outcome")
		}
		return true, true, nil
	case "refused":
		retire, recognised = inboundRefusalRetires[reason]
		return retire, recognised, nil
	default:
		return false, false, fmt.Errorf("evidence: invalid inbound acknowledgement outcome")
	}
}

type inboundAck struct {
	DeviceID         string                   `json:"device_id"`
	ArtifactType     string                   `json:"artifact_type"`
	ArtifactDigest   string                   `json:"artifact_digest"`
	Outcome          string                   `json:"outcome"`
	Reason           string                   `json:"reason"`
	AcknowledgedAtMS int64                    `json:"acknowledged_at_ms"`
	Auth             *contracts.HeartbeatAuth `json:"auth"`
}

func authorityQueueRouting(queued QueuedArtifact) (string, AuthorityArtifactType, error) {
	// Courier opacity: the routing device_id is read, never the declared v.
	var value struct {
		DeviceID string `json:"device_id"`
	}
	if err := decodeRouting(queued.Payload, &value); err != nil || value.DeviceID == "" {
		return "", "", fmt.Errorf("evidence: invalid queued authority artifact")
	}
	switch queued.Type {
	case artifactDeliveryReceipt:
		return value.DeviceID, AuthorityDeliveryReceipt, nil
	case artifactEpochConfirmation:
		return value.DeviceID, AuthorityEpochConfirmation, nil
	case artifactCustodyAcknowledgement:
		return value.DeviceID, InboundCustodyAcknowledgement, nil
	default:
		return "", "", fmt.Errorf("evidence: invalid authority return queue type")
	}
}
