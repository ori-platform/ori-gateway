// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

type ReturnPublisher struct {
	sink           *DurableAuthoritySink
	publish        EvidencePublishFunc
	envelopeSecret string
	verifier       *mqttauth.Verifier
	now            func() time.Time
	retry          time.Duration
	wake           chan struct{}

	mu      sync.Mutex
	blocked bool
}

func NewReturnPublisher(
	sink *DurableAuthoritySink,
	publish EvidencePublishFunc,
	envelopeSecret string,
	previousEnvelopeSecret string,
	now func() time.Time,
	retry time.Duration,
) (*ReturnPublisher, error) {
	if sink == nil || publish == nil || envelopeSecret == "" {
		return nil, fmt.Errorf("evidence: return publisher requires sink, publisher, and envelope secret")
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
		sink: sink, publish: publish, envelopeSecret: envelopeSecret,
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
	envelope := inboundEnvelope{
		DeviceID: deviceID, ArtifactType: string(kind), Artifact: json.RawMessage(queued.Payload),
	}
	signedAt := p.now().UnixMilli()
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
	return p.publish(ctx, topic, 1, false, payload)
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
	if err := validateInboundDecision(ack.Outcome, ack.Reason); err != nil {
		return err
	}
	retire := ack.Outcome == "applied" && ack.Reason == ""
	if ack.Outcome == "refused" {
		retire = bytePermanentRefusal(ack.Reason)
	}
	if !retire {
		p.mu.Lock()
		p.blocked = true
		p.mu.Unlock()
		return nil
	}
	if err := p.sink.queue.Remove(queued.ID); err != nil {
		return err
	}
	p.Notify()
	return nil
}

func validateInboundDecision(outcome, reason string) error {
	if outcome == "applied" && reason == "" {
		return nil
	}
	if outcome != "refused" {
		return fmt.Errorf("evidence: invalid inbound acknowledgement outcome")
	}
	switch reason {
	case "malformed", "bad_authenticator", "binding_mismatch", "non_contiguous_range",
		"unrecognised_version", "wrong_purpose", "unknown_key", "retired_key", "unknown_sequence":
		return nil
	default:
		return fmt.Errorf("evidence: invalid inbound acknowledgement reason")
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
	var value struct {
		V        int    `json:"v"`
		DeviceID string `json:"device_id"`
	}
	if err := json.Unmarshal(queued.Payload, &value); err != nil || value.V != 1 || value.DeviceID == "" {
		return "", "", fmt.Errorf("evidence: invalid queued authority artifact")
	}
	switch queued.Type {
	case artifactDeliveryReceipt:
		return value.DeviceID, AuthorityDeliveryReceipt, nil
	case artifactEpochConfirmation:
		return value.DeviceID, AuthorityEpochConfirmation, nil
	default:
		return "", "", fmt.Errorf("evidence: invalid authority return queue type")
	}
}

func bytePermanentRefusal(reason string) bool {
	switch reason {
	case "malformed", "bad_authenticator", "binding_mismatch", "non_contiguous_range":
		return true
	default:
		return false
	}
}
