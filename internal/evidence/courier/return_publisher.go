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
	"log/slog"
	"reflect"
	"strconv"
	"strings"
	"sync"
	"time"

	canonicaljson "github.com/ori-platform/ori-canonicaljson"
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
	// after arms the retry timer; tests replace it. afterIteration is a test
	// seam called at the end of every loop iteration, nil in production.
	after          func(time.Duration) <-chan time.Time
	afterIteration func()
	// peeked is a test seam called by HandleAck once it has read the head
	// and before it decides; nil in production.
	peeked func()

	// mu also spans an acknowledgement's decision, from reading the head to
	// holding or retiring it, so a decision is never applied to a head another
	// acknowledgement has since replaced.
	mu sync.Mutex
	// heldID is the queue entry a non-retiring refusal was decided on, and
	// heldDevice the device it is for. Only that entry is held; a different
	// head is carried. Only that device's receiver state change releases it.
	heldID     string
	heldDevice string
	// log reports a head that can never be carried; stuckID is the head it
	// last reported, so each is logged once.
	log     *slog.Logger
	stuckID string
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
		verifier: verifier, now: now, retry: retry, wake: make(chan struct{}, 1), after: time.After,
	}, nil
}

// SetLogger sets where the publisher reports a head it can never carry.
func (p *ReturnPublisher) SetLogger(log *slog.Logger) {
	p.mu.Lock()
	p.log = log
	p.mu.Unlock()
}

// Notify wakes the loop to carry the head, as when a return is queued or the
// head retires. It never releases a hold: a queue wake is not a receiver state
// change.
func (p *ReturnPublisher) Notify() {
	if p == nil {
		return
	}
	select {
	case p.wake <- struct{}{}:
	default:
	}
}

// ReceiverStateChanged retries a retained authority artifact after an event
// on deviceID that can repair a receiver-state refusal: a hold on an artifact
// for that device is released, and a hold for any other device is kept
// (gateway-evidence-carriage/v1, a relevant runtime state change). It is
// deliberately event-driven: unknown keys, versions, and sequences do not
// become valid merely because a short retry timer elapsed.
func (p *ReturnPublisher) ReceiverStateChanged(deviceID string) {
	if p == nil {
		return
	}
	p.mu.Lock()
	if p.heldDevice == deviceID {
		p.heldID, p.heldDevice = "", ""
	}
	p.mu.Unlock()
	p.Notify()
}

func (p *ReturnPublisher) Run(ctx context.Context) error {
	timer := p.after(0)
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-p.wake:
		case <-timer:
		}
		_ = p.publishHead(ctx)
		timer = p.after(p.retry)
		if p.afterIteration != nil {
			p.afterIteration()
		}
	}
}

func (p *ReturnPublisher) publishHead(ctx context.Context) error {
	p.mu.Lock()
	queued, ok := p.sink.queue.Peek()
	held := ok && queued.ID == p.heldID
	recentlyCarried := ok && queued.ID == p.lastID && p.now().Sub(p.lastAt) < p.retry
	p.mu.Unlock()
	if !ok || held || recentlyCarried {
		return nil
	}
	deviceID, kind, err := authorityQueueRouting(queued)
	if err != nil {
		return err
	}
	// Every delivery attempt is a newly signed envelope, across restarts too:
	// the durable clock guarantees a later signed_at_ms than any earlier
	// attempt, so a retained entry re-emitted under a regressed clock cannot
	// be byte-identical to one the runtime has already refused or applied.
	signedAt, err := p.signingClock.Next()
	if err != nil {
		return err
	}
	payload, err := encodeInbound(deviceID, kind, queued.Payload, signedAt, p.envelopeSecret)
	if err != nil {
		p.reportStuck(queued, kind, err)
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

// reportStuck logs, once per head, a staged artifact that can never be
// encoded as an inbound message: one staged before staging applied the
// member rule or the inbound maximum. The return queue has one head for the
// site, so it blocks every device's returns until an operator removes it.
func (p *ReturnPublisher) reportStuck(queued QueuedArtifact, kind AuthorityArtifactType, err error) {
	if !errors.Is(err, errInboundRefusedByMemberRule) && !errors.Is(err, errInboundOverMaximum) &&
		!errors.Is(err, errQueuedArtifactNotCanonical) {
		return
	}
	p.mu.Lock()
	log, first := p.log, p.stuckID != queued.ID
	p.stuckID = queued.ID
	p.mu.Unlock()
	if log == nil || !first {
		return
	}
	log.Error("evidence return queue head can never be carried to the runtime; it blocks every return until removed",
		"artifact_type", string(kind), "artifact_digest", payloadDigest(queued.Payload), "queue_record", queued.ID,
		"reason", err.Error())
}

// encodeInbound signs an inbound evidence message and encodes the whole
// payload, auth included, as canonical JSON, so the artifact occupies exactly
// its canonical bytes and staging's bound keeps the payload within its topic
// maximum (gateway-api/v2 Payload Size).
func encodeInbound(deviceID string, kind AuthorityArtifactType, artifact []byte, signedAt int64, secret string) ([]byte, error) {
	envelope := inboundEnvelope{DeviceID: deviceID, ArtifactType: string(kind), Artifact: json.RawMessage(artifact)}
	auth, err := mqttauth.Sign(envelope, contracts.EvidenceInboundMessageType, deviceID, "", signedAt, secret)
	if err != nil {
		return nil, errQueuedArtifactNotCanonical
	}
	envelope.Auth = &auth
	encoded, err := json.Marshal(envelope)
	if err != nil {
		return nil, err
	}
	payload, err := canonicaljson.MarshalWire(encoded)
	if err != nil {
		// The canonicaliser's error can quote a member name from the artifact.
		return nil, errQueuedArtifactNotCanonical
	}
	if len(payload) > contracts.PayloadMaxima[contracts.FamilyEvidenceInbound] {
		return nil, errInboundOverMaximum
	}
	// A producer never emits a payload the member rule refuses; an artifact
	// staged before staging applied the rule is held back here.
	if err := contracts.AdmitPayload(contracts.FamilyEvidenceInbound, payload); err != nil {
		return nil, errInboundRefusedByMemberRule
	}
	return payload, nil
}

// inboundMemberRefusal judges an authority artifact as the Payload Member
// Rule judges the inbound message that would carry it: the artifact exactly as
// received, inside an envelope of the inbound topic's members, so the depth
// and members the envelope adds count. The routing values are placeholders of
// the right type; the rule judges names and structure, not values. It returns
// the *contracts.MemberRuleError, or nil.
func inboundMemberRefusal(artifact AuthorityArtifact) error {
	payload := make([]byte, 0, len(artifact.Payload)+256)
	payload = append(payload, `{"device_id":"","artifact_type":"`...)
	payload = append(payload, string(artifact.Type)...)
	payload = append(payload, `","artifact":`...)
	payload = append(payload, artifact.Payload...)
	payload = append(payload, `,"auth":{"scheme":"hmac-sha256","signed_at_ms":1,"signature":"hmac-sha256:"}}`...)
	return contracts.AdmitPayload(contracts.FamilyEvidenceInbound, payload)
}

func (p *ReturnPublisher) HandleAck(topic string, payload []byte) error {
	deviceID, err := contracts.DeviceIDFromEvidenceInboundAckTopic(topic)
	if err != nil {
		return err
	}
	// The replay key is recorded only for an acknowledgement that decides the
	// queued artifact: one that names nothing the gateway holds adds no state
	// (gateway-api/v2 Payload Volume).
	return p.verifier.VerifyJSONThenDecide(payload, contracts.EvidenceInboundAckMessageType, deviceID, "",
		func(map[string]any) (bool, error) { return p.decideAck(deviceID, payload) })
}

// decideAck applies an authenticated acknowledgement to the queued head. It
// reports whether the acknowledgement decided that head, retiring or holding
// it.
func (p *ReturnPublisher) decideAck(deviceID string, payload []byte) (bool, error) {
	var ack inboundAck
	if err := refuseAmbiguousMembers(payload, &ack); err != nil {
		return false, fmt.Errorf("evidence: malformed inbound acknowledgement")
	}
	if err := requireInboundAckTypes(payload); err != nil {
		return false, fmt.Errorf("evidence: malformed inbound acknowledgement")
	}
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&ack); err != nil || decoder.Decode(&struct{}{}) != io.EOF {
		return false, fmt.Errorf("evidence: malformed inbound acknowledgement")
	}
	// The verifier already binds the body's device_id to the topic; this is defence in depth.
	if ack.DeviceID != deviceID || ack.AcknowledgedAtMS <= 0 {
		return false, fmt.Errorf("evidence: inbound acknowledgement binding mismatch")
	}
	p.mu.Lock()
	defer p.mu.Unlock()
	queued, ok := p.sink.queue.Peek()
	if !ok {
		return false, fmt.Errorf("evidence: acknowledgement names an empty return queue")
	}
	if p.peeked != nil {
		p.peeked()
	}
	queuedDevice, queuedType, err := authorityQueueRouting(queued)
	if err != nil {
		return false, err
	}
	// The acknowledgement names the artifact by its canonical bytes, the bytes
	// the runtime received (gateway-api/v2 Inbound Evidence Delivery).
	canonical, err := canonicaljson.MarshalWire(queued.Payload)
	if err != nil {
		return false, errQueuedArtifactNotCanonical
	}
	digest := sha256.Sum256(canonical)
	if queuedDevice != deviceID || string(queuedType) != ack.ArtifactType || ack.ArtifactDigest != "sha256:"+hex.EncodeToString(digest[:]) {
		return false, fmt.Errorf("evidence: acknowledgement does not name the queued authority artifact")
	}
	retire, recognised, err := inboundDecision(ack.Outcome, ack.Reason)
	if err != nil {
		return false, err
	}
	if !retire {
		p.heldID, p.heldDevice = queued.ID, queuedDevice
		if !recognised {
			return true, NewUnrecognisedInboundReasonError(ack.Reason)
		}
		return true, nil
	}
	if err := p.sink.queue.Remove(queued.ID); err != nil {
		return false, err
	}
	if p.heldID == queued.ID {
		p.heldID, p.heldDevice = "", ""
	}
	p.Notify()
	return true, nil
}

// ErrUnrecognisedInboundReason reports a refusal whose reason is outside the
// receiver rejection vocabulary. The artifact is held, not retired, and is
// carried again only on a receiver state change.
var ErrUnrecognisedInboundReason = errors.New("evidence: inbound refusal reason unrecognised; authority artifact held")

// UnrecognisedInboundReasonError carries the unrecognised reason, quoted and
// bounded so it is safe to log. It matches ErrUnrecognisedInboundReason.
type UnrecognisedInboundReasonError struct {
	Reason string
}

// NewUnrecognisedInboundReasonError quotes reason and bounds it to
// maxLoggedReasonBytes.
func NewUnrecognisedInboundReasonError(reason string) *UnrecognisedInboundReasonError {
	return &UnrecognisedInboundReasonError{Reason: boundedQuote(reason)}
}

func (e *UnrecognisedInboundReasonError) Error() string {
	return "evidence: inbound refusal reason " + e.Reason + " unrecognised; authority artifact held"
}

func (e *UnrecognisedInboundReasonError) Is(target error) bool {
	return target == ErrUnrecognisedInboundReason
}

const maxLoggedReasonBytes = 64

func boundedQuote(reason string) string {
	if len(reason) > maxLoggedReasonBytes {
		return strconv.Quote(reason[:maxLoggedReasonBytes]) + "..."
	}
	return strconv.Quote(reason)
}

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

// inboundAckStrings are the acknowledgement's string members, read from
// inboundAck so a member added there is covered. Each is required as a
// string: encoding/json decodes an absent member or a null as "", and an empty
// reason is applied's.
var inboundAckStrings = stringMembers(reflect.TypeFor[inboundAck]())

// stringMembers lists the json member names of typ's string fields.
func stringMembers(typ reflect.Type) []string {
	var members []string
	for field := range typ.Fields() {
		name, _, _ := strings.Cut(field.Tag.Get("json"), ",")
		if field.Type.Kind() == reflect.String && name != "" && name != "-" {
			members = append(members, name)
		}
	}
	return members
}

// requireInboundAckTypes requires every string member to be present as a JSON
// string, on the wire bytes, before the acknowledgement is decoded. An absent,
// null or non-integer acknowledged_at_ms already decodes to a refusal.
func requireInboundAckTypes(payload []byte) error {
	var members map[string]json.RawMessage
	if err := json.Unmarshal(payload, &members); err != nil {
		return err
	}
	for _, name := range inboundAckStrings {
		if raw, ok := members[name]; !ok || len(raw) == 0 || raw[0] != '"' {
			return fmt.Errorf("member %s is not a string", name)
		}
	}
	return nil
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
