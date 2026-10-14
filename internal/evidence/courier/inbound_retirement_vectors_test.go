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
	"path/filepath"
	"slices"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

type inboundRetirementCorpus struct {
	Contract   string   `json:"contract"`
	Vocabulary []string `json:"vocabulary"`
	Cases      []struct {
		Name            string `json:"name"`
		Acknowledgement *struct {
			Outcome string `json:"outcome"`
			Reason  string `json:"reason"`
		} `json:"acknowledgement"`
		Retire     bool    `json:"retire"`
		Head       string  `json:"head"`
		Redelivery *string `json:"redelivery"`
	} `json:"cases"`
}

func loadInboundRetirementCorpus(t *testing.T) inboundRetirementCorpus {
	t.Helper()
	raw, err := specvectors.Read("gateway-evidence-carriage/vectors/inbound-retirement-v1.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus inboundRetirementCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	if corpus.Contract != "gateway-evidence-carriage/v1" || len(corpus.Vocabulary) == 0 || len(corpus.Cases) == 0 {
		t.Fatalf("unexpected inbound retirement corpus: %s with %d reasons and %d cases", corpus.Contract, len(corpus.Vocabulary), len(corpus.Cases))
	}
	return corpus
}

const (
	retirementRetry  = 100 * time.Millisecond
	retirementWindow = 6 * retirementRetry
	retirementWait   = 3 * time.Second
	retirementSecret = "runtime-gateway-envelope-secret"
	retirementDevice = "site-a-edge-01"
	retirementTopic  = "ori/site-a-edge-01/evidence/inbound/ack"
)

// returnRig runs the real return publisher loop over one queued receipt and
// records every envelope it puts on the wire.
type returnRig struct {
	t         *testing.T
	sink      *DurableAuthoritySink
	publisher *ReturnPublisher
	artifact  []byte
	headID    string
	published chan []byte
}

func newReturnRig(t *testing.T) *returnRig {
	t.Helper()
	sink, err := OpenDurableAuthoritySink(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := validReceiptBytes(retirementDevice, 1, 1)
	if err := sink.Store(context.Background(), AuthorityArtifact{
		Type: AuthorityDeliveryReceipt, DeviceID: retirementDevice, Payload: artifact,
	}); err != nil {
		t.Fatal(err)
	}
	head, ok := sink.queue.Peek()
	if !ok {
		t.Fatal("the receipt was not queued")
	}
	published := make(chan []byte, 128)
	publisher, err := NewReturnPublisher(sink, testAckClock(t, time.Now),
		func(_ context.Context, _ string, _ byte, _ bool, payload []byte) error {
			published <- append([]byte(nil), payload...)
			return nil
		}, retirementSecret, "", time.Now, retirementRetry)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = publisher.Run(ctx)
	}()
	t.Cleanup(func() {
		cancel()
		<-done
	})
	return &returnRig{t: t, sink: sink, publisher: publisher, artifact: artifact, headID: head.ID, published: published}
}

func (r *returnRig) next() []byte {
	r.t.Helper()
	select {
	case payload := <-r.published:
		return payload
	case <-time.After(retirementWait):
		r.t.Fatal("the return publisher carried nothing")
		return nil
	}
}

func (r *returnRig) quietFor(window time.Duration) {
	r.t.Helper()
	deadline := time.After(window)
	carried := 0
	for {
		select {
		case <-r.published:
			carried++
		case <-deadline:
			if carried != 0 {
				r.t.Fatalf("the head was carried %d times in %v without a receiver state change", carried, window)
			}
			return
		}
	}
}

// decision signs the runtime's acknowledgement of the queued receipt.
func (r *returnRig) decision(outcome, reason string, mutate func(*inboundAck)) []byte {
	r.t.Helper()
	digest := sha256.Sum256(r.artifact)
	ack := inboundAck{
		DeviceID: retirementDevice, ArtifactType: string(AuthorityDeliveryReceipt),
		ArtifactDigest: "sha256:" + hex.EncodeToString(digest[:]),
		Outcome:        outcome, Reason: reason, AcknowledgedAtMS: time.Now().UnixMilli(),
	}
	if mutate != nil {
		mutate(&ack)
	}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceInboundAckMessageType, retirementDevice, "", time.Now().UnixMilli(), retirementSecret)
	if err != nil {
		r.t.Fatal(err)
	}
	ack.Auth = &auth
	payload, err := json.Marshal(ack)
	if err != nil {
		r.t.Fatal(err)
	}
	return payload
}

func (r *returnRig) headKept() {
	r.t.Helper()
	head, ok := r.sink.queue.Peek()
	if !ok || head.ID != r.headID || r.sink.Len() != 1 {
		r.t.Fatalf("the head was not kept: %d queued", r.sink.Len())
	}
}

// freshEnvelope requires a later delivery of the same artifact under a newly
// signed envelope.
func (r *returnRig) freshEnvelope(earlier, later []byte) {
	r.t.Helper()
	var a, b inboundEnvelope
	if err := json.Unmarshal(earlier, &a); err != nil {
		r.t.Fatal(err)
	}
	if err := json.Unmarshal(later, &b); err != nil {
		r.t.Fatal(err)
	}
	if string(b.Artifact) != string(r.artifact) || a.Auth == nil || b.Auth == nil || b.Auth.SignedAtMS <= a.Auth.SignedAtMS {
		r.t.Fatal("the head was not redelivered as the same artifact in a newly signed envelope")
	}
}

// TestInboundRetirementCorpus drives every vendored case through the real
// acknowledgement handler and the publisher loop: whether the decision is
// accepted, whether the head retires, and how it is carried again.
func TestInboundRetirementCorpus(t *testing.T) {
	corpus := loadInboundRetirementCorpus(t)
	for reason := range inboundRefusalRetires {
		if !slices.Contains(corpus.Vocabulary, reason) {
			t.Errorf("the gateway accepts %q, which the receiver rejection vocabulary does not list", reason)
		}
	}
	covered := map[string]bool{}
	for _, c := range corpus.Cases {
		if c.Acknowledgement != nil {
			covered[c.Acknowledgement.Reason] = true
		}
	}
	for _, reason := range corpus.Vocabulary {
		if !covered[reason] {
			t.Errorf("the corpus vocabulary lists %q but no case decides it", reason)
		}
	}
	for _, c := range corpus.Cases {
		t.Run(c.Name, func(t *testing.T) {
			t.Parallel()
			if (c.Head == "retired") != c.Retire || (c.Head != "retired" && c.Head != "kept") {
				t.Fatalf("case head %q disagrees with retire %v", c.Head, c.Retire)
			}
			rig := newReturnRig(t)
			first := rig.next()
			if c.Acknowledgement == nil {
				if c.Retire || c.Redelivery == nil || *c.Redelivery != "fresh_envelope" {
					t.Fatalf("unhandled unacknowledged case: retire %v", c.Retire)
				}
				rig.freshEnvelope(first, rig.next())
				rig.headKept()
				return
			}
			ack := c.Acknowledgement
			err := rig.publisher.HandleAck(retirementTopic, rig.decision(ack.Outcome, ack.Reason, nil))
			recognised := ack.Outcome == "applied" || slices.Contains(corpus.Vocabulary, ack.Reason)
			switch {
			case recognised && err != nil:
				t.Fatalf("a decision in the vocabulary was refused: %v", err)
			case !recognised && !errors.Is(err, ErrUnrecognisedInboundReason):
				t.Fatalf("a reason outside the vocabulary = %v, want it held as unrecognised", err)
			}
			if c.Retire {
				if c.Redelivery != nil {
					t.Fatalf("a retired head names redelivery %q", *c.Redelivery)
				}
				if rig.sink.Len() != 0 {
					t.Fatal("the head did not retire")
				}
				rig.quietFor(retirementWindow)
				return
			}
			if c.Redelivery == nil || *c.Redelivery != "on_state_change" {
				t.Fatal("unhandled held case")
			}
			rig.headKept()
			rig.quietFor(retirementWindow)
			rig.headKept()
			rig.publisher.ReceiverStateChanged()
			rig.freshEnvelope(first, rig.next())
			rig.headKept()
		})
	}
}

// TestUnknownInboundReasonIsHeld sends refusals whose reason is outside the
// vocabulary, including near spellings of retiring reasons: each is held,
// never retired and never carried on the retry interval, and carried again on
// a receiver state change.
func TestUnknownInboundReasonIsHeld(t *testing.T) {
	corpus := loadInboundRetirementCorpus(t)
	reasons := []string{
		"", "Superseded", "SUPERSEDED", " superseded", "superseded ", "superseded\n",
		"superseded\x00", "restatement-conflict", "restatementconflict", "Restatement_Conflict",
		"ѕuperseded", "applied", "queue_full", "retire", "malformed,superseded",
	}
	for _, c := range corpus.Cases {
		if c.Acknowledgement != nil && c.Acknowledgement.Outcome == "refused" && !slices.Contains(corpus.Vocabulary, c.Acknowledgement.Reason) {
			reasons = append(reasons, c.Acknowledgement.Reason)
		}
	}
	for _, reason := range reasons {
		t.Run(reason, func(t *testing.T) {
			t.Parallel()
			if slices.Contains(corpus.Vocabulary, reason) {
				t.Fatalf("%q is in the vocabulary", reason)
			}
			rig := newReturnRig(t)
			first := rig.next()
			if err := rig.publisher.HandleAck(retirementTopic, rig.decision("refused", reason, nil)); !errors.Is(err, ErrUnrecognisedInboundReason) {
				t.Fatalf("HandleAck = %v, want the refusal held as unrecognised", err)
			}
			rig.headKept()
			rig.quietFor(retirementWindow)
			rig.headKept()
			rig.publisher.ReceiverStateChanged()
			rig.freshEnvelope(first, rig.next())
			if err := rig.publisher.HandleAck(retirementTopic, rig.decision("refused", "superseded", nil)); err != nil || rig.sink.Len() != 0 {
				t.Fatalf("a retiring refusal after the hold = %v with %d queued; want it retired", err, rig.sink.Len())
			}
		})
	}
}

// TestAMisboundRetiringRefusalRetiresNothing sends a retiring reason in a
// decision that is not the runtime's decision on the queued head: it neither
// retires nor holds, and the head is carried again as if unacknowledged.
func TestAMisboundRetiringRefusalRetiresNothing(t *testing.T) {
	cases := map[string]struct {
		topic   string
		outcome string
		mutate  func(*inboundAck)
		payload func([]byte) []byte
	}{
		"applied with a reason":  {outcome: "applied"},
		"capitalised outcome":    {outcome: "Refused"},
		"empty outcome":          {outcome: ""},
		"another artifact":       {outcome: "refused", mutate: func(a *inboundAck) { a.ArtifactDigest = "sha256:" + hex.EncodeToString(make([]byte, 32)) }},
		"another artifact type":  {outcome: "refused", mutate: func(a *inboundAck) { a.ArtifactType = string(AuthorityEpochConfirmation) }},
		"another device in body": {outcome: "refused", mutate: func(a *inboundAck) { a.DeviceID = "site-a-edge-02" }},
		"another device topic":   {outcome: "refused", topic: "ori/site-a-edge-02/evidence/inbound/ack"},
		"no decision time":       {outcome: "refused", mutate: func(a *inboundAck) { a.AcknowledgedAtMS = 0 }},
		"tampered after signing": {outcome: "refused", payload: func(p []byte) []byte { return replaceOnce(p, `"superseded"`, `"malformed"`) }},
		"retiring reason renamed": {outcome: "refused", payload: func(p []byte) []byte {
			return replaceOnce(p, `"reason":"superseded"`, `"reason":"restatement_conflict"`)
		}},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			rig := newReturnRig(t)
			first := rig.next()
			payload := rig.decision(c.outcome, "superseded", c.mutate)
			if c.payload != nil {
				payload = c.payload(payload)
			}
			topic := retirementTopic
			if c.topic != "" {
				topic = c.topic
			}
			err := rig.publisher.HandleAck(topic, payload)
			if err == nil || errors.Is(err, ErrUnrecognisedInboundReason) {
				t.Fatalf("HandleAck = %v, want the decision refused", err)
			}
			rig.headKept()
			rig.publisher.mu.Lock()
			blocked := rig.publisher.blocked
			rig.publisher.mu.Unlock()
			if blocked {
				t.Fatal("a refused decision held the head")
			}
			rig.freshEnvelope(first, rig.next())
			rig.headKept()
		})
	}
}

func replaceOnce(payload []byte, from, to string) []byte {
	out := bytes.Replace(payload, []byte(from), []byte(to), 1)
	if bytes.Equal(out, payload) {
		panic("replaceOnce: " + from + " not in payload")
	}
	return out
}
