// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/contracts/membertest"
)

func TestEvidencePayloadStructsNameOnlyDefinedMembers(t *testing.T) {
	for _, c := range []struct {
		family string
		value  any
	}{
		{contracts.FamilyEvidenceOutbound, outboundCarriage{}},
		{contracts.FamilyEvidenceOutboundAck, outboundAck{}},
		{contracts.FamilyEvidenceInbound, inboundEnvelope{}},
		{contracts.FamilyEvidenceInboundAck, inboundAck{}},
	} {
		if undefined := membertest.UndefinedNames(c.family, c.value); len(undefined) != 0 {
			t.Errorf("%s: %T names %v, which the family does not define", c.family, c.value, undefined)
		}
	}
}

// memberChannel answers dev-a's envelope with the artifacts it is given.
type memberChannel struct {
	artifacts func(localSeq int) []AuthorityArtifact
}

func (c *memberChannel) Deliver(_ context.Context, queued QueuedArtifact) (DeliveryResult, error) {
	var routing struct {
		LocalSeq int `json:"local_seq"`
	}
	_ = json.Unmarshal(queued.Payload, &routing)
	return DeliveryResult{Accepted: true, AuthorityArtifacts: c.artifacts(routing.LocalSeq)}, nil
}

func memberWorker(t *testing.T, artifacts func(int) []AuthorityArtifact) (*DurableQueue, *DurableAuthoritySink, *DeliveryWorker, *syncBuffer) {
	t.Helper()
	q := openTestQueue(t, 10, 1<<20)
	enqueueEnvelopes(t, q, "dev-a", 1, 1)
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "ret"), MaxItems: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	worker, err := NewDeliveryWorker(q, &memberChannel{artifacts: artifacts}, sink, DeliveryWorkerOptions{
		RetryInterval: time.Hour, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.NewJSONHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	return q, sink, worker, logs
}

// TestAnAuthorityArtifactTheMemberRuleRefusesIsUnrecognised returns a
// covering receipt and then an epoch confirmation that the gateway could not
// forward without emitting an inbound message the member rule refuses. As for
// an artifact over the inbound maximum (evidence-transport/v2), the attempt is
// an unrecognised outcome: the receipt staged before it stays staged, the
// confirmation is not staged, the envelope is retained and backs off, delivery
// reads degraded, and the refusal is logged with its reason.
func TestAnAuthorityArtifactTheMemberRuleRefusesIsUnrecognised(t *testing.T) {
	for _, c := range []struct {
		name, confirmation, reason string
	}{
		{"duplicate", `{"v":1,"device_id":"dev-a","anchor_epoch_id":"epoch","anchor_epoch_id":"other","signature":"ed25519:authority"}`, contracts.ReasonDuplicateMember},
		{"case variant of an envelope member", `{"v":1,"device_id":"dev-a","anchor_epoch_id":"epoch","signature":"ed25519:authority","SIGNATURE":"other"}`, contracts.ReasonCaseVariantMember},
		{"too deep inside the envelope", `{"v":1,"device_id":"dev-a","anchor_epoch_id":"epoch","signature":"ed25519:authority","x":` + strings.Repeat("[", 31) + strings.Repeat("]", 31) + `}`, contracts.ReasonNestingTooDeep},
	} {
		t.Run(c.name, func(t *testing.T) {
			q, sink, worker, logs := memberWorker(t, func(seq int) []AuthorityArtifact {
				return []AuthorityArtifact{
					{Type: AuthorityDeliveryReceipt, Payload: validReceiptBytes("dev-a", seq, seq)},
					{Type: AuthorityEpochConfirmation, Payload: []byte(c.confirmation)},
				}
			})
			waitFor(t, "the lane to back off", func() bool {
				e, ok := laneEntry(worker, LaneKey{Device: "dev-a", Lane: LaneEvidence})
				return ok && e.State == DeviceBackingOff
			})
			if q.Len() != 1 {
				t.Fatal("the envelope whose response carried a refused artifact was not retained")
			}
			if sink.Len() != 1 {
				t.Fatalf("%d artifacts staged, want the receipt alone", sink.Len())
			}
			if head, _ := sink.queue.Peek(); head.Type != artifactDeliveryReceipt {
				t.Fatalf("staged %s, want the receipt", head.Type)
			}
			if status := worker.Status(); !status.Degraded {
				t.Fatalf("status = %+v, want degraded", status)
			}
			if !strings.Contains(logs.String(), `"reason":"`+c.reason+`"`) {
				t.Fatalf("the refusal was not logged with %s:\n%s", c.reason, logs.String())
			}
		})
	}
}

// TestARefusedCoveringReceiptLeavesTheAcceptanceUnrecognised refuses the
// receipt itself: the response is then an acceptance without the receipt it
// requires, an unrecognised outcome, and the envelope stays and backs off.
func TestARefusedCoveringReceiptLeavesTheAcceptanceUnrecognised(t *testing.T) {
	q, sink, worker, _ := memberWorker(t, func(seq int) []AuthorityArtifact {
		receipt := fmt.Sprintf(`{"v":1,"device_id":"dev-a","from_seq":%d,"to_seq":%d,"to_seq":%d,"signature":"ed25519:authority"}`, seq, seq, seq)
		return []AuthorityArtifact{{Type: AuthorityDeliveryReceipt, Payload: []byte(receipt)}}
	})
	waitFor(t, "the lane to back off", func() bool {
		e, ok := laneEntry(worker, LaneKey{Device: "dev-a", Lane: LaneEvidence})
		return ok && e.State == DeviceBackingOff
	})
	if q.Len() != 1 || sink.Len() != 0 {
		t.Fatalf("%d queued, %d staged; want the envelope kept and nothing staged", q.Len(), sink.Len())
	}
}

// TestRoutingIsJudgedBeforeTheMemberRule returns, beside a valid covering
// receipt, artifacts the member rule would refuse that also fail routing:
// another device's, unparseable, or a case variant of a routing member. Each
// stays an unrecognised outcome, as before the rule applied: nothing is
// staged and the envelope is kept.
func TestRoutingIsJudgedBeforeTheMemberRule(t *testing.T) {
	for _, bad := range []string{
		`{"v":1,"device_id":"dev-b","anchor_epoch_id":"epoch","signature":"ed25519:authority","a":1,"A":2}`,
		`not json`,
		"\ufeff" + `{"v":1,"device_id":"dev-a","anchor_epoch_id":"epoch","signature":"ed25519:authority"}`,
		`{"v":1,"device_id":"dev-a","Device_ID":"dev-b","anchor_epoch_id":"epoch","signature":"ed25519:authority"}`,
	} {
		q, sink, worker, logs := memberWorker(t, func(seq int) []AuthorityArtifact {
			return []AuthorityArtifact{
				{Type: AuthorityEpochConfirmation, Payload: []byte(bad)},
				{Type: AuthorityDeliveryReceipt, Payload: validReceiptBytes("dev-a", seq, seq)},
			}
		})
		waitFor(t, "the lane to back off", func() bool {
			e, ok := laneEntry(worker, LaneKey{Device: "dev-a", Lane: LaneEvidence})
			return ok && e.State == DeviceBackingOff
		})
		if q.Len() != 1 || sink.Len() != 0 {
			t.Fatalf("%q: %d queued, %d staged; want the envelope kept and nothing staged", bad, q.Len(), sink.Len())
		}
		// Refused by routing, so the member rule never judged it.
		if strings.Contains(logs.String(), "payload member rule") {
			t.Fatalf("%q reached the member rule before routing refused it:\n%s", bad, logs.String())
		}
	}
}

// TestAnInboundMessageTheMemberRuleRefusesIsNeverPublished encodes an
// artifact staged before staging applied the rule: the gateway never emits
// the inbound message carrying it.
func TestAnInboundMessageTheMemberRuleRefusesIsNeverPublished(t *testing.T) {
	legacy := []byte(`{"v":1,"device_id":"dev-a","anchor_epoch_id":"epoch","signature":"ed25519:authority","Key_ID":"k"}`)
	if _, err := encodeInbound("dev-a", AuthorityEpochConfirmation, legacy, 1, "runtime-gateway-envelope-secret"); !errors.Is(err, errInboundRefusedByMemberRule) {
		t.Fatalf("encodeInbound = %v, want refused under the member rule", err)
	}
	clean := validEpochConfirmationBytes("dev-a")
	if _, err := encodeInbound("dev-a", AuthorityEpochConfirmation, clean, 1, "runtime-gateway-envelope-secret"); err != nil {
		t.Fatalf("a conforming artifact was refused: %v", err)
	}
}

// TestAHeadThatCanNeverBeCarriedIsLoggedOnce stages, as code before the
// member rule could, an artifact the rule refuses ahead of a clean one: the
// publisher carries neither, and logs the stuck head once, with no member
// value.
func TestAHeadThatCanNeverBeCarriedIsLoggedOnce(t *testing.T) {
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "ret"), MaxItems: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	// Enqueued directly: Store now refuses it.
	if _, err := sink.queue.enqueue(artifactEpochConfirmation, []byte(`{"v":1,"device_id":"dev-a","anchor_epoch_id":"epoch","signature":"ed25519:authority","Key_ID":"secret-value"}`)); err != nil {
		t.Fatal(err)
	}
	if err := sink.Store(context.Background(), AuthorityArtifact{Type: AuthorityEpochConfirmation, DeviceID: "dev-b", Payload: validEpochConfirmationBytes("dev-b")}); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1787000003000)
	var published int
	p, err := NewReturnPublisher(sink, testAckClock(t, func() time.Time { return now }),
		func(context.Context, string, byte, bool, []byte) error { published++; return nil },
		"runtime-gateway-envelope-secret", "", func() time.Time { return now }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	logs := &syncBuffer{}
	p.SetLogger(slog.New(slog.NewJSONHandler(logs, nil)))
	for range 3 {
		if err := p.publishHead(context.Background()); !errors.Is(err, errInboundRefusedByMemberRule) {
			t.Fatalf("publishHead = %v, want the head refused under the member rule", err)
		}
	}
	if published != 0 || sink.Len() != 2 {
		t.Fatalf("%d published, %d staged; want nothing published and both kept", published, sink.Len())
	}
	text := logs.String()
	if strings.Count(text, "can never be carried") != 1 || strings.Contains(text, "secret-value") {
		t.Fatalf("want one log line without member values:\n%s", text)
	}
}
