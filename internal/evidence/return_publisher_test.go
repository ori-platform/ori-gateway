// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

func TestReturnPublisherRetiresOnlyAfterAuthenticatedRuntimeDecision(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	sink, err := OpenDurableAuthoritySink(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := validReceiptBytes("site-a-edge-01", 1, 1)
	if err := sink.Store(context.Background(), AuthorityArtifact{
		Type: AuthorityDeliveryReceipt, DeviceID: "site-a-edge-01", Payload: artifact,
	}); err != nil {
		t.Fatal(err)
	}
	var published publishedEvidenceMessage
	publisher, err := NewReturnPublisher(
		sink, testAckClock(t, func() time.Time { return now }),
		func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
			published = publishedEvidenceMessage{topic, qos, retained, append([]byte(nil), payload...)}
			return nil
		},
		"runtime-gateway-envelope-secret", "", func() time.Time { return now }, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.publishHead(context.Background()); err != nil {
		t.Fatal(err)
	}
	if sink.Len() != 1 {
		t.Fatal("PUBACK-equivalent publish retired the authority artifact")
	}
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{
		SharedSecret: "runtime-gateway-envelope-secret", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyJSON(
		published.payload, contracts.EvidenceInboundMessageType, "site-a-edge-01", "",
	); err != nil {
		t.Fatalf("published authority envelope did not verify: %v", err)
	}
	digest := sha256.Sum256(artifact)
	ack := inboundAck{
		DeviceID: "site-a-edge-01", ArtifactType: "delivery_receipt",
		ArtifactDigest: "sha256:" + hex.EncodeToString(digest[:]),
		Outcome:        "applied", Reason: "", AcknowledgedAtMS: now.UnixMilli(),
	}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceInboundAckMessageType, ack.DeviceID, "", now.UnixMilli(), "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	ack.Auth = &auth
	payload, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.HandleAck("ori/site-a-edge-01/evidence/inbound/ack", payload); err != nil {
		t.Fatal(err)
	}
	if sink.Len() != 0 {
		t.Fatal("applied authenticated acknowledgement did not retire the artifact")
	}
}

func TestReceiverStateRefusalDoesNotRetireOrShortLoop(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	sink, err := OpenDurableAuthoritySink(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20,
	})
	if err != nil {
		t.Fatal(err)
	}
	artifact := validEpochConfirmationBytes("site-a-edge-01")
	if err := sink.Store(context.Background(), AuthorityArtifact{
		Type: AuthorityEpochConfirmation, DeviceID: "site-a-edge-01", Payload: artifact,
	}); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewReturnPublisher(
		sink, testAckClock(t, time.Now), func(context.Context, string, byte, bool, []byte) error { return nil },
		"runtime-gateway-envelope-secret", "", func() time.Time { return now }, time.Second,
	)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifact)
	ack := inboundAck{
		DeviceID: "site-a-edge-01", ArtifactType: "epoch_confirmation",
		ArtifactDigest: "sha256:" + hex.EncodeToString(digest[:]),
		Outcome:        "refused", Reason: "unknown_key", AcknowledgedAtMS: now.UnixMilli(),
	}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceInboundAckMessageType, ack.DeviceID, "", now.UnixMilli(), "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	ack.Auth = &auth
	payload, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.HandleAck("ori/site-a-edge-01/evidence/inbound/ack", payload); err != nil {
		t.Fatal(err)
	}
	if sink.Len() != 1 || !publisher.blocked {
		t.Fatal("receiver-state refusal retired or kept short-looping the artifact")
	}
	publisher.ReceiverStateChanged()
	if publisher.blocked {
		t.Fatal("receiver state change did not make the retained artifact eligible for retry")
	}
	select {
	case <-publisher.wake:
	default:
		t.Fatal("receiver state change did not wake the return publisher")
	}
}

func TestMalformedRuntimeDecisionCannotRetireOrBlockAuthorityArtifact(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	artifact := validReceiptBytes("site-a-edge-01", 1, 1)
	if err := sink.Store(context.Background(), AuthorityArtifact{Type: AuthorityDeliveryReceipt, DeviceID: "site-a-edge-01", Payload: artifact}); err != nil {
		t.Fatal(err)
	}
	publisher, err := NewReturnPublisher(sink, testAckClock(t, func() time.Time { return now }), func(context.Context, string, byte, bool, []byte) error { return nil }, "runtime-gateway-envelope-secret", "", func() time.Time { return now }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(artifact)
	ack := inboundAck{DeviceID: "site-a-edge-01", ArtifactType: "delivery_receipt", ArtifactDigest: "sha256:" + hex.EncodeToString(digest[:]), Outcome: "ignored", Reason: "", AcknowledgedAtMS: now.UnixMilli()}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceInboundAckMessageType, ack.DeviceID, "", now.UnixMilli(), "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	ack.Auth = &auth
	payload, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.HandleAck("ori/site-a-edge-01/evidence/inbound/ack", payload); err == nil {
		t.Fatal("malformed authenticated decision was accepted")
	}
	if sink.Len() != 1 || publisher.blocked {
		t.Fatal("malformed decision changed authority return state")
	}
}

func TestReturnPublisherDoesNotCarryTheSameHeadTwiceWithinARetryInterval(t *testing.T) {
	clock := time.UnixMilli(1787000000000)
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	if err := sink.Store(context.Background(), AuthorityArtifact{Type: AuthorityDeliveryReceipt, DeviceID: "site-a-edge-01", Payload: validReceiptBytes("site-a-edge-01", 1, 1)}); err != nil {
		t.Fatal(err)
	}
	published := 0
	publisher, err := NewReturnPublisher(sink, testAckClock(t, time.Now), func(context.Context, string, byte, bool, []byte) error {
		published++
		return nil
	}, "runtime-gateway-envelope-secret", "", func() time.Time { return clock }, 5*time.Second)
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 3; i++ {
		if err := publisher.publishHead(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	if published != 1 {
		t.Fatalf("head carried %d times within one interval, want once", published)
	}
	clock = clock.Add(5 * time.Second)
	if err := publisher.publishHead(context.Background()); err != nil {
		t.Fatal(err)
	}
	if published != 2 {
		t.Fatalf("head not retried after the interval: carried %d times", published)
	}
}

func TestReturnPublisherReemitsAfterARestartWithAFreshEnvelope(t *testing.T) {
	// The runtime stays up across a gateway restart and the gateway's clock has
	// regressed. The retained receipt must go out again under a signature the
	// runtime's replay defence has not seen.
	frozen := time.UnixMilli(1787000000000)
	now := func() time.Time { return frozen }
	dir := filepath.Join(t.TempDir(), "return")
	var published [][]byte
	record := func(_ context.Context, _ string, _ byte, _ bool, payload []byte) error {
		published = append(published, append([]byte(nil), payload...))
		return nil
	}
	boot := func() *ReturnPublisher {
		sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 20, Now: now})
		if err != nil {
			t.Fatal(err)
		}
		if sink.Len() == 0 {
			if err := sink.Store(context.Background(), AuthorityArtifact{Type: AuthorityDeliveryReceipt, DeviceID: "site-a-edge-01", Payload: validReceiptBytes("site-a-edge-01", 1, 1)}); err != nil {
				t.Fatal(err)
			}
		}
		clock, err := OpenReturnSigningClock(dir, now)
		if err != nil {
			t.Fatal(err)
		}
		publisher, err := NewReturnPublisher(sink, clock, record, "runtime-gateway-envelope-secret", "", now, 5*time.Second)
		if err != nil {
			t.Fatal(err)
		}
		return publisher
	}
	if err := boot().publishHead(context.Background()); err != nil {
		t.Fatal(err)
	}
	regressed := frozen.Add(-2 * time.Second)
	now = func() time.Time { return regressed }
	if err := boot().publishHead(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(published) != 2 {
		t.Fatalf("published %d deliveries, want 2", len(published))
	}
	if bytes.Equal(published[0], published[1]) {
		t.Fatal("the restarted gateway re-emitted the retained entry byte for byte")
	}
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "runtime-gateway-envelope-secret", Now: func() time.Time { return frozen }})
	if err != nil {
		t.Fatal(err)
	}
	var signedAt []int64
	for i, payload := range published {
		if _, err := verifier.VerifyJSON(payload, contracts.EvidenceInboundMessageType, "site-a-edge-01", ""); err != nil {
			t.Fatalf("delivery %d refused by a replay-aware verifier: %v", i, err)
		}
		var envelope inboundEnvelope
		if err := json.Unmarshal(payload, &envelope); err != nil {
			t.Fatal(err)
		}
		signedAt = append(signedAt, envelope.Auth.SignedAtMS)
	}
	if signedAt[1] <= signedAt[0] {
		t.Fatalf("signed_at_ms did not advance across the restart: %d then %d", signedAt[0], signedAt[1])
	}
}
