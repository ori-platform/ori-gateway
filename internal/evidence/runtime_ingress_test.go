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
	"path/filepath"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

type publishedEvidenceMessage struct {
	topic    string
	qos      byte
	retained bool
	payload  []byte
}

func TestRuntimeIngressQueuesBeforeAuthenticatedAckAndCustody(t *testing.T) {
	courier, queue := testCourier(t, 10)
	var published []publishedEvidenceMessage
	now := time.UnixMilli(1787000000950)
	returns := testReturnSink(t, now)
	notified := false
	handler, err := NewRuntimeIngress(
		courier,
		returns,
		func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
			if queue.Len() != 1 {
				t.Fatal("message published before the artifact was durable")
			}
			published = append(published, publishedEvidenceMessage{topic, qos, retained, append([]byte(nil), payload...)})
			return nil
		},
		"runtime-gateway-envelope-secret",
		func() time.Time { return now },
	)
	if err != nil {
		t.Fatal(err)
	}
	handler.SetReturnNotifier(func() { notified = true })
	artifact := validEnvelopeBytes(12)
	carriage, err := json.Marshal(map[string]any{
		"device_id": "site-a-edge-01", "artifact_type": "delivery_envelope",
		"artifact_b64": base64.StdEncoding.EncodeToString(artifact),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), "ori/site-a-edge-01/evidence/outbound", carriage); err != nil {
		t.Fatal(err)
	}
	if len(published) != 1 {
		t.Fatalf("published %d messages, want only the queue acknowledgement", len(published))
	}
	if returns.Len() != 1 {
		t.Fatalf("custody acknowledgement not queued for return: %d entries", returns.Len())
	}
	if !notified {
		t.Fatal("the return publisher was not woken for the queued custody")
	}
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{
		SharedSecret: "runtime-gateway-envelope-secret", Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	if published[0].topic != "ori/site-a-edge-01/evidence/outbound/ack" || published[0].qos != 1 || published[0].retained {
		t.Fatalf("wrong outbound acknowledgement publish: %#v", published[0])
	}
	if _, err := verifier.VerifyJSON(
		published[0].payload, contracts.EvidenceOutboundAckMessageType, "site-a-edge-01", "",
	); err != nil {
		t.Fatalf("outbound acknowledgement did not verify: %v", err)
	}
	// The return publisher carries the queued custody on the inbound route,
	// and only the runtime's acknowledgement retires it.
	var inbound []publishedEvidenceMessage
	publisher, err := NewReturnPublisher(returns, func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
		inbound = append(inbound, publishedEvidenceMessage{topic, qos, retained, append([]byte(nil), payload...)})
		return nil
	}, "runtime-gateway-envelope-secret", "", func() time.Time { return now }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := publisher.publishHead(context.Background()); err != nil {
		t.Fatal(err)
	}
	if len(inbound) != 1 || inbound[0].topic != "ori/site-a-edge-01/evidence/inbound" || inbound[0].qos != 1 || inbound[0].retained {
		t.Fatalf("wrong custody publish: %#v", inbound)
	}
	unsigned, err := verifier.VerifyJSON(inbound[0].payload, contracts.EvidenceInboundMessageType, "site-a-edge-01", "")
	if err != nil {
		t.Fatalf("custody transport envelope did not verify: %v", err)
	}
	if unsigned["artifact_type"] != "custody_acknowledgement" {
		t.Fatalf("queued custody carried as %v", unsigned["artifact_type"])
	}
	// The runtime digests the artifact's canonical bytes, as evidence-exchange
	// fixes them; the queue must hold exactly those bytes for the digests to meet.
	custody, err := mqttauth.CanonicalJSONWithoutAuth(unsigned["artifact"])
	if err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(custody)
	ack := signedInboundAck(t, "site-a-edge-01", "custody_acknowledgement", "sha256:"+hex.EncodeToString(digest[:]), "applied", "", now)
	if err := publisher.HandleAck("ori/site-a-edge-01/evidence/inbound/ack", ack); err != nil {
		t.Fatalf("runtime acknowledgement refused: %v", err)
	}
	if returns.Len() != 0 {
		t.Fatalf("applied custody acknowledgement was not retired: %d entries", returns.Len())
	}
}

func TestRuntimeIngressReacknowledgesReadmittedBytesWithAFreshSignature(t *testing.T) {
	courier, _ := testCourier(t, 10)
	var published []publishedEvidenceMessage
	clock := time.UnixMilli(1787000000950)
	handler, err := NewRuntimeIngress(courier, testReturnSink(t, clock), func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
		published = append(published, publishedEvidenceMessage{topic, qos, retained, append([]byte(nil), payload...)})
		return nil
	}, "runtime-gateway-envelope-secret", func() time.Time { return clock })
	if err != nil {
		t.Fatal(err)
	}
	artifact := validCheckpointBytes()
	carriage, err := json.Marshal(map[string]any{
		"device_id": "site-a-edge-01", "artifact_type": "checkpoint",
		"artifact_b64": base64.StdEncoding.EncodeToString(artifact),
	})
	if err != nil {
		t.Fatal(err)
	}
	// The clock does not move between the two admissions: this is the case a
	// runtime's drain and its flush produce within one millisecond.
	for i := 0; i < 2; i++ {
		if err := handler.Handle(context.Background(), "ori/site-a-edge-01/evidence/outbound", carriage); err != nil {
			t.Fatal(err)
		}
	}
	if len(published) != 2 {
		t.Fatalf("published %d acknowledgements, want 2", len(published))
	}
	if bytes.Equal(published[0].payload, published[1].payload) {
		t.Fatal("re-acknowledgement is byte-identical to the first; the runtime would refuse it as a replay")
	}
	var first, second outboundAck
	if err := json.Unmarshal(published[0].payload, &first); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal(published[1].payload, &second); err != nil {
		t.Fatal(err)
	}
	if first.AcknowledgedAtMS != second.AcknowledgedAtMS || first.ArtifactDigest != second.ArtifactDigest {
		t.Fatalf("re-admission changed the queue decision: %+v vs %+v", first, second)
	}
	if first.Auth.SignedAtMS != clock.UnixMilli() || second.Auth.SignedAtMS != clock.UnixMilli()+1 {
		t.Fatalf("signed_at_ms is not strictly monotonic under a frozen clock: %d then %d", first.Auth.SignedAtMS, second.Auth.SignedAtMS)
	}
	if first.Auth.Signature == second.Auth.Signature {
		t.Fatal("the two acknowledgements carry the same signature")
	}
	for i, message := range published {
		verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "runtime-gateway-envelope-secret", Now: func() time.Time { return clock }})
		if err != nil {
			t.Fatal(err)
		}
		if _, err := verifier.VerifyJSON(message.payload, contracts.EvidenceOutboundAckMessageType, "site-a-edge-01", ""); err != nil {
			t.Fatalf("acknowledgement %d did not verify: %v", i, err)
		}
	}
}

func TestRuntimeIngressRejectsTopicWrapperAndArtifactBindingMismatch(t *testing.T) {
	courier, _ := testCourier(t, 10)
	var published []publishedEvidenceMessage
	handler, err := NewRuntimeIngress(courier, testReturnSink(t, time.Now()), func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
		published = append(published, publishedEvidenceMessage{topic, qos, retained, append([]byte(nil), payload...)})
		return nil
	}, "runtime-gateway-envelope-secret", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	artifact := validEnvelopeBytes(1)
	carriage, err := json.Marshal(map[string]any{
		"device_id": "other-device", "artifact_type": "delivery_envelope",
		"artifact_b64": base64.StdEncoding.EncodeToString(artifact),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), "ori/site-a-edge-01/evidence/outbound", carriage); err != nil {
		t.Fatal(err)
	}
	if len(published) != 1 {
		t.Fatalf("binding refusal published %d messages", len(published))
	}
	var ack outboundAck
	if err := json.Unmarshal(published[0].payload, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Outcome != "refused" || ack.Reason != "binding_mismatch" {
		t.Fatalf("binding refusal = %#v", ack)
	}
}

func TestRuntimeIngressSignsExplicitQueueFullRefusalWithoutCustody(t *testing.T) {
	courier, _ := testCourier(t, 1)
	if _, err := courier.Admit(ArtifactDeliveryEnvelope, validEnvelopeBytes(1)); err != nil {
		t.Fatal(err)
	}
	now := time.UnixMilli(1787000000950)
	var published []publishedEvidenceMessage
	handler, err := NewRuntimeIngress(courier, testReturnSink(t, time.Now()), func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
		published = append(published, publishedEvidenceMessage{topic, qos, retained, append([]byte(nil), payload...)})
		return nil
	}, "runtime-gateway-envelope-secret", func() time.Time { return now })
	if err != nil {
		t.Fatal(err)
	}
	artifact := validEnvelopeBytes(2)
	carriage, err := json.Marshal(map[string]any{
		"device_id": "site-a-edge-01", "artifact_type": "delivery_envelope",
		"artifact_b64": base64.StdEncoding.EncodeToString(artifact),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), "ori/site-a-edge-01/evidence/outbound", carriage); err != nil {
		t.Fatal(err)
	}
	if len(published) != 1 || published[0].topic != "ori/site-a-edge-01/evidence/outbound/ack" || published[0].qos != 1 || published[0].retained {
		t.Fatalf("queue-full publication = %#v", published)
	}
	var ack outboundAck
	if err := json.Unmarshal(published[0].payload, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Outcome != "refused" || ack.Reason != "queue_full" {
		t.Fatalf("queue-full acknowledgement = %#v", ack)
	}
	verifier, err := mqttauth.NewVerifier(mqttauth.Config{SharedSecret: "runtime-gateway-envelope-secret", Now: func() time.Time { return now }})
	if err != nil {
		t.Fatal(err)
	}
	if _, err := verifier.VerifyJSON(published[0].payload, contracts.EvidenceOutboundAckMessageType, "site-a-edge-01", ""); err != nil {
		t.Fatalf("queue-full acknowledgement did not verify: %v", err)
	}
}

func testReturnSink(t *testing.T, now time.Time) *DurableAuthoritySink {
	t.Helper()
	sink, err := OpenDurableAuthoritySink(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20,
		Now: func() time.Time { return now },
	})
	if err != nil {
		t.Fatal(err)
	}
	return sink
}

func validCheckpointBytes() []byte {
	payload, _ := json.Marshal(map[string]any{
		"v": 1, "device_id": "site-a-edge-01", "high_water_seq": 12, "anchor_epoch_id": "sha256:" + hex.EncodeToString(bytes.Repeat([]byte{0x7f}, 32)),
		"boot_id": 7, "key_id": "sha256:" + hex.EncodeToString(bytes.Repeat([]byte{0x63}, 32)), "issued_at_ms": 1787000900000,
		"signature": "ed25519:" + base64.StdEncoding.EncodeToString(bytes.Repeat([]byte{0x11}, 64)),
	})
	return payload
}

func signedInboundAck(t *testing.T, deviceID, artifactType, digest, outcome, reason string, now time.Time) []byte {
	t.Helper()
	ack := inboundAck{DeviceID: deviceID, ArtifactType: artifactType, ArtifactDigest: digest, Outcome: outcome, Reason: reason, AcknowledgedAtMS: now.UnixMilli()}
	auth, err := mqttauth.Sign(ack, contracts.EvidenceInboundAckMessageType, deviceID, "", now.UnixMilli(), "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	ack.Auth = &auth
	payload, err := json.Marshal(ack)
	if err != nil {
		t.Fatal(err)
	}
	return payload
}

func TestRuntimeIngressRefusesACommissioningAuthorisationFromTheDeviceSide(t *testing.T) {
	courier, queue := testCourier(t, 10)
	var published []publishedEvidenceMessage
	handler, err := NewRuntimeIngress(courier, testReturnSink(t, time.Now()), func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
		published = append(published, publishedEvidenceMessage{topic, qos, retained, append([]byte(nil), payload...)})
		return nil
	}, "runtime-gateway-envelope-secret", time.Now)
	if err != nil {
		t.Fatal(err)
	}
	carriage, err := json.Marshal(map[string]any{
		"device_id": "site-a-edge-01", "artifact_type": "commissioning_authorization",
		"artifact_b64": base64.StdEncoding.EncodeToString([]byte(`{"v":1,"device_id":"site-a-edge-01","actor":"x"}`)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := handler.Handle(context.Background(), "ori/site-a-edge-01/evidence/outbound", carriage); err != nil {
		t.Fatal(err)
	}
	if queue.Len() != 0 {
		t.Fatal("a commissioning authorisation was queued; the device never holds one")
	}
	var ack outboundAck
	if len(published) != 1 {
		t.Fatalf("published %d messages, want one refusal", len(published))
	}
	if err := json.Unmarshal(published[0].payload, &ack); err != nil {
		t.Fatal(err)
	}
	if ack.Outcome != "refused" || ack.Reason != "malformed" {
		t.Fatalf("commissioning authorisation was answered %+v, want refused/malformed", ack)
	}
}
