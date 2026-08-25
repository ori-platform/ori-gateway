// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"context"
	"encoding/base64"
	"encoding/json"
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
	handler, err := NewRuntimeIngress(
		courier,
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
	if len(published) != 2 {
		t.Fatalf("published %d messages, want queue ack and custody", len(published))
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
	if published[1].topic != "ori/site-a-edge-01/evidence/inbound" || published[1].qos != 1 || published[1].retained {
		t.Fatalf("wrong custody publish: %#v", published[1])
	}
	if _, err := verifier.VerifyJSON(
		published[1].payload, contracts.EvidenceInboundMessageType, "site-a-edge-01", "",
	); err != nil {
		t.Fatalf("custody transport envelope did not verify: %v", err)
	}
}

func TestRuntimeIngressRejectsTopicWrapperAndArtifactBindingMismatch(t *testing.T) {
	courier, _ := testCourier(t, 10)
	var published []publishedEvidenceMessage
	handler, err := NewRuntimeIngress(courier, func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
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
	handler, err := NewRuntimeIngress(courier, func(_ context.Context, topic string, qos byte, retained bool, payload []byte) error {
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
