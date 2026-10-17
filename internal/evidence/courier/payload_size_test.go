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
	"log/slog"
	"net/http"
	"path/filepath"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"

	canonicaljson "github.com/ori-platform/ori-canonicaljson"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

const sizeTopInt = 9007199254740991

type sizeEnvelopeCase struct {
	Name     string `json:"name"`
	Topic    string `json:"topic"`
	Encoding string `json:"encoding"`
	Length   int    `json:"length"`
	SHA256   string `json:"sha256"`
	Verdict  string `json:"verdict"`
}

func sizeEnvelope(t *testing.T, name string) sizeEnvelopeCase {
	t.Helper()
	raw, err := specvectors.Read("gateway-api/vectors/payload-size-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Envelopes []sizeEnvelopeCase `json:"envelopes"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, c := range corpus.Envelopes {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the corpus has no %s envelope", name)
	return sizeEnvelopeCase{}
}

var hmacSignature = regexp.MustCompile(`"hmac-sha256:[0-9a-f]{64}"`)

// unsigned replaces the one HMAC in payload with the corpus's zero signature,
// so the bytes the gateway's own encoder produced can be compared with the
// corpus digest.
func unsigned(t *testing.T, payload []byte) []byte {
	t.Helper()
	if n := len(hmacSignature.FindAll(payload, -1)); n != 1 {
		t.Fatalf("payload carries %d signatures, want 1", n)
	}
	return hmacSignature.ReplaceAll(payload, []byte(`"hmac-sha256:`+strings.Repeat("0", 64)+`"`))
}

func digestOf(payload []byte) string {
	sum := sha256.Sum256(payload)
	return hex.EncodeToString(sum[:])
}

// TestRequiredRepliesFitTheFloor builds the outbound acknowledgement with the
// ingress's own encoder at the corpus's bounds and requires the corpus's exact
// payload, member for member, within the floor.
func TestRequiredRepliesFitTheFloor(t *testing.T) {
	want := sizeEnvelope(t, "evidence_outbound_ack_at_bound")
	encoded, err := encodeOutboundAck(outboundAck{
		DeviceID: strings.Repeat("a", 128), ArtifactType: "anchor_registration",
		ArtifactDigest: "sha256:" + strings.Repeat("0", 64), Outcome: "refused", Reason: "binding_mismatch",
		AcknowledgedAtMS: sizeTopInt,
	}, sizeTopInt, "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	canonical, err := canonicaljson.MarshalWire(unsigned(t, encoded))
	if err != nil {
		t.Fatal(err)
	}
	if len(encoded) != want.Length || digestOf(canonical) != want.SHA256 {
		t.Fatalf("acknowledgement is %d bytes, digest %s; the corpus has %d bytes, %s", len(encoded), digestOf(canonical), want.Length, want.SHA256)
	}
	if len(encoded) > contracts.PayloadFloor || want.Verdict != "admitted" {
		t.Fatalf("acknowledgement is %d bytes against the %d-byte floor", len(encoded), contracts.PayloadFloor)
	}
}

// TestInboundEvidenceIsPublishedCanonical builds the corpus's inbound message,
// an artifact of 1048576 canonical bytes, with the publisher's own encoder:
// the whole payload is canonical, the artifact occupies exactly its canonical
// bytes, and the payload is the corpus's, within 1114112. The escaped form of
// the same message would be refused.
func TestInboundEvidenceIsPublishedCanonical(t *testing.T) {
	want := sizeEnvelope(t, "evidence_inbound_canonical_at_bound")
	escaped := sizeEnvelope(t, "evidence_inbound_escaped_at_bound")
	pad := strings.Repeat("é", (contracts.MaxAuthorityArtifactBytes-len(`{"pad":""}`))/2)
	artifact := []byte(`{ "pad" : "` + pad + `" }`) // not canonical as staged
	canonicalArtifact, err := canonicaljson.MarshalWire(artifact)
	if err != nil || len(canonicalArtifact) != contracts.MaxAuthorityArtifactBytes {
		t.Fatalf("artifact canonical length %d: %v", len(canonicalArtifact), err)
	}
	payload, err := encodeInbound(strings.Repeat("a", 128), InboundCustodyAcknowledgement, artifact, sizeTopInt, "runtime-gateway-envelope-secret")
	if err != nil {
		t.Fatal(err)
	}
	recanonical, err := canonicaljson.MarshalWire(payload)
	if err != nil || !bytes.Equal(recanonical, payload) {
		t.Fatal("the published inbound message is not canonical JSON")
	}
	if !bytes.Contains(payload, append([]byte(`"artifact":`), canonicalArtifact...)) {
		t.Fatal("the artifact does not occupy exactly its canonical bytes")
	}
	if len(payload) != want.Length || digestOf(unsigned(t, payload)) != want.SHA256 || want.Verdict != "admitted" {
		t.Fatalf("inbound message is %d bytes, digest %s; the corpus has %d bytes, %s", len(payload), digestOf(unsigned(t, payload)), want.Length, want.SHA256)
	}
	if len(payload) > contracts.PayloadMaxima[contracts.FamilyEvidenceInbound] || escaped.Verdict != "payload_too_large" {
		t.Fatalf("inbound message is %d bytes over its maximum", len(payload))
	}
}

// authorityArtifactOfLength is an epoch confirmation for device whose
// canonical bytes are exactly length long.
func authorityArtifactOfLength(t *testing.T, device string, length int) []byte {
	t.Helper()
	base := fmt.Sprintf(`{"anchor_epoch_id":"epoch","device_id":%q,"pad":"","signature":"ed25519:authority","v":1}`, device)
	artifact := []byte(strings.Replace(base, `"pad":""`, `"pad":"`+strings.Repeat("a", length-len(base))+`"`, 1))
	canonical, err := canonicaljson.MarshalWire(artifact)
	if err != nil || len(canonical) != length {
		t.Fatalf("built %d canonical bytes, want %d: %v", len(canonical), length, err)
	}
	return artifact
}

func TestStagingAdmitsNoAuthorityArtifactOverTheMaximum(t *testing.T) {
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "ret"), MaxItems: 10, MaxBytes: 1 << 24})
	if err != nil {
		t.Fatal(err)
	}
	over := AuthorityArtifact{Type: AuthorityEpochConfirmation, DeviceID: "dev-a",
		Payload: authorityArtifactOfLength(t, "dev-a", contracts.MaxAuthorityArtifactBytes+1)}
	if err := sink.Store(context.Background(), over); !errors.Is(err, errAuthorityArtifactOverMaximum) || sink.Len() != 0 {
		t.Fatalf("Store = %v with %d staged, want the artifact refused and nothing staged", err, sink.Len())
	}
	// Whitespace a canonical form drops does not count against the bound.
	at := authorityArtifactOfLength(t, "dev-a", contracts.MaxAuthorityArtifactBytes)
	spaced := append([]byte("  "), at...)
	if err := sink.Store(context.Background(), AuthorityArtifact{Type: AuthorityEpochConfirmation, DeviceID: "dev-a", Payload: spaced}); err != nil || sink.Len() != 1 {
		t.Fatalf("Store at the maximum = %v with %d staged", err, sink.Len())
	}
}

// oversizeChannel answers dev-a's evidence lane with a receipt followed by
// an epoch confirmation over the inbound maximum, and every other artifact
// through the lane authority. The HTTP channel caps a response below that
// size, so the fake stands in for a channel that does not.
type oversizeChannel struct {
	inner    EvidenceChannel
	big      []byte
	mu       sync.Mutex
	oversize int
	other    map[LaneKey]int
}

func (c *oversizeChannel) Deliver(ctx context.Context, queued QueuedArtifact) (DeliveryResult, error) {
	device := artifactDeviceID(queued.Payload)
	if device == "dev-a" && queued.Type == ArtifactDeliveryEnvelope {
		c.mu.Lock()
		c.oversize++
		c.mu.Unlock()
		var routing struct {
			LocalSeq int `json:"local_seq"`
		}
		_ = json.Unmarshal(queued.Payload, &routing)
		return DeliveryResult{Accepted: true, AuthorityArtifacts: []AuthorityArtifact{
			{Type: AuthorityDeliveryReceipt, Payload: validReceiptBytes("dev-a", routing.LocalSeq, routing.LocalSeq)},
			{Type: AuthorityEpochConfirmation, Payload: c.big},
		}}, nil
	}
	c.mu.Lock()
	c.other[LaneKey{Device: device, Lane: laneOf(queued.Type)}]++
	c.mu.Unlock()
	return c.inner.Deliver(ctx, queued)
}

func laneOf(kind ArtifactType) Lane {
	if kind == ArtifactAnchorRegistration {
		return LaneRegistration
	}
	return LaneEvidence
}

// TestAnOversizeAuthorityArtifactHoldsOnlyItsLane delivers a response
// carrying a receipt and then an authority artifact over the inbound maximum.
// The attempt is an unrecognised outcome: the receipt staged before it stays
// staged, the oversized artifact is not, the envelope is retained and backed off, and
// delivery reads degraded. The device's registration lane and another
// device's envelopes deliver meanwhile.
func TestAnOversizeAuthorityArtifactHoldsOnlyItsLane(t *testing.T) {
	q := openTestQueue(t, 100, 1<<24)
	enqueueEnvelopes(t, q, "dev-a", 1, 1)
	if _, err := q.Enqueue(ArtifactAnchorRegistration, deviceRegistration("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	enqueueEnvelopes(t, q, "dev-b", 1, 3)
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "ret"), MaxItems: 100, MaxBytes: 1 << 24})
	if err != nil {
		t.Fatal(err)
	}
	inner, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: newLaneAuthority()},
	})
	if err != nil {
		t.Fatal(err)
	}
	channel := &oversizeChannel{inner: inner, other: map[LaneKey]int{},
		big: authorityArtifactOfLength(t, "dev-a", contracts.MaxAuthorityArtifactBytes+1)}
	worker, err := NewDeliveryWorker(q, channel, sink, DeliveryWorkerOptions{
		RetryInterval: time.Hour, MaxBackoff: 8 * time.Hour, BlockedReminderInterval: time.Hour, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	startWorker(t, worker)
	devB := LaneKey{Device: "dev-b", Lane: LaneEvidence}
	waitFor(t, "dev-b's envelopes and dev-a's registration to deliver", func() bool {
		return q.LenLane(devB) == 0 && q.LenLane(regLane) == 0
	})
	waitFor(t, "dev-a's evidence lane to back off", func() bool {
		e, ok := laneEntry(worker, evLane)
		return ok && e.State == DeviceBackingOff
	})
	if q.LenLane(evLane) != 1 {
		t.Fatal("the envelope whose response carried an oversized artifact was not retained")
	}
	head, _ := q.PeekLane(evLane)
	if _, ok := q.HoldFor(head.ID); ok {
		t.Fatal("an unrecognised outcome was held")
	}
	channel.mu.Lock()
	attempts := channel.oversize
	channel.mu.Unlock()
	if attempts != 1 {
		t.Fatalf("the oversize response was attempted %d times, want once before its back-off", attempts)
	}
	status := worker.Status()
	if !status.Degraded || status.Blocked || len(status.Devices) != 1 || status.Devices[0].Lane != LaneEvidence {
		t.Fatalf("status = %+v, want dev-a's evidence lane alone backing off, degraded", status)
	}
	// The receipt was staged, the oversized artifact was not; the registration
	// lane returned nothing to stage.
	staged := map[AuthorityArtifactType]int{}
	devAReceipt := false
	sink.queue.mu.Lock()
	defer sink.queue.mu.Unlock()
	for _, record := range sink.queue.order {
		switch sink.queue.entries[record].Type {
		case artifactDeliveryReceipt:
			staged[AuthorityDeliveryReceipt]++
			devAReceipt = devAReceipt || artifactDeviceID(sink.queue.entries[record].Payload) == "dev-a"
		case artifactEpochConfirmation:
			staged[AuthorityEpochConfirmation]++
		}
	}
	if staged[AuthorityDeliveryReceipt] != 4 || staged[AuthorityEpochConfirmation] != 0 {
		t.Fatalf("staged %v, want the four receipts and no oversized confirmation", staged)
	}
	if !devAReceipt {
		t.Fatal("dev-a's receipt, staged before the oversized artifact, was not kept")
	}
}

// TestAnAcknowledgementThatDecidesNothingAddsNoReplayState sends signed
// acknowledgements that name nothing the gateway holds: for an empty return
// queue, and, with a head queued, for another type, another digest and an
// artifact already retired. None adds a replay entry. A decision that applies
// does, so its replay is still refused.
func TestAnAcknowledgementThatDecidesNothingAddsNoReplayState(t *testing.T) {
	const secret = "runtime-gateway-envelope-secret"
	now := time.UnixMilli(1787000003000)
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "return"), MaxItems: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	p, err := NewReturnPublisher(sink, testAckClock(t, func() time.Time { return now }),
		func(context.Context, string, byte, bool, []byte) error { return nil }, secret, "", func() time.Time { return now }, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	replayEntries := p.verifier.ReplayEntries
	signedAt := now.UnixMilli()
	ack := func(kind, digest string) []byte {
		signedAt++
		a := inboundAck{DeviceID: "site-a-edge-01", ArtifactType: kind, ArtifactDigest: digest, Outcome: "applied", AcknowledgedAtMS: now.UnixMilli()}
		auth, err := mqttauth.Sign(a, contracts.EvidenceInboundAckMessageType, a.DeviceID, "", signedAt, secret)
		if err != nil {
			t.Fatal(err)
		}
		a.Auth = &auth
		raw, err := json.Marshal(a)
		if err != nil {
			t.Fatal(err)
		}
		return raw
	}
	const topic = "ori/site-a-edge-01/evidence/inbound/ack"
	other := "sha256:" + strings.Repeat("a", 64)
	if err := p.HandleAck(topic, ack("delivery_receipt", other)); err == nil || replayEntries() != 0 {
		t.Fatalf("empty queue: err=%v, %d replay entries; want refused, none", err, replayEntries())
	}
	receipt := validReceiptBytes("site-a-edge-01", 1, 1)
	if err := sink.Store(context.Background(), AuthorityArtifact{Type: AuthorityDeliveryReceipt, DeviceID: "site-a-edge-01", Payload: receipt}); err != nil {
		t.Fatal(err)
	}
	digest := "sha256:" + digestOf(canonicalBytes(t, receipt))
	for _, refused := range [][]byte{ack("epoch_confirmation", digest), ack("delivery_receipt", other)} {
		if err := p.HandleAck(topic, refused); err == nil || replayEntries() != 0 || sink.Len() != 1 {
			t.Fatalf("mismatched ack: err=%v, %d replay entries, %d queued", err, replayEntries(), sink.Len())
		}
	}
	applied := ack("delivery_receipt", digest)
	if err := p.HandleAck(topic, applied); err != nil || replayEntries() != 1 || sink.Len() != 0 {
		t.Fatalf("applied ack: err=%v, %d replay entries, %d queued; want retired and recorded", err, replayEntries(), sink.Len())
	}
	if err := p.HandleAck(topic, applied); err == nil || !strings.Contains(err.Error(), "replay") {
		t.Fatalf("a replayed decision = %v, want refused as a replay", err)
	}
	if err := p.HandleAck(topic, ack("delivery_receipt", digest)); err == nil || replayEntries() != 1 {
		t.Fatalf("ack for a retired artifact: err=%v, %d replay entries", err, replayEntries())
	}
}
