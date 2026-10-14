// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"slices"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/evidence/custody"
)

const otherRigDevice = "site-a-edge-02"

// recordedLanes records the devices whose lanes a handoff woke.
type recordedLanes struct {
	mu      sync.Mutex
	devices []string
}

func (l *recordedLanes) NotifyDevice(deviceID string) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.devices = append(l.devices, deviceID)
}

func (l *recordedLanes) woken() []string {
	l.mu.Lock()
	defer l.mu.Unlock()
	return slices.Clone(l.devices)
}

// rigIngress is the runtime ingress over the rig's return queue, wired to the
// rig's publisher as the gateway wires it: the custody return wakes the
// publisher, and an admission is the shared Handoff.
type rigIngress struct {
	*RuntimeIngress
	lanes *recordedLanes
	mu    sync.Mutex
	acks  []outboundAck
}

func (r *returnRig) ingress(c *Courier) *rigIngress {
	r.t.Helper()
	ri := &rigIngress{lanes: &recordedLanes{}}
	h, err := NewRuntimeIngress(c, r.sink, testAckClock(r.t, r.now),
		func(_ context.Context, _ string, _ byte, _ bool, payload []byte) error {
			var ack outboundAck
			if err := json.Unmarshal(payload, &ack); err != nil {
				r.t.Error(err)
			}
			ri.mu.Lock()
			ri.acks = append(ri.acks, ack)
			ri.mu.Unlock()
			return nil
		}, rigSecret, r.now)
	if err != nil {
		r.t.Fatal(err)
	}
	h.SetReturnNotifier(r.publisher.Notify)
	h.SetHandoff(Handoff(ri.lanes, r.publisher))
	ri.RuntimeIngress = h
	return ri
}

func (ri *rigIngress) published() []outboundAck {
	ri.mu.Lock()
	defer ri.mu.Unlock()
	return slices.Clone(ri.acks)
}

func rigCourier(t *testing.T, maxItems int, devices ...string) *Courier {
	t.Helper()
	q, err := OpenDurableQueue(QueueOptions{
		Directory: filepath.Join(t.TempDir(), "outbound"), MaxItems: maxItems, MaxBytes: 1 << 20,
		Devices: devices, Now: func() time.Time { return time.UnixMilli(1787000000900) },
	})
	if err != nil {
		t.Fatal(err)
	}
	signer, err := custody.NewSigner("published-test-custody-secret-with-enough-entropy-0123456789")
	if err != nil {
		t.Fatal(err)
	}
	c, err := NewCourier(q, signer)
	if err != nil {
		t.Fatal(err)
	}
	return c
}

func envelopeFor(device string, seq int) []byte {
	return []byte(`{"v":1,"device_id":"` + device + `","local_seq":` + strconv.Itoa(seq) + `,"signature":"ed25519:opaque"}`)
}

func carriageOf(device string, kind ArtifactType, artifact []byte) []byte {
	raw, _ := json.Marshal(map[string]string{
		"device_id": device, "artifact_type": string(kind),
		"artifact_b64": base64.StdEncoding.EncodeToString(artifact),
	})
	return raw
}

func outboundTopic(device string) string {
	return "ori/" + device + "/evidence/outbound"
}

// waitCarried waits for the loop to carry something and returns what it
// carried. The deadline is a liveness bound only.
func (r *returnRig) waitCarried() [][]byte {
	r.t.Helper()
	select {
	case payload := <-r.published:
		return append([][]byte{payload}, r.drain()...)
	case <-time.After(rigWait):
		r.t.Fatal("the return publisher carried nothing")
		return nil
	}
}

// TestAnotherDevicesHandoffNeverReleasesAHold holds edge-01's head, then
// edge-02 hands off a delivery envelope through the real ingress: its custody
// return is queued and wakes the publisher, and its admission is a handoff for
// edge-02 alone. Neither releases edge-01's hold. edge-01's own handoff does.
func TestAnotherDevicesHandoffNeverReleasesAHold(t *testing.T) {
	r := newReturnRig(t)
	in := r.ingress(rigCourier(t, 10, rigDevice, otherRigDevice))
	if err := r.handle(rigTopic, r.decision(rigDevice, "refused", "unknown_key", nil)); err != nil {
		t.Fatal(err)
	}
	r.held()

	before := r.sink.Len()
	carried := r.whilePaused(func() {
		r.advance()
		if err := in.Handle(context.Background(), outboundTopic(otherRigDevice),
			carriageOf(otherRigDevice, ArtifactDeliveryEnvelope, envelopeFor(otherRigDevice, 12))); err != nil {
			t.Fatal(err)
		}
	})
	if r.sink.Len() != before+1 {
		t.Fatal("edge-02's custody return was not queued")
	}
	if len(carried) != 0 {
		t.Fatalf("edge-02's handoff released edge-01's hold and carried %d envelopes", len(carried))
	}
	r.held()
	if got := in.lanes.woken(); !slices.Equal(got, []string{otherRigDevice}) {
		t.Fatalf("lanes woken = %v, want edge-02's alone", got)
	}

	r.advance()
	if err := in.Handle(context.Background(), outboundTopic(rigDevice),
		carriageOf(rigDevice, ArtifactCheckpoint, validCheckpointBytes())); err != nil {
		t.Fatal(err)
	}
	r.carriedAgain(r.waitCarried(), r.artifact)
	if got := in.lanes.woken(); !slices.Equal(got, []string{otherRigDevice, rigDevice}) {
		t.Fatalf("lanes woken = %v, want edge-02's then edge-01's", got)
	}
}

// TestAQueueWakeNeverReleasesAHold wakes the publisher as a queued return or a
// retirement does: the held head is not carried, on the wake or on the retry
// interval, until a receiver state change for its device.
func TestAQueueWakeNeverReleasesAHold(t *testing.T) {
	r := newReturnRig(t)
	if err := r.handle(rigTopic, r.decision(rigDevice, "refused", "unknown_key", nil)); err != nil {
		t.Fatal(err)
	}
	r.held()
	r.advance()
	r.publisher.Notify()
	r.waitIteration()
	if carried := r.drain(); len(carried) != 0 {
		t.Fatalf("a queue wake released the hold and carried %d envelopes", len(carried))
	}
	r.held()
	r.advance()
	r.publisher.ReceiverStateChanged(otherRigDevice)
	r.waitIteration()
	if carried := r.drain(); len(carried) != 0 {
		t.Fatalf("another device's state change released the hold and carried %d envelopes", len(carried))
	}
	r.held()
	r.carriedAgain(r.stateChange(), r.artifact)
}

// TestARefusedAdmissionIsNotAHandoff hands edge-01's carriages that the
// ingress refuses, each for its declared reason: none wakes a lane and none
// releases edge-01's held head.
func TestARefusedAdmissionIsNotAHandoff(t *testing.T) {
	full := func(t *testing.T) *Courier {
		c := rigCourier(t, 1)
		if _, err := c.Admit(ArtifactDeliveryEnvelope, envelopeFor(rigDevice, 1)); err != nil {
			t.Fatal(err)
		}
		return c
	}
	configured := func(t *testing.T) *Courier { return rigCourier(t, 10, rigDevice, otherRigDevice) }
	cases := map[string]struct {
		courier  func(*testing.T) *Courier
		topic    string
		carriage []byte
		// reason is the refusal acknowledged, or "" when the carriage is
		// refused with no acknowledgement.
		reason string
	}{
		"carriage for another device": {configured, outboundTopic(rigDevice),
			carriageOf(otherRigDevice, ArtifactDeliveryEnvelope, envelopeFor(otherRigDevice, 2)), "binding_mismatch"},
		"artifact for another device": {configured, outboundTopic(rigDevice),
			carriageOf(rigDevice, ArtifactDeliveryEnvelope, envelopeFor(otherRigDevice, 2)), "binding_mismatch"},
		"artifact with no device": {configured, outboundTopic(rigDevice),
			carriageOf(rigDevice, ArtifactDeliveryEnvelope, []byte(`{"v":1,"local_seq":2,"signature":"ed25519:opaque"}`)), "malformed"},
		"envelope with no sequence": {configured, outboundTopic(rigDevice),
			carriageOf(rigDevice, ArtifactDeliveryEnvelope, []byte(`{"v":1,"device_id":"`+rigDevice+`","signature":"ed25519:opaque"}`)), "malformed"},
		"full queue": {full, outboundTopic(rigDevice),
			carriageOf(rigDevice, ArtifactDeliveryEnvelope, envelopeFor(rigDevice, 2)), "queue_full"},
		"unconfigured device": {func(t *testing.T) *Courier { return rigCourier(t, 10, otherRigDevice) }, outboundTopic(rigDevice),
			carriageOf(rigDevice, ArtifactDeliveryEnvelope, envelopeFor(rigDevice, 2)), ""},
		"carriage that is not JSON": {configured, outboundTopic(rigDevice), []byte(`{"device_id":`), ""},
	}
	for name, c := range cases {
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			r := newReturnRig(t)
			in := r.ingress(c.courier(t))
			if err := r.handle(rigTopic, r.decision(rigDevice, "refused", "unknown_key", nil)); err != nil {
				t.Fatal(err)
			}
			r.held()
			returns := r.sink.Len()
			err := in.Handle(context.Background(), c.topic, c.carriage)
			acks := in.published()
			if c.reason == "" {
				if err == nil || len(acks) != 0 {
					t.Fatalf("Handle = %v with %d acknowledgements, want an unacknowledged refusal", err, len(acks))
				}
			} else if err != nil || len(acks) != 1 || acks[0].Outcome != "refused" || acks[0].Reason != c.reason {
				t.Fatalf("Handle = %v with acknowledgements %+v, want one refused %s", err, acks, c.reason)
			}
			if r.sink.Len() != returns {
				t.Fatal("a refused carriage queued a custody return")
			}
			if got := in.lanes.woken(); len(got) != 0 {
				t.Fatalf("a refused carriage woke lanes %v", got)
			}
			r.held()
			if r.heldID() != r.headID {
				t.Fatal("a refused carriage released the hold")
			}
		})
	}
}
