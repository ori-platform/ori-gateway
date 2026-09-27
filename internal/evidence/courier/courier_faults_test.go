// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"github.com/ori-platform/ori-gateway/internal/evidence/custody"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"github.com/ori-platform/ori-gateway/internal/evidence/signingclock"
	"log/slog"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type faultIngress struct {
	ingress *RuntimeIngress
	out     string
	ret     string
	q       *DurableQueue
	returns *DurableAuthoritySink
	faults  *faults.Recorder
	acks    []map[string]any
	// publishErr, when set, fails every acknowledgement publication.
	publishErr error
	published  []string
}

func newFaultIngress(t *testing.T) *faultIngress {
	t.Helper()
	return newFaultIngressFor(t, t.TempDir(), nil)
}

// newFaultIngressFor opens the stores under dir, reserving shares for devices
// when set, so a test can reopen them as a restart would.
func newFaultIngressFor(t *testing.T, dir string, devices []string) *faultIngress {
	t.Helper()
	f := &faultIngress{out: filepath.Join(dir, "out"), ret: filepath.Join(dir, "ret"), faults: faults.NewRecorder()}
	maxItems, maxBytes := 10, int64(1<<24)
	if len(devices) > 0 {
		maxItems, maxBytes = 10*len(devices), int64(1<<24)*int64(len(devices))
	}
	q, err := OpenDurableQueue(QueueOptions{Directory: f.out, MaxItems: maxItems, MaxBytes: maxBytes, Devices: devices, Faults: f.faults, FaultSource: "outbound"})
	if err != nil {
		t.Fatal(err)
	}
	f.q = q
	signer, err := custody.NewSigner("published-test-custody-secret-with-enough-entropy-0123456789")
	if err != nil {
		t.Fatal(err)
	}
	courier, err := NewCourier(q, signer)
	if err != nil {
		t.Fatal(err)
	}
	returns, err := OpenDurableAuthoritySink(QueueOptions{Directory: f.ret, MaxItems: 10, MaxBytes: 1 << 24, Faults: f.faults, FaultSource: "return"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Join(dir, "clock"), 0o700); err != nil {
		t.Fatal(err)
	}
	clock, err := signingclock.OpenAck(filepath.Join(dir, "clock"), nil)
	if err != nil {
		t.Fatal(err)
	}
	f.returns = returns
	f.ingress, err = NewRuntimeIngress(courier, returns, clock, func(_ context.Context, topic string, _ byte, _ bool, payload []byte) error {
		f.published = append(f.published, topic)
		if f.publishErr != nil {
			return f.publishErr
		}
		var ack map[string]any
		_ = json.Unmarshal(payload, &ack)
		f.acks = append(f.acks, ack)
		return nil
	}, "gateway-envelope-secret-with-at-least-32-bytes", nil)
	if err != nil {
		t.Fatal(err)
	}
	f.ingress.SetFaultRecorder(f.faults)
	return f
}

func (f *faultIngress) handoff(t *testing.T, kind string, artifact []byte) error {
	t.Helper()
	device := recordDevice(artifact)
	carriage, _ := json.Marshal(map[string]string{
		"device_id": device, "artifact_type": kind, "artifact_b64": base64.StdEncoding.EncodeToString(artifact),
	})
	return f.ingress.Handle(context.Background(), "ori/"+device+"/evidence/outbound", carriage)
}

func TestStoreFaultIsActiveUntilTheStoreRecovers(t *testing.T) {
	f := newFaultIngress(t)
	restore := unwritable(t, f.out)
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1)); err == nil {
		t.Fatal("admission into an unwritable store succeeded")
	}
	if got := f.faults.Active(); !reflect.DeepEqual(got, []string{faults.AdmissionFailed, faults.StoreUnavailable}) {
		t.Fatalf("faults = %v", got)
	}
	// Nothing else happening: the condition stays visible.
	time.Sleep(10 * time.Millisecond)
	if len(f.faults.Active()) != 2 {
		t.Fatal("a fault expired without its condition clearing")
	}
	restore()
	// A later successful admission clears admission_failed but never
	// store_unavailable: only a successful probe of the store clears it
	// (gateway-evidence-carriage/v1).
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 2)); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); !reflect.DeepEqual(got, []string{faults.StoreUnavailable}) {
		t.Fatalf("faults after a successful write = %v, want store_unavailable alone", got)
	}
	if err := f.q.Probe(); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); len(got) != 0 {
		t.Fatalf("faults after a successful probe = %v", got)
	}
}

// TestStoreFaultIsPerStore: each durable store raises and clears its own
// store_unavailable. A failure of one store is not cleared by a successful
// probe of the other, nor by successful writes to either.
func TestStoreFaultIsPerStore(t *testing.T) {
	f := newFaultIngress(t)
	restoreOut := unwritable(t, f.out)
	restoreRet := unwritable(t, f.ret)
	if err := f.q.Probe(); err == nil {
		t.Fatal("probe of an unwritable outbound store succeeded")
	}
	if err := f.returns.Probe(); err == nil {
		t.Fatal("probe of an unwritable return store succeeded")
	}
	restoreOut()
	restoreRet()
	// Successful writes to both stores: an envelope admission queues the
	// artifact and stages its custody return.
	if err := f.handoff(t, "delivery_envelope", deviceEnvelope("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); !reflect.DeepEqual(got, []string{faults.StoreUnavailable}) {
		t.Fatalf("faults after successful writes = %v", got)
	}
	if err := f.q.Probe(); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); !reflect.DeepEqual(got, []string{faults.StoreUnavailable}) {
		t.Fatalf("the outbound probe cleared the return store's fault: %v", got)
	}
	if err := f.returns.Probe(); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); len(got) != 0 {
		t.Fatalf("faults after both probes = %v", got)
	}
}

func TestQueueFullIsNotAnAdmissionFault(t *testing.T) {
	f := newFaultIngress(t)
	for i := 1; i <= 11; i++ {
		_ = f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", i))
	}
	last := f.acks[len(f.acks)-1]
	if last["reason"] != "queue_full" {
		t.Fatalf("last ack = %v", last)
	}
	if got := f.faults.Active(); len(got) != 0 {
		t.Fatalf("a full share raised %v", got)
	}
}

func TestCustodyStoreFailureIsAnAdmissionFault(t *testing.T) {
	f := newFaultIngress(t)
	restore := unwritable(t, f.ret)
	if err := f.handoff(t, "delivery_envelope", deviceEnvelope("dev-a", 1)); err == nil {
		t.Fatal("custody that could not be retained was reported admitted")
	}
	if got := f.faults.Active(); !reflect.DeepEqual(got, []string{faults.AdmissionFailed, faults.StoreUnavailable}) {
		t.Fatalf("faults = %v", got)
	}
	restore()
	if err := f.handoff(t, "delivery_envelope", deviceEnvelope("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); !reflect.DeepEqual(got, []string{faults.StoreUnavailable}) {
		t.Fatalf("faults after a completed admission = %v, want store_unavailable alone", got)
	}
	if err := f.returns.Probe(); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); len(got) != 0 {
		t.Fatalf("faults after the return store's probe = %v", got)
	}
}

func TestFaultDegradesWithoutBlocking(t *testing.T) {
	f := newFaultIngress(t)
	authority := newDeviceAuthority()
	channel, err := NewHTTPChannel(HTTPChannelOptions{
		Endpoint: fakeAuthorityURL, ClientID: "site-a-gateway",
		Secret: "evidence-ingest-secret-with-at-least-32-bytes", HTTPClient: &http.Client{Transport: authority},
	})
	if err != nil {
		t.Fatal(err)
	}
	worker, err := NewDeliveryWorker(f.q, channel, &fakeAuthoritySink{}, DeliveryWorkerOptions{
		RetryInterval: time.Hour, Faults: f.faults, Logger: slog.New(slog.DiscardHandler),
	})
	if err != nil {
		t.Fatal(err)
	}
	restore := unwritable(t, f.out)
	_ = f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1))
	status := worker.Status()
	if !status.Degraded || status.Blocked || len(status.Devices) != 0 ||
		!reflect.DeepEqual(status.Faults, []string{faults.AdmissionFailed, faults.StoreUnavailable}) {
		t.Fatalf("status = %#v", status)
	}
	restore()
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if err := f.q.Probe(); err != nil {
		t.Fatal(err)
	}
	if status := worker.Status(); status.Degraded || len(status.Faults) != 0 {
		t.Fatalf("status after recovery = %#v", status)
	}
	var nilRecorder *faults.Recorder
	if got := nilRecorder.Active(); got == nil || len(got) != 0 {
		t.Fatalf("a nil recorder reports %v", got)
	}
}

// TestAcceptanceOfAnUnknownVersionRetiresAndReturnsItsReceipt: an envelope and
// a receipt of a version the courier does not know are routed by their routing
// fields alone, on both hops.
func TestAcceptanceOfAnUnknownVersionRetiresAndReturnsItsReceipt(t *testing.T) {
	q := openTestQueue(t, 10, 1<<20)
	if _, err := q.Enqueue(ArtifactDeliveryEnvelope, []byte(`{"v":2,"device_id":"dev-a","local_seq":1,"new":true}`)); err != nil {
		t.Fatal(err)
	}
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(t.TempDir(), "ret"), MaxItems: 10, MaxBytes: 1 << 20})
	if err != nil {
		t.Fatal(err)
	}
	receipt := []byte(`{"v":3,"device_id":"dev-a","from_seq":1,"to_seq":1,"new":true}`)
	worker, err := NewDeliveryWorker(q, &fakeEvidenceChannel{result: DeliveryResult{
		Accepted: true, AuthorityArtifacts: []AuthorityArtifact{{Type: AuthorityDeliveryReceipt, Payload: receipt}},
	}}, sink, DeliveryWorkerOptions{})
	if err != nil {
		t.Fatal(err)
	}
	if delivered, err := worker.deliverHead(context.Background()); err != nil || !delivered {
		t.Fatalf("an unknown-version envelope was not retired: %v, %v", delivered, err)
	}
	if sink.Len() != 1 {
		t.Fatal("the unknown-version receipt was not staged for the runtime")
	}
	staged, _ := sink.queue.Peek()
	if device, kind, err := authorityQueueRouting(staged); err != nil || device != "dev-a" || kind != AuthorityDeliveryReceipt {
		t.Fatalf("return routing = %s %s %v", device, kind, err)
	}
}

// TestAVersionTheCourierDoesNotKnowIsAdmittedAndDelivered: courier opacity.
// A v2 artifact is admitted, sent, retained on 422 unrecognised_version, and
// sent fresh on the next handoff.
func TestAVersionTheCourierDoesNotKnowIsAdmittedAndDelivered(t *testing.T) {
	f := newFaultIngress(t)
	for _, tc := range []struct {
		kind     string
		artifact []byte
	}{
		{"checkpoint", []byte(`{"v":2,"device_id":"dev-a","high_water_seq":1,"new_field":{"x":1}}`)},
		{"checkpoint", []byte(`{"v":"two","device_id":"dev-a","high_water_seq":2}`)},
		{"checkpoint", []byte(`{"device_id":"dev-a","high_water_seq":3}`)},
		{"delivery_envelope", []byte(`{"v":7,"device_id":"dev-a","local_seq":4}`)},
	} {
		if err := f.handoff(t, tc.kind, tc.artifact); err != nil {
			t.Fatal(err)
		}
		if last := f.acks[len(f.acks)-1]; last["outcome"] != "queued" {
			t.Fatalf("%s was not admitted: %v", tc.artifact, last)
		}
	}
	// Routing fields that are missing or unparseable are still refused.
	for _, bad := range [][]byte{
		[]byte(`{"v":2,"high_water_seq":1}`),
		[]byte(`{"v":2,"device_id":7}`),
	} {
		carriage, _ := json.Marshal(map[string]string{
			"device_id": "dev-a", "artifact_type": "checkpoint", "artifact_b64": base64.StdEncoding.EncodeToString(bad),
		})
		_ = f.ingress.Handle(context.Background(), "ori/dev-a/evidence/outbound", carriage)
		if last := f.acks[len(f.acks)-1]; last["outcome"] != "refused" {
			t.Fatalf("%s without routing fields was admitted", bad)
		}
	}

	q := openTestQueue(t, 10, 1<<20)
	v2 := []byte(`{"v":2,"device_id":"dev-a","high_water_seq":1,"new_field":{"x":1}}`)
	if _, err := q.Enqueue(ArtifactCheckpoint, v2); err != nil {
		t.Fatal(err)
	}
	authority := newDeviceAuthority()
	authority.refuse("dev-a", &fakeRefusal{status: http.StatusUnprocessableEntity, reason: "unrecognised_version"})
	worker := deviceWorker(t, q, authority, nil, 10*time.Millisecond)
	startWorker(t, worker)
	waitFor(t, "the refusal", func() bool { e, ok := laneState(worker, "dev-a"); return ok && e.State == DeviceWaitingHandoff })
	time.Sleep(50 * time.Millisecond)
	if got := authority.count("dev-a"); got != 1 {
		t.Fatalf("unsupported version retried on a timer: %d attempts", got)
	}
	authority.refuse("dev-a", nil) // the authority is upgraded
	worker.NotifyDevice("dev-a")
	waitFor(t, "delivery after the upgrade", func() bool { return q.Len() == 0 })
	if got := authority.count("dev-a"); got != 2 {
		t.Fatalf("attempts = %d, want 2", got)
	}
}
