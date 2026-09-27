// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package courier

import (
	"bytes"
	"context"
	"encoding/base64"
	"errors"
	"github.com/ori-platform/ori-gateway/internal/evidence/faults"
	"maps"
	"net/http"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"testing"
	"time"
)

func activeFault(f *faults.Recorder, fault string) bool {
	return slices.Contains(f.Active(), fault)
}

func runProbeLoop(t *testing.T, q *DurableQueue, interval time.Duration) {
	t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- q.ProbeLoop(ctx, interval) }()
	t.Cleanup(func() { cancel(); <-done })
}

// TestAnIdleStoreThatCannotBeWrittenFaultsWithinTheInterval: no admission and
// no handoff happens; the probe alone raises store_unavailable, although the
// directory still reads, and clears it when the store recovers.
func TestAnIdleStoreThatCannotBeWrittenFaultsWithinTheInterval(t *testing.T) {
	recorder := faults.NewRecorder()
	dir := filepath.Join(t.TempDir(), "out")
	q, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 24, Faults: recorder, FaultSource: "outbound"})
	if err != nil {
		t.Fatal(err)
	}
	if activeFault(recorder, faults.StoreUnavailable) {
		t.Fatal("a healthy store opened with store_unavailable")
	}
	runProbeLoop(t, q, 20*time.Millisecond)
	restore := unwritable(t, dir)
	if _, err := os.ReadDir(dir); err != nil {
		t.Fatalf("the directory must still read: %v", err)
	}
	waitFor(t, "the probe to raise store_unavailable", func() bool { return activeFault(recorder, faults.StoreUnavailable) })
	if activeFault(recorder, faults.AdmissionFailed) {
		t.Fatal("a probe raised admission_failed")
	}
	restore()
	waitFor(t, "the probe to clear store_unavailable", func() bool { return !activeFault(recorder, faults.StoreUnavailable) })
}

// TestAStoreIsProbedWhenItOpens: the first probe runs at open, before any
// interval elapses and without any admission.
func TestAStoreIsProbedWhenItOpens(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 24}); err != nil {
		t.Fatal(err)
	}
	unwritable(t, dir)
	recorder := faults.NewRecorder()
	if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 24, Faults: recorder, FaultSource: "outbound"}); err != nil {
		t.Fatalf("an unwritable store must still open and report: %v", err)
	}
	if !activeFault(recorder, faults.StoreUnavailable) {
		t.Fatal("opening an unwritable store raised no store_unavailable")
	}
}

// TestAStoreThatCannotBeReadFaultsAlthoughItWrites: the probe's read is a check
// of its own, not a precondition of the write.
func TestAStoreThatCannotBeReadFaultsAlthoughItWrites(t *testing.T) {
	recorder := faults.NewRecorder()
	dir := filepath.Join(t.TempDir(), "out")
	q, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 24, Faults: recorder, FaultSource: "outbound"})
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Chmod(dir, 0o300); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	if _, err := os.ReadDir(dir); err == nil {
		t.Skip("directory permissions do not stop reads here (running as root?)")
	}
	if err := q.Probe(); err == nil || !activeFault(recorder, faults.StoreUnavailable) {
		t.Fatalf("an unreadable store probed clean: %v", err)
	}
	if _, err := os.Stat(filepath.Join(dir, probeFileName)); err != nil {
		t.Fatalf("the probe file is missing: %v", err)
	}
}

type storeSnapshot struct {
	Files    map[string]string
	Len      int
	Bytes    int64
	NextSeq  int64
	Order    []string
	Holds    map[string]HoldRecord
	Backoffs map[string]BackoffRecord
	Items    map[string]int
	DevBytes map[string]int64
}

func snapshotStore(t *testing.T, q *DurableQueue) storeSnapshot {
	t.Helper()
	entries, err := os.ReadDir(q.dir)
	if err != nil {
		t.Fatal(err)
	}
	files := map[string]string{}
	for _, entry := range entries {
		if entry.Name() == probeFileName {
			continue
		}
		raw, err := os.ReadFile(filepath.Join(q.dir, entry.Name()))
		if err != nil {
			t.Fatal(err)
		}
		files[entry.Name()] = string(raw)
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	return storeSnapshot{
		Files: files, Len: len(q.entries), Bytes: q.bytes, NextSeq: q.nextSeq,
		Order: slices.Clone(q.order), Holds: cloneMap(q.holds), Backoffs: cloneMap(q.backoffs),
		Items: cloneMap(q.deviceItems), DevBytes: cloneMap(q.deviceBytes),
	}
}

func cloneMap[K comparable, V any](m map[K]V) map[K]V {
	out := make(map[K]V, len(m))
	maps.Copy(out, m)
	return out
}

// TestAProbeNeverTouchesAQueueRecord: records, length, charged bytes, order,
// holds, back-offs and sequence are identical after probes, in process and
// after a reopen that reads the probe file back.
func TestAProbeNeverTouchesAQueueRecord(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "out")
	open := func() *DurableQueue {
		q, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 20, MaxBytes: 1 << 24, Devices: []string{"dev-a", "dev-b"}})
		if err != nil {
			t.Fatal(err)
		}
		return q
	}
	q := open()
	for i := 1; i <= 3; i++ {
		for _, device := range []string{"dev-a", "dev-b"} {
			if _, err := q.Enqueue(ArtifactCheckpoint, deviceCheckpoint(device, i)); err != nil {
				t.Fatal(err)
			}
		}
	}
	head, _ := q.PeekDevice("dev-a")
	if err := q.Hold(HoldRecord{
		QueueRecord: head.ID, ArtifactDigest: payloadDigest(head.Payload), ArtifactType: head.Type,
		RefusalStatus: http.StatusConflict, Reason: "conflict", FirstHeldAtMS: 1787000000000,
	}); err != nil {
		t.Fatal(err)
	}
	other, _ := q.PeekDevice("dev-b")
	if err := q.SetBackoff(BackoffRecord{QueueRecord: other.ID, NotBeforeMS: 1787000000000, Attempts: 3}); err != nil {
		t.Fatal(err)
	}
	before := snapshotStore(t, q)
	for range 5 {
		if err := q.Probe(); err != nil {
			t.Fatal(err)
		}
	}
	if after := snapshotStore(t, q); !reflect.DeepEqual(before, after) {
		t.Fatalf("a probe changed the store:\nbefore %#v\nafter  %#v", before, after)
	}
	if _, err := os.Stat(filepath.Join(dir, probeFileName)); err != nil {
		t.Fatalf("no probe file was written: %v", err)
	}
	reopened := open()
	if after := snapshotStore(t, reopened); !reflect.DeepEqual(before, after) {
		t.Fatalf("a reopen after probes changed the store:\nbefore %#v\nafter  %#v", before, after)
	}
}

// TestTheTwoStoresAreProbedIndependently: one store's success never clears the
// other's store_unavailable.
func TestTheTwoStoresAreProbedIndependently(t *testing.T) {
	recorder := faults.NewRecorder()
	root := t.TempDir()
	out, err := OpenDurableQueue(QueueOptions{Directory: filepath.Join(root, "out"), MaxItems: 10, MaxBytes: 1 << 24, Faults: recorder, FaultSource: "outbound"})
	if err != nil {
		t.Fatal(err)
	}
	sink, err := OpenDurableAuthoritySink(QueueOptions{Directory: filepath.Join(root, "ret"), MaxItems: 10, MaxBytes: 1 << 24, Faults: recorder, FaultSource: "return"})
	if err != nil {
		t.Fatal(err)
	}
	restore := unwritable(t, filepath.Join(root, "ret"))
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- sink.ProbeLoop(ctx, 10*time.Millisecond) }()
	t.Cleanup(func() { cancel(); <-done })
	waitFor(t, "the return probe to fault", func() bool { return activeFault(recorder, faults.StoreUnavailable) })
	if err := out.Probe(); err != nil {
		t.Fatal(err)
	}
	if !activeFault(recorder, faults.StoreUnavailable) {
		t.Fatal("the outbound store's probe cleared the return store's fault")
	}
	restore()
	waitFor(t, "the return probe to clear", func() bool { return !activeFault(recorder, faults.StoreUnavailable) })
}

// TestAProbeNeverClearsAdmissionFailed: admission_failed clears only by a later
// completed admission of the same form.
func TestAProbeNeverClearsAdmissionFailed(t *testing.T) {
	f := newFaultIngress(t)
	restore := unwritable(t, f.out)
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1)); err == nil {
		t.Fatal("admission into an unwritable store succeeded")
	}
	restore()
	if err := f.q.Probe(); err != nil {
		t.Fatal(err)
	}
	if activeFault(f.faults, faults.StoreUnavailable) || !activeFault(f.faults, faults.AdmissionFailed) {
		t.Fatalf("after a clean probe, faults = %v; want admission_failed alone", f.faults.Active())
	}
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if got := f.faults.Active(); len(got) != 0 {
		t.Fatalf("faults after a completed admission = %v", got)
	}
}

// TestAnAcknowledgementFailureAfterAdmissionIsNotAdmissionFailed: the queueing
// is durable, so failing to publish its acknowledgement is not admission.
func TestAnAcknowledgementFailureAfterAdmissionIsNotAdmissionFailed(t *testing.T) {
	f := newFaultIngress(t)
	f.publishErr = errors.New("broker disconnected")
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1)); err == nil {
		t.Fatal("a failed acknowledgement was reported published")
	}
	if f.q.Len() != 1 {
		t.Fatal("the artifact was not durably admitted")
	}
	if got := f.faults.Active(); len(got) != 0 {
		t.Fatalf("an acknowledgement failure raised %v", got)
	}
}

// TestCustodyFormFaultClearsOnlyOnAnEnvelopeAdmission: a completed checkpoint
// admission is not the custody-staging form and does not clear it.
func TestCustodyFormFaultClearsOnlyOnAnEnvelopeAdmission(t *testing.T) {
	f := newFaultIngress(t)
	restore := unwritable(t, f.ret)
	if err := f.handoff(t, "delivery_envelope", deviceEnvelope("dev-a", 1)); err == nil {
		t.Fatal("custody that could not be staged was reported admitted")
	}
	restore()
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if !activeFault(f.faults, faults.AdmissionFailed) {
		t.Fatal("a checkpoint admission cleared the custody form's admission_failed")
	}
	if err := f.handoff(t, "delivery_envelope", deviceEnvelope("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	if activeFault(f.faults, faults.AdmissionFailed) {
		t.Fatal("a completed envelope admission left admission_failed active")
	}
}

// TestAnUnconfiguredDeviceIsRefusedBeforeAdmission: nothing is admitted,
// custodied or acknowledged, and device_unconfigured survives time and restart
// until that device, once configured, has a handoff admitted.
func TestAnUnconfiguredDeviceIsRefusedBeforeAdmission(t *testing.T) {
	dir := t.TempDir()
	f := newFaultIngressFor(t, dir, []string{"dev-a"})
	for _, tc := range []struct {
		kind     string
		artifact []byte
	}{
		{"checkpoint", deviceCheckpoint("dev-b", 1)},
		{"delivery_envelope", deviceEnvelope("dev-b", 1)},
	} {
		if err := f.handoff(t, tc.kind, tc.artifact); !errors.Is(err, faults.ErrUnconfiguredDevice) {
			t.Fatalf("%s for an unconfigured device = %v", tc.kind, err)
		}
	}
	// Rejected on the topic before the carriage is read: a mismatched or
	// malformed carriage on an unconfigured topic is not acknowledged either.
	for _, carriage := range [][]byte{
		[]byte(`{"device_id":"dev-a","artifact_type":"checkpoint","artifact_b64":"` + base64.StdEncoding.EncodeToString(deviceCheckpoint("dev-a", 9)) + `"}`),
		[]byte(`{"device_id":"dev-b","artifact_type":"checkpoint","artifact_b64":"` + base64.StdEncoding.EncodeToString([]byte(`{"v":1}`)) + `"}`),
	} {
		if err := f.ingress.Handle(context.Background(), "ori/dev-b/evidence/outbound", carriage); !errors.Is(err, faults.ErrUnconfiguredDevice) {
			t.Fatalf("carriage on an unconfigured topic = %v", err)
		}
	}
	if len(f.published) != 0 || f.q.Len() != 0 {
		t.Fatalf("published %v and queued %d for an unconfigured device", f.published, f.q.Len())
	}
	if got := f.faults.Active(); !reflect.DeepEqual(got, []string{faults.DeviceUnconfigured}) {
		t.Fatalf("faults = %v", got)
	}
	for _, fault := range f.faults.Active() {
		if fault != faults.DeviceUnconfigured {
			t.Fatalf("fault %q", fault)
		}
	}
	// A configured device's admission does not clear it, nor does time.
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-a", 1)); err != nil {
		t.Fatal(err)
	}
	time.Sleep(20 * time.Millisecond)
	if !activeFault(f.faults, faults.DeviceUnconfigured) {
		t.Fatal("device_unconfigured cleared without that device's admission")
	}
	// A restart with the device still unconfigured keeps it.
	f = newFaultIngressFor(t, dir, []string{"dev-a"})
	if !activeFault(f.faults, faults.DeviceUnconfigured) {
		t.Fatal("device_unconfigured did not survive a restart")
	}
	// Configured, but no handoff yet: still raised.
	f = newFaultIngressFor(t, dir, []string{"dev-a", "dev-b"})
	if !activeFault(f.faults, faults.DeviceUnconfigured) {
		t.Fatal("configuring the device alone cleared the fault")
	}
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-b", 1)); err != nil {
		t.Fatal(err)
	}
	if activeFault(f.faults, faults.DeviceUnconfigured) {
		t.Fatal("an admitted handoff for the now-configured device left the fault")
	}
	f = newFaultIngressFor(t, dir, []string{"dev-a", "dev-b"})
	if activeFault(f.faults, faults.DeviceUnconfigured) {
		t.Fatal("the cleared fault returned after a restart")
	}
	if _, err := os.Stat(filepath.Join(f.out, unconfiguredFileName)); !os.IsNotExist(err) {
		t.Fatalf("the cleared record remains: %v", err)
	}
}

// TestUnconfiguredDeviceOverflowIsNeverClearedByAdmission: past the bound, a
// device is not individually recorded, so no single admission clears it.
func TestUnconfiguredDeviceOverflowIsNeverClearedByAdmission(t *testing.T) {
	dir := t.TempDir()
	f := newFaultIngressFor(t, dir, []string{"dev-a"})
	for i := range maxUnconfiguredDevices + 1 {
		if err := f.q.NoteUnconfiguredDevice("dev-x" + string(rune('a'+i%26)) + string(rune('a'+i/26))); err != nil {
			t.Fatal(err)
		}
	}
	f = newFaultIngressFor(t, dir, []string{"dev-a", "dev-xaa"})
	if err := f.handoff(t, "checkpoint", deviceCheckpoint("dev-xaa", 1)); err != nil {
		t.Fatal(err)
	}
	for device := range f.q.unconfigured {
		f.q.NoteAdmittedDevice(device)
	}
	if !activeFault(f.faults, faults.DeviceUnconfigured) {
		t.Fatal("an overflowed record cleared by admission")
	}
}

// TestAnUnroutableUnconfiguredDeviceIsCountedNotRecorded: a device ID outside
// the routing domain can never be admitted, so it is overflow, which bounds the
// record's size.
func TestAnUnroutableUnconfiguredDeviceIsCountedNotRecorded(t *testing.T) {
	f := newFaultIngressFor(t, t.TempDir(), []string{"dev-a"})
	long := string(bytes.Repeat([]byte("d"), 129))
	if err := f.q.NoteUnconfiguredDevice(long); err != nil {
		t.Fatal(err)
	}
	if len(f.q.unconfigured) != 0 || !f.q.unconfiguredOverflow || !activeFault(f.faults, faults.DeviceUnconfigured) {
		t.Fatalf("recorded %d, overflow %v, faults %v", len(f.q.unconfigured), f.q.unconfiguredOverflow, f.faults.Active())
	}
}

// TestACorruptUnconfiguredRecordRefusesToOpen: the record is never guessed at.
func TestACorruptUnconfiguredRecordRefusesToOpen(t *testing.T) {
	for _, body := range []string{
		`{"v":2,"devices":["dev-b"],"overflow":false}`,
		`{"v":1,"devices":[""],"overflow":false}`,
		`{"v":1,"devices":["site a"],"overflow":false}`,
		`{"v":1,"devices":["dev-b"],"overflow":false,"extra":1}`,
		`{"v":1,"devices":["dev-b"],"overflow":false}{}`,
		`not json`,
	} {
		dir := filepath.Join(t.TempDir(), "out")
		if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 24}); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(dir, unconfiguredFileName), []byte(body), 0o600); err != nil {
			t.Fatal(err)
		}
		if _, err := OpenDurableQueue(QueueOptions{Directory: dir, MaxItems: 10, MaxBytes: 1 << 24}); err == nil {
			t.Fatalf("opened over %s", body)
		}
	}
}
