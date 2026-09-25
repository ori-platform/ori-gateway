// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

const (
	// probeFileName is the fixed private file a store probe replaces. It is
	// never a queue record: the loader skips it, and it is never counted,
	// ordered or delivered.
	probeFileName = ".ori-evidence-probe"

	// unconfiguredFileName persists the devices whose carriage was observed
	// while they were not configured. It is private to the gateway and never
	// projected.
	unconfiguredFileName    = ".ori-evidence-unconfigured"
	unconfiguredFileVersion = 1
	// maxUnconfiguredDevices bounds the persisted set. Past it the fault is
	// still raised and kept, and further devices are counted as overflow,
	// which only an operator can clear by removing the file.
	maxUnconfiguredDevices = 256

	// devicesFaultSource is the fault source for device_unconfigured.
	devicesFaultSource = "devices"
)

// Probe checks the store without touching a queue record: it reads the
// directory, then atomically replaces the fixed probe file — a private temp
// file with fresh bytes, synced, renamed over the probe file, and the
// directory synced. A failure raises this store's store_unavailable and a
// success clears it: a successful probe is the only thing that clears it
// (gateway-api/v1). Neither affects admission_failed, and a failing archive
// move keeps its own source raised until the move completes.
func (q *DurableQueue) Probe() error {
	if q == nil {
		return fmt.Errorf("evidence: nil durable queue")
	}
	err := q.probe()
	q.faults.Note(FaultStoreUnavailable, q.faultSource, err)
	return err
}

func (q *DurableQueue) probe() error {
	if _, err := os.ReadDir(q.dir); err != nil {
		return fmt.Errorf("evidence: probe read: %w", err)
	}
	fresh := make([]byte, 16)
	if _, err := rand.Read(fresh); err != nil {
		return fmt.Errorf("evidence: probe bytes: %w", err)
	}
	body := []byte(fmt.Sprintf("%s %d\n", hex.EncodeToString(fresh), time.Now().UnixNano()))
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.writeFileAtomicRaw(probeFileName, body)
}

// ProbeLoop probes the store every interval until ctx ends. It runs
// independently of admissions and of health publication.
func (q *DurableQueue) ProbeLoop(ctx context.Context, interval time.Duration) error {
	if q == nil || interval <= 0 {
		return fmt.Errorf("evidence: store probe requires a queue and a positive interval")
	}
	ticker := time.NewTicker(interval)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return nil
		case <-ticker.C:
			_ = q.Probe()
		}
	}
}

// Configured reports whether device holds a reserved share of this queue. A
// queue opened without a device set reserves none and accepts every device.
func (q *DurableQueue) Configured(device string) bool {
	if q == nil {
		return false
	}
	return q.configured == nil || q.configured[device]
}

type unconfiguredRecord struct {
	V        int      `json:"v"`
	Devices  []string `json:"devices"`
	Overflow bool     `json:"overflow"`
}

// NoteUnconfiguredDevice records that a carriage was observed for a device
// that is not configured and raises device_unconfigured. The record is
// durable, so neither time, silence, a lost connection nor a restart clears
// it; only a later admission for that device, once configured, does.
func (q *DurableQueue) NoteUnconfiguredDevice(device string) error {
	if q == nil {
		return fmt.Errorf("evidence: nil durable queue")
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.faults.Note(FaultDeviceUnconfigured, devicesFaultSource, errUnconfiguredDevice)
	if q.unconfigured[device] {
		return nil
	}
	// A device outside the routing domain can never be admitted, so it is
	// counted as overflow rather than recorded; this also bounds the record.
	if len(q.unconfigured) >= maxUnconfiguredDevices || !validRoutingDeviceID(device) {
		if q.unconfiguredOverflow {
			return nil
		}
		q.unconfiguredOverflow = true
	} else {
		q.unconfigured[device] = true
	}
	return q.writeUnconfiguredLocked()
}

// NoteAdmittedDevice records a completed admission for device. When the device
// was observed unconfigured, that record is cleared, and the fault once none
// remains: a device that is now configured and admitted is no longer refused.
func (q *DurableQueue) NoteAdmittedDevice(device string) {
	if q == nil {
		return
	}
	q.mu.Lock()
	defer q.mu.Unlock()
	q.noteAdmittedDeviceLocked(device)
}

func (q *DurableQueue) noteAdmittedDeviceLocked(device string) {
	if !q.unconfigured[device] {
		return
	}
	delete(q.unconfigured, device)
	if err := q.writeUnconfiguredLocked(); err != nil {
		// Kept in memory as cleared; the durable record still names it, so a
		// restart raises the fault again rather than losing it.
		return
	}
	if len(q.unconfigured) == 0 && !q.unconfiguredOverflow {
		q.faults.Note(FaultDeviceUnconfigured, devicesFaultSource, nil)
	}
}

func (q *DurableQueue) writeUnconfiguredLocked() error {
	devices := make([]string, 0, len(q.unconfigured))
	for device := range q.unconfigured {
		devices = append(devices, device)
	}
	sort.Strings(devices)
	encoded, err := json.Marshal(unconfiguredRecord{V: unconfiguredFileVersion, Devices: devices, Overflow: q.unconfiguredOverflow})
	if err != nil {
		return err
	}
	if len(devices) == 0 && !q.unconfiguredOverflow {
		err := os.Remove(filepath.Join(q.dir, unconfiguredFileName))
		if err != nil && !os.IsNotExist(err) {
			q.noteStore(err)
			return err
		}
		err = syncDirectory(q.dir)
		q.noteStore(err)
		return err
	}
	return q.writeFileAtomic(unconfiguredFileName, encoded)
}

// loadUnconfigured restores the durable record and its fault.
func (q *DurableQueue) loadUnconfigured(present bool) error {
	if !present {
		return nil
	}
	raw, err := readPrivateSidecar(filepath.Join(q.dir, unconfiguredFileName), "unconfigured-device", "record")
	if err != nil {
		return err
	}
	var record unconfiguredRecord
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&record); err != nil || decoder.Decode(&struct{}{}) != io.EOF ||
		record.V != unconfiguredFileVersion || len(record.Devices) > maxUnconfiguredDevices {
		return fmt.Errorf("evidence: corrupt unconfigured-device record")
	}
	for _, device := range record.Devices {
		if !validRoutingDeviceID(device) {
			return fmt.Errorf("evidence: corrupt unconfigured-device record")
		}
		q.unconfigured[device] = true
	}
	q.unconfiguredOverflow = record.Overflow
	if len(q.unconfigured) > 0 || q.unconfiguredOverflow {
		q.faults.Note(FaultDeviceUnconfigured, devicesFaultSource, errUnconfiguredDevice)
	}
	return nil
}
