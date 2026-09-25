// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package evidence

import (
	"errors"
	"sort"
	"sync"
)

// Courier-level faults, the closed vocabulary gateway-api/v1 projects in
// evidence_delivery.faults.
const (
	// FaultStoreUnavailable: the courier cannot read, write, or atomically
	// update its durable store.
	FaultStoreUnavailable = "store_unavailable"
	// FaultAdmissionFailed: the courier failed to admit evidence it is
	// required to retain.
	FaultAdmissionFailed = "admission_failed"
	// FaultDeliveryImpaired: the courier knows some device's delivery is not
	// clean but cannot enumerate it in devices. This gateway always projects
	// devices from its per-device-and-lane state, so it never needs to raise it.
	FaultDeliveryImpaired = "delivery_impaired"
	// FaultDeviceUnconfigured: an outbound carriage was observed for a device
	// that is not configured. The device ID is never projected.
	FaultDeviceUnconfigured = "device_unconfigured"
)

var errUnconfiguredDevice = errors.New("evidence: carriage for an unconfigured device")

// ErrUnconfiguredDevice is returned for a carriage whose topic names a device
// that is not configured; nothing was admitted or acknowledged.
var ErrUnconfiguredDevice = errUnconfiguredDevice

// FaultRecorder holds the courier's active faults. A fault is raised by the
// boundary that observes the failure and stays active until the same boundary
// next succeeds, so a fault reports a condition, not an event: one failure
// followed by quiet stays visible, and it clears only when the condition does.
// Each boundary is a source; a fault is active while any source holds it.
type FaultRecorder struct {
	mu     sync.Mutex
	active map[string]map[string]bool // fault -> source -> active
}

func NewFaultRecorder() *FaultRecorder {
	return &FaultRecorder{active: make(map[string]map[string]bool)}
}

// Note raises fault for source when err is non-nil and clears it when nil.
func (r *FaultRecorder) Note(fault, source string, err error) {
	if r == nil {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	sources := r.active[fault]
	if err != nil {
		if sources == nil {
			sources = make(map[string]bool)
			r.active[fault] = sources
		}
		sources[source] = true
		return
	}
	delete(sources, source)
	if len(sources) == 0 {
		delete(r.active, fault)
	}
}

// Active lists the active faults, sorted and without repeats; it is empty,
// never nil, when none is active.
func (r *FaultRecorder) Active() []string {
	out := []string{}
	if r == nil {
		return out
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	for fault := range r.active {
		out = append(out, fault)
	}
	sort.Strings(out)
	return out
}
