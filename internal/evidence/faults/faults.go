// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package faults is the courier's closed fault vocabulary and the recorder
// that holds which faults are active.
package faults

import (
	"errors"
	"sort"
	"sync"
)

// Courier-level faults, the closed vocabulary gateway-evidence-carriage/v1 projects in
// evidence_delivery.faults.
const (
	// StoreUnavailable: the courier cannot read, write, or atomically
	// update its durable store.
	StoreUnavailable = "store_unavailable"
	// AdmissionFailed: the courier failed to admit evidence it is
	// required to retain.
	AdmissionFailed = "admission_failed"
	// DeliveryImpaired: the courier knows some device's delivery is not
	// clean but cannot enumerate it in devices. This gateway always projects
	// devices from its per-device-and-lane state, so it never needs to raise it.
	DeliveryImpaired = "delivery_impaired"
	// DeviceUnconfigured: an outbound carriage was observed for a device
	// that is not configured. The device ID is never projected.
	DeviceUnconfigured = "device_unconfigured"
)

// ErrUnconfiguredDevice is returned for a carriage whose topic names a device
// that is not configured; nothing was admitted or acknowledged.
var ErrUnconfiguredDevice = errors.New("evidence: carriage for an unconfigured device")

// Recorder holds the courier's active faults. A fault is raised by the
// boundary that observes the failure and stays active until the same boundary
// next succeeds, so a fault reports a condition, not an event: one failure
// followed by quiet stays visible, and it clears only when the condition does.
// Each boundary is a source; a fault is active while any source holds it.
type Recorder struct {
	mu     sync.Mutex
	active map[string]map[string]bool // fault -> source -> active
}

func NewRecorder() *Recorder {
	return &Recorder{active: make(map[string]map[string]bool)}
}

// Note raises fault for source when err is non-nil and clears it when nil.
func (r *Recorder) Note(fault, source string, err error) {
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
func (r *Recorder) Active() []string {
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
