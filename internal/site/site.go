// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"sync"
)

type NodeHeartbeat struct {
	DeviceID       string   `json:"device_id"`
	Status         string   `json:"status"`
	LastSeenMS     int64    `json:"last_seen_ms"`
	GatewaySeen    int64    `json:"gateway_seen_ms"`
	ActiveTriggers []string `json:"active_triggers"`
	// DegradationReasons is nil when the heartbeat omitted the field. It is
	// never normalised to an empty slice: absent and present-empty are
	// different states, and the latter is malformed.
	DegradationReasons []string         `json:"degradation_reasons,omitempty"`
	Posture            *SiteNodePosture `json:"posture,omitempty"`
	Evidence           *NodeEvidence    `json:"evidence,omitempty"`
	// ExportSeverity is the severity last read from the node's runtime health
	// export, kept by the registry across heartbeats and eviction: only a
	// later export reading changes it. It is not a projected member.
	ExportSeverity string `json:"-"`
}

type Registry struct {
	mu    sync.Mutex
	nodes map[string]NodeHeartbeat
	// evidence outlives individual heartbeats: it remembers recent chain
	// heads per device so truncation/rollback stays detectable and sticky.
	evidence map[string]*evidenceTrack
	// exportSeverity is the severity last read from each device's health
	// export. A heartbeat never carries one, so it outlives every heartbeat
	// and eviction, bounded like the registry by the configured devices.
	exportSeverity map[string]string
	// silentCritical holds the devices EvictStale has retained as critical
	// since their last heartbeat, so the retention is reported once.
	silentCritical map[string]struct{}
}

func NewRegistry() *Registry {
	return &Registry{
		nodes:          map[string]NodeHeartbeat{},
		evidence:       map[string]*evidenceTrack{},
		exportSeverity: map[string]string{},
		silentCritical: map[string]struct{}{},
	}
}

func (r *Registry) Upsert(heartbeat NodeHeartbeat) {
	if heartbeat.ActiveTriggers == nil {
		heartbeat.ActiveTriggers = []string{}
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	if heartbeat.Evidence != nil {
		track, ok := r.evidence[heartbeat.DeviceID]
		if !ok {
			track = &evidenceTrack{}
			r.evidence[heartbeat.DeviceID] = track
		}
		enriched := track.observe(*heartbeat.Evidence)
		heartbeat.Evidence = &enriched
	} else if track, ok := r.evidence[heartbeat.DeviceID]; ok {
		// Evidence omitted by a device with evidence history: keep the
		// evidence surface with Available=false so the projection degrades
		// instead of silently forgetting the chain.
		missing := track.observeMissing()
		heartbeat.Evidence = &missing
	}
	// A heartbeat replaces the node's record, posture included, but not the
	// severity its health export last read: a heartbeat says nothing about
	// it, so a critical export is never folded by the next heartbeat.
	heartbeat.ExportSeverity = r.exportSeverity[heartbeat.DeviceID]
	r.nodes[heartbeat.DeviceID] = heartbeat
	delete(r.silentCritical, heartbeat.DeviceID)
}

func (r *Registry) Snapshot() []NodeHeartbeat {
	r.mu.Lock()
	defer r.mu.Unlock()
	return r.snapshotLocked()
}

// snapshotLocked is Snapshot, with r.mu held.
func (r *Registry) snapshotLocked() []NodeHeartbeat {
	out := make([]NodeHeartbeat, 0, len(r.nodes))
	for _, hb := range r.nodes {
		// Every reference-type field must be copied here, not just the
		// pointers. A NodeHeartbeat value copy duplicates a slice header
		// and shares its backing array, so a caller mutating what it
		// received would reach back into registry state.
		hb.ActiveTriggers = cloneStrings(hb.ActiveTriggers)
		hb.DegradationReasons = cloneStrings(hb.DegradationReasons)
		hb.Posture = cloneSiteNodePosture(hb.Posture)
		if hb.Evidence != nil {
			e := *hb.Evidence
			hb.Evidence = &e
		}
		out = append(out, hb)
	}
	return out
}

// snapshotWithExportSeverity returns Snapshot and a copy of every recorded
// export severity, taken under one lock, so a projection reads both at the
// same moment, the severity of a device with no node included.
func (r *Registry) snapshotWithExportSeverity() ([]NodeHeartbeat, map[string]string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	// Copied under the lock with nodes, so a projection never pairs a node
	// set with a later severity. An explicit loop, not maps.Clone, so the
	// race detector sees the read (TestProjectionReadsSeverityUnderTheLock).
	// Only a reading is recorded, never an empty severity.
	severity := make(map[string]string, len(r.exportSeverity))
	for deviceID, value := range r.exportSeverity {
		if value != "" {
			severity[deviceID] = value
		}
	}
	return r.snapshotLocked(), severity
}

// cloneStrings copies a string slice, preserving nil. The nil/empty
// distinction is load-bearing for DegradationReasons — absent and
// present-empty are different states on the wire — and harmless for
// ActiveTriggers, which Upsert normalises to empty anyway.
func cloneStrings(in []string) []string {
	if in == nil {
		return nil
	}
	out := make([]string, len(in))
	copy(out, in)
	return out
}

// cloneSiteNodePosture returns a deep copy so callers cannot mutate registry state.
func cloneSiteNodePosture(p *SiteNodePosture) *SiteNodePosture {
	if p == nil {
		return nil
	}
	clone := &SiteNodePosture{ExportSeverity: p.ExportSeverity}
	if p.BrokerHardening != nil {
		b := *p.BrokerHardening
		clone.BrokerHardening = &b
	}
	if p.Encryption != nil {
		e := *p.Encryption
		clone.Encryption = &e
	}
	if p.AlertOutbox != nil {
		a := *p.AlertOutbox
		clone.AlertOutbox = &a
	}
	return clone
}

// UpsertPosture records the severity a health export read, when the posture
// carries one, which the device keeps until a later export reading, and
// updates the posture for an already-registered node. The severity is
// recorded even while the device is not registered, such as a fetch that
// returns after its node was evicted, so its next heartbeat carries it; the
// posture itself is not. The caller passes only configured devices, whose
// heartbeats alone reach the registry, so the record stays bounded by them.
func (r *Registry) UpsertPosture(deviceID string, posture SiteNodePosture) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if posture.ExportSeverity != "" {
		r.exportSeverity[deviceID] = posture.ExportSeverity
	}
	hb, ok := r.nodes[deviceID]
	if !ok {
		return
	}
	hb.ExportSeverity = r.exportSeverity[deviceID]
	hb.Posture = &posture
	r.nodes[deviceID] = hb
}

// EvictStale removes nodes where nowMS-lastSeenMS > ttlMS and returns them.
// A node exactly at the TTL boundary is retained. Future-dated heartbeats are
// evicted because clock-skewed nodes must not become immortal in the registry.
// A node whose last heartbeat or health export read critical is never
// evicted: see EvictStaleRetainingCritical.
func (r *Registry) EvictStale(nowMS, ttlMS int64) []NodeHeartbeat {
	evicted, _ := r.EvictStaleRetainingCritical(nowMS, ttlMS)
	return evicted
}

// EvictStaleRetainingCritical evicts as EvictStale does, except that a node
// whose last accepted heartbeat was critical, or whose health export last
// read critical, stays in the registry, stale and critical, until it reports
// again (and, for the export, until a later export reads otherwise): silence
// after critical may be the failure itself, and a projection must not fold
// critical. A critical node is silent
// when the gateway has accepted no heartbeat from it for longer than ttlMS,
// judged on GatewaySeen, the gateway's own receipt time. retained lists the
// nodes first found silent by this call, so the caller can report each silence
// once.
// The retention is bounded by the configured devices, the only ones whose
// heartbeats reach the registry; a decommissioned node is cleared by removing
// it from gateway.device_ids and restarting the gateway.
func (r *Registry) EvictStaleRetainingCritical(nowMS, ttlMS int64) (evicted, retained []NodeHeartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	evicted = make([]NodeHeartbeat, 0)
	for deviceID, heartbeat := range r.nodes {
		if heartbeat.Status == NodeStatusCritical || heartbeat.ExportSeverity == ExportSeverityCritical {
			// Silence is the gateway's observation: the time since it last
			// accepted a heartbeat, never the producer's last_seen_ms.
			if nowMS-heartbeat.GatewaySeen > ttlMS {
				if _, already := r.silentCritical[deviceID]; !already {
					r.silentCritical[deviceID] = struct{}{}
					retained = append(retained, heartbeat)
				}
			}
			continue
		}
		if heartbeat.LastSeenMS > nowMS || nowMS-heartbeat.LastSeenMS > ttlMS {
			evicted = append(evicted, heartbeat)
			delete(r.nodes, deviceID)
			delete(r.evidence, deviceID)
		}
	}
	return evicted, retained
}
