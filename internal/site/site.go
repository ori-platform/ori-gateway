// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import "sync"

type NodeHeartbeat struct {
	DeviceID       string           `json:"device_id"`
	Status         string           `json:"status"`
	LastSeenMS     int64            `json:"last_seen_ms"`
	GatewaySeen    int64            `json:"gateway_seen_ms"`
	ActiveTriggers []string         `json:"active_triggers"`
	Posture        *SiteNodePosture `json:"posture,omitempty"`
	Evidence       *NodeEvidence    `json:"evidence,omitempty"`
}

type Registry struct {
	mu    sync.Mutex
	nodes map[string]NodeHeartbeat
	// evidence outlives individual heartbeats: it remembers recent chain
	// heads per device so truncation/rollback stays detectable and sticky.
	evidence map[string]*evidenceTrack
}

func NewRegistry() *Registry {
	return &Registry{
		nodes:    map[string]NodeHeartbeat{},
		evidence: map[string]*evidenceTrack{},
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
	r.nodes[heartbeat.DeviceID] = heartbeat
}

func (r *Registry) Snapshot() []NodeHeartbeat {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NodeHeartbeat, 0, len(r.nodes))
	for _, hb := range r.nodes {
		hb.Posture = cloneSiteNodePosture(hb.Posture)
		if hb.Evidence != nil {
			e := *hb.Evidence
			hb.Evidence = &e
		}
		out = append(out, hb)
	}
	return out
}

// cloneSiteNodePosture returns a deep copy so callers cannot mutate registry state.
func cloneSiteNodePosture(p *SiteNodePosture) *SiteNodePosture {
	if p == nil {
		return nil
	}
	clone := &SiteNodePosture{}
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

// UpsertPosture updates the posture for an already-registered node.
// It is a no-op if the device has not yet been registered via Upsert.
func (r *Registry) UpsertPosture(deviceID string, posture SiteNodePosture) {
	r.mu.Lock()
	defer r.mu.Unlock()
	hb, ok := r.nodes[deviceID]
	if !ok {
		return
	}
	hb.Posture = &posture
	r.nodes[deviceID] = hb
}

// EvictStale removes nodes where nowMS-lastSeenMS > ttlMS and returns them.
// A node exactly at the TTL boundary is retained. Future-dated heartbeats are
// evicted because clock-skewed nodes must not become immortal in the registry.
func (r *Registry) EvictStale(nowMS, ttlMS int64) []NodeHeartbeat {
	r.mu.Lock()
	defer r.mu.Unlock()
	evicted := make([]NodeHeartbeat, 0)
	for deviceID, heartbeat := range r.nodes {
		if heartbeat.LastSeenMS > nowMS || nowMS-heartbeat.LastSeenMS > ttlMS {
			evicted = append(evicted, heartbeat)
			delete(r.nodes, deviceID)
			delete(r.evidence, deviceID)
		}
	}
	return evicted
}
