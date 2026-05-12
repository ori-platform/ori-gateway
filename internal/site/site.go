// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import "sync"

type NodeHeartbeat struct {
	DeviceID    string `json:"device_id"`
	Status      string `json:"status"`
	LastSeenMS  int64  `json:"last_seen_ms"`
	GatewaySeen int64  `json:"gateway_seen_ms"`
}

type Registry struct {
	mu    sync.Mutex
	nodes map[string]NodeHeartbeat
}

func NewRegistry() *Registry {
	return &Registry{nodes: map[string]NodeHeartbeat{}}
}

func (r *Registry) Upsert(heartbeat NodeHeartbeat) {
	r.mu.Lock()
	defer r.mu.Unlock()
	r.nodes[heartbeat.DeviceID] = heartbeat
}

func (r *Registry) Snapshot() []NodeHeartbeat {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := make([]NodeHeartbeat, 0, len(r.nodes))
	for _, heartbeat := range r.nodes {
		out = append(out, heartbeat)
	}
	return out
}
