// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import "testing"

func TestRegistryUpsertAndSnapshot(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-1", Status: "ok", LastSeenMS: 10, GatewaySeen: 20})
	reg.Upsert(NodeHeartbeat{DeviceID: "node-1", Status: "degraded", LastSeenMS: 30, GatewaySeen: 40})

	snapshot := reg.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one node, got %d", len(snapshot))
	}
	if snapshot[0].Status != "degraded" {
		t.Fatalf("expected updated heartbeat, got %q", snapshot[0].Status)
	}
}
