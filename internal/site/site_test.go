// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
)

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

func TestEvictStaleRemovesOldNode(t *testing.T) {
	reg := NewRegistry()
	want := NodeHeartbeat{
		DeviceID:       "node-stale",
		Status:         "ok",
		LastSeenMS:     0,
		GatewaySeen:    10,
		ActiveTriggers: []string{},
	}
	reg.Upsert(want)

	evicted := reg.EvictStale(1000, 100)
	if len(evicted) != 1 {
		t.Fatalf("expected one eviction, got %d", len(evicted))
	}
	if !reflect.DeepEqual(evicted[0], want) {
		t.Fatalf("unexpected evicted heartbeat: %#v", evicted[0])
	}
	if snapshot := reg.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("expected empty registry, got %d nodes", len(snapshot))
	}
}

func TestEvictStaleKeepsFreshNode(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:    "node-fresh",
		Status:      "ok",
		LastSeenMS:  950,
		GatewaySeen: 960,
	})

	evicted := reg.EvictStale(1000, 100)
	if len(evicted) != 0 {
		t.Fatalf("expected no evictions, got %d", len(evicted))
	}
	if snapshot := reg.Snapshot(); len(snapshot) != 1 {
		t.Fatalf("expected one node, got %d", len(snapshot))
	}
}

func TestEvictStaleAtTTLBoundary(t *testing.T) {
	const (
		nowMS = int64(1000)
		ttlMS = int64(100)
	)

	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:   "node-at-ttl",
		Status:     "ok",
		LastSeenMS: nowMS - ttlMS,
	})

	evicted := reg.EvictStale(nowMS, ttlMS)
	if len(evicted) != 0 {
		t.Fatalf("node exactly at TTL should be retained, got %d evictions", len(evicted))
	}

	reg.Upsert(NodeHeartbeat{
		DeviceID:   "node-past-ttl",
		Status:     "ok",
		LastSeenMS: nowMS - ttlMS - 1,
	})

	evicted = reg.EvictStale(nowMS, ttlMS)
	if len(evicted) != 1 {
		t.Fatalf("node past TTL should be evicted, got %d evictions", len(evicted))
	}
	if evicted[0].DeviceID != "node-past-ttl" {
		t.Fatalf("unexpected evicted device_id: %q", evicted[0].DeviceID)
	}
}

func TestNodeHeartbeatActiveTriggersJSON(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:   "node-1",
		Status:     "ok",
		LastSeenMS: 10,
	})

	snapshot := reg.Snapshot()
	if snapshot[0].ActiveTriggers == nil {
		t.Fatal("expected non-nil ActiveTriggers after upsert")
	}
	if len(snapshot[0].ActiveTriggers) != 0 {
		t.Fatalf("expected empty ActiveTriggers, got %#v", snapshot[0].ActiveTriggers)
	}

	encoded, err := json.Marshal(snapshot[0])
	if err != nil {
		t.Fatal(err)
	}
	if string(encoded) != `{"device_id":"node-1","status":"ok","last_seen_ms":10,"gateway_seen_ms":0,"active_triggers":[]}` {
		t.Fatalf("unexpected JSON: %s", encoded)
	}

	var raw map[string]json.RawMessage
	if err := json.Unmarshal(encoded, &raw); err != nil {
		t.Fatal(err)
	}
	if string(raw["active_triggers"]) != "[]" {
		t.Fatalf("expected active_triggers to marshal as [], got %s", raw["active_triggers"])
	}
}

func TestRegistryConcurrentAccess(t *testing.T) {
	reg := NewRegistry()
	const workers = 8
	var wg sync.WaitGroup
	wg.Add(workers * 3)

	for i := 0; i < workers; i++ {
		go func(id int) {
			defer wg.Done()
			reg.Upsert(NodeHeartbeat{
				DeviceID:    "node-1",
				Status:      "ok",
				LastSeenMS:  int64(id),
				GatewaySeen: int64(id + 1),
			})
		}(i)
	}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_ = reg.Snapshot()
		}()
	}
	for i := 0; i < workers; i++ {
		go func() {
			defer wg.Done()
			_ = reg.EvictStale(1000, 100)
		}()
	}

	wg.Wait()
}

func TestUpsertPostureUpdatesExistingNode(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-1", Status: "ok", LastSeenMS: 100})

	posture := SiteNodePosture{
		BrokerHardening: &SiteNodeBrokerPosture{Available: true, RequiresACLHardening: true},
	}
	reg.UpsertPosture("node-1", posture)

	snapshot := reg.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one node, got %d", len(snapshot))
	}
	if snapshot[0].Posture == nil {
		t.Fatal("expected posture to be set")
	}
	if !snapshot[0].Posture.BrokerHardening.Available {
		t.Error("broker hardening available should be true")
	}
	if !snapshot[0].Posture.BrokerHardening.RequiresACLHardening {
		t.Error("requires_acl_hardening should be true")
	}
}

func TestUpsertPostureIsNoOpForUnregisteredDevice(t *testing.T) {
	reg := NewRegistry()
	reg.UpsertPosture("ghost", SiteNodePosture{
		BrokerHardening: &SiteNodeBrokerPosture{Available: true},
	})
	if snapshot := reg.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("UpsertPosture for unregistered device should not insert, got %d nodes", len(snapshot))
	}
}

func TestUpsertPostureDoesNotClearOtherFields(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:       "node-1",
		Status:         "healthy",
		LastSeenMS:     999,
		ActiveTriggers: []string{"grid_low"},
	})
	reg.UpsertPosture("node-1", SiteNodePosture{
		Encryption: &SiteNodeEncryptionPosture{Satisfied: true},
	})

	snapshot := reg.Snapshot()
	if len(snapshot) != 1 {
		t.Fatalf("expected one node, got %d", len(snapshot))
	}
	n := snapshot[0]
	if n.Status != "healthy" || n.LastSeenMS != 999 || len(n.ActiveTriggers) != 1 {
		t.Fatalf("UpsertPosture must not clear other fields: %#v", n)
	}
	if n.Posture == nil || n.Posture.Encryption == nil || !n.Posture.Encryption.Satisfied {
		t.Fatalf("posture encryption not set: %#v", n.Posture)
	}
}

func TestSnapshotPostureIsDeepCopied(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-1", Status: "ok", LastSeenMS: 100})
	reg.UpsertPosture("node-1", SiteNodePosture{
		BrokerHardening: &SiteNodeBrokerPosture{Available: true},
	})

	snap := reg.Snapshot()
	if snap[0].Posture == nil || snap[0].Posture.BrokerHardening == nil {
		t.Fatal("expected posture in snapshot")
	}
	// Mutate through the snapshot pointer — must not change registry state.
	snap[0].Posture.BrokerHardening.Available = false

	snap2 := reg.Snapshot()
	if !snap2[0].Posture.BrokerHardening.Available {
		t.Error("snapshot mutation leaked into registry: registry posture was changed")
	}
}

func TestEvictStaleRemovesFutureDatedNode(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:   "node-future",
		Status:     "healthy",
		LastSeenMS: 2000,
	})

	evicted := reg.EvictStale(1000, 100)
	if len(evicted) != 1 {
		t.Fatalf("future-dated node should be evicted, got %d evictions", len(evicted))
	}
	if evicted[0].DeviceID != "node-future" {
		t.Fatalf("unexpected evicted device_id: %q", evicted[0].DeviceID)
	}
}
