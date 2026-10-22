// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"
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

func TestEvictStaleRemovesANodeTheGatewayHasNotHeardFrom(t *testing.T) {
	clock := newFakeReceiptClock(10)
	reg := clock.registry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-stale", Status: "ok", LastSeenMS: 5})

	clock.advance(101 * time.Millisecond)
	evicted := reg.EvictStale(100 * time.Millisecond)
	if len(evicted) != 1 {
		t.Fatalf("expected one eviction, got %d", len(evicted))
	}
	want := NodeHeartbeat{DeviceID: "node-stale", Status: "ok", LastSeenMS: 5, GatewaySeen: 10, ActiveTriggers: []string{}}
	if !reflect.DeepEqual(evicted[0], want) {
		t.Fatalf("unexpected evicted heartbeat: %#v", evicted[0])
	}
	if snapshot := reg.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("expected empty registry, got %d nodes", len(snapshot))
	}
}

func TestEvictStaleKeepsFreshNode(t *testing.T) {
	clock := newFakeReceiptClock(960)
	reg := clock.registry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-fresh", Status: "ok", LastSeenMS: 950})

	clock.advance(40 * time.Millisecond)
	if evicted := reg.EvictStale(100 * time.Millisecond); len(evicted) != 0 {
		t.Fatalf("expected no evictions, got %d", len(evicted))
	}
	if snapshot := reg.Snapshot(); len(snapshot) != 1 {
		t.Fatalf("expected one node, got %d", len(snapshot))
	}
}

func TestEvictStaleAtTTLBoundary(t *testing.T) {
	const ttl = 100 * time.Millisecond
	clock := newFakeReceiptClock(1000)
	reg := clock.registry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-past-ttl", Status: "ok", LastSeenMS: 1000})
	clock.advance(time.Millisecond)
	reg.Upsert(NodeHeartbeat{DeviceID: "node-at-ttl", Status: "ok", LastSeenMS: 1001})

	clock.advance(ttl)
	evicted := reg.EvictStale(ttl)
	if len(evicted) != 1 || evicted[0].DeviceID != "node-past-ttl" {
		t.Fatalf("only the node past the TTL should be evicted, got %#v", evicted)
	}
	if snapshot := reg.Snapshot(); len(snapshot) != 1 || snapshot[0].DeviceID != "node-at-ttl" {
		t.Fatalf("the node exactly at the TTL should be retained, got %#v", snapshot)
	}
}

func TestNodeHeartbeatActiveTriggersJSON(t *testing.T) {
	reg := newFakeReceiptClock(20).registry()
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
	if string(encoded) != `{"device_id":"node-1","status":"ok","last_seen_ms":10,"gateway_seen_ms":20,"active_triggers":[]}` {
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

	for i := range workers {
		go func(id int) {
			defer wg.Done()
			reg.Upsert(NodeHeartbeat{
				DeviceID:   "node-1",
				Status:     "ok",
				LastSeenMS: int64(id),
			})
		}(i)
	}
	for range workers {
		go func() {
			defer wg.Done()
			_ = reg.Snapshot()
		}()
	}
	for range workers {
		go func() {
			defer wg.Done()
			_ = reg.EvictStale(100 * time.Millisecond)
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

// The node's own clock never decides whether it is still reporting
// (#126's acceptance criteria): a node reporting every interval is kept, with
// its clock far behind or a little ahead of the gateway's.
func TestAReportingNodeIsNeverEvictedWhateverItsClockSays(t *testing.T) {
	const (
		interval = 5 * time.Second
		ttl      = 3 * interval
	)
	for name, skew := range map[string]time.Duration{"200 s behind": -200 * time.Second, "2 s ahead": 2 * time.Second} {
		t.Run(name, func(t *testing.T) {
			clock := newFakeReceiptClock(1_800_000_000_000)
			reg := clock.registry()
			for range 20 {
				reg.Upsert(NodeHeartbeat{
					DeviceID: "node-1", Status: NodeStatusHealthy,
					LastSeenMS: clock.Wall().Add(skew).UnixMilli(),
				})
				clock.advance(interval)
				if evicted := reg.EvictStale(ttl); len(evicted) != 0 {
					t.Fatalf("a reporting node was evicted: %#v", evicted)
				}
			}
		})
	}
}

// A node that stops reporting is evicted at the TTL measured from its last
// receipt, whatever its last_seen_ms claimed.
func TestASilentNodeIsEvictedAtTheTTLFromItsLastReceipt(t *testing.T) {
	const ttl = 15 * time.Second
	clock := newFakeReceiptClock(1_800_000_000_000)
	reg := clock.registry()
	// A last_seen_ms far ahead of the gateway's clock buys no extra life.
	reg.Upsert(NodeHeartbeat{DeviceID: "node-1", Status: NodeStatusHealthy, LastSeenMS: clock.Wall().Add(time.Hour).UnixMilli()})

	clock.advance(ttl)
	if evicted := reg.EvictStale(ttl); len(evicted) != 0 {
		t.Fatalf("evicted at the TTL boundary: %#v", evicted)
	}
	clock.advance(time.Millisecond)
	if evicted := reg.EvictStale(ttl); len(evicted) != 1 {
		t.Fatalf("a node silent past the TTL from its last receipt was kept: %#v", evicted)
	}
}

// Receipt age runs on the monotonic clock (#128): a wall clock that steps
// forward makes no reporting node silent, and one that steps back hides no
// silence.
func TestAWallClockStepNeitherSilencesNorHidesANode(t *testing.T) {
	const ttl = 15 * time.Second
	t.Run("forward", func(t *testing.T) {
		clock := newFakeReceiptClock(1_800_000_000_000)
		reg := clock.registry()
		reg.Upsert(NodeHeartbeat{DeviceID: "node-1", Status: NodeStatusHealthy, LastSeenMS: 1})
		clock.stepWall(time.Hour)
		clock.advance(time.Second)
		if evicted := reg.EvictStale(ttl); len(evicted) != 0 {
			t.Fatalf("a forward wall step evicted a reporting node: %#v", evicted)
		}
	})
	t.Run("back", func(t *testing.T) {
		clock := newFakeReceiptClock(1_800_000_000_000)
		reg := clock.registry()
		reg.Upsert(NodeHeartbeat{DeviceID: "node-1", Status: NodeStatusHealthy, LastSeenMS: 1})
		clock.stepWall(-time.Hour)
		clock.advance(ttl + time.Millisecond)
		if evicted := reg.EvictStale(ttl); len(evicted) != 1 {
			t.Fatalf("a backward wall step hid a silence: %#v", evicted)
		}
	})
}
