// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func headHash(seed byte) string {
	return strings.Repeat(string([]byte{'a' + seed%6, '0' + seed%10}), 32)
}

func evidenceBeat(deviceID, head string, gaps int, available bool) NodeHeartbeat {
	return NodeHeartbeat{
		DeviceID:   deviceID,
		Status:     NodeStatusHealthy,
		LastSeenMS: 1234567890000,
		Evidence: &NodeEvidence{
			ChainHeadHash:       head,
			AttestationGapCount: gaps,
			Available:           available,
		},
	}
}

func TestRegistryEvidenceNormalAdvanceRaisesNoFlags(t *testing.T) {
	reg := NewRegistry()
	for seed := byte(0); seed < 5; seed++ {
		reg.Upsert(evidenceBeat("dev-01", headHash(seed), 0, true))
	}
	got := reg.Snapshot()[0].Evidence
	if got.TruncationSuspected || got.HeadRegressed {
		t.Fatalf("advancing chain must not raise flags: %#v", got)
	}
	if got.ChainHeadHash != headHash(4) {
		t.Fatalf("latest head not retained: %#v", got)
	}
}

func TestRegistryEvidenceEmptyHeadOnNewDeviceIsNormal(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(evidenceBeat("dev-01", "", 0, true))
	got := reg.Snapshot()[0].Evidence
	if got.TruncationSuspected {
		t.Fatalf("empty chain on a brand-new device is not truncation: %#v", got)
	}
}

func TestRegistryEvidenceDetectsTruncationAndStaysSticky(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
	reg.Upsert(evidenceBeat("dev-01", "", 0, true)) // chain reset underneath the runtime

	got := reg.Snapshot()[0].Evidence
	if !got.TruncationSuspected {
		t.Fatalf("non-empty -> empty head must flag truncation: %#v", got)
	}

	// A later normal-looking heartbeat must not clear suspicion.
	reg.Upsert(evidenceBeat("dev-01", headHash(2), 0, true))
	got = reg.Snapshot()[0].Evidence
	if !got.TruncationSuspected {
		t.Fatalf("truncation suspicion must be sticky: %#v", got)
	}
}

func TestRegistryEvidenceDetectsHeadRegression(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
	reg.Upsert(evidenceBeat("dev-01", headHash(2), 0, true))
	reg.Upsert(evidenceBeat("dev-01", headHash(3), 0, true))
	// The chain reports a head it had already advanced past: a rollback.
	reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))

	got := reg.Snapshot()[0].Evidence
	if !got.HeadRegressed {
		t.Fatalf("returning to an older head must flag regression: %#v", got)
	}

	// Sticky across subsequent normal advances.
	reg.Upsert(evidenceBeat("dev-01", headHash(4), 0, true))
	got = reg.Snapshot()[0].Evidence
	if !got.HeadRegressed {
		t.Fatalf("regression flag must be sticky: %#v", got)
	}
}

func TestRegistryEvidenceMemoryIsPerDeviceAndClearedOnEviction(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
	reg.Upsert(evidenceBeat("dev-02", headHash(1), 0, true))
	reg.Upsert(evidenceBeat("dev-01", "", 0, true))

	for _, hb := range reg.Snapshot() {
		if hb.DeviceID == "dev-02" && hb.Evidence.TruncationSuspected {
			t.Fatalf("dev-01 truncation must not leak to dev-02")
		}
	}

	// Eviction drops the evidence memory: a re-registered device starts fresh.
	reg.EvictStale(time.Now().UnixMilli()+10_000_000_000, 1)
	reg.Upsert(evidenceBeat("dev-01", "", 0, true))
	got := reg.Snapshot()[0].Evidence
	if got.TruncationSuspected {
		t.Fatalf("evicted device must not retain evidence memory: %#v", got)
	}
}

func TestRegistryEvidenceOmissionAfterHistoryMarksUnavailable(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
	// The next heartbeat omits the evidence block entirely: this must not
	// silently downgrade the node to "no evidence surface".
	reg.Upsert(NodeHeartbeat{
		DeviceID:   "dev-01",
		Status:     NodeStatusHealthy,
		LastSeenMS: 1234567890001,
	})

	got := reg.Snapshot()[0].Evidence
	if got == nil {
		t.Fatal("evidence surface must survive an omitted evidence block")
	}
	if got.Available {
		t.Fatalf("omitted evidence must report Available=false: %#v", got)
	}
	if got.ChainHeadHash != headHash(1) {
		t.Fatalf("last known head must be preserved: %#v", got)
	}

	// Evidence reporting resumes with an advanced head: available again,
	// still no integrity flags.
	reg.Upsert(evidenceBeat("dev-01", headHash(2), 0, true))
	got = reg.Snapshot()[0].Evidence
	if !got.Available || got.TruncationSuspected || got.HeadRegressed {
		t.Fatalf("resumed evidence must clear unavailability without flags: %#v", got)
	}
}

func TestRegistryEvidenceOmissionWithoutHistoryStaysNil(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:   "dev-01",
		Status:     NodeStatusHealthy,
		LastSeenMS: 1234567890000,
	})
	if reg.Snapshot()[0].Evidence != nil {
		t.Fatal("a device that never reported evidence must not grow an evidence surface")
	}
}

func TestRegistrySnapshotClonesEvidence(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
	first := reg.Snapshot()[0].Evidence
	first.TruncationSuspected = true
	if reg.Snapshot()[0].Evidence.TruncationSuspected {
		t.Fatal("snapshot must not expose mutable registry state")
	}
}

func TestRuntimeHeartbeatHandlerAcceptsAndValidatesEvidence(t *testing.T) {
	reg := NewRegistry()
	handler, err := NewRuntimeHeartbeatHandler(reg, RuntimeHeartbeatHandlerOptions{
		Now: func() time.Time { return time.UnixMilli(1234567895000) },
	})
	if err != nil {
		t.Fatal(err)
	}

	valid := runtimeHeartbeatPayload(t, contracts.RuntimeNodeHeartbeat{
		DeviceID:   "dev-01",
		Status:     NodeStatusHealthy,
		LastSeenMS: 1234567890000,
		Evidence: &contracts.RuntimeNodeHeartbeatEvidence{
			ChainHeadHash:       headHash(1),
			AttestationGapCount: 2,
			Available:           true,
		},
	})
	if err := handler.Handle("ori/dev-01/runtime/heartbeat", valid); err != nil {
		t.Fatal(err)
	}
	got := reg.Snapshot()[0].Evidence
	if got == nil || got.ChainHeadHash != headHash(1) || got.AttestationGapCount != 2 || !got.Available {
		t.Fatalf("evidence not propagated: %#v", got)
	}

	for name, evidence := range map[string]contracts.RuntimeNodeHeartbeatEvidence{
		"short hash":    {ChainHeadHash: "abc123"},
		"uppercase hex": {ChainHeadHash: strings.ToUpper(headHash(1))},
		"negative gaps": {ChainHeadHash: headHash(1), AttestationGapCount: -1},
	} {
		payload := runtimeHeartbeatPayload(t, contracts.RuntimeNodeHeartbeat{
			DeviceID:   "dev-01",
			Status:     NodeStatusHealthy,
			LastSeenMS: 1234567890000,
			Evidence:   &evidence,
		})
		if err := handler.Handle("ori/dev-01/runtime/heartbeat", payload); err == nil {
			t.Fatalf("%s: expected validation error", name)
		}
	}
}

func TestProjectorDegradesNodeOnEvidenceIntegrityFlags(t *testing.T) {
	cases := map[string]func(reg *Registry){
		"truncation": func(reg *Registry) {
			reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
			reg.Upsert(evidenceBeat("dev-01", "", 0, true))
		},
		"regression": func(reg *Registry) {
			reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
			reg.Upsert(evidenceBeat("dev-01", headHash(2), 0, true))
			reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
		},
		"unavailable": func(reg *Registry) {
			reg.Upsert(evidenceBeat("dev-01", "", 0, false))
		},
		"omitted after history": func(reg *Registry) {
			reg.Upsert(evidenceBeat("dev-01", headHash(1), 0, true))
			reg.Upsert(NodeHeartbeat{
				DeviceID:   "dev-01",
				Status:     NodeStatusHealthy,
				LastSeenMS: 1234567890001,
			})
		},
	}
	for name, drive := range cases {
		reg := NewRegistry()
		drive(reg)

		projector := NewProjector(reg, ProjectOptions{
			ExpectedDeviceIDs: []string{"dev-01"},
			NodeTTLMS:         10_000_000_000_000,
		})
		health := projector.Project(
			time.UnixMilli(1234567891000), GatewayView{Status: SiteStatusHealthy},
		)
		if health.Nodes[0].Status != SiteStatusDegraded {
			t.Fatalf("%s: evidence integrity problem must degrade the node", name)
		}
		if health.Status != SiteStatusDegraded {
			t.Fatalf("%s: degraded node must degrade the site", name)
		}
	}
}

func TestProjectorGapCountAloneStaysInformational(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(evidenceBeat("dev-01", headHash(1), 3, true))
	projector := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"dev-01"},
		NodeTTLMS:         10_000_000_000_000,
	})
	health := projector.Project(time.UnixMilli(1234567891000), GatewayView{Status: SiteStatusHealthy})
	node := health.Nodes[0]
	if node.Status != SiteStatusHealthy {
		t.Fatalf("gap count alone must not degrade the node: %#v", node)
	}
	if node.Evidence == nil || node.Evidence.AttestationGapCount != 3 {
		t.Fatalf("gap count must remain visible in the projection: %#v", node.Evidence)
	}
}
