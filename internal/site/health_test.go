// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"encoding/json"
	"strings"
	"testing"
	"time"
)

func healthyGateway() GatewayView {
	return GatewayView{
		Status:       SiteStatusHealthy,
		ProviderName: "test-provider",
		UptimeS:      120,
	}
}

func TestProjectSiteHealthAllNodesHealthy(t *testing.T) {
	reg := NewRegistry()
	nowMS := int64(10000)
	ttlMS := int64(5000)

	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: nowMS - 100})
	reg.Upsert(NodeHeartbeat{DeviceID: "node-b", Status: NodeStatusHealthy, LastSeenMS: nowMS - 200})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-a", "node-b"},
		NodeTTLMS:         ttlMS,
	})
	h := p.Project(time.UnixMilli(nowMS), healthyGateway())

	if h.Status != SiteStatusHealthy {
		t.Errorf("expected %q, got %q", SiteStatusHealthy, h.Status)
	}
	if len(h.Nodes) != 2 {
		t.Fatalf("expected 2 nodes, got %d", len(h.Nodes))
	}
	for _, n := range h.Nodes {
		if n.Status != SiteStatusHealthy {
			t.Errorf("node %q: expected healthy, got %q", n.DeviceID, n.Status)
		}
		if n.Stale {
			t.Errorf("node %q: expected not stale", n.DeviceID)
		}
		if !n.GatewayObserved {
			t.Errorf("node %q: expected gateway_observed=true", n.DeviceID)
		}
	}
	if h.GeneratedAtMS != nowMS {
		t.Errorf("expected generated_at_ms=%d, got %d", nowMS, h.GeneratedAtMS)
	}
}

func TestProjectSiteHealthStaleNode(t *testing.T) {
	reg := NewRegistry()
	const (
		nowMS = int64(10000)
		ttlMS = int64(1000)
	)

	reg.Upsert(NodeHeartbeat{DeviceID: "node-fresh", Status: NodeStatusHealthy, LastSeenMS: nowMS - 500})
	reg.Upsert(NodeHeartbeat{DeviceID: "node-stale", Status: NodeStatusHealthy, LastSeenMS: nowMS - ttlMS - 1})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-fresh", "node-stale"},
		NodeTTLMS:         ttlMS,
	})
	h := p.Project(time.UnixMilli(nowMS), healthyGateway())

	if h.Status != SiteStatusDegraded {
		t.Errorf("expected degraded, got %q", h.Status)
	}
	nodesByID := make(map[string]SiteNode, len(h.Nodes))
	for _, n := range h.Nodes {
		nodesByID[n.DeviceID] = n
	}
	if fresh := nodesByID["node-fresh"]; fresh.Stale {
		t.Error("node-fresh should not be stale")
	}
	if stale := nodesByID["node-stale"]; !stale.Stale {
		t.Error("node-stale should be stale")
	}
	if stale := nodesByID["node-stale"]; stale.Status != SiteStatusDegraded {
		t.Errorf("node-stale: expected degraded, got %q", stale.Status)
	}
}

func TestProjectSiteHealthStaleAtTTLBoundary(t *testing.T) {
	reg := NewRegistry()
	const (
		nowMS = int64(10000)
		ttlMS = int64(1000)
	)
	// Exactly at boundary: nowMS - ttlMS — should NOT be stale (mirrors EvictStale).
	reg.Upsert(NodeHeartbeat{DeviceID: "node-boundary", Status: NodeStatusHealthy, LastSeenMS: nowMS - ttlMS})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-boundary"},
		NodeTTLMS:         ttlMS,
	})
	h := p.Project(time.UnixMilli(nowMS), healthyGateway())

	if h.Status != SiteStatusHealthy {
		t.Errorf("node at TTL boundary should not be stale, got site status %q", h.Status)
	}
	if h.Nodes[0].Stale {
		t.Error("node at TTL boundary should not be stale")
	}
}

func TestProjectSiteHealthMissingNode(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-present", Status: NodeStatusHealthy, LastSeenMS: 9900})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-present", "node-missing"},
		NodeTTLMS:         5000,
	})
	h := p.Project(time.UnixMilli(10000), healthyGateway())

	if h.Status != SiteStatusDegraded {
		t.Errorf("expected degraded due to missing node, got %q", h.Status)
	}
	nodesByID := make(map[string]SiteNode, len(h.Nodes))
	for _, n := range h.Nodes {
		nodesByID[n.DeviceID] = n
	}
	missing := nodesByID["node-missing"]
	if missing.GatewayObserved {
		t.Error("missing node should have gateway_observed=false")
	}
	if !missing.Stale {
		t.Error("missing node should be stale")
	}
	if missing.Status != SiteStatusDegraded {
		t.Errorf("missing node: expected degraded, got %q", missing.Status)
	}
	if missing.LastSeenMS != 0 {
		t.Errorf("missing node: expected last_seen_ms=0, got %d", missing.LastSeenMS)
	}
}

func TestProjectSiteHealthGatewayDegradedStatus(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 9900})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-a"},
		NodeTTLMS:         5000,
	})

	for _, gwStatus := range []string{"degraded", "starting", ""} {
		gw := GatewayView{Status: gwStatus, ProviderName: "test"}
		h := p.Project(time.UnixMilli(10000), gw)
		if h.Status != SiteStatusDegraded {
			t.Errorf("gateway status %q: expected site degraded, got %q", gwStatus, h.Status)
		}
	}
}

func TestProjectSiteHealthEvidenceDeliveryFailureDegradesSite(t *testing.T) {
	reg := NewRegistry()
	p := NewProjector(reg, ProjectOptions{})
	gw := healthyGateway()
	gw.EvidenceDelivery = &GatewayEvidenceDeliveryView{
		Pending:         3,
		Degraded:        true,
		Blocked:         true,
		LastFailureAtMS: 10_000,
		LastError:       "channel_permanent_refusal",
	}

	h := p.Project(time.UnixMilli(10_000), gw)
	if h.Status != SiteStatusDegraded {
		t.Fatalf("blocked evidence delivery must degrade site, got %q", h.Status)
	}
	if h.Gateway.EvidenceDelivery == nil || !h.Gateway.EvidenceDelivery.Blocked ||
		h.Gateway.EvidenceDelivery.Pending != 3 {
		t.Fatalf("evidence delivery projection = %#v", h.Gateway.EvidenceDelivery)
	}
}

func TestProjectSiteHealthNodeDegradedStatus(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusDegraded, LastSeenMS: 9900})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-a"},
		NodeTTLMS:         5000,
	})
	h := p.Project(time.UnixMilli(10000), healthyGateway())

	if h.Status != SiteStatusDegraded {
		t.Errorf("expected degraded due to degraded node, got %q", h.Status)
	}
	if h.Nodes[0].Status != SiteStatusDegraded {
		t.Errorf("node: expected degraded, got %q", h.Nodes[0].Status)
	}
	if h.Nodes[0].Stale {
		t.Error("node is degraded but not stale")
	}
}

func TestProjectSiteHealthDisabledWebhookBridgeIsInert(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 9900})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-a"},
		NodeTTLMS:         5000,
	})
	gw := GatewayView{
		Status:               SiteStatusHealthy,
		ProviderName:         "test",
		WebhookBridgeEnabled: false,
		WebhookBridgeReady:   false,
	}
	h := p.Project(time.UnixMilli(10000), gw)

	if h.Status != SiteStatusHealthy {
		t.Errorf("disabled webhook bridge should not degrade site, got %q", h.Status)
	}
}

func TestProjectSiteHealthActiveTriggerCountNotStrings(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:       "node-a",
		Status:         NodeStatusHealthy,
		LastSeenMS:     9900,
		ActiveTriggers: []string{"trigger-x", "trigger-y"},
	})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-a"},
		NodeTTLMS:         5000,
	})
	h := p.Project(time.UnixMilli(10000), healthyGateway())

	if h.Nodes[0].ActiveTriggerCount != 2 {
		t.Errorf("expected active_trigger_count=2, got %d", h.Nodes[0].ActiveTriggerCount)
	}

	encoded, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	// The projection must not expose trigger name strings.
	if strings.Contains(string(encoded), "trigger-x") || strings.Contains(string(encoded), "trigger-y") {
		t.Errorf("SiteHealth JSON must not contain trigger name strings: %s", encoded)
	}
}

func TestProjectSiteHealthNoSecretsOrURLsInJSON(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{
		DeviceID:       "node-a",
		Status:         NodeStatusHealthy,
		LastSeenMS:     9900,
		ActiveTriggers: []string{"low_battery"},
	})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-a"},
		NodeTTLMS:         5000,
	})
	gw := GatewayView{
		Status:               SiteStatusHealthy,
		ProviderName:         "test-provider",
		UptimeS:              60,
		WebhookBridgeEnabled: true,
		WebhookBridgeReady:   true,
		EvidenceDelivery: &GatewayEvidenceDeliveryView{
			Pending: 2, Degraded: true, LastError: "channel_unavailable",
		},
	}
	h := p.Project(time.UnixMilli(10000), gw)

	encoded, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	js := string(encoded)

	forbidden := []string{
		"mqtt://", "mqtts://", "http://", "https://",
		"password", "secret", "token", "hmac",
		"/home/", "/var/", "/etc/",
		"+2", "07", "08", // phone number prefixes
	}
	for _, pattern := range forbidden {
		if strings.Contains(strings.ToLower(js), strings.ToLower(pattern)) {
			t.Errorf("SiteHealth JSON contains forbidden pattern %q: %s", pattern, js)
		}
	}
}

func TestProjectSiteHealthFutureDatedNodeIsStale(t *testing.T) {
	reg := NewRegistry()
	const nowMS = int64(5000)
	// future-dated: LastSeenMS > nowMS
	reg.Upsert(NodeHeartbeat{DeviceID: "node-future", Status: NodeStatusHealthy, LastSeenMS: nowMS + 1000})

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-future"},
		NodeTTLMS:         60000,
	})
	h := p.Project(time.UnixMilli(nowMS), healthyGateway())

	if h.Status != SiteStatusDegraded {
		t.Errorf("future-dated node should degrade site, got %q", h.Status)
	}
	if !h.Nodes[0].Stale {
		t.Error("future-dated node should be marked stale")
	}
}

func TestProjectSiteHealthNoExpectedNodes(t *testing.T) {
	reg := NewRegistry()

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{},
		NodeTTLMS:         5000,
	})
	h := p.Project(time.UnixMilli(10000), healthyGateway())

	if h.Status != SiteStatusHealthy {
		t.Errorf("empty expected set with healthy gateway: expected healthy, got %q", h.Status)
	}
	if len(h.Nodes) != 0 {
		t.Errorf("expected 0 nodes, got %d", len(h.Nodes))
	}
}

func TestProjectSiteHealthNodePosturePassedThrough(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 9900})
	reg.UpsertPosture("node-a", SiteNodePosture{
		BrokerHardening: &SiteNodeBrokerPosture{Available: true, RequiresACLHardening: true},
		Encryption:      &SiteNodeEncryptionPosture{Available: true, Satisfied: true},
	})

	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTLMS: 5000})
	h := p.Project(time.UnixMilli(10000), healthyGateway())

	if len(h.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(h.Nodes))
	}
	n := h.Nodes[0]
	if n.Posture == nil {
		t.Fatal("expected posture to be present in projected node")
	}
	if !n.Posture.BrokerHardening.Available || !n.Posture.BrokerHardening.RequiresACLHardening {
		t.Errorf("broker hardening posture not projected correctly: %#v", n.Posture.BrokerHardening)
	}
	if !n.Posture.Encryption.Satisfied {
		t.Errorf("encryption posture not projected correctly: %#v", n.Posture.Encryption)
	}
	if n.Posture.AlertOutbox != nil {
		t.Errorf("alert outbox should be nil when not set: %#v", n.Posture.AlertOutbox)
	}
}

func TestProjectSiteHealthNodeWithNoPostureOmitsField(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 9900})

	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTLMS: 5000})
	h := p.Project(time.UnixMilli(10000), healthyGateway())

	if len(h.Nodes) != 1 {
		t.Fatalf("expected 1 node, got %d", len(h.Nodes))
	}
	if h.Nodes[0].Posture != nil {
		t.Errorf("expected nil posture when not set, got %#v", h.Nodes[0].Posture)
	}

	encoded, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(encoded), `"posture"`) {
		t.Errorf("posture field should be omitted from JSON when nil: %s", encoded)
	}
}

func TestProjectSiteHealthPostureLockoutRiskLevelsNeverInOutput(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 9900})
	reg.UpsertPosture("node-a", SiteNodePosture{
		BrokerHardening: &SiteNodeBrokerPosture{Available: true},
	})

	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTLMS: 5000})
	h := p.Project(time.UnixMilli(10000), healthyGateway())

	encoded, err := json.Marshal(h)
	if err != nil {
		t.Fatal(err)
	}
	// LockoutRiskLevels keys are phone numbers — must never appear.
	forbidden := []string{"lockout_risk", "risk_level", "locked_out"}
	for _, pattern := range forbidden {
		if strings.Contains(strings.ToLower(string(encoded)), pattern) {
			t.Errorf("SiteHealth JSON must not contain %q (lockout risk data): %s", pattern, encoded)
		}
	}
}

func TestSiteHealthViewerInterface(t *testing.T) {
	reg := NewRegistry()
	// Projector must satisfy the Viewer interface.
	var _ Viewer = NewProjector(reg, ProjectOptions{})
}

// TestExportSeverityOnlyRaisesANode projects every heartbeat status beside
// every export severity: the node takes the more severe, an export that read
// critical is critical, one that read anything but ok is degraded, and no
// export severity lowers what the heartbeat reported.
func TestExportSeverityOnlyRaisesANode(t *testing.T) {
	nowMS := int64(10000)
	want := map[string]map[string]string{
		NodeStatusHealthy: {
			"": SiteStatusHealthy, ExportSeverityOK: SiteStatusHealthy, ExportSeverityDegraded: SiteStatusDegraded,
			ExportSeverityUnknown: SiteStatusDegraded, ExportSeverityCritical: SiteStatusCritical, "healthy": SiteStatusDegraded,
		},
		NodeStatusDegraded: {
			"": SiteStatusDegraded, ExportSeverityOK: SiteStatusDegraded, ExportSeverityDegraded: SiteStatusDegraded,
			ExportSeverityUnknown: SiteStatusDegraded, ExportSeverityCritical: SiteStatusCritical, "healthy": SiteStatusDegraded,
		},
		NodeStatusCritical: {
			"": SiteStatusCritical, ExportSeverityOK: SiteStatusCritical, ExportSeverityDegraded: SiteStatusCritical,
			ExportSeverityUnknown: SiteStatusCritical, ExportSeverityCritical: SiteStatusCritical, "healthy": SiteStatusCritical,
		},
	}
	for heartbeat, bySeverity := range want {
		for severity, status := range bySeverity {
			reg := NewRegistry()
			reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: heartbeat, LastSeenMS: nowMS - 100})
			reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: severity})
			h := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTLMS: 5000}).
				Project(time.UnixMilli(nowMS), healthyGateway())
			if h.Nodes[0].Status != status || h.Status != status {
				t.Errorf("heartbeat %q, export %q: node %q site %q, want %q", heartbeat, severity, h.Nodes[0].Status, h.Status, status)
			}
			if encoded, _ := json.Marshal(h); strings.Contains(string(encoded), "export_severity") || strings.Contains(string(encoded), "ExportSeverity") {
				t.Errorf("export severity projected as a member: %s", encoded)
			}
		}
	}
}

// TestExportSeverityOutlivesEveryHeartbeat records a critical export reading
// and then heartbeats, a posture fetch that carries no severity, and an
// eviction: the node stays critical through each, and only a later export
// reading changes it.
func TestExportSeverityOutlivesEveryHeartbeat(t *testing.T) {
	nowMS := int64(10000)
	reg := NewRegistry()
	project := func(at int64) SiteHealth {
		return NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTLMS: 5000}).
			Project(time.UnixMilli(at), healthyGateway())
	}
	expect := func(step string, at int64, want string) {
		t.Helper()
		if h := project(at); h.Nodes[0].Status != want || h.Status != want {
			t.Fatalf("%s: node %q site %q, want %q", step, h.Nodes[0].Status, h.Status, want)
		}
	}
	beat := func(at int64) {
		reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: at, GatewaySeen: at})
	}
	beat(nowMS)
	reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: ExportSeverityCritical})
	expect("critical export", nowMS, SiteStatusCritical)
	beat(nowMS + 1)
	expect("heartbeat after a critical export", nowMS+1, SiteStatusCritical)
	reg.UpsertPosture("node-a", SiteNodePosture{BrokerHardening: &SiteNodeBrokerPosture{Available: true}})
	expect("posture without a severity", nowMS+1, SiteStatusCritical)
	reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: ExportSeverityOK})
	beat(nowMS + 2)
	expect("a later ok export", nowMS+2, SiteStatusHealthy)

	reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: ExportSeverityUnknown})
	if evicted := reg.EvictStale(nowMS+100_000, 5000); len(evicted) != 1 {
		t.Fatalf("a silent node whose export read unknown should be evicted, got %v", evicted)
	}
	beat(nowMS + 100_001)
	expect("a heartbeat after eviction", nowMS+100_001, SiteStatusDegraded)
	if got := reg.Snapshot()[0].ExportSeverity; got != ExportSeverityUnknown {
		t.Fatalf("export severity after eviction %q, want unknown", got)
	}
}

// TestASilentNodeWhoseExportReadCriticalStaysCritical lets a node whose
// heartbeats were healthy but whose export read critical go silent: it is
// retained, reported once, and projected stale and critical.
func TestASilentNodeWhoseExportReadCriticalStaysCritical(t *testing.T) {
	reg := NewRegistry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 1000, GatewaySeen: 1000})
	reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: ExportSeverityCritical})
	evicted, retained := reg.EvictStaleRetainingCritical(100_000, 5000)
	if len(evicted) != 0 || len(retained) != 1 {
		t.Fatalf("evicted %v retained %v, want the node retained", evicted, retained)
	}
	if _, again := reg.EvictStaleRetainingCritical(200_000, 5000); len(again) != 0 {
		t.Fatalf("a silence is reported once, got %v", again)
	}
	h := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTLMS: 5000}).
		Project(time.UnixMilli(200_000), healthyGateway())
	if n := h.Nodes[0]; n.Status != SiteStatusCritical || !n.Stale || h.Status != SiteStatusCritical {
		t.Fatalf("silent node %+v, site %q; want stale and critical", n, h.Status)
	}
}
