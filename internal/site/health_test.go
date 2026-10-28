// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"encoding/json"
	"strings"
	"sync"
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
		NodeTTL:           time.Duration(ttlMS) * time.Millisecond,
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
	const (
		nowMS = int64(10000)
		ttlMS = int64(1000)
	)
	clock := newFakeReceiptClock(nowMS - ttlMS - 1)
	reg := clock.registry()
	// node-stale was last received past the TTL, so a last_seen_ms claiming
	// it is current freshens nothing; node-fresh was received within it.
	reg.Upsert(NodeHeartbeat{DeviceID: "node-stale", Status: NodeStatusHealthy, LastSeenMS: nowMS})
	clock.moveTo(nowMS - 500)
	reg.Upsert(NodeHeartbeat{DeviceID: "node-fresh", Status: NodeStatusHealthy, LastSeenMS: nowMS - 500})
	clock.moveTo(nowMS)

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-fresh", "node-stale"},
		NodeTTL:           time.Duration(ttlMS) * time.Millisecond,
	})
	h := p.Project(clock.Wall(), healthyGateway())

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
	const (
		nowMS = int64(10000)
		ttlMS = int64(1000)
	)
	// Received exactly the TTL ago: not stale, as EvictStale keeps it.
	clock := newFakeReceiptClock(nowMS - ttlMS)
	reg := clock.registry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-boundary", Status: NodeStatusHealthy, LastSeenMS: nowMS - ttlMS})
	clock.moveTo(nowMS)

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{"node-boundary"},
		NodeTTL:           time.Duration(ttlMS) * time.Millisecond,
	})
	h := p.Project(clock.Wall(), healthyGateway())

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
		NodeTTL:           5 * time.Second,
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
		NodeTTL:           5 * time.Second,
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
		NodeTTL:           5 * time.Second,
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
		NodeTTL:           5 * time.Second,
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
		NodeTTL:           5 * time.Second,
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
		NodeTTL:           5 * time.Second,
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

// A node's clock never decides staleness (#126): a node reporting with its
// clock ahead of the gateway's, or 200 s behind it, is not stale while its
// report advances, and silence past the TTL is stale whatever last_seen_ms
// claims.
func TestProjectSiteHealthAnOffsetNodeClockIsNotStale(t *testing.T) {
	const (
		nowMS = int64(1_800_000_000_000)
		ttlMS = int64(60000)
	)
	for name, offset := range map[string]int64{"2 s ahead": 2000, "200 s behind": -200_000} {
		t.Run(name, func(t *testing.T) {
			clock := newFakeReceiptClock(nowMS)
			reg := clock.registry()
			p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: time.Duration(ttlMS) * time.Millisecond})
			for range 5 {
				lastSeen := clock.Wall().UnixMilli() + offset
				reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: lastSeen})
				clock.advance(30 * time.Second)
				n := p.Project(clock.Wall(), healthyGateway()).Nodes[0]
				if n.Stale || !n.GatewayObserved || n.LastSeenMS != lastSeen {
					t.Fatalf("a reporting node: %+v, want not stale, observed, last_seen_ms as reported", n)
				}
			}
			clock.advance(time.Duration(ttlMS) * time.Millisecond)
			if n := p.Project(clock.Wall(), healthyGateway()).Nodes[0]; !n.Stale {
				t.Fatalf("a node silent past the TTL is not stale: %+v", n)
			}
		})
	}
}

// A runtime clock stepped back once moves again with its next heartbeat: a
// report advances when last_seen_ms passes the previous heartbeat's, not the
// highest ever seen.
func TestProjectSiteHealthASteppedBackNodeClockAdvancesAgain(t *testing.T) {
	const ttlMS = int64(5000)
	clock := newFakeReceiptClock(10_000)
	reg := clock.registry()
	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: time.Duration(ttlMS) * time.Millisecond})
	lastSeen := int64(1_000_000)
	for _, step := range []int64{1000, 1000, -3_600_000, 1000, 1000, 1000, 1000, 1000, 1000, 1000} {
		lastSeen += step
		reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: lastSeen})
		clock.advance(time.Second)
		if n := p.Project(clock.Wall(), healthyGateway()).Nodes[0]; n.Stale {
			t.Fatalf("a node whose clock was stepped back once reads stale while it advances again: %+v", n)
		}
	}
}

// A publisher that keeps sending while the node's own last_seen_ms stops
// advancing, a stuck main loop behind a live heartbeat, goes stale once its
// report is older than the TTL, though the gateway keeps receiving it.
func TestProjectSiteHealthAStuckReportGoesStaleWhileReceived(t *testing.T) {
	const ttlMS = int64(5000)
	clock := newFakeReceiptClock(10_000)
	reg := clock.registry()
	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: time.Duration(ttlMS) * time.Millisecond})
	for range 10 {
		reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 10_000})
		clock.advance(time.Second)
	}
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 10_000})
	n := p.Project(clock.Wall(), healthyGateway()).Nodes[0]
	if !n.Stale || !n.GatewayObserved || n.Status != SiteStatusDegraded {
		t.Fatalf("a stuck report received every second: %+v, want stale, observed and degraded", n)
	}
	if evicted := reg.EvictStale(time.Duration(ttlMS) * time.Millisecond); len(evicted) != 0 {
		t.Fatalf("a node the gateway is still receiving was evicted: %v", evicted)
	}
}

// Staleness runs on the monotonic receipt clock (#128): a forward wall step
// makes no reporting node stale, and a backward one hides no silence.
func TestProjectSiteHealthAWallClockStepNeitherStalesNorFreshens(t *testing.T) {
	const ttlMS = int64(5000)
	clock := newFakeReceiptClock(10_000)
	reg := clock.registry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 10_000})
	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: time.Duration(ttlMS) * time.Millisecond})

	clock.stepWall(time.Hour)
	if h := p.Project(clock.Wall(), healthyGateway()); h.Nodes[0].Stale {
		t.Fatal("a forward wall step made a reporting node stale")
	}
	clock.stepWall(-2 * time.Hour)
	clock.advance(time.Duration(ttlMS+1) * time.Millisecond)
	if h := p.Project(clock.Wall(), healthyGateway()); !h.Nodes[0].Stale {
		t.Fatal("a backward wall step hid a silence")
	}
}

func TestProjectSiteHealthNoExpectedNodes(t *testing.T) {
	reg := NewRegistry()

	p := NewProjector(reg, ProjectOptions{
		ExpectedDeviceIDs: []string{},
		NodeTTL:           5 * time.Second,
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

	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: 5 * time.Second})
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

	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: 5 * time.Second})
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

	p := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: 5 * time.Second})
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
			h := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: 5 * time.Second}).
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
	clock := newFakeReceiptClock(nowMS)
	reg := clock.registry()
	project := func(at int64) SiteHealth {
		clock.moveTo(at)
		return NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: 5 * time.Second}).
			Project(clock.Wall(), healthyGateway())
	}
	expect := func(step string, at int64, want string) {
		t.Helper()
		if h := project(at); h.Nodes[0].Status != want || h.Status != want {
			t.Fatalf("%s: node %q site %q, want %q", step, h.Nodes[0].Status, h.Status, want)
		}
	}
	beat := func(at int64) {
		clock.moveTo(at)
		reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: at})
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
	clock.moveTo(nowMS + 100_000)
	if evicted := reg.EvictStale(5 * time.Second); len(evicted) != 1 {
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
	clock := newFakeReceiptClock(1000)
	reg := clock.registry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 1000})
	reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: ExportSeverityCritical})
	clock.moveTo(100_000)
	evicted, retained := reg.EvictStaleRetainingCritical(5 * time.Second)
	if len(evicted) != 0 || len(retained) != 1 {
		t.Fatalf("evicted %v retained %v, want the node retained", evicted, retained)
	}
	clock.moveTo(200_000)
	if _, again := reg.EvictStaleRetainingCritical(5 * time.Second); len(again) != 0 {
		t.Fatalf("a silence is reported once, got %v", again)
	}
	h := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: 5 * time.Second}).
		Project(clock.Wall(), healthyGateway())
	if n := h.Nodes[0]; n.Status != SiteStatusCritical || !n.Stale || h.Status != SiteStatusCritical {
		t.Fatalf("silent node %+v, site %q; want stale and critical", n, h.Status)
	}
}

// TestALateExportReadingAfterEvictionIsKept records an ok export, evicts the
// node, and then records a critical export reading that arrives while the
// node is not registered, as a fetch that outlives its node's eviction does:
// the node's next heartbeat carries critical.
func TestALateExportReadingAfterEvictionIsKept(t *testing.T) {
	clock := newFakeReceiptClock(1000)
	reg := clock.registry()
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 1000})
	reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: ExportSeverityOK})
	clock.moveTo(100_000)
	if evicted := reg.EvictStale(5 * time.Second); len(evicted) != 1 {
		t.Fatalf("expected the node evicted, got %v", evicted)
	}
	reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: ExportSeverityCritical})
	if snapshot := reg.Snapshot(); len(snapshot) != 0 {
		t.Fatalf("a posture for an unregistered node registered it: %v", snapshot)
	}
	clock.moveTo(100_001)
	reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 100_001})
	h := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a"}, NodeTTL: 5 * time.Second}).
		Project(clock.Wall(), healthyGateway())
	if h.Nodes[0].Status != SiteStatusCritical || h.Status != SiteStatusCritical {
		t.Fatalf("node %q site %q after a late critical reading, want critical", h.Nodes[0].Status, h.Status)
	}
	if h.Nodes[0].Posture != nil {
		t.Fatalf("a posture recorded while the node was not registered was attached: %+v", h.Nodes[0].Posture)
	}
}

// TestAnUnobservedNodeKeepsItsRecordedExportSeverity evicts a node and then
// records a late export reading with no heartbeat after it: the unobserved
// node is critical when the reading is critical, and degraded otherwise, ok
// included, since it is still unobserved.
func TestAnUnobservedNodeKeepsItsRecordedExportSeverity(t *testing.T) {
	for severity, want := range map[string]string{
		ExportSeverityCritical: SiteStatusCritical,
		ExportSeverityUnknown:  SiteStatusDegraded,
		ExportSeverityDegraded: SiteStatusDegraded,
		ExportSeverityOK:       SiteStatusDegraded,
	} {
		clock := newFakeReceiptClock(1000)
		reg := clock.registry()
		reg.Upsert(NodeHeartbeat{DeviceID: "node-a", Status: NodeStatusHealthy, LastSeenMS: 1000})
		clock.moveTo(100_000)
		if evicted := reg.EvictStale(5 * time.Second); len(evicted) != 1 {
			t.Fatalf("expected the node evicted, got %v", evicted)
		}
		reg.UpsertPosture("node-a", SiteNodePosture{ExportSeverity: severity})
		clock.moveTo(100_001)
		h := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a", "node-b"}, NodeTTL: 5 * time.Second}).
			Project(clock.Wall(), healthyGateway())
		if n := h.Nodes[0]; n.Status != want || n.GatewayObserved || !n.Stale || h.Status != want {
			t.Errorf("late %s reading: node %+v site %q, want %q, unobserved and stale", severity, n, h.Status, want)
		}
		if n := h.Nodes[1]; n.Status != SiteStatusDegraded {
			t.Errorf("a device with no reading: %q, want degraded", n.Status)
		}
	}
}

// TestProjectionReadsSeverityUnderTheLock projects while export readings
// arrive for evicted nodes: the projection copies the recorded severities
// under the registry's lock. It guards under -race.
func TestProjectionReadsSeverityUnderTheLock(t *testing.T) {
	reg := NewRegistry()
	projector := NewProjector(reg, ProjectOptions{ExpectedDeviceIDs: []string{"node-a", "node-b"}, NodeTTL: 5 * time.Second})
	stop := make(chan struct{})
	var wg sync.WaitGroup
	// The writer runs for as long as the projector reads, so they overlap.
	wg.Go(func() {
		for i := 0; ; i++ {
			select {
			case <-stop:
				return
			default:
			}
			severity := ExportSeverityCritical
			if i%2 == 0 {
				severity = ExportSeverityOK
			}
			reg.UpsertPosture([]string{"node-a", "node-b"}[i%2], SiteNodePosture{ExportSeverity: severity})
		}
	})
	for range 2000 {
		projector.Project(time.UnixMilli(100_000), healthyGateway())
	}
	close(stop)
	wg.Wait()
}

// gateway-api/v2 Site Health: the TTL is 90 seconds by default and never
// outside 90 seconds to 24 hours. The gateway offers no setting, so its one
// value is the default.
func TestTheNodeTTLIsTheContractsNinetySeconds(t *testing.T) {
	if NodeTTL != 90*time.Second {
		t.Fatalf("NodeTTL = %v, want the contract's 90 s", NodeTTL)
	}
}
