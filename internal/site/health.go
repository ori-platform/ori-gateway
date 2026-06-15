// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import "time"

const (
	SiteStatusHealthy  = "healthy"
	SiteStatusDegraded = "degraded"
)

// SiteHealth is a read-only, advisory projection of site state at a point in time.
// It does not expose secrets, phone numbers, MQTT URLs, broker credentials, or
// filesystem paths. ActiveTriggers from node heartbeats are represented only as a
// count to prevent any trigger-name strings from leaking into the output.
type SiteHealth struct {
	GeneratedAtMS int64       `json:"generated_at_ms"`
	Status        string      `json:"status"`
	Nodes         []SiteNode  `json:"nodes"`
	Gateway       GatewayView `json:"gateway"`
}

// SiteNode is a safe projection of one runtime node's observed registry state.
type SiteNode struct {
	DeviceID           string `json:"device_id"`
	Status             string `json:"status"`
	LastSeenMS         int64  `json:"last_seen_ms"`
	GatewayObserved    bool   `json:"gateway_observed"`
	Stale              bool   `json:"stale"`
	ActiveTriggerCount int    `json:"active_trigger_count"`
}

// GatewayView carries the gateway's operational state contributed to a site health
// projection. It must not contain target URLs, MQTT URLs, env var names, bearer
// tokens, HMAC secrets, phone numbers, or filesystem paths.
type GatewayView struct {
	Status               string  `json:"status"`
	ProviderName         string  `json:"provider_name"`
	UptimeS              float64 `json:"uptime_s"`
	WebhookBridgeEnabled bool    `json:"webhook_bridge_enabled"`
	WebhookBridgeReady   bool    `json:"webhook_bridge_ready"`
}

// ProjectOptions configures the site health projection parameters.
type ProjectOptions struct {
	// ExpectedDeviceIDs lists the device IDs the gateway expects to observe heartbeats from.
	ExpectedDeviceIDs []string
	// NodeTTLMS is the staleness threshold: nodes not seen within this window are stale.
	NodeTTLMS int64
}

// Viewer is the narrow interface for consumers of site health projections.
// Future ori-cloud sync or dashboard HTTP layers depend on this interface rather
// than on MQTT or registry internals.
type Viewer interface {
	Project(now time.Time, gateway GatewayView) SiteHealth
}

// Projector computes a SiteHealth from the node registry and caller-supplied gateway state.
type Projector struct {
	registry *Registry
	opts     ProjectOptions
}

// NewProjector constructs a Projector.
func NewProjector(registry *Registry, opts ProjectOptions) *Projector {
	return &Projector{registry: registry, opts: opts}
}

// Project returns a point-in-time SiteHealth snapshot.
func (p *Projector) Project(now time.Time, gateway GatewayView) SiteHealth {
	nowMS := now.UnixMilli()
	snapshot := p.registry.Snapshot()
	observed := make(map[string]NodeHeartbeat, len(snapshot))
	for _, n := range snapshot {
		observed[n.DeviceID] = n
	}

	nodes := make([]SiteNode, 0, len(p.opts.ExpectedDeviceIDs))
	for _, deviceID := range p.opts.ExpectedDeviceIDs {
		n, seen := observed[deviceID]
		if !seen {
			nodes = append(nodes, SiteNode{
				DeviceID:        deviceID,
				Status:          SiteStatusDegraded,
				GatewayObserved: false,
				Stale:           true,
			})
			continue
		}
		stale := siteNodeIsStale(n.LastSeenMS, nowMS, p.opts.NodeTTLMS)
		status := SiteStatusHealthy
		if stale || n.Status == NodeStatusDegraded {
			status = SiteStatusDegraded
		}
		nodes = append(nodes, SiteNode{
			DeviceID:           deviceID,
			Status:             status,
			LastSeenMS:         n.LastSeenMS,
			GatewayObserved:    true,
			Stale:              stale,
			ActiveTriggerCount: len(n.ActiveTriggers),
		})
	}

	status := SiteStatusHealthy
	if gateway.Status != SiteStatusHealthy {
		status = SiteStatusDegraded
	}
	for _, n := range nodes {
		if n.Status == SiteStatusDegraded {
			status = SiteStatusDegraded
			break
		}
	}

	return SiteHealth{
		GeneratedAtMS: nowMS,
		Status:        status,
		Nodes:         nodes,
		Gateway:       gateway,
	}
}

// siteNodeIsStale mirrors the staleness predicate in Registry.EvictStale.
// A node exactly at the TTL boundary is not stale. Future-dated heartbeats are stale.
func siteNodeIsStale(lastSeenMS, nowMS, ttlMS int64) bool {
	return lastSeenMS > nowMS || nowMS-lastSeenMS > ttlMS
}
