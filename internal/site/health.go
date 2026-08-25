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
	// DegradationReasons names which runtime subsystems are degraded. Safe to
	// project in full, unlike trigger names: these tokens are contract-owned
	// and non-sensitive, carry no subordinate device identity, and are
	// validated against a closed vocabulary before reaching here.
	DegradationReasons []string         `json:"degradation_reasons,omitempty"`
	Posture            *SiteNodePosture `json:"posture,omitempty"`
	// Evidence carries the node's chain signal plus gateway-side integrity
	// observations. A chain head hash is not a secret; no device paths or
	// key material appear here.
	Evidence *NodeEvidence `json:"evidence,omitempty"`
}

// SiteNodePosture carries the runtime security posture for a projected site node.
// Defined independently of runtimeclient posture types to preserve the GW-18
// zero-import invariant. LockoutRiskLevels is intentionally excluded — its keys
// are phone numbers (sender identities) and must never appear in projected output.
type SiteNodePosture struct {
	BrokerHardening *SiteNodeBrokerPosture      `json:"broker_hardening,omitempty"`
	Encryption      *SiteNodeEncryptionPosture  `json:"encryption,omitempty"`
	AlertOutbox     *SiteNodeAlertOutboxPosture `json:"alert_outbox,omitempty"`
}

// SiteNodeBrokerPosture mirrors runtimeclient.GatewayBrokerPosture for the site projection.
type SiteNodeBrokerPosture struct {
	Available             bool   `json:"available"`
	GatewayEnabled        bool   `json:"gateway_enabled"`
	DeploymentCheck       string `json:"deployment_check"`
	AnonymousAccess       string `json:"anonymous_access"`
	ACLPolicy             string `json:"acl_policy"`
	RequireCredentials    bool   `json:"require_credentials"`
	CredentialsConfigured bool   `json:"credentials_configured"`
	RequiresACLHardening  bool   `json:"requires_acl_hardening"`
}

// SiteNodeEncryptionPosture mirrors runtimeclient.StateStoreEncryptionPosture.
type SiteNodeEncryptionPosture struct {
	Available            bool   `json:"available"`
	Mode                 string `json:"mode"`
	Satisfied            bool   `json:"satisfied"`
	MarkerConfigured     bool   `json:"marker_configured"`
	PathPrefixConfigured bool   `json:"path_prefix_configured"`
}

// SiteNodeAlertOutboxPosture mirrors runtimeclient.AlertOutboxPosture.
type SiteNodeAlertOutboxPosture struct {
	Available                     bool    `json:"available"`
	BacklogCount                  int     `json:"backlog_count"`
	OldestQueuedOriginalMS        int64   `json:"oldest_queued_original_ts"`
	OldestQueuedAgeMS             int64   `json:"oldest_queued_age_ms"`
	RetryIntervalMinutes          float64 `json:"retry_interval_minutes"`
	MaxNonTierDAttempts           int     `json:"max_non_tier_d_attempts"`
	TierDCriticalWarningThreshold int     `json:"tier_d_critical_warning_threshold"`
	BatchSize                     int     `json:"batch_size"`
}

// GatewayView carries the gateway's operational state contributed to a site health
// projection. It must not contain target URLs, MQTT URLs, env var names, bearer
// tokens, HMAC secrets, phone numbers, or filesystem paths.
type GatewayView struct {
	Status               string                       `json:"status"`
	ProviderName         string                       `json:"provider_name"`
	UptimeS              float64                      `json:"uptime_s"`
	WebhookBridgeEnabled bool                         `json:"webhook_bridge_enabled"`
	WebhookBridgeReady   bool                         `json:"webhook_bridge_ready"`
	EvidenceDelivery     *GatewayEvidenceDeliveryView `json:"evidence_delivery,omitempty"`
}

// GatewayEvidenceDeliveryView is the safe operational projection of the
// durable evidence courier. LastError is a closed contract-owned reason; raw
// transport errors, endpoints, credentials, and paths never reach this view.
type GatewayEvidenceDeliveryView struct {
	Pending         int    `json:"pending"`
	Degraded        bool   `json:"degraded"`
	Blocked         bool   `json:"blocked"`
	LastFailureAtMS int64  `json:"last_failure_at_ms,omitempty"`
	LastError       string `json:"last_error,omitempty"`
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
		// An evidence-integrity problem degrades the node: a truncated or
		// rolled-back chain, or signing reported unavailable while evidence
		// is enabled, means Tier C/D actions are accumulating without
		// verifiable attestation. Gap count alone stays informational —
		// transient signing failures repair via runtime reconciliation.
		if n.Evidence != nil &&
			(n.Evidence.TruncationSuspected || n.Evidence.HeadRegressed || !n.Evidence.Available) {
			status = SiteStatusDegraded
		}
		nodes = append(nodes, SiteNode{
			DeviceID:           deviceID,
			Status:             status,
			LastSeenMS:         n.LastSeenMS,
			GatewayObserved:    true,
			Stale:              stale,
			ActiveTriggerCount: len(n.ActiveTriggers),
			DegradationReasons: cloneStrings(n.DegradationReasons),
			Posture:            n.Posture,
			Evidence:           n.Evidence,
		})
	}

	status := SiteStatusHealthy
	if gateway.Status != SiteStatusHealthy ||
		(gateway.EvidenceDelivery != nil &&
			(gateway.EvidenceDelivery.Degraded || gateway.EvidenceDelivery.Blocked)) {
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
