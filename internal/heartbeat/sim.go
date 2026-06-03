// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import "github.com/ori-platform/ori-gateway/internal/config"

// SIMStatus reports whether outbound SMS hardware is available for heartbeats.
type SIMStatus struct {
	Enabled bool
	Probe   func() bool
}

// FromConfig builds SIMStatus from gateway config.
func FromConfig(cfg config.SIMConfig) SIMStatus {
	return SIMStatus{Enabled: cfg.Enabled}
}

// Available reports sim_available for the heartbeat payload.
func (s SIMStatus) Available() bool {
	if !s.Enabled {
		return false
	}
	if s.Probe != nil {
		return s.Probe()
	}
	return false
}
