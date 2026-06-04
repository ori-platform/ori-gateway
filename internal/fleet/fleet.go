// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package fleet owns the gateway fleet optional-module boundary.
package fleet

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ori-platform/ori-gateway/internal/config"
)

const (
	StatusDisabled  = "disabled"
	StatusConnected = "connected"
	StatusDegraded  = "degraded"
)

var ErrNotImplemented = errors.New("fleet: enabled fleet support is not implemented")

// CredentialFunc retrieves fleet credentials. Disabled clients never call it.
// A nil CredentialFunc is intentional and means the health check is unauthenticated.
type CredentialFunc func(ctx context.Context) (string, error)

// HealthCheckFunc checks fleet cloud reachability. Disabled clients never call it.
type HealthCheckFunc func(ctx context.Context, cloudURL string, token string) (bool, error)

// Options injects optional cloud integration points.
type Options struct {
	Credential CredentialFunc
	Health     HealthCheckFunc
}

// Client represents the fleet optional module.
type Client struct {
	enabled    bool
	cloudURL   string
	credential CredentialFunc
	health     HealthCheckFunc
}

// Status reports fleet module posture without forcing callers to touch network or auth.
type Status struct {
	Enabled   bool
	Connected bool
	State     string
	CloudURL  string
	Error     string
}

// New constructs a fleet client. Disabled fleet is deliberately inert.
func New(cfg config.FleetConfig, opts Options) (*Client, error) {
	if !cfg.Enabled {
		return &Client{enabled: false}, nil
	}
	cloudURL := strings.TrimSpace(cfg.CloudURL)
	if cloudURL == "" {
		return nil, fmt.Errorf("fleet: cloud_url must not be empty when enabled")
	}
	if opts.Health == nil {
		return nil, ErrNotImplemented
	}
	return &Client{
		enabled:    true,
		cloudURL:   cloudURL,
		credential: opts.Credential,
		health:     opts.Health,
	}, nil
}

// Status checks fleet availability. Disabled clients return without DNS, HTTP, or auth work.
func (c *Client) Status(ctx context.Context) Status {
	if c == nil || !c.enabled {
		return Status{Enabled: false, Connected: false, State: StatusDisabled}
	}

	token := ""
	if c.credential != nil {
		var err error
		token, err = c.credential(ctx)
		if err != nil {
			return c.degraded(err)
		}
	}

	connected, err := c.health(ctx, c.cloudURL, token)
	if err != nil {
		return c.degraded(err)
	}
	if !connected {
		return c.degraded(errors.New("health check returned false"))
	}
	return Status{
		Enabled:   true,
		Connected: true,
		State:     StatusConnected,
		CloudURL:  c.cloudURL,
	}
}

func (c *Client) degraded(err error) Status {
	return Status{
		Enabled:   true,
		Connected: false,
		State:     StatusDegraded,
		CloudURL:  c.cloudURL,
		Error:     err.Error(),
	}
}

// Connected is the boolean view used by future gateway health wiring.
func (c *Client) Connected(ctx context.Context) bool {
	return c.Status(ctx).Connected
}
