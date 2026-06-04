// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package sim owns the gateway SIM optional-module boundary.
package sim

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/ori-platform/ori-gateway/internal/config"
)

const (
	StatusDisabled    = "disabled"
	StatusAvailable   = "available"
	StatusUnavailable = "unavailable"
)

var ErrNotImplemented = errors.New("sim: enabled SIM support is not implemented")

// ProbeFunc checks whether a configured modem is currently available.
// Implementations may touch serial hardware, so disabled clients never call it.
type ProbeFunc func(ctx context.Context, modemPath string) (bool, error)

// Options injects optional hardware integration points.
type Options struct {
	Probe ProbeFunc
}

// Client represents the SIM optional module.
type Client struct {
	enabled   bool
	modemPath string
	probe     ProbeFunc
}

// Status reports SIM module posture without forcing callers to touch hardware.
type Status struct {
	Enabled   bool
	Available bool
	State     string
	ModemPath string
	Error     string
}

// New constructs a SIM client. Disabled SIM is deliberately inert.
func New(cfg config.SIMConfig, opts Options) (*Client, error) {
	if !cfg.Enabled {
		return &Client{enabled: false}, nil
	}
	modemPath := strings.TrimSpace(cfg.ModemPath)
	if modemPath == "" {
		return nil, fmt.Errorf("sim: modem_path must not be empty when enabled")
	}
	if opts.Probe == nil {
		return nil, ErrNotImplemented
	}
	return &Client{
		enabled:   true,
		modemPath: modemPath,
		probe:     opts.Probe,
	}, nil
}

// Status checks availability. Disabled clients return without probing hardware.
func (c *Client) Status(ctx context.Context) Status {
	if c == nil || !c.enabled {
		return Status{Enabled: false, Available: false, State: StatusDisabled}
	}
	available, err := c.probe(ctx, c.modemPath)
	if err != nil {
		return Status{
			Enabled:   true,
			Available: false,
			State:     StatusUnavailable,
			ModemPath: c.modemPath,
			Error:     err.Error(),
		}
	}
	state := StatusUnavailable
	if available {
		state = StatusAvailable
	}
	return Status{
		Enabled:   true,
		Available: available,
		State:     state,
		ModemPath: c.modemPath,
	}
}

// Available is the boolean view used by heartbeat wiring.
func (c *Client) Available(ctx context.Context) bool {
	return c.Status(ctx).Available
}
