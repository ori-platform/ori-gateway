// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package sim

import (
	"context"
	"errors"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/config"
)

func TestDisabledSIMNoSerialProbe(t *testing.T) {
	probeCalled := false
	client, err := New(config.SIMConfig{
		Enabled:   false,
		ModemPath: "/dev/ttyUSB0",
	}, Options{
		Probe: func(context.Context, string) (bool, error) {
			probeCalled = true
			t.Fatal("disabled SIM must not probe serial hardware")
			return false, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if probeCalled {
		t.Fatal("disabled SIM probe was called")
	}
	if status.Enabled || status.Available || status.State != StatusDisabled {
		t.Fatalf("unexpected disabled status: %#v", status)
	}
	if client.Available(context.Background()) {
		t.Fatal("disabled SIM should not be available")
	}
}

func TestEnabledSIMWithoutProbeIsExplicitlyNotImplemented(t *testing.T) {
	_, err := New(config.SIMConfig{
		Enabled:   true,
		ModemPath: "/dev/ttyUSB0",
	}, Options{})
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("error = %v, want ErrNotImplemented", err)
	}
}

func TestEnabledSIMRejectsEmptyModemPath(t *testing.T) {
	_, err := New(config.SIMConfig{
		Enabled:   true,
		ModemPath: " ",
	}, Options{
		Probe: func(context.Context, string) (bool, error) {
			t.Fatal("probe must not run with invalid modem path")
			return false, nil
		},
	})
	if err == nil {
		t.Fatal("expected empty modem path error")
	}
}

func TestEnabledSIMProbeReportsAvailability(t *testing.T) {
	var seenPath string
	client, err := New(config.SIMConfig{
		Enabled:   true,
		ModemPath: " /dev/ttyUSB0 ",
	}, Options{
		Probe: func(_ context.Context, path string) (bool, error) {
			seenPath = path
			return true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if seenPath != "/dev/ttyUSB0" {
		t.Fatalf("probe path = %q", seenPath)
	}
	if !status.Enabled || !status.Available || status.State != StatusAvailable {
		t.Fatalf("unexpected enabled status: %#v", status)
	}
}

func TestEnabledSIMProbeErrorReportsUnavailable(t *testing.T) {
	client, err := New(config.SIMConfig{
		Enabled:   true,
		ModemPath: "/dev/ttyUSB0",
	}, Options{
		Probe: func(context.Context, string) (bool, error) {
			return false, errors.New("modem unavailable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if status.Available || status.State != StatusUnavailable || status.Error == "" {
		t.Fatalf("unexpected error status: %#v", status)
	}
}
