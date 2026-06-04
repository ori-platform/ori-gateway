// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package fleet

import (
	"context"
	"errors"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/config"
)

func TestDisabledFleetNoNetwork(t *testing.T) {
	credentialCalled := false
	healthCalled := false
	client, err := New(config.FleetConfig{
		Enabled:  false,
		CloudURL: "https://cloud.example.test",
	}, Options{
		Credential: func(context.Context) (string, error) {
			credentialCalled = true
			t.Fatal("disabled fleet must not read credentials")
			return "", nil
		},
		Health: func(context.Context, string, string) (bool, error) {
			healthCalled = true
			t.Fatal("disabled fleet must not perform network health checks")
			return false, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if credentialCalled {
		t.Fatal("disabled fleet credential provider was called")
	}
	if healthCalled {
		t.Fatal("disabled fleet health check was called")
	}
	if status.Enabled || status.Connected || status.State != StatusDisabled {
		t.Fatalf("unexpected disabled status: %#v", status)
	}
	if client.Connected(context.Background()) {
		t.Fatal("disabled fleet should not be connected")
	}
}

func TestEnabledFleetWithoutHealthCheckIsExplicitlyNotImplemented(t *testing.T) {
	_, err := New(config.FleetConfig{
		Enabled:  true,
		CloudURL: "https://cloud.example.test",
	}, Options{})
	if !errors.Is(err, ErrNotImplemented) {
		t.Fatalf("error = %v, want ErrNotImplemented", err)
	}
}

func TestEnabledFleetRejectsEmptyCloudURL(t *testing.T) {
	_, err := New(config.FleetConfig{
		Enabled:  true,
		CloudURL: " ",
	}, Options{
		Health: func(context.Context, string, string) (bool, error) {
			t.Fatal("health check must not run with invalid cloud URL")
			return false, nil
		},
	})
	if err == nil {
		t.Fatal("expected empty cloud URL error")
	}
}

func TestEnabledFleetHealthCheckReportsConnected(t *testing.T) {
	var seenURL string
	var seenToken string
	client, err := New(config.FleetConfig{
		Enabled:  true,
		CloudURL: " https://cloud.example.test ",
	}, Options{
		Credential: func(context.Context) (string, error) {
			return "token", nil
		},
		Health: func(_ context.Context, cloudURL string, token string) (bool, error) {
			seenURL = cloudURL
			seenToken = token
			return true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if seenURL != "https://cloud.example.test" {
		t.Fatalf("cloud URL = %q", seenURL)
	}
	if seenToken != "token" {
		t.Fatalf("token = %q", seenToken)
	}
	if !status.Enabled || !status.Connected || status.State != StatusConnected {
		t.Fatalf("unexpected connected status: %#v", status)
	}
	if !client.Connected(context.Background()) {
		t.Fatal("expected Connected to mirror connected status")
	}
}

func TestEnabledFleetWithNilCredentialUsesEmptyToken(t *testing.T) {
	var seenToken string
	client, err := New(config.FleetConfig{
		Enabled:  true,
		CloudURL: "https://cloud.example.test",
	}, Options{
		Health: func(_ context.Context, _ string, token string) (bool, error) {
			seenToken = token
			return true, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if seenToken != "" {
		t.Fatalf("token = %q, want empty token for nil credential", seenToken)
	}
	if !status.Connected {
		t.Fatalf("unexpected status: %#v", status)
	}
}

func TestEnabledFleetCredentialErrorReportsDegraded(t *testing.T) {
	client, err := New(config.FleetConfig{
		Enabled:  true,
		CloudURL: "https://cloud.example.test",
	}, Options{
		Credential: func(context.Context) (string, error) {
			return "", errors.New("missing token")
		},
		Health: func(context.Context, string, string) (bool, error) {
			t.Fatal("health check must not run when credentials fail")
			return false, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if status.Connected || status.State != StatusDegraded || status.Error == "" {
		t.Fatalf("unexpected degraded status: %#v", status)
	}
}

func TestEnabledFleetHealthErrorReportsDegraded(t *testing.T) {
	client, err := New(config.FleetConfig{
		Enabled:  true,
		CloudURL: "https://cloud.example.test",
	}, Options{
		Credential: func(context.Context) (string, error) {
			return "token", nil
		},
		Health: func(context.Context, string, string) (bool, error) {
			return false, errors.New("network unavailable")
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if status.Connected || status.State != StatusDegraded || status.Error == "" {
		t.Fatalf("unexpected degraded status: %#v", status)
	}
}

func TestEnabledFleetHealthFalseReportsDegradedWithError(t *testing.T) {
	client, err := New(config.FleetConfig{
		Enabled:  true,
		CloudURL: "https://cloud.example.test",
	}, Options{
		Health: func(context.Context, string, string) (bool, error) {
			return false, nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	status := client.Status(context.Background())
	if status.Connected || status.State != StatusDegraded || status.Error == "" {
		t.Fatalf("unexpected degraded status for false health: %#v", status)
	}
}
