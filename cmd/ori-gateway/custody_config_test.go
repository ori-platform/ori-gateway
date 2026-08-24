// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/provider"
)

// TestCustodySecretMustNotBeAnEnvelopeSecret is the separation check that
// matters, because it is the one a name comparison cannot make.
func TestCustodySecretMustNotBeAnEnvelopeSecret(t *testing.T) {
	envelope := gatewayAuthSecrets{Enabled: true, CurrentSecret: "shared", PreviousSecret: "previous-shared"}

	for _, tc := range []struct {
		name    string
		value   string
		wantErr bool
	}{
		{"distinct", "custody-secret", false},
		{"same as current envelope secret", "shared", true},
		{"same as previous envelope secret", "previous-shared", true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv("ORI_TEST_CUSTODY", tc.value)
			got, err := resolveCustodySecret(
				config.GatewayCustodyConfig{SecretEnv: "ORI_TEST_CUSTODY"}, envelope)
			if tc.wantErr {
				if err == nil {
					t.Fatalf("accepted a custody secret equal to an envelope secret")
				}
				return
			}
			if err != nil {
				t.Fatalf("unexpected refusal: %v", err)
			}
			if got != tc.value {
				t.Fatalf("got %q want %q", got, tc.value)
			}
		})
	}
}

// TestUnconfiguredCustodyIsNotAnError separates a choice from a broken
// credential: absence is optional, but a named variable that is empty is not.
func TestUnconfiguredCustodyIsNotAnError(t *testing.T) {
	secret, err := resolveCustodySecret(config.GatewayCustodyConfig{}, gatewayAuthSecrets{})
	if err != nil || secret != "" {
		t.Fatalf("absent custody config should be optional, got %q err %v", secret, err)
	}

	t.Setenv("ORI_TEST_CUSTODY_EMPTY", "")
	if _, err := resolveCustodySecret(
		config.GatewayCustodyConfig{SecretEnv: "ORI_TEST_CUSTODY_EMPTY"}, gatewayAuthSecrets{},
	); err == nil {
		t.Fatal("a configured but empty custody variable must stop startup")
	}
}

// TestCustodyConfigurationIsValidatedDuringStartup drives the production path,
// not the resolver in isolation. A unit test of resolveCustodySecret proves the
// rule and nothing about whether anything applies it, and custody validation
// that no startup calls is a claim the gateway does not keep. Removing the call
// from runGateway must fail here.
func TestCustodyConfigurationIsValidatedDuringStartup(t *testing.T) {
	const (
		envelopeEnv    = "ORI_TEST_STARTUP_ENVELOPE_SECRET"
		custodyEnv     = "ORI_TEST_STARTUP_CUSTODY_SECRET"
		envelopeSecret = "envelope-secret"
	)

	for _, tc := range []struct {
		name    string
		custody string
		wantErr string
	}{
		{"configured but empty", "", custodyEnv},
		{"reuses the envelope secret", envelopeSecret, "key material of its own"},
		{"carries surrounding whitespace", "custody-secret\n", "whitespace"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Setenv(envelopeEnv, envelopeSecret)
			t.Setenv(custodyEnv, tc.custody)

			cfg := validConfig()
			cfg.Gateway.Auth = config.GatewayAuthConfig{Enabled: true, SharedSecretEnv: envelopeEnv}
			cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: custodyEnv}

			fb := newFakeBroker()
			fp := &fakeProvider{healthy: true}
			hb := newFakeHeartbeat()
			deps := baseDeps(t, cfg, fb, fp, hb)
			providerConstructed := false
			deps.newProvider = func(config.ProviderConfig) (provider.Provider, error) {
				providerConstructed = true
				return fp, nil
			}

			// Bounded, so dropping the validation fails this test on the
			// returned error rather than hanging in a gateway that started.
			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			defer cancel()

			err := runGateway(ctx, "gateway.yaml", deps)
			if err == nil || !strings.Contains(err.Error(), tc.wantErr) {
				t.Fatalf("runGateway error = %v, want one mentioning %q", err, tc.wantErr)
			}
			if providerConstructed {
				t.Fatal("broken custody configuration must stop startup before any module is constructed")
			}
		})
	}
}

// TestWellFormedCustodyConfigurationStartsTheGateway is the other half: the
// refusals above would also pass if custody configuration stopped every
// startup.
func TestWellFormedCustodyConfigurationStartsTheGateway(t *testing.T) {
	t.Setenv("ORI_TEST_STARTUP_ENVELOPE_SECRET", "envelope-secret")
	t.Setenv("ORI_TEST_STARTUP_CUSTODY_SECRET", "custody-secret")

	cfg := validConfig()
	cfg.Gateway.Auth = config.GatewayAuthConfig{
		Enabled:         true,
		SharedSecretEnv: "ORI_TEST_STARTUP_ENVELOPE_SECRET",
	}
	cfg.Gateway.Custody = config.GatewayCustodyConfig{SecretEnv: "ORI_TEST_STARTUP_CUSTODY_SECRET"}

	fb := newFakeBroker()
	fp := &fakeProvider{healthy: true}
	hb := newFakeHeartbeat()
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", baseDeps(t, cfg, fb, fp, hb)) }()

	select {
	case <-fb.subscribed:
	case err := <-done:
		t.Fatalf("gateway stopped before subscribing: %v", err)
	case <-time.After(time.Second):
		t.Fatal("gateway did not subscribe")
	}
	cancel()

	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("gateway did not stop after cancellation")
	}
}
