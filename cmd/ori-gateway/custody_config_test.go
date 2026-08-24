// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"testing"

	"github.com/ori-platform/ori-gateway/internal/config"
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
