// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package contracts_test

import (
	"testing"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/contracts/membertest"
)

// TestRuntimeFacingStructsNameOnlyDefinedMembers holds every struct the
// gateway decodes a runtime payload into, or encodes one from, to its
// family's defined members, so a struct decode reads members by exact name.
func TestRuntimeFacingStructsNameOnlyDefinedMembers(t *testing.T) {
	for _, c := range []struct {
		family string
		value  any
	}{
		{contracts.FamilyReasoningRequest, contracts.ReasoningRequest{}},
		{contracts.FamilyReasoningResponse, contracts.ReasoningResponse{}},
		{contracts.FamilyEnrichmentRequest, contracts.TierCEnrichmentRequest{}},
		{contracts.FamilyEnrichmentResponse, contracts.TierCEnrichmentResponse{}},
		{contracts.FamilyRuntimeHeartbeat, contracts.RuntimeNodeHeartbeat{}},
		{contracts.FamilyGatewayHealth, contracts.Heartbeat{}},
	} {
		if undefined := membertest.UndefinedNames(c.family, c.value); len(undefined) != 0 {
			t.Errorf("%s: %T names %v, which the family does not define", c.family, c.value, undefined)
		}
	}
}
