// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func TestEchoProviderPreservesRequestIDAndTier(t *testing.T) {
	req := contracts.ReasoningRequest{
		RequestID:      "req-1",
		DeviceID:       "site-a",
		Prompt:         "p",
		ActionTierHint: contracts.ActionTierC,
	}
	resp, err := EchoProvider{}.Reason(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	if resp.RequestID != req.RequestID {
		t.Fatalf("request id changed: %q", resp.RequestID)
	}
	if resp.ActionTier != contracts.ActionTierC {
		t.Fatalf("tier changed: %q", resp.ActionTier)
	}
}
