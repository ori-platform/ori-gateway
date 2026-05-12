// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package provider

import (
	"context"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

type Provider interface {
	Name() string
	Reason(ctx context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error)
}

type EchoProvider struct {
	ModelName string
}

func (p EchoProvider) Name() string {
	if p.ModelName == "" {
		return "echo"
	}
	return p.ModelName
}

func (p EchoProvider) Reason(_ context.Context, req contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	return contracts.ReasoningResponse{
		RequestID:  req.RequestID,
		Text:       "Gateway provider not configured.",
		Model:      p.Name(),
		TokensUsed: 0,
		LatencyMS:  0,
		Confidence: 0,
		ActionTier: req.ActionTierHint,
	}, nil
}
