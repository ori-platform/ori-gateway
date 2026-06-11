// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package enrichment handles advisory Tier C proposal enrichment over MQTT.
package enrichment

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

const defaultPublishTimeout = 5 * time.Second

var errProviderPanic = errors.New("provider panic")

// Publisher is the MQTT subset Handler needs. broker.Client satisfies this.
type Publisher interface {
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error
}

// Provider generates advisory-only text for an existing Tier C proposal.
type Provider interface {
	EnrichTierC(ctx context.Context, req contracts.TierCEnrichmentRequest) (contracts.TierCEnrichmentResponse, error)
}

// Options configures Handler.
type Options struct {
	PublishTimeout time.Duration
	Logger         *slog.Logger
}

// Handler validates Tier C enrichment requests and publishes one correlated response.
type Handler struct {
	publisher      Publisher
	provider       Provider
	publishTimeout time.Duration
	logger         *slog.Logger
}

// NewHandler constructs a Tier C enrichment handler.
func NewHandler(publisher Publisher, provider Provider, opts Options) (*Handler, error) {
	if publisher == nil {
		return nil, fmt.Errorf("enrichment: publisher must not be nil")
	}
	if provider == nil {
		return nil, fmt.Errorf("enrichment: provider must not be nil")
	}
	publishTimeout := opts.PublishTimeout
	if publishTimeout == 0 {
		publishTimeout = defaultPublishTimeout
	}
	if publishTimeout < 0 {
		return nil, fmt.Errorf("enrichment: publish timeout must not be negative")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	return &Handler{
		publisher:      publisher,
		provider:       provider,
		publishTimeout: publishTimeout,
		logger:         logger,
	}, nil
}

// HandleRequest validates payload, calls the provider, and publishes an advisory response.
// Invalid malformed payloads return local errors because they may not contain trustworthy
// correlation fields. Valid-but-rejected requests receive a correlated error response when possible.
func (h *Handler) HandleRequest(ctx context.Context, topic string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	deviceID, err := contracts.DeviceIDFromTierCEnrichmentRequestTopic(topic)
	if err != nil {
		return fmt.Errorf("enrichment: request topic: %w", err)
	}

	var req contracts.TierCEnrichmentRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return fmt.Errorf("enrichment: decode request: %w", err)
	}
	if req.DeviceID != deviceID {
		return fmt.Errorf("enrichment: request topic %q does not match device_id %q", topic, req.DeviceID)
	}
	if err := contracts.ValidateTierCEnrichmentRequest(req); err != nil {
		return h.publishError(req, validationErrorMessage(err))
	}

	resp, err := h.callProvider(ctx, req)
	if err != nil {
		return h.publishError(req, providerErrorMessage(err))
	}
	resp.RequestID = req.RequestID
	resp.ProposalID = req.ProposalID
	if err := contracts.ValidateTierCEnrichmentResponseForRequest(req, resp); err != nil {
		h.logger.Warn("tier c enrichment provider returned invalid response", "request_id", req.RequestID, "error", err)
		return h.publishError(req, "provider returned invalid response")
	}
	return h.publishResponse(req, resp)
}

func (h *Handler) callProvider(ctx context.Context, req contracts.TierCEnrichmentRequest) (resp contracts.TierCEnrichmentResponse, err error) {
	providerTimeout := time.Duration(req.TimeoutMS) * time.Millisecond
	providerCtx, cancel := context.WithTimeout(ctx, providerTimeout)
	defer cancel()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", errProviderPanic, recovered)
		}
	}()
	return h.provider.EnrichTierC(providerCtx, req)
}

func (h *Handler) publishError(req contracts.TierCEnrichmentRequest, message string) error {
	resp := contracts.NewTierCEnrichmentErrorResponse(req.RequestID, req.ProposalID, message)
	return h.publishResponse(req, resp)
}

func (h *Handler) publishResponse(req contracts.TierCEnrichmentRequest, resp contracts.TierCEnrichmentResponse) error {
	topic, err := contracts.TierCEnrichmentResponseTopic(req.DeviceID)
	if err != nil {
		return fmt.Errorf("enrichment: response topic: %w", err)
	}
	payload, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("enrichment: encode response: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(context.Background(), h.publishTimeout)
	defer cancel()
	if err := h.publisher.Publish(publishCtx, topic, broker.QoSReasoning, false, payload); err != nil {
		return fmt.Errorf("enrichment: publish response: %w", err)
	}
	return nil
}

func validationErrorMessage(error) string {
	return "invalid enrichment request"
}

func providerErrorMessage(err error) string {
	if errors.Is(err, context.DeadlineExceeded) {
		return "provider timeout"
	}
	if errors.Is(err, context.Canceled) {
		return "provider canceled"
	}
	if errors.Is(err, errProviderPanic) {
		return "provider panic"
	}
	return "provider error"
}
