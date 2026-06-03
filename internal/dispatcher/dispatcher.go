// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package dispatcher routes validated runtime reasoning requests to a Tier 3
// provider and publishes exactly one correlated response for every valid request.
package dispatcher

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/provider"
)

const defaultPublishTimeout = 5 * time.Second

var errProviderPanic = errors.New("provider panic")

// Publisher is the MQTT subset Dispatcher needs. broker.Client satisfies this.
type Publisher interface {
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error
}

// Dispatcher handles MQTT reasoning request payloads.
type Dispatcher struct {
	publisher       Publisher
	provider        provider.Provider
	providerTimeout time.Duration
	publishTimeout  time.Duration

	mu       sync.Mutex
	inFlight map[string]struct{}
}

// Options configures Dispatcher.
type Options struct {
	ProviderTimeoutMS int
	PublishTimeout    time.Duration
}

// New constructs a Dispatcher.
func New(publisher Publisher, reasoningProvider provider.Provider, opts Options) (*Dispatcher, error) {
	if publisher == nil {
		return nil, fmt.Errorf("dispatcher: publisher must not be nil")
	}
	if reasoningProvider == nil {
		return nil, fmt.Errorf("dispatcher: provider must not be nil")
	}
	if opts.ProviderTimeoutMS < 0 {
		return nil, fmt.Errorf("dispatcher: provider timeout must not be negative")
	}
	providerTimeout := time.Duration(opts.ProviderTimeoutMS) * time.Millisecond
	if opts.ProviderTimeoutMS == 0 {
		providerTimeout = time.Duration(config.DefaultProviderTimeoutMS) * time.Millisecond
	}
	publishTimeout := opts.PublishTimeout
	if publishTimeout == 0 {
		publishTimeout = defaultPublishTimeout
	}
	if publishTimeout < 0 {
		return nil, fmt.Errorf("dispatcher: publish timeout must not be negative")
	}
	return &Dispatcher{
		publisher:       publisher,
		provider:        reasoningProvider,
		providerTimeout: providerTimeout,
		publishTimeout:  publishTimeout,
		inFlight:        map[string]struct{}{},
	}, nil
}

// HandleRequest validates payload, calls the provider, and publishes a response.
// Invalid payloads are returned as local errors because they may not contain a
// trustworthy device_id/request_id for response correlation.
func (d *Dispatcher) HandleRequest(ctx context.Context, topic string, payload []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var req contracts.ReasoningRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		return fmt.Errorf("dispatcher: decode request: %w", err)
	}
	if err := contracts.ValidateRequest(req); err != nil {
		return fmt.Errorf("dispatcher: validate request: %w", err)
	}
	if req.TimeoutMS < 0 {
		return fmt.Errorf("dispatcher: validate request: timeout_ms must not be negative")
	}
	if err := validateRequestTopic(req, topic); err != nil {
		return err
	}
	if !d.begin(req.RequestID) {
		return nil
	}
	defer d.finish(req.RequestID)

	resp, err := d.callProvider(ctx, req)
	if err != nil {
		resp = contracts.NewErrorResponse(req.RequestID, req.ActionTierHint, providerErrorMessage(err))
	} else if err := contracts.ValidateResponseForRequest(req, resp); err != nil {
		resp = contracts.NewErrorResponse(req.RequestID, req.ActionTierHint, "provider returned invalid response")
	}

	return d.publishResponse(req, resp)
}

func (d *Dispatcher) begin(requestID string) bool {
	d.mu.Lock()
	defer d.mu.Unlock()
	if _, exists := d.inFlight[requestID]; exists {
		return false
	}
	d.inFlight[requestID] = struct{}{}
	return true
}

func (d *Dispatcher) finish(requestID string) {
	d.mu.Lock()
	defer d.mu.Unlock()
	delete(d.inFlight, requestID)
}

func validateRequestTopic(req contracts.ReasoningRequest, topic string) error {
	// Empty topic is allowed for direct/internal callers that already have a
	// decoded request. MQTT subscribers should pass the actual topic so the
	// dispatcher can verify topic/device correlation.
	if topic == "" {
		return nil
	}
	want, err := contracts.RequestTopic(req.DeviceID)
	if err != nil {
		return fmt.Errorf("dispatcher: request topic: %w", err)
	}
	if topic != want {
		return fmt.Errorf("dispatcher: request topic %q does not match device_id %q", topic, req.DeviceID)
	}
	return nil
}

func (d *Dispatcher) callProvider(ctx context.Context, req contracts.ReasoningRequest) (resp contracts.ReasoningResponse, err error) {
	providerTimeout := d.providerTimeout
	if req.TimeoutMS > 0 {
		requested := time.Duration(req.TimeoutMS) * time.Millisecond
		if requested < providerTimeout {
			providerTimeout = requested
		}
	}
	providerCtx, cancel := context.WithTimeout(ctx, providerTimeout)
	defer cancel()

	defer func() {
		if recovered := recover(); recovered != nil {
			err = fmt.Errorf("%w: %v", errProviderPanic, recovered)
		}
	}()
	return d.provider.Reason(providerCtx, req)
}

func (d *Dispatcher) publishResponse(req contracts.ReasoningRequest, resp contracts.ReasoningResponse) error {
	topic, err := contracts.ResponseTopic(req.DeviceID)
	if err != nil {
		return fmt.Errorf("dispatcher: response topic: %w", err)
	}
	payload, err := json.Marshal(resp)
	if err != nil {
		return fmt.Errorf("dispatcher: encode response: %w", err)
	}
	publishCtx, cancel := context.WithTimeout(context.Background(), d.publishTimeout)
	defer cancel()
	if err := d.publisher.Publish(publishCtx, topic, broker.QoSReasoning, false, payload); err != nil {
		return fmt.Errorf("dispatcher: publish response: %w", err)
	}
	return nil
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
