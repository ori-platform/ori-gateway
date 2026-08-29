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
	"math"
	"strings"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
	"github.com/ori-platform/ori-gateway/internal/provider"
)

const defaultPublishTimeout = 5 * time.Second

var (
	errProviderPanic = errors.New("provider panic")
	// Refused before any side effect and never answered on the response topic.
	errUnauthenticated = errors.New("unauthenticated reasoning request")
)

// confidenceDecimals bounds `confidence` to four decimal places at emission.
// Go and CPython spell a non-zero float below 1e-4 differently, and the
// runtime verifies a signed response by re-serialising it, so an emitted
// value is either exactly 0 or at least 0.0001.
const confidenceDecimals = 1e4

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
	verifier        *mqttauth.Verifier
	signingSecret   string
	now             func() time.Time

	mu       sync.Mutex
	inFlight map[string]struct{}
}

// Options configures Dispatcher.
type Options struct {
	ProviderTimeoutMS int
	PublishTimeout    time.Duration
	// AuthVerifier and SigningSecret are set together; the previous secret
	// verifies through AuthVerifier and never signs.
	AuthVerifier  *mqttauth.Verifier
	SigningSecret string
	Now           func() time.Time
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
	signingSecret := strings.TrimSpace(opts.SigningSecret)
	if (opts.AuthVerifier == nil) != (signingSecret == "") {
		return nil, fmt.Errorf("dispatcher: auth verifier and signing secret must be configured together")
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	return &Dispatcher{
		publisher:       publisher,
		provider:        reasoningProvider,
		providerTimeout: providerTimeout,
		publishTimeout:  publishTimeout,
		verifier:        opts.AuthVerifier,
		signingSecret:   signingSecret,
		now:             now,
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
	// Verification precedes every side effect: the provider is never invoked
	// and nothing is published for a request the runtime did not sign. The
	// topic-device check above and this check each hold on their own.
	if d.verifier != nil {
		if _, err := d.verifier.VerifyJSON(payload, contracts.ReasoningRequestMessageType, req.DeviceID, req.RequestID); err != nil {
			return fmt.Errorf("dispatcher: %w: %v", errUnauthenticated, err)
		}
	}
	req.Auth = nil
	if !d.begin(req.RequestID) {
		return nil
	}
	defer d.finish(req.RequestID)

	resp, err := d.callProvider(ctx, req)
	if err != nil {
		resp = contracts.NewErrorResponse(req.RequestID, req.DeviceID, req.ActionTierHint, providerErrorMessage(err))
	} else {
		resp.DeviceID = req.DeviceID
		if err := contracts.ValidateResponseForRequest(req, resp); err != nil {
			resp = contracts.NewErrorResponse(req.RequestID, req.DeviceID, req.ActionTierHint, "provider returned invalid response")
		}
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
	resp.Confidence = math.Round(resp.Confidence*confidenceDecimals) / confidenceDecimals
	resp.Auth = nil
	if d.signingSecret != "" {
		auth, err := mqttauth.Sign(resp, contracts.ReasoningResponseMessageType, req.DeviceID, req.RequestID, d.now().UnixMilli(), d.signingSecret)
		if err != nil {
			return fmt.Errorf("dispatcher: sign response: %w", err)
		}
		resp.Auth = &auth
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
