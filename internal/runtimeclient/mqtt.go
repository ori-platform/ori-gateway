// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"maps"
	"math"
	"strings"
	"sync"
	"time"

	canonicaljson "github.com/ori-platform/ori-canonicaljson"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

const (
	exportTypeHealth           = "health"
	exportTypeSensorHistory    = "sensor_history"
	exportTypeActionLog        = "action_log"
	exportTypeTierCDecisionLog = "tier_c_decision_log"
	exportTypeReasoningLog     = "reasoning_log"
	maxExportPages             = 100
	// defaultExportWait bounds each export request: from publishing it until
	// a verified response arrives (gateway-api/v2 Export Auth Envelope). With
	// maxExportPages it bounds an export as a whole.
	defaultExportWait = 30 * time.Second
	// exportRatePerSecond and exportBurst pace export requests to each
	// device below the rate its runtime admits them, 2 a second in bursts of
	// 10 (ori-runtime ori/gateway/payload_bounds.py, INGRESS_RATES): a request
	// beyond that is dropped unanswered under gateway-api/v2 Payload Volume,
	// which leaves the value to the runtime, and would only wait out its
	// timeout. The runtime judges arrivals, not sends, so the margin absorbs
	// delivery jitter, and each device's bucket starts empty, so a restarted
	// gateway never spends a burst the runtime has not yet refilled.
	exportRatePerSecond = 1.6
	exportBurst         = 8
)

// exportEnvelope is every member gateway-api/v2's Runtime Export Response
// Payload requires on an export page, validation failures included. A page
// missing one is malformed, never complete, empty or successful.
var exportEnvelope = []string{"request_id", "export_type", "device_id", "items", "next_page_token", "complete", "error"}

type mqttBroker interface {
	Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error
	Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error
}

type requestIDFunc func() (string, error)

// MQTTClient implements Client over runtime MQTT export request/response topics.
type MQTTClient struct {
	log                 *slog.Logger
	signingClock        *mqttauth.SigningClock
	exportWait          time.Duration
	broker              mqttBroker
	qos                 byte
	requestID           requestIDFunc
	authSecret          string
	previousAuthSecret  string
	authRequested       bool
	encryptionRequested bool
	verifier            *mqttauth.Verifier
	encryptor           *mqttauth.Encryptor
	optionErr           error
	now                 func() time.Time
	pacer               *exportPacer
	// lifetime ends the client's own background work, the shared subscribe,
	// when its owner stops.
	lifetime context.Context
	// health reads every health export's severity, and holds each device's
	// latch for as long as the client runs.
	health healthReader

	mu         sync.Mutex
	pending    map[string]pendingExport
	subscribed map[string]bool
	// subscribing holds the subscribe in flight for each topic: every export
	// to that device waits on it, each within its own remaining wait.
	subscribing map[string]*subscribeAttempt
	// notes writes the client's log records off every caller's path, and
	// queued logs through it.
	notes  noteLog
	queued *slog.Logger
	// beforeRelease, when set, runs as a request is released, before the
	// lock is taken; a test sets it to hand a response over at exactly that
	// point.
	beforeRelease func()
}

// subscribeAttempt is one subscribe to a response topic; done is closed once
// it has ended, with err its outcome.
type subscribeAttempt struct {
	done chan struct{}
	err  error
}

// MQTTClientOption customises MQTTClient construction.
type MQTTClientOption func(*MQTTClient)

// WithRequestIDFunc overrides request id generation. It is intended for tests.
func WithRequestIDFunc(fn requestIDFunc) MQTTClientOption {
	return func(c *MQTTClient) {
		if fn != nil {
			c.requestID = fn
		}
	}
}

// WithMessageAuth enables runtime-compatible HMAC signing and verification.
func WithMessageAuth(sharedSecret string, previousSharedSecret string, now func() time.Time) MQTTClientOption {
	return func(c *MQTTClient) {
		c.authRequested = true
		c.authSecret = sharedSecret
		c.previousAuthSecret = previousSharedSecret
		if now != nil {
			c.now = now
		}
	}
}

// ErrExportTimeout ends an export request no verified response answered
// within the wait bound.
var ErrExportTimeout = errors.New("runtimeclient: export request timed out")

// WithExportWait sets the bound on each export request's wait. It is intended
// for tests; production uses defaultExportWait.
func WithExportWait(wait time.Duration) MQTTClientOption {
	return func(c *MQTTClient) {
		if wait > 0 {
			c.exportWait = wait
		}
	}
}

// WithLifetime ties the client's own background work to ctx: when it ends, a
// subscribe still in flight is abandoned.
func WithLifetime(ctx context.Context) MQTTClientOption {
	return func(c *MQTTClient) {
		if ctx != nil {
			c.lifetime = ctx
		}
	}
}

// WithExportPacingClock replaces the clock export pacing reads and the wait it
// sleeps on. It is intended for tests; production paces on the monotonic
// clock.
func WithExportPacingClock(now func() time.Time, sleep func(context.Context, time.Duration) error) MQTTClientOption {
	return func(c *MQTTClient) {
		if now != nil {
			c.pacer.now = now
		}
		if sleep != nil {
			c.pacer.sleep = sleep
		}
	}
}

// WithSigningClock sets the clock that issues every export request's
// signed_at_ms; without it the client keeps its own in-process clock.
func WithSigningClock(clock *mqttauth.SigningClock) MQTTClientOption {
	return func(c *MQTTClient) {
		if clock != nil {
			c.signingClock = clock
		}
	}
}

// WithLogger sets where the client records a response it refuses.
func WithLogger(log *slog.Logger) MQTTClientOption {
	return func(c *MQTTClient) {
		if log != nil {
			c.log = log
		}
	}
}

// WithMessageEncryption enables AES-GCM decryption for sensitive runtime export responses.
func WithMessageEncryption(sharedSecret string) MQTTClientOption {
	return func(c *MQTTClient) {
		c.encryptionRequested = true
		encryptor, err := mqttauth.NewEncryptor(sharedSecret)
		if err != nil {
			c.optionErr = fmt.Errorf("message encryption: %w", err)
			return
		}
		c.encryptor = encryptor
	}
}

// NewMQTTClient constructs a runtime export client backed by the gateway broker.
func NewMQTTClient(b mqttBroker, opts ...MQTTClientOption) (*MQTTClient, error) {
	if b == nil {
		return nil, fmt.Errorf("runtimeclient: broker must not be nil")
	}
	client := &MQTTClient{
		broker:      b,
		qos:         broker.QoSReasoning,
		requestID:   randomRequestID,
		log:         slog.Default(),
		exportWait:  defaultExportWait,
		now:         time.Now,
		pacer:       newExportPacer(),
		lifetime:    context.Background(),
		pending:     make(map[string]pendingExport),
		subscribed:  make(map[string]bool),
		subscribing: make(map[string]*subscribeAttempt),
	}
	for _, opt := range opts {
		opt(client)
	}
	// Built once every option is applied, so the verifier logs through the
	// client's own queue, never on the callback that delivers responses.
	client.queued = slog.New(&queuedHandler{inner: client.log.Handler(), notes: &client.notes})
	if client.authRequested && client.optionErr == nil {
		verifier, err := mqttauth.NewVerifier(mqttauth.Config{
			SharedSecret:         client.authSecret,
			PreviousSharedSecret: client.previousAuthSecret,
			Now:                  client.now,
			Logger:               client.queued,
		})
		if err != nil {
			return nil, fmt.Errorf("runtimeclient: message auth: %w", err)
		}
		client.verifier = verifier
	}
	if client.signingClock == nil {
		client.signingClock = mqttauth.NewSigningClock(client.now, client.log)
	}
	if client.optionErr != nil {
		return nil, fmt.Errorf("runtimeclient: %w", client.optionErr)
	}
	if client.authRequested && client.verifier == nil {
		return nil, fmt.Errorf("runtimeclient: invalid message auth configuration")
	}
	if client.encryptionRequested && client.encryptor == nil {
		return nil, fmt.Errorf("runtimeclient: invalid message encryption configuration")
	}
	if client.encryptor != nil && client.verifier == nil {
		return nil, fmt.Errorf("runtimeclient: message encryption requires message auth")
	}
	return client, nil
}

func (c *MQTTClient) Health(ctx context.Context, req HealthRequest) (HealthSnapshot, error) {
	normalized, err := NormalizeHealthRequest(req)
	if err != nil {
		return HealthSnapshot{}, err
	}
	items, err := c.exportAll(ctx, exportRequest{DeviceID: normalized.DeviceID, ExportType: exportTypeHealth})
	if err != nil {
		return HealthSnapshot{}, err
	}
	if len(items) == 0 {
		return HealthSnapshot{}, fmt.Errorf("runtimeclient: health export returned no items")
	}
	// Severity is read only here, after the export has verified, from the
	// device the request named and the response topic carried; every
	// consumer of the export reads it through this one reading.
	reading, fault := c.health.read(items[0], normalized.DeviceID)
	if fault != "" {
		c.warn("runtime health export severity unreadable; reported unknown",
			"device_id", normalized.DeviceID, "fault", fault)
	}
	return mapHealthSnapshot(items[0], normalized.DeviceID, reading.severity), nil
}

func (c *MQTTClient) SensorHistory(ctx context.Context, req SensorHistoryRequest) ([]SensorAggregate, error) {
	normalized, err := NormalizeSensorHistoryRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeSensorHistory,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
		Params: map[string]any{
			"sensor_id": normalized.SensorID,
			"bucket_ms": normalized.BucketMS,
		},
	})
	if err != nil {
		return nil, err
	}
	out := make([]SensorAggregate, 0, len(items))
	for _, item := range items {
		row, err := mapSensorAggregate(item, normalized.DeviceID, normalized.SensorID)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *MQTTClient) ActionLog(ctx context.Context, req ActionLogRequest) ([]ActionLogEntry, error) {
	normalized, err := NormalizeActionLogRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeActionLog,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]ActionLogEntry, 0, len(items))
	for _, item := range items {
		row := mapActionLogEntry(item, normalized.DeviceID)
		out = append(out, row)
	}
	return out, nil
}

func (c *MQTTClient) TierCDecisionLog(ctx context.Context, req TierCDecisionLogRequest) ([]TierCDecisionEntry, error) {
	normalized, err := NormalizeTierCDecisionLogRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeTierCDecisionLog,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
	})
	if err != nil {
		return nil, err
	}
	out := make([]TierCDecisionEntry, 0, len(items))
	for _, item := range items {
		row, err := mapTierCDecisionEntry(item, normalized.DeviceID)
		if err != nil {
			return nil, err
		}
		out = append(out, row)
	}
	return out, nil
}

func (c *MQTTClient) ReasoningLog(ctx context.Context, req ReasoningLogRequest) ([]ReasoningLogEntry, error) {
	normalized, err := NormalizeReasoningLogRequest(req)
	if err != nil {
		return nil, err
	}
	items, err := c.exportAll(ctx, exportRequest{
		DeviceID:   normalized.DeviceID,
		ExportType: exportTypeReasoningLog,
		SinceMS:    normalized.SinceMS,
		UntilMS:    normalized.UntilMS,
		Limit:      normalized.Limit,
		Params:     reasoningLogParams(normalized),
	})
	if err != nil {
		return nil, err
	}
	out := make([]ReasoningLogEntry, 0, len(items))
	for _, item := range items {
		out = append(out, mapReasoningLogEntry(item, normalized.DeviceID))
	}
	return out, nil
}

func reasoningLogParams(req ReasoningLogRequest) map[string]any {
	params := map[string]any{}
	if req.TierUsed != "" {
		params["tier_used"] = req.TierUsed
	}
	if req.ActionTier != "" {
		params["action_tier"] = req.ActionTier
	}
	if req.ReasoningStatus != "" {
		params["reasoning_status"] = req.ReasoningStatus
	}
	if req.CorrelationID != "" {
		params["correlation_id"] = req.CorrelationID
	}
	if len(params) == 0 {
		return nil
	}
	return params
}

func (c *MQTTClient) exportAll(ctx context.Context, base exportRequest) ([]map[string]any, error) {
	// Subscribing is bounded like a request, so a stalled broker never holds
	// the caller.
	subscribeCtx, cancel := context.WithTimeoutCause(ctx, c.exportWait, ErrExportTimeout)
	err := c.ensureResponseSubscription(subscribeCtx, base.DeviceID)
	expired := errors.Is(context.Cause(subscribeCtx), ErrExportTimeout) || errors.Is(err, ErrExportTimeout)
	cancel()
	if err != nil {
		if expired {
			c.warn("runtime export response subscription timed out",
				"device_id", base.DeviceID, "export_type", base.ExportType, "wait", c.exportWait)
			return nil, fmt.Errorf("%w: subscribing for %s after %s", ErrExportTimeout, base.ExportType, c.exportWait)
		}
		return nil, err
	}

	var all []map[string]any
	pageToken := ""
	seenPageTokens := make(map[string]bool)
	for range maxExportPages {
		base.PageToken = pageToken
		resp, err := c.requestPage(ctx, base)
		if err != nil {
			return nil, err
		}
		if resp.Error != "" {
			return nil, fmt.Errorf("runtimeclient: runtime export %s failed: %s", base.ExportType, resp.Error)
		}
		if len(all)+len(resp.Items) > MaxLimit {
			return nil, fmt.Errorf("runtimeclient: runtime export %s exceeded max item limit %d", base.ExportType, MaxLimit)
		}
		all = append(all, resp.Items...)
		if resp.Complete {
			return all, nil
		}
		if resp.NextPageToken == "" {
			return nil, fmt.Errorf("runtimeclient: runtime export %s incomplete without next_page_token", base.ExportType)
		}
		if seenPageTokens[resp.NextPageToken] {
			return nil, fmt.Errorf("runtimeclient: runtime export %s repeated next_page_token %q", base.ExportType, resp.NextPageToken)
		}
		seenPageTokens[resp.NextPageToken] = true
		pageToken = resp.NextPageToken
	}
	return nil, fmt.Errorf("runtimeclient: runtime export %s exceeded max page limit %d", base.ExportType, maxExportPages)
}

// ensureResponseSubscription subscribes to deviceID's export responses, once.
// Each caller waits for a subscribe in flight within its own ctx, which no
// lock and no transport window extends; a subscribe that outlives its caller
// still completes, and the next caller finds the topic subscribed.
func (c *MQTTClient) ensureResponseSubscription(ctx context.Context, deviceID string) error {
	topic, err := contracts.ExportResponseTopicFilter(deviceID)
	if err != nil {
		return err
	}
	c.mu.Lock()
	if c.subscribed[topic] {
		c.mu.Unlock()
		return nil
	}
	attempt, inFlight := c.subscribing[topic]
	if !inFlight {
		attempt = &subscribeAttempt{done: make(chan struct{})}
		c.subscribing[topic] = attempt
		go c.subscribe(topic, attempt)
	}
	c.mu.Unlock()
	select {
	case <-attempt.done:
		return attempt.err
	case <-ctx.Done():
		return ctx.Err()
	}
}

// subscribe makes one subscribe to topic, bounded by the export wait, and
// records its outcome for every caller waiting on it.
func (c *MQTTClient) subscribe(topic string, attempt *subscribeAttempt) {
	ctx, cancel := context.WithTimeoutCause(c.lifetime, c.exportWait, ErrExportTimeout)
	defer cancel()
	err := c.broker.Subscribe(ctx, topic, c.qos, c.handleResponseMessage)
	c.mu.Lock()
	defer c.mu.Unlock()
	delete(c.subscribing, topic)
	switch {
	case err != nil && errors.Is(context.Cause(ctx), ErrExportTimeout):
		attempt.err = fmt.Errorf("%w: subscribe export response: %w", ErrExportTimeout, err)
	case err != nil:
		attempt.err = fmt.Errorf("runtimeclient: subscribe export response: %w", err)
	default:
		c.subscribed[topic] = true
	}
	close(attempt.done)
}

func (c *MQTTClient) requestPage(ctx context.Context, req exportRequest) (exportResponse, error) {
	requestID, err := c.requestID()
	if err != nil {
		return exportResponse{}, err
	}
	if requestID == "" {
		return exportResponse{}, fmt.Errorf("runtimeclient: request_id generator returned empty id")
	}
	if _, err := contracts.ExportResponseTopic(req.DeviceID, requestID); err != nil {
		return exportResponse{}, err
	}
	req.RequestID = requestID

	// Paced to the rate the device's runtime admits, before the request's
	// own wait starts; the pacing wait is bounded the same way.
	paceCtx, cancelPace := context.WithTimeoutCause(ctx, c.exportWait, ErrExportTimeout)
	err = c.pacer.acquire(paceCtx, req.DeviceID)
	paceExpired := errors.Is(context.Cause(paceCtx), ErrExportTimeout)
	cancelPace()
	if err != nil {
		if paceExpired {
			c.warn("runtime export request not sent within the wait bound at the runtime's admission rate",
				"device_id", req.DeviceID, "export_type", req.ExportType, "wait", c.exportWait)
			return exportResponse{}, fmt.Errorf("%w: pacing %s after %s", ErrExportTimeout, req.ExportType, c.exportWait)
		}
		return exportResponse{}, err
	}

	// The wait is bounded from publishing the request, the publish included,
	// and nothing received extends it. On expiry the request ends as failed,
	// is logged, and its pending state is released by the deferred removal; a
	// later attempt is a new request, newly signed (gateway-api/v2 Export
	// Auth Envelope). A response is handed over only while the wait has not
	// expired, decided in the step that hands it over.
	waitCtx, cancel := context.WithTimeoutCause(ctx, c.exportWait, ErrExportTimeout)
	defer cancel()
	responseCh := make(chan exportResult, 1)
	c.mu.Lock()
	if _, exists := c.pending[requestID]; exists {
		c.mu.Unlock()
		return exportResponse{}, fmt.Errorf("runtimeclient: duplicate request_id %q", requestID)
	}
	c.pending[requestID] = pendingExport{ch: responseCh, req: req, wait: waitCtx}
	c.mu.Unlock()
	defer c.removePending(requestID)

	payload, err := c.encodeRequest(req)
	if err != nil {
		return exportResponse{}, err
	}
	if len(payload) > contracts.PayloadMaxima[contracts.FamilyExportRequest] {
		return exportResponse{}, fmt.Errorf("runtimeclient: export request exceeds its topic maximum")
	}
	topic, err := contracts.ExportRequestTopic(req.DeviceID)
	if err != nil {
		return exportResponse{}, err
	}
	timedOut := func() (exportResponse, error) {
		if !errors.Is(context.Cause(waitCtx), ErrExportTimeout) {
			return exportResponse{}, ctx.Err()
		}
		c.warn("runtime export request timed out; no verified response arrived",
			"device_id", req.DeviceID, "export_type", req.ExportType, "request_id", requestID, "wait", c.exportWait)
		return exportResponse{}, fmt.Errorf("%w: %s after %s", ErrExportTimeout, req.ExportType, c.exportWait)
	}
	if err := c.broker.Publish(waitCtx, topic, c.qos, false, payload); err != nil {
		// A response handed over in time completes the request even when the
		// publish then reports failure, a timeout included: the runtime has
		// answered, and its replay key is already recorded.
		if result, ok := c.settle(requestID, responseCh); ok {
			return c.acceptResponse(req, requestID, result)
		}
		if waitCtx.Err() != nil {
			return timedOut()
		}
		return exportResponse{}, fmt.Errorf("runtimeclient: publish export request: %w", err)
	}

	select {
	case <-waitCtx.Done():
		// A response handed over before expiry is taken, whichever case
		// the select chose: deliver decided it was in time.
		if result, ok := c.settle(requestID, responseCh); ok {
			return c.acceptResponse(req, requestID, result)
		}
		return timedOut()
	case result := <-responseCh:
		return c.acceptResponse(req, requestID, result)
	}
}

// acceptResponse checks a response deliver handed over, before its request's
// deadline, against the request it answers.
func (c *MQTTClient) acceptResponse(req exportRequest, requestID string, result exportResult) (exportResponse, error) {
	if result.err != nil {
		return exportResponse{}, result.err
	}
	resp := result.resp
	if resp.RequestID != requestID {
		return exportResponse{}, fmt.Errorf("runtimeclient: response request_id %q does not match %q", resp.RequestID, requestID)
	}
	if resp.DeviceID != "" && resp.DeviceID != req.DeviceID {
		return exportResponse{}, fmt.Errorf("runtimeclient: response device_id %q does not match %q", resp.DeviceID, req.DeviceID)
	}
	if resp.ExportType != "" && resp.ExportType != req.ExportType {
		return exportResponse{}, fmt.Errorf("runtimeclient: response export_type %q does not match %q", resp.ExportType, req.ExportType)
	}
	return resp, nil
}

func (c *MQTTClient) handleResponseMessage(topic string, payload []byte) {
	// A response is correlated by its topic, which names the device and the
	// request it answers, and is then verified under that request's device_id
	// and request_id: one signed for another device or request is not
	// verified (gateway-api/v2 Export Auth Envelope).
	deviceID, requestID, ok := exportResponseTopicRouting(topic)
	if !ok {
		return
	}
	c.mu.Lock()
	pending := c.pending[requestID]
	c.mu.Unlock()
	if pending.ch == nil || pending.req.DeviceID != deviceID {
		return
	}

	if c.verifier == nil {
		raw, _, err := decodePayloadMap(payload)
		if err != nil {
			return
		}
		resp, err := c.decodeResponsePayload(raw, pending.req)
		if !c.refusedByMemberRule(err) {
			c.deliver(requestID, pending.ch, exportResult{resp: resp, err: err})
		}
		return
	}
	// The response is correlated with its pending request under the replay
	// lock, and its replay key recorded only when it is delivered to that
	// request: a response whose request is gone, even one cancelled while it
	// was being verified, adds no state (gateway-api/v2 Payload Volume).
	decided := false
	err := c.verifier.VerifyJSONThenDecide(payload, contracts.ExportResponseMessageType, pending.req.DeviceID, pending.req.RequestID,
		func(verified map[string]any) (bool, error) {
			decided = true
			resp, err := c.decodeResponsePayload(verified, pending.req)
			// A decrypted plaintext the member rule refuses answers nothing:
			// the request stays pending and no replay key is recorded.
			if c.refusedByMemberRule(err) {
				return false, nil
			}
			return c.deliver(requestID, pending.ch, exportResult{resp: resp, err: err}), nil
		})
	// A response that is not verified is dropped and logged, and the request
	// keeps waiting: a broker client cannot end an exchange by publishing on
	// its response topic (gateway-api/v2 Export Auth Envelope).
	if err != nil && !decided {
		c.warn("runtime export response not verified; dropped",
			"device_id", pending.req.DeviceID, "export_type", pending.req.ExportType)
	}
}

// warn logs a warning without waiting on the logger.
func (c *MQTTClient) warn(msg string, args ...any) { c.queued.Warn(msg, args...) }

// refusedByMemberRule reports, and logs with its reason only, a response whose
// decrypted plaintext the Payload Member Rule refuses. Such a response changes
// no state: it is neither delivered nor recorded (gateway-api/v2).
func (c *MQTTClient) refusedByMemberRule(err error) bool {
	refused, ok := errors.AsType[*contracts.MemberRuleError](err)
	if ok {
		c.warn("runtime export response refused under the payload member rule", "reason", refused.Reason)
	}
	return ok
}

// deliver hands result to the pending request it answers, if that request is
// still pending, not yet answered and its wait not expired, and reports
// whether it did. A response refused here answers nothing, so its replay key
// is never recorded.
func (c *MQTTClient) deliver(requestID string, ch chan exportResult, result exportResult) bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	pending := c.pending[requestID]
	if pending.ch != ch {
		return false
	}
	if expired(pending.wait) {
		return false
	}
	select {
	case ch <- result:
		return true
	default:
		return false
	}
}

func (c *MQTTClient) encodeRequest(req exportRequest) ([]byte, error) {
	if c.verifier == nil {
		return json.Marshal(req)
	}
	payload, err := mapFromValue(req)
	if err != nil {
		return nil, err
	}
	signedAt, err := c.signingClock.Next()
	if err != nil {
		return nil, err
	}
	auth, err := mqttauth.Sign(payload, contracts.ExportRequestMessageType, req.DeviceID, req.RequestID, signedAt, c.authSecret)
	if err != nil {
		return nil, err
	}
	payload["auth"] = auth
	return json.Marshal(payload)
}

// decodeResponsePayload reads a response already verified, when message auth
// is configured.
func (c *MQTTClient) decodeResponsePayload(decoded map[string]any, req exportRequest) (exportResponse, error) {

	if isSensitiveExport(req.ExportType) {
		if c.encryptor != nil {
			if encrypted, _ := decoded["encrypted"].(bool); !encrypted {
				return exportResponse{}, fmt.Errorf("runtimeclient: sensitive export response is not encrypted")
			}
			plain, err := c.encryptor.Decrypt(decoded, contracts.ExportResponseMessageType, req.DeviceID, req.RequestID)
			if err != nil {
				return exportResponse{}, fmt.Errorf("runtimeclient: decrypt export response: %w", err)
			}
			decoded = plain
		} else if encrypted, _ := decoded["encrypted"].(bool); encrypted {
			return exportResponse{}, fmt.Errorf("runtimeclient: encrypted export response received but encryption is not configured")
		}
	}

	encoded, err := json.Marshal(decoded)
	if err != nil {
		return exportResponse{}, err
	}
	// Absence is never read as complete, empty or successful: a page missing
	// any envelope member is malformed, and the export it belongs to fails.
	if _, err := canonicaljson.DecodeStrict(encoded, &canonicaljson.Shape{
		Members:  contracts.DefinedMembers(contracts.FamilyExportResponse),
		Required: exportEnvelope,
	}); err != nil {
		return exportResponse{}, fmt.Errorf("runtimeclient: malformed export response: %w", err)
	}
	var resp exportResponse
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&resp); err != nil {
		return exportResponse{}, err
	}
	return resp, nil
}

// exportResponseTopicRouting reads the device and request an export response
// topic names: ori/{device_id}/export/response/{request_id}.
func exportResponseTopicRouting(topic string) (deviceID, requestID string, ok bool) {
	parts := strings.Split(topic, "/")
	if len(parts) != 5 || parts[0] != "ori" || parts[2] != "export" || parts[3] != "response" ||
		parts[1] == "" || parts[4] == "" {
		return "", "", false
	}
	return parts[1], parts[4], true
}

func decodePayloadMap(payload []byte) (map[string]any, string, error) {
	var raw map[string]any
	decoder := json.NewDecoder(bytes.NewReader(payload))
	decoder.UseNumber()
	if err := decoder.Decode(&raw); err != nil {
		return nil, "", err
	}
	return raw, fmt.Sprint(raw["request_id"]), nil
}

func mapFromValue(value any) (map[string]any, error) {
	encoded, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var out map[string]any
	decoder := json.NewDecoder(bytes.NewReader(encoded))
	decoder.UseNumber()
	if err := decoder.Decode(&out); err != nil {
		return nil, err
	}
	return out, nil
}

func isSensitiveExport(exportType string) bool {
	switch exportType {
	case exportTypeSensorHistory, exportTypeActionLog, exportTypeTierCDecisionLog, exportTypeReasoningLog:
		return true
	default:
		return false
	}
}

// settle releases requestID's pending state, then takes a response deliver
// handed over before that. deliver hands over only under c.mu while the
// request is pending, so none is handed over once settle has released it, and
// none it handed over is left unread.
func (c *MQTTClient) settle(requestID string, ch chan exportResult) (exportResult, bool) {
	c.removePending(requestID)
	select {
	case result := <-ch:
		return result, true
	default:
		return exportResult{}, false
	}
}

func (c *MQTTClient) removePending(requestID string) {
	if c.beforeRelease != nil {
		c.beforeRelease()
	}
	c.mu.Lock()
	delete(c.pending, requestID)
	c.mu.Unlock()
}

type pendingExport struct {
	ch  chan exportResult
	req exportRequest
	// wait is the request's wait, from publishing it; a response is handed
	// over only before it expires.
	wait context.Context
}

// expired reports whether wait has ended, or its deadline has passed though
// its timer has not yet fired.
func expired(wait context.Context) bool {
	if wait == nil {
		return false
	}
	if wait.Err() != nil {
		return true
	}
	deadline, ok := wait.Deadline()
	return ok && !time.Now().Before(deadline)
}

type exportResult struct {
	resp exportResponse
	err  error
}

type exportRequest struct {
	RequestID  string         `json:"request_id"`
	ExportType string         `json:"export_type"`
	DeviceID   string         `json:"device_id"`
	SinceMS    int64          `json:"since_ms,omitempty"`
	UntilMS    int64          `json:"until_ms,omitempty"`
	Limit      int            `json:"limit,omitempty"`
	PageToken  string         `json:"page_token,omitempty"`
	Params     map[string]any `json:"params,omitempty"`
}

type exportResponse struct {
	RequestID     string           `json:"request_id"`
	ExportType    string           `json:"export_type"`
	DeviceID      string           `json:"device_id"`
	Items         []map[string]any `json:"items"`
	NextPageToken string           `json:"next_page_token"`
	Complete      bool             `json:"complete"`
	Error         string           `json:"error"`
}

func randomRequestID() (string, error) {
	var buf [16]byte
	if _, err := rand.Read(buf[:]); err != nil {
		return "", fmt.Errorf("runtimeclient: generate request_id: %w", err)
	}
	return hex.EncodeToString(buf[:]), nil
}

// mapHealthSnapshot maps a health export item already read as severity. The
// snapshot names the device the export was requested from, never the item's
// own device_id, which its sender writes.
func mapHealthSnapshot(item map[string]any, deviceID string, severity string) HealthSnapshot {
	lastReadingMS := int64Value(item, "last_reading_ms")
	if lastReadingMS == 0 {
		lastReadingMS = latestSensorSeenMS(item["sensors"])
	}
	gatewaySeen := boolValue(item, "gateway_seen")
	if posture, ok := item["capability_posture"].(map[string]any); ok {
		gatewaySeen = gatewaySeen || boolValue(posture, "gateway_reachable")
	}
	policyStatus := stringValue(item, "policy_status", "")
	if policy, ok := item["device_policy"].(map[string]any); ok {
		policyStatus = stringValue(policy, "tier", policyStatus)
		if policyStatus == "" && boolValue(policy, "enabled") {
			policyStatus = "enabled"
		}
	}
	return HealthSnapshot{
		DeviceID:             deviceID,
		Status:               severity,
		UptimeS:              floatValue(item, "uptime_s"),
		LastReadingMS:        lastReadingMS,
		GatewaySeen:          gatewaySeen,
		PolicyStatus:         policyStatus,
		GatewayBrokerPosture: mapGatewayBrokerPosture(item),
		StateStoreEncryption: mapStateStoreEncryptionPosture(item),
		AlertOutbox:          mapAlertOutboxPosture(item),
		LockoutRiskLevels:    mapLockoutRiskLevels(item),
	}
}

func mapGatewayBrokerPosture(item map[string]any) *GatewayBrokerPosture {
	posture, ok := subMap(item, "gateway_broker_posture")
	if !ok {
		return nil
	}
	return &GatewayBrokerPosture{
		Available:             boolValue(posture, "available"),
		GatewayEnabled:        boolValue(posture, "gateway_enabled"),
		DeploymentCheck:       stringValue(posture, "deployment_check", ""),
		AnonymousAccess:       stringValue(posture, "anonymous_access", ""),
		ACLPolicy:             stringValue(posture, "acl_policy", ""),
		RequireCredentials:    boolValue(posture, "require_credentials"),
		CredentialsConfigured: boolValue(posture, "credentials_configured"),
		RequiresACLHardening:  boolValue(posture, "requires_acl_hardening"),
	}
}

func mapStateStoreEncryptionPosture(item map[string]any) *StateStoreEncryptionPosture {
	posture, ok := subMap(item, "state_store_encryption")
	if !ok {
		return nil
	}
	return &StateStoreEncryptionPosture{
		Available:            boolValue(posture, "available"),
		Mode:                 stringValue(posture, "mode", ""),
		Satisfied:            boolValue(posture, "satisfied"),
		MarkerConfigured:     boolValue(posture, "marker_configured"),
		PathPrefixConfigured: boolValue(posture, "path_prefix_configured"),
	}
}

func mapAlertOutboxPosture(item map[string]any) *AlertOutboxPosture {
	posture, ok := subMap(item, "alert_outbox")
	if !ok {
		return nil
	}
	return &AlertOutboxPosture{
		Available:                     true,
		BacklogCount:                  intValue(posture, "backlog_count"),
		OldestQueuedOriginalMS:        int64Value(posture, "oldest_queued_original_ts"),
		OldestQueuedAgeMS:             int64Value(posture, "oldest_queued_age_ms"),
		RetryIntervalMinutes:          floatValue(posture, "retry_interval_minutes"),
		MaxNonTierDAttempts:           intValue(posture, "max_non_tier_d_attempts"),
		TierDCriticalWarningThreshold: intValue(posture, "tier_d_critical_warning_threshold"),
		BatchSize:                     intValue(posture, "batch_size"),
	}
}

func subMap(item map[string]any, key string) (map[string]any, bool) {
	value, ok := item[key]
	if !ok || value == nil {
		return nil, false
	}
	out, ok := value.(map[string]any)
	return out, ok
}

func mapSensorAggregate(item map[string]any, fallbackDeviceID string, fallbackSensorID string) (SensorAggregate, error) {
	timestamp := int64Value(item, "timestamp")
	startMS := int64Value(item, "start_ms")
	if startMS == 0 {
		startMS = timestamp
	}
	endMS := int64Value(item, "end_ms")
	if endMS == 0 {
		endMS = timestamp
	}
	avgValue := floatValue(item, "avg_value")
	if v, ok := item["avg_value"]; !ok || v == nil {
		avgValue = floatValue(item, "value")
	}
	return SensorAggregate{
		DeviceID:     stringValue(item, "device_id", fallbackDeviceID),
		SensorID:     stringValue(item, "sensor_id", fallbackSensorID),
		Type:         stringValue(item, "sensor_type", ""),
		Unit:         stringValue(item, "unit", ""),
		StartMS:      startMS,
		EndMS:        endMS,
		AvgValue:     avgValue,
		MinValue:     floatValue(item, "min_value"),
		MaxValue:     floatValue(item, "max_value"),
		Samples:      intValue(item, "sample_count"),
		Quality:      floatValue(item, "quality"),
		Metadata:     mapValue(item, "metadata"),
		ReceivedAtMS: receiptValue(item, "received_at_ms"),
	}, nil
}

// receiptValue reads a receipt: a positive whole number of milliseconds
// below 2^53, parsed exactly from the decoder's json.Number so a digit is
// never rounded away. Absent, null, non-numeric, fractional, non-positive,
// non-finite and out-of-range values all read as 0, which means the row
// carries no receipt; none of them is ever a copy of the producer's time.
func receiptValue(m map[string]any, key string) int64 {
	const bound = int64(1) << 53
	value, ok := m[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case json.Number:
		n, err := v.Int64()
		if err != nil || n <= 0 || n >= bound {
			return 0
		}
		return n
	case int:
		if v <= 0 || int64(v) >= bound {
			return 0
		}
		return int64(v)
	case int64:
		if v <= 0 || v >= bound {
			return 0
		}
		return v
	case float64:
		if math.IsNaN(v) || math.IsInf(v, 0) || v <= 0 || v >= float64(bound) || v != math.Trunc(v) {
			return 0
		}
		return int64(v)
	default:
		return 0
	}
}

func mapActionLogEntry(item map[string]any, fallbackDeviceID string) ActionLogEntry {
	return ActionLogEntry{
		DeviceID:              stringValue(item, "device_id", fallbackDeviceID),
		CreatedAtMS:           int64Value(item, "timestamp"),
		ActionName:            stringValue(item, "action_name", ""),
		Tier:                  stringValue(item, "tier", ""),
		SkillName:             stringValue(item, "skill_name", ""),
		TriggerName:           stringValue(item, "trigger_name", ""),
		SensorType:            stringValue(item, "sensor_type", ""),
		SafeDefaultUsed:       boolValue(item, "safe_default_used"),
		Success:               boolValue(item, "executed"),
		ActionTaken:           stringValue(item, "action_taken", ""),
		AttestationStatus:     stringValue(item, "attestation_status", ""),
		AttestationSeq:        optionalInt64Value(item, "attestation_seq"),
		InputAttestationGrade: stringValue(item, "input_attestation_grade", ""),
		InputPosture:          stringValue(item, "input_posture", ""),
		Result:                mapValue(item, "result"),
	}
}

func mapTierCDecisionEntry(item map[string]any, fallbackDeviceID string) (TierCDecisionEntry, error) {
	history, err := mapHistoryWindow(item["history_window"])
	if err != nil {
		return TierCDecisionEntry{}, err
	}
	return TierCDecisionEntry{
		DeviceID:          stringValue(item, "device_id", fallbackDeviceID),
		ProposalID:        stringValue(item, "proposal_id", ""),
		CreatedAtMS:       int64Value(item, "created_at"),
		SkillName:         stringValue(item, "skill_name", ""),
		TriggerName:       stringValue(item, "trigger_name", ""),
		SensorID:          stringValue(item, "sensor_id", ""),
		SensorType:        stringValue(item, "sensor_type", ""),
		ReadingValue:      floatValue(item, "reading_value"),
		HistoryWindow:     history,
		ProposedAction:    stringValue(item, "proposed_action", ""),
		Confidence:        floatValue(item, "confidence"),
		OperatorDecision:  stringValue(item, "operator_decision", ""),
		DecisionLatencyS:  floatValue(item, "decision_latency_s"),
		SafeDefaultUsed:   boolValue(item, "safe_default_used"),
		FinalActionResult: mapValue(item, "final_action_result"),
		SiteType:          stringValue(item, "site_type", ""),
		Location:          stringValue(item, "location", ""),
		Timezone:          stringValue(item, "timezone", ""),
		LaterOutcome:      mapValue(item, "later_outcome"),
	}, nil
}

func mapReasoningLogEntry(item map[string]any, fallbackDeviceID string) ReasoningLogEntry {
	createdAtMS := int64Value(item, "created_at_ms")
	if createdAtMS == 0 {
		createdAtMS = int64Value(item, "created_at", int64Value(item, "timestamp"))
	}
	return ReasoningLogEntry{
		DeviceID:        stringValue(item, "device_id", fallbackDeviceID),
		SkillName:       stringValue(item, "skill_name", ""),
		TriggerName:     stringValue(item, "trigger_name", ""),
		SensorID:        stringValue(item, "sensor_id", ""),
		SensorType:      stringValue(item, "sensor_type", ""),
		TierUsed:        stringValue(item, "tier_used", ""),
		Model:           stringValue(item, "model", ""),
		PromptText:      stringValue(item, "prompt_text", ""),
		ReasoningText:   stringValue(item, "reasoning_text", ""),
		Confidence:      floatValue(item, "confidence"),
		ProposedAction:  stringValue(item, "proposed_action", ""),
		ActionTier:      stringValue(item, "action_tier", ""),
		TokenCount:      intValue(item, "token_count"),
		LatencyMS:       int64Value(item, "latency_ms"),
		ReasoningStatus: stringValue(item, "reasoning_status", ""),
		CorrelationID:   stringValue(item, "correlation_id", ""),
		CreatedAtMS:     createdAtMS,
	}
}

func mapLockoutRiskLevels(item map[string]any) map[string]LockoutRiskState {
	if raw, ok := item["lockout_risk_levels"].(map[string]any); ok && len(raw) > 0 {
		out := make(map[string]LockoutRiskState, len(raw))
		for key, value := range raw {
			m, ok := value.(map[string]any)
			if !ok {
				continue
			}
			out[key] = lockoutRiskStateFromMap(m)
		}
		return out
	}

	lockout, ok := item["remote_command_lockout"].(map[string]any)
	if !ok {
		return nil
	}
	senders, ok := lockout["senders"].([]any)
	if !ok || len(senders) == 0 {
		return nil
	}
	out := make(map[string]LockoutRiskState, len(senders))
	for _, value := range senders {
		m, ok := value.(map[string]any)
		if !ok {
			continue
		}
		key := stringValue(m, "channel", "") + ":" + stringValue(m, "from_number", "")
		out[key] = lockoutRiskStateFromMap(m)
	}
	return out
}

func lockoutRiskStateFromMap(m map[string]any) LockoutRiskState {
	return LockoutRiskState{
		RiskLevel:   stringValue(m, "risk_level", ""),
		LockedOut:   boolValue(m, "locked_out"),
		Stale:       boolValue(m, "stale"),
		CheckedAtMS: int64Value(m, "checked_at_ms"),
	}
}

func latestSensorSeenMS(value any) int64 {
	items, ok := value.([]any)
	if !ok {
		return 0
	}
	var latest int64
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			continue
		}
		seen := int64Value(m, "last_seen_ms")
		if seen > latest {
			latest = seen
		}
	}
	return latest
}

func mapHistoryWindow(value any) ([]HistorySample, error) {
	items, ok := value.([]any)
	if !ok {
		if value == nil {
			return nil, nil
		}
		return nil, fmt.Errorf("runtimeclient: history_window must be an array")
	}
	out := make([]HistorySample, 0, len(items))
	for _, item := range items {
		m, ok := item.(map[string]any)
		if !ok {
			return nil, fmt.Errorf("runtimeclient: history_window item must be an object")
		}
		out = append(out, HistorySample{
			SensorID:    stringValue(m, "sensor_id", ""),
			SensorType:  stringValue(m, "sensor_type", ""),
			Unit:        stringValue(m, "unit", ""),
			TimestampMS: int64Value(m, "timestamp", int64Value(m, "timestamp_ms")),
			Value:       floatValue(m, "value"),
			Quality:     floatValue(m, "quality"),
		})
	}
	return out, nil
}

func stringValue(m map[string]any, key string, fallback string) string {
	value, ok := m[key]
	if !ok || value == nil {
		return fallback
	}
	switch v := value.(type) {
	case string:
		if v == "" {
			return fallback
		}
		return v
	default:
		return fmt.Sprint(v)
	}
}

func intValue(m map[string]any, key string) int {
	return int(int64Value(m, key))
}

func int64Value(m map[string]any, key string, fallback ...int64) int64 {
	var def int64
	if len(fallback) > 0 {
		def = fallback[0]
	}
	value, ok := m[key]
	if !ok || value == nil {
		return def
	}
	switch v := value.(type) {
	case int:
		return int64(v)
	case int64:
		return v
	case float64:
		return int64(v)
	case json.Number:
		parsed, err := v.Int64()
		if err == nil {
			return parsed
		}
		f, err := v.Float64()
		if err == nil {
			return int64(f)
		}
	}
	return def
}

func optionalInt64Value(m map[string]any, key string) *int64 {
	value, ok := m[key]
	if !ok || value == nil {
		return nil
	}
	parsed := int64Value(m, key)
	return &parsed
}

func floatValue(m map[string]any, key string) float64 {
	value, ok := m[key]
	if !ok || value == nil {
		return 0
	}
	switch v := value.(type) {
	case int:
		return float64(v)
	case int64:
		return float64(v)
	case float64:
		return v
	case json.Number:
		parsed, err := v.Float64()
		if err == nil {
			return parsed
		}
	}
	return 0
}

func boolValue(m map[string]any, key string) bool {
	value, ok := m[key]
	if !ok || value == nil {
		return false
	}
	switch v := value.(type) {
	case bool:
		return v
	case float64:
		return v != 0
	case int:
		return v != 0
	}
	return false
}

func mapValue(m map[string]any, key string) map[string]any {
	value, ok := m[key]
	if !ok || value == nil {
		return nil
	}
	raw, ok := value.(map[string]any)
	if !ok {
		return nil
	}
	out := make(map[string]any, len(raw))
	maps.Copy(out, raw)
	return out
}

// exportPacer paces export requests to each device within the rate its
// runtime admits: a token bucket per device, exportRatePerSecond sustained and
// exportBurst at once.
type exportPacer struct {
	now   func() time.Time
	sleep func(context.Context, time.Duration) error
	// rate and burst are exportRatePerSecond and exportBurst.
	rate  float64
	burst float64

	mu      sync.Mutex
	buckets map[string]*paceBucket
}

type paceBucket struct {
	tokens float64
	last   time.Time
}

// pacingClock is the clock and wait every new pacer starts with; a test
// stands in one a wait advances at once.
var pacingClock = func() (func() time.Time, func(context.Context, time.Duration) error) {
	return time.Now, sleepContext
}

func newExportPacer() *exportPacer {
	now, sleep := pacingClock()
	return &exportPacer{now: now, sleep: sleep, rate: exportRatePerSecond, burst: exportBurst, buckets: make(map[string]*paceBucket)}
}

// acquire waits, within ctx, until deviceID may be sent one more request.
func (p *exportPacer) acquire(ctx context.Context, deviceID string) error {
	for {
		p.mu.Lock()
		now := p.now()
		bucket, ok := p.buckets[deviceID]
		if !ok {
			bucket = &paceBucket{last: now}
			p.buckets[deviceID] = bucket
		}
		if elapsed := now.Sub(bucket.last); elapsed > 0 {
			bucket.tokens = min(p.burst, bucket.tokens+elapsed.Seconds()*p.rate)
		}
		bucket.last = now
		if bucket.tokens >= 1 {
			bucket.tokens--
			p.mu.Unlock()
			return nil
		}
		// Rounded up and never zero, so a wait always moves the clock on.
		wait := max(time.Duration(math.Ceil((1-bucket.tokens)/p.rate*float64(time.Second))), time.Nanosecond)
		p.mu.Unlock()
		if err := p.sleep(ctx, wait); err != nil {
			return err
		}
	}
}

func sleepContext(ctx context.Context, d time.Duration) error {
	timer := time.NewTimer(d)
	defer timer.Stop()
	select {
	case <-timer.C:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
