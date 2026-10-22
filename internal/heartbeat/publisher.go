// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package heartbeat publishes the gateway LAN health signal on ori/gateway/health.
package heartbeat

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/asynclog"
	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

// PublishFunc publishes a heartbeat payload to the LAN.
type PublishFunc func(ctx context.Context, payload []byte) error

// ProviderStatus is the narrow provider health surface needed by heartbeat.
// It intentionally stays separate from provider.Provider, which is reasoning-only.
type ProviderStatus interface {
	Name() string
	Healthy(ctx context.Context) bool
}

// Publisher publishes contracts.Heartbeat on an interval with crash supervision.
type Publisher struct {
	publish      PublishFunc
	provider     ProviderStatus
	sim          SIMStatus
	interval     time.Duration
	failureLimit int
	startedAt    time.Time
	now          func() time.Time
	// log queues every record off the loop's path (asynclog), so a logger
	// that blocks never keeps the loop from its failure limit and exit.
	log          *slog.Logger
	logs         *asynclog.Queue
	fatal        func(code int)
	auth         AuthConfig
	webhook      func() *contracts.WebhookBridgePosture
	signingClock *mqttauth.SigningClock

	mu            sync.Mutex
	everPublished bool
	// failing is whether the latest publish attempt failed, and
	// lastPublished when the latest one succeeded, on the monotonic clock.
	failing       bool
	lastPublished time.Time
}

// Options configures Publisher.
type Options struct {
	Interval      time.Duration
	FailureLimit  int
	StartedAt     time.Time
	Now           func() time.Time
	Logger        *slog.Logger
	Fatal         func(code int)
	Auth          AuthConfig
	WebhookBridge func() *contracts.WebhookBridgePosture
	// SigningClock issues each heartbeat's timestamp_ms, which signs it; nil
	// gives the publisher its own in-process clock, which never repeats a
	// value within the process.
	SigningClock *mqttauth.SigningClock
}

// NewPublisher constructs a supervised heartbeat publisher.
func NewPublisher(publish PublishFunc, prov ProviderStatus, sim SIMStatus, opts Options) (*Publisher, error) {
	if publish == nil {
		return nil, fmt.Errorf("heartbeat: publish func must not be nil")
	}
	if prov == nil {
		return nil, fmt.Errorf("heartbeat: provider must not be nil")
	}
	if opts.Interval <= 0 {
		return nil, fmt.Errorf("heartbeat: interval must be positive")
	}
	if opts.Auth.Enabled && strings.TrimSpace(opts.Auth.SharedSecret) == "" {
		return nil, fmt.Errorf("heartbeat: auth shared secret must not be empty")
	}

	failureLimit := opts.FailureLimit
	if failureLimit <= 0 {
		failureLimit = DefaultFailureLimit
	}

	now := opts.Now
	if now == nil {
		now = time.Now
	}

	startedAt := opts.StartedAt
	if startedAt.IsZero() {
		startedAt = now()
	}

	log := opts.Logger
	if log == nil {
		log = slog.Default()
	}

	fatal := opts.Fatal
	if fatal == nil {
		fatal = os.Exit
	}

	signingClock := opts.SigningClock
	if signingClock == nil {
		signingClock = mqttauth.NewSigningClock(now, log)
	}
	queuedLog, logs := asynclog.New(log.Handler(), "heartbeat log lines dropped behind a logger that blocked")
	return &Publisher{
		publish:      publish,
		provider:     prov,
		sim:          sim,
		interval:     opts.Interval,
		failureLimit: failureLimit,
		startedAt:    startedAt,
		now:          now,
		log:          queuedLog,
		logs:         logs,
		fatal:        fatal,
		auth:         opts.Auth,
		webhook:      opts.WebhookBridge,
		signingClock: signingClock,
	}, nil
}

// PublishFromBroker adapts broker.Client to PublishFunc.
func PublishFromBroker(client *broker.Client) PublishFunc {
	return func(ctx context.Context, payload []byte) error {
		if client == nil {
			return fmt.Errorf("heartbeat: broker client must not be nil")
		}
		return client.Publish(ctx, contracts.GatewayHealthTopic, broker.QoSHeartbeat, false, payload)
	}
}

// Run publishes immediately and on interval until ctx is cancelled.
// The loop restarts after panics. Repeated publish failures invoke Fatal.
func (p *Publisher) Run(ctx context.Context) error {
	for {
		if err := ctx.Err(); err != nil {
			return err
		}
		if p.runLoop(ctx) {
			return ctx.Err()
		}
		p.log.Warn("heartbeat loop panicked, restarting")
	}
}

func (p *Publisher) runLoop(ctx context.Context) (done bool) {
	defer func() {
		if r := recover(); r != nil {
			p.log.Warn("heartbeat loop panic", "recover", r)
			done = false
		}
	}()
	return p.loop(ctx)
}

func (p *Publisher) loop(ctx context.Context) bool {
	ticker := time.NewTicker(p.interval)
	defer ticker.Stop()

	failures := 0
	if p.publishOnce(ctx, &failures) {
		return true
	}

	for {
		select {
		case <-ctx.Done():
			return true
		case <-ticker.C:
			if p.publishOnce(ctx, &failures) {
				return true
			}
		}
	}
}

func (p *Publisher) publishOnce(ctx context.Context, failures *int) bool {
	if err := ctx.Err(); err != nil {
		return false
	}

	payload, err := p.buildPayload(ctx)
	if err != nil {
		// The stop closes the signing clock before it cancels the run, so a
		// heartbeat between the two is refused by its own stop, not failing.
		if stopping(ctx, err) {
			return false
		}
		return p.recordFailure(failures, err)
	}
	if err := p.publish(ctx, payload); err != nil {
		// A publish the run's own stop interrupted is not a failure: the
		// broker wait ends with its context.
		if stopping(ctx, err) {
			return false
		}
		return p.recordFailure(failures, err)
	}

	p.mu.Lock()
	p.everPublished = true
	p.failing = false
	p.lastPublished = time.Now()
	p.mu.Unlock()
	*failures = 0
	return false
}

// Publishing reports whether heartbeats are going out: one has been
// published, the latest attempt succeeded, and it did so within three
// intervals on the monotonic clock. Site health reads it, so the gateway
// never reports itself healthy once its heartbeat has stopped (GW-6).
func (p *Publisher) Publishing() bool {
	p.mu.Lock()
	defer p.mu.Unlock()
	return p.everPublished && !p.failing && time.Since(p.lastPublished) <= 3*p.interval
}

// stopping reports whether err comes from the run's own stop: its context
// ended, or its signing clock was closed by the decision to stop.
func stopping(ctx context.Context, err error) bool {
	return ctx.Err() != nil || errors.Is(err, mqttauth.ErrSigningClockClosed)
}

func (p *Publisher) recordFailure(failures *int, err error) bool {
	*failures++
	p.mu.Lock()
	p.failing = true
	p.mu.Unlock()
	p.log.Warn("heartbeat publish failed", "error", err, "failures", *failures, "limit", p.failureLimit)
	if *failures >= p.failureLimit {
		p.log.Error("heartbeat failure limit reached, exiting")
		// The line is given a bounded chance to be written; a logger that
		// blocks cannot keep the process alive past it.
		p.logs.Flush(asynclog.ExitFlushBound)
		p.fatal(1)
		return true
	}
	return false
}

func (p *Publisher) buildPayload(ctx context.Context) ([]byte, error) {
	now := p.now()
	// Rounded to milliseconds so the emitted value is either exactly 0 or at least
	// 0.001. Go and CPython choose different notation for a non-zero magnitude below
	// 1e-4 -- Go writes 0.000035 where CPython writes 3.5e-05 -- and the runtime
	// verifies by re-serialising, so an unrounded value would fail authentication.
	// Run publishes immediately at startup, which is exactly when uptime is smallest.
	uptimeS := math.Round(now.Sub(p.startedAt).Seconds()*1000) / 1000

	// The heartbeat is signed at its timestamp, so it comes from the signing
	// clock, which never issues a value twice.
	timestampMS, err := p.signingClock.Next()
	if err != nil {
		return nil, fmt.Errorf("heartbeat: timestamp: %w", err)
	}
	// Read after the timestamp is issued: an episode that issuing it opened
	// is reported by this heartbeat, not the next.
	status := p.status(ctx)

	beat := contracts.Heartbeat{
		Status:        status,
		UptimeS:       uptimeS,
		Provider:      p.provider.Name(),
		SIMAvailable:  p.sim.Available(),
		TimestampMS:   timestampMS,
		WebhookBridge: p.webhookPosture(),
	}
	if p.auth.Enabled {
		signed, err := SignHeartbeat(beat, p.auth.SharedSecret)
		if err != nil {
			return nil, err
		}
		beat = signed
	}
	return json.Marshal(beat)
}

func (p *Publisher) status(ctx context.Context) string {
	p.mu.Lock()
	everPublished := p.everPublished
	p.mu.Unlock()

	// An episode of the signing clock's mark writes is reported, never waited
	// on, and does not stop the heartbeat (gateway-config/v2
	// gateway.state_directory rule 11).
	if p.signingClock.Degraded() {
		return StatusDegraded
	}
	if !everPublished {
		return StatusStarting
	}
	if p.provider.Healthy(ctx) {
		return StatusHealthy
	}
	return StatusDegraded
}

func (p *Publisher) webhookPosture() *contracts.WebhookBridgePosture {
	if p.webhook == nil {
		return nil
	}
	posture := p.webhook()
	if posture == nil {
		return nil
	}
	cloned := *posture
	return &cloned
}
