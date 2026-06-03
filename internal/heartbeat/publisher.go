// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

// Package heartbeat publishes the gateway LAN health signal on ori/gateway/health.
package heartbeat

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/provider"
)

// PublishFunc publishes a heartbeat payload to the LAN.
type PublishFunc func(ctx context.Context, payload []byte) error

// Publisher publishes contracts.Heartbeat on an interval with crash supervision.
type Publisher struct {
	publish      PublishFunc
	provider     provider.Provider
	sim          SIMStatus
	interval     time.Duration
	failureLimit int
	startedAt    time.Time
	now          func() time.Time
	log          *slog.Logger
	fatal        func(code int)

	mu            sync.Mutex
	everPublished bool
	lastTimestamp int64
}

// Options configures Publisher.
type Options struct {
	Interval     time.Duration
	FailureLimit int
	StartedAt    time.Time
	Now          func() time.Time
	Logger       *slog.Logger
	Fatal        func(code int)
}

// NewPublisher constructs a supervised heartbeat publisher.
func NewPublisher(publish PublishFunc, prov provider.Provider, sim SIMStatus, opts Options) (*Publisher, error) {
	if publish == nil {
		return nil, fmt.Errorf("heartbeat: publish func must not be nil")
	}
	if prov == nil {
		return nil, fmt.Errorf("heartbeat: provider must not be nil")
	}
	if opts.Interval <= 0 {
		return nil, fmt.Errorf("heartbeat: interval must be positive")
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

	return &Publisher{
		publish:      publish,
		provider:     prov,
		sim:          sim,
		interval:     opts.Interval,
		failureLimit: failureLimit,
		startedAt:    startedAt,
		now:          now,
		log:          log,
		fatal:        fatal,
	}, nil
}

// PublishFromBroker adapts broker.Client to PublishFunc.
func PublishFromBroker(client *broker.Client) PublishFunc {
	return func(ctx context.Context, payload []byte) error {
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
	p.publishOnce(ctx, &failures)

	for {
		select {
		case <-ctx.Done():
			return true
		case <-ticker.C:
			p.publishOnce(ctx, &failures)
		}
	}
}

func (p *Publisher) publishOnce(ctx context.Context, failures *int) {
	if err := ctx.Err(); err != nil {
		return
	}

	payload, err := p.buildPayload(ctx)
	if err != nil {
		p.recordFailure(failures, err)
		return
	}
	if err := p.publish(ctx, payload); err != nil {
		p.recordFailure(failures, err)
		return
	}

	p.mu.Lock()
	p.everPublished = true
	p.mu.Unlock()
	*failures = 0
}

func (p *Publisher) recordFailure(failures *int, err error) {
	*failures++
	p.log.Warn("heartbeat publish failed", "error", err, "failures", *failures, "limit", p.failureLimit)
	if *failures >= p.failureLimit {
		p.log.Error("heartbeat failure limit reached, exiting")
		p.fatal(1)
	}
}

func (p *Publisher) buildPayload(ctx context.Context) ([]byte, error) {
	now := p.now()
	status := p.status(ctx)
	uptimeS := now.Sub(p.startedAt).Seconds()

	p.mu.Lock()
	timestampMS := now.UnixMilli()
	if timestampMS <= p.lastTimestamp {
		timestampMS = p.lastTimestamp + 1
	}
	p.lastTimestamp = timestampMS
	p.mu.Unlock()

	beat := contracts.Heartbeat{
		Status:       status,
		UptimeS:      uptimeS,
		Provider:     p.provider.Name(),
		SIMAvailable: p.sim.Available(),
		TimestampMS:  timestampMS,
	}
	return json.Marshal(beat)
}

func (p *Publisher) status(ctx context.Context) string {
	p.mu.Lock()
	everPublished := p.everPublished
	p.mu.Unlock()

	if !everPublished {
		return StatusStarting
	}
	if p.provider.Healthy(ctx) {
		return StatusHealthy
	}
	return StatusDegraded
}
