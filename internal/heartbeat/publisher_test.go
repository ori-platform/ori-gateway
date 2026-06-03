// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	mqttsrv "github.com/mochi-mqtt/server/v2"
	"github.com/mochi-mqtt/server/v2/hooks/auth"
	"github.com/mochi-mqtt/server/v2/listeners"
	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/provider"
)

type stubProvider struct {
	name    string
	healthy bool
}

func (s stubProvider) Name() string { return s.name }

func (s stubProvider) Healthy(context.Context) bool { return s.healthy }

func (s stubProvider) Reason(context.Context, contracts.ReasoningRequest) (contracts.ReasoningResponse, error) {
	return contracts.ReasoningResponse{}, errors.New("not implemented")
}

func startTestBroker(t *testing.T) (brokerURL string, stop func()) {
	t.Helper()

	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := ln.Addr().String()
	if err := ln.Close(); err != nil {
		t.Fatal(err)
	}

	srv := mqttsrv.New(&mqttsrv.Options{
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err := srv.AddHook(new(auth.AllowHook), nil); err != nil {
		t.Fatal(err)
	}
	tcp := listeners.NewTCP(listeners.Config{ID: "test", Address: addr})
	if err := srv.AddListener(tcp); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		_ = srv.Serve()
		close(done)
	}()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			break
		}
		time.Sleep(20 * time.Millisecond)
	}

	return "tcp://" + addr, func() {
		_ = srv.Close()
		select {
		case <-done:
		case <-time.After(5 * time.Second):
			t.Log("mqtt test broker shutdown timed out")
		}
	}
}

func TestHeartbeatPublishes(t *testing.T) {
	brokerURL, stop := startTestBroker(t)
	defer stop()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	client, err := broker.New(broker.Options{
		BrokerURL: brokerURL,
		ClientID:  "heartbeat-sub",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := client.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer client.Disconnect(ctx)

	received := make(chan contracts.Heartbeat, 1)
	if err := client.Subscribe(ctx, contracts.GatewayHealthTopic, broker.QoSHeartbeat, func(_ string, payload []byte) {
		var beat contracts.Heartbeat
		if err := json.Unmarshal(payload, &beat); err != nil {
			t.Errorf("unmarshal heartbeat: %v", err)
			return
		}
		select {
		case received <- beat:
		default:
		}
	}); err != nil {
		t.Fatal(err)
	}

	pubClient, err := broker.New(broker.Options{
		BrokerURL: brokerURL,
		ClientID:  "heartbeat-pub",
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := pubClient.Connect(ctx); err != nil {
		t.Fatal(err)
	}
	defer pubClient.Disconnect(ctx)

	startedAt := time.Unix(1_700_000_000, 0)
	now := startedAt
	publisher, err := NewPublisher(PublishFromBroker(pubClient), stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval:  time.Hour,
		StartedAt: startedAt,
		Now:       func() time.Time { return now },
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	select {
	case beat := <-received:
		if beat.Status != StatusStarting {
			t.Fatalf("status = %q, want %q", beat.Status, StatusStarting)
		}
		if beat.Provider != "echo" {
			t.Fatalf("provider = %q", beat.Provider)
		}
		if beat.UptimeS < 0 {
			t.Fatalf("uptime_s = %v", beat.UptimeS)
		}
		if beat.TimestampMS <= 0 {
			t.Fatalf("timestamp_ms = %d", beat.TimestampMS)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("timed out waiting for heartbeat")
	}
}

func TestHeartbeatPublishesHealthyAfterFirst(t *testing.T) {
	var payloads [][]byte
	var mu sync.Mutex
	publish := func(_ context.Context, payload []byte) error {
		mu.Lock()
		payloads = append(payloads, append([]byte(nil), payload...))
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewPublisher(publish, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval: 10 * time.Millisecond,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(payloads)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) < 2 {
		t.Fatal("expected second heartbeat")
	}
	var second contracts.Heartbeat
	if err := json.Unmarshal(payloads[1], &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != StatusHealthy {
		t.Fatalf("status = %q, want healthy", second.Status)
	}
}

func TestHeartbeatRestartOnPanic(t *testing.T) {
	var calls atomic.Int32
	publish := func(context.Context, []byte) error {
		if calls.Add(1) == 1 {
			panic("boom")
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewPublisher(publish, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval: time.Hour,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if calls.Load() >= 2 {
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatalf("publish calls = %d, want >= 2 after panic restart", calls.Load())
}

func TestHeartbeatFailureLimit(t *testing.T) {
	fatalCh := make(chan struct{}, 1)
	publish := func(context.Context, []byte) error {
		return errors.New("publish failed")
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewPublisher(publish, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval:     1 * time.Millisecond,
		FailureLimit: 3,
		Fatal:        func(int) { fatalCh <- struct{}{} },
		Logger:       slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	select {
	case <-fatalCh:
	case <-time.After(2 * time.Second):
		t.Fatal("expected fatal after repeated publish failures")
	}
}

func TestHeartbeatPublishesImmediately(t *testing.T) {
	var firstAt time.Time
	var mu sync.Mutex
	publish := func(_ context.Context, _ []byte) error {
		mu.Lock()
		defer mu.Unlock()
		if firstAt.IsZero() {
			firstAt = time.Now()
		}
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	started := time.Now()
	publisher, err := NewPublisher(publish, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval: time.Hour,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		got := !firstAt.IsZero()
		mu.Unlock()
		if got {
			mu.Lock()
			elapsed := firstAt.Sub(started)
			mu.Unlock()
			if elapsed > 500*time.Millisecond {
				t.Fatalf("first heartbeat took %v, expected immediate publish", elapsed)
			}
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("first heartbeat was not published immediately")
}

func TestHeartbeatProviderDegradedStatus(t *testing.T) {
	var payloads [][]byte
	var mu sync.Mutex
	publish := func(_ context.Context, payload []byte) error {
		mu.Lock()
		payloads = append(payloads, append([]byte(nil), payload...))
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewPublisher(publish, stubProvider{name: "llama_cpp", healthy: false}, SIMStatus{}, Options{
		Interval: 20 * time.Millisecond,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(payloads)
		mu.Unlock()
		if n >= 2 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	if len(payloads) < 2 {
		t.Fatal("expected at least two heartbeats")
	}
	var first contracts.Heartbeat
	if err := json.Unmarshal(payloads[0], &first); err != nil {
		t.Fatal(err)
	}
	if first.Status != StatusStarting {
		t.Fatalf("first status = %q", first.Status)
	}
	var second contracts.Heartbeat
	if err := json.Unmarshal(payloads[1], &second); err != nil {
		t.Fatal(err)
	}
	if second.Status != StatusDegraded {
		t.Fatalf("second status = %q, want degraded", second.Status)
	}
}

func TestHeartbeatContextCancellationStopsCleanly(t *testing.T) {
	publish := func(context.Context, []byte) error { return nil }

	ctx, cancel := context.WithCancel(context.Background())
	publisher, err := NewPublisher(publish, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval: time.Hour,
		Logger:   slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	done := make(chan error, 1)
	go func() { done <- publisher.Run(ctx) }()

	time.Sleep(50 * time.Millisecond)
	cancel()

	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Run returned %v, want context.Canceled", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("Run did not stop after context cancellation")
	}
}

func TestHeartbeatTimestampsMonotonic(t *testing.T) {
	var timestamps []int64
	var mu sync.Mutex
	now := time.Unix(1_700_000_000, 0)
	publish := func(_ context.Context, payload []byte) error {
		var beat contracts.Heartbeat
		if err := json.Unmarshal(payload, &beat); err != nil {
			return err
		}
		mu.Lock()
		timestamps = append(timestamps, beat.TimestampMS)
		mu.Unlock()
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	publisher, err := NewPublisher(publish, stubProvider{name: "echo", healthy: true}, SIMStatus{}, Options{
		Interval:  1 * time.Millisecond,
		StartedAt: now,
		Now: func() time.Time {
			now = now.Add(1 * time.Millisecond)
			return now
		},
		Logger: slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	deadline := time.Now().Add(2 * time.Second)
	for time.Now().Before(deadline) {
		mu.Lock()
		n := len(timestamps)
		mu.Unlock()
		if n >= 3 {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}

	mu.Lock()
	defer mu.Unlock()
	for i := 1; i < len(timestamps); i++ {
		if timestamps[i] <= timestamps[i-1] {
			t.Fatalf("timestamps not monotonic: %v", timestamps)
		}
	}
}

func TestHeartbeatUptimeIsFloat64(t *testing.T) {
	var payload []byte
	publish := func(_ context.Context, p []byte) error {
		payload = append([]byte(nil), p...)
		return nil
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	startedAt := time.Unix(0, 0)
	now := startedAt.Add(2500 * time.Millisecond)
	publisher, err := NewPublisher(publish, provider.EchoProvider{}, SIMStatus{}, Options{
		Interval:  time.Hour,
		StartedAt: startedAt,
		Now:       func() time.Time { return now },
		Logger:    slog.New(slog.NewTextHandler(io.Discard, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}

	go func() { _ = publisher.Run(ctx) }()

	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if payload != nil {
			break
		}
		time.Sleep(10 * time.Millisecond)
	}
	if payload == nil {
		t.Fatal("no heartbeat published")
	}

	var raw map[string]any
	if err := json.Unmarshal(payload, &raw); err != nil {
		t.Fatal(err)
	}
	uptime, ok := raw["uptime_s"].(float64)
	if !ok {
		t.Fatalf("uptime_s type = %T, want float64", raw["uptime_s"])
	}
	if uptime != 2.5 {
		t.Fatalf("uptime_s = %v, want 2.5", uptime)
	}
}

func TestSIMAvailableFromConfig(t *testing.T) {
	sim := SIMStatus{Enabled: true, Probe: func() bool { return true }}
	if !sim.Available() {
		t.Fatal("expected sim available")
	}
	sim = SIMStatus{Enabled: false, Probe: func() bool { return true }}
	if sim.Available() {
		t.Fatal("expected sim unavailable when disabled")
	}
}
