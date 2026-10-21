// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

// instantExportPacing gives a runtime export client a pacing clock that a wait
// advances at once, for gateway tests that are not about pacing.
func instantExportPacing() runtimeclient.MQTTClientOption {
	var mu sync.Mutex
	at := time.Unix(1_787_000_000, 0)
	return runtimeclient.WithExportPacingClock(
		func() time.Time { mu.Lock(); defer mu.Unlock(); return at },
		func(_ context.Context, d time.Duration) error { mu.Lock(); at = at.Add(d); mu.Unlock(); return nil },
	)
}

// stallingExportBroker stalls every export response subscribe until its
// context ends, and reports when it does.
type stallingExportBroker struct {
	brokerClient
	started, ended chan struct{}
}

func (b *stallingExportBroker) Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error {
	if !strings.Contains(topic, "/export/response/") {
		return b.brokerClient.Subscribe(ctx, topic, qos, handler)
	}
	close(b.started)
	<-ctx.Done()
	close(b.ended)
	return ctx.Err()
}

// TestTheRuntimeExportClientEndsWithTheRun builds the runtime export client
// from the options the gateway passes, stalls its export subscribe, and stops
// the gateway: the stalled subscribe ends with the run, not at its bound.
func TestTheRuntimeExportClientEndsWithTheRun(t *testing.T) {
	cfg := validConfig()
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: "127.0.0.1:0"}
	fb := newFakeBroker()
	deps := baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	stalling := &stallingExportBroker{started: make(chan struct{}), ended: make(chan struct{})}
	built := make(chan runtimeclient.Client, 1)
	deps.newRuntimeClient = func(b brokerClient, opts ...runtimeclient.MQTTClientOption) (runtimeclient.Client, error) {
		stalling.brokerClient = b
		c, err := runtimeclient.NewMQTTClient(stalling, append(opts, instantExportPacing())...)
		if err == nil {
			built <- c
		}
		return c, err
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() { done <- runGateway(ctx, "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not subscribe")
	}
	client := <-built
	exportCtx, cancelExport := context.WithTimeout(context.Background(), 20*time.Millisecond)
	_, _ = client.Health(exportCtx, runtimeclient.HealthRequest{DeviceID: "dev-01"})
	cancelExport()
	select {
	case <-stalling.started:
	case <-time.After(2 * time.Second):
		t.Fatal("the export never subscribed")
	}
	select {
	case <-stalling.ended:
		t.Fatal("the stalled subscribe ended before the run did")
	default:
	}
	cancel()
	select {
	case err := <-done:
		if err != nil {
			t.Fatalf("runGateway returned error: %v", err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("gateway did not stop")
	}
	select {
	case <-stalling.ended:
	case <-time.After(2 * time.Second):
		t.Fatal("a stalled export subscribe outlived the run")
	}
}
