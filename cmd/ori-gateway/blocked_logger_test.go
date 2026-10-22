// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/asynclog"
	"github.com/ori-platform/ori-gateway/internal/config"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// blockedLog blocks every write, and every level check, until the test ends:
// a log destination that has stopped accepting writes.
type blockedLog struct{ gate chan struct{} }

func newBlockedLog(t *testing.T) *blockedLog {
	b := &blockedLog{gate: make(chan struct{})}
	t.Cleanup(func() { close(b.gate) })
	return b
}

func (b *blockedLog) Enabled(context.Context, slog.Level) bool  { <-b.gate; return true }
func (b *blockedLog) Handle(context.Context, slog.Record) error { <-b.gate; return nil }
func (b *blockedLog) WithAttrs([]slog.Attr) slog.Handler        { return b }
func (b *blockedLog) WithGroup(string) slog.Handler             { return b }
func (b *blockedLog) Write(p []byte) (int, error)               { <-b.gate; return len(p), nil }

// GW-6: the reasoning request escalation ends the gateway though every log
// line it writes would block: each failure is counted without waiting to be
// logged.
func TestTheRequestEscalationEndsTheGatewayBehindALoggerThatBlocks(t *testing.T) {
	fb := newFakeBroker()
	fb.publishErr = errors.New("broker disconnected")
	deps := baseDeps(t, validConfig(), fb, &fakeProvider{healthy: true}, newFakeHeartbeat())
	deps.logger = slog.New(newBlockedLog(t))
	done := make(chan error, 1)
	go func() { done <- runGateway(t.Context(), "gateway.yaml", deps) }()
	select {
	case <-fb.subscribed:
	case <-time.After(10 * time.Second):
		t.Fatal("gateway did not subscribe")
	}
	handler := fb.currentHandler()
	for i := range defaultRequestFailureLimit {
		handler("ori/site-a/reasoning/request", mustJSON(t, contracts.ReasoningRequest{
			RequestID: fmt.Sprintf("req-%d", i), DeviceID: "site-a", SensorType: "current_clamp",
			TriggerName: "overcurrent", Prompt: "Explain anomaly.", ActionTierHint: contracts.ActionTierC, TimeoutMS: 1000,
		}))
	}
	select {
	case err := <-done:
		if err == nil || !strings.Contains(err.Error(), "reasoning request failure limit reached") {
			t.Fatalf("unexpected error: %v", err)
		}
	case <-time.After(10 * time.Second):
		t.Fatal("a logger that blocks kept the request escalation from ending the gateway")
	}
}

// GW-6: runCLI reports a failure and returns its exit status though stderr
// blocks every write: the process exits whether or not the line is taken.
func TestRunCLIExitsBehindAStderrThatBlocks(t *testing.T) {
	stderr := newBlockedLog(t)
	code := make(chan int, 1)
	go func() {
		code <- runCLI([]string{"-config", t.TempDir() + "/missing.yaml"}, &strings.Builder{}, stderr)
	}()
	select {
	case got := <-code:
		if got != 1 {
			t.Fatalf("exit status %d, want 1", got)
		}
	case <-time.After(asynclog.ExitFlushBound + 10*time.Second):
		t.Fatal("a stderr that blocks kept runCLI from exiting")
	}
}

// GW-6: site health never reports the gateway healthy once its heartbeat has
// stopped.
func TestSiteHealthIsDegradedOnceHeartbeatsStop(t *testing.T) {
	l, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	addr := l.Addr().String()
	l.Close()
	cfg := validConfig()
	cfg.SiteHealth = config.SiteHealthConfig{Enabled: true, ListenAddr: addr}
	fb := newFakeBroker()
	hb := newFakeHeartbeat()
	go func() {
		_ = runGateway(t.Context(), "gateway.yaml", baseDeps(t, cfg, fb, &fakeProvider{healthy: true}, hb))
	}()
	gatewayStatus := func() string {
		t.Helper()
		deadline := time.Now().Add(10 * time.Second)
		for {
			resp, err := http.Get("http://" + addr + "/health") //nolint:noctx
			if err == nil {
				var health struct {
					Gateway struct {
						Status string `json:"status"`
					} `json:"gateway"`
				}
				decodeErr := json.NewDecoder(resp.Body).Decode(&health)
				resp.Body.Close()
				if decodeErr != nil {
					t.Fatal(decodeErr)
				}
				return health.Gateway.Status
			}
			if time.Now().After(deadline) {
				t.Fatalf("site health server did not become ready: %v", err)
			}
			time.Sleep(10 * time.Millisecond)
		}
	}
	if got := gatewayStatus(); got != "healthy" {
		t.Fatalf("gateway status %q while heartbeats go out, want healthy", got)
	}
	hb.stopped.Store(true)
	if got := gatewayStatus(); got != "degraded" {
		t.Fatalf("gateway status %q once heartbeats stopped, want degraded", got)
	}
}
