// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// TestAnExportRequestOverItsMaximumIsNotPublished pages an export whose
// runtime returns a next_page_token too long to send back: the next request
// would exceed its topic maximum, so it is not published and the export ends
// with an error.
func TestAnExportRequestOverItsMaximumIsNotPublished(t *testing.T) {
	b := newFakeBroker()
	client := newTestMQTTClient(t, b, "page-1", "page-2")
	b.publishHook = func(_ string, payload []byte) {
		if len(payload) > contracts.PayloadMaxima[contracts.FamilyExportRequest] {
			t.Errorf("published a %d-byte export request over its maximum", len(payload))
		}
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Fatal(err)
		}
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "action_log", "complete": false, "items": []any{},
			"next_page_token": strings.Repeat("t", contracts.PayloadMaxima[contracts.FamilyExportRequest]),
		})
	}
	_, err := client.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err == nil || !strings.Contains(err.Error(), "exceeds its topic maximum") {
		t.Fatalf("ActionLog = %v, want the oversized request refused", err)
	}
	if got := len(b.publishedRequests()); got != 1 {
		t.Fatalf("%d export requests published, want only the first page's", got)
	}
}

// TestACancelledExportResponseAddsNoReplayState pauses a signed response
// inside verification, cancels its export meanwhile, then lets verification
// finish: the request is gone, so the response adds no replay state. A
// response delivered to a pending request does record its key.
func TestACancelledExportResponseAddsNoReplayState(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	b := newFakeBroker()
	entered, release, callbackDone := make(chan struct{}), make(chan struct{}), make(chan struct{})
	var armed atomic.Bool
	clock := func() time.Time {
		if armed.CompareAndSwap(true, false) {
			close(entered)
			<-release
		}
		return now
	}
	ids := []string{"cancelled-export", "answered-export"}
	c, err := NewMQTTClient(b, WithRequestIDFunc(func() (string, error) {
		id := ids[0]
		ids = ids[1:]
		return id, nil
	}), WithMessageAuth("current-secret", "", clock))
	if err != nil {
		t.Fatal(err)
	}
	pause := true
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		response := signedExportPayload(t, withEnvelope(map[string]any{
			"device_id": req.DeviceID, "request_id": req.RequestID, "export_type": "action_log", "complete": true, "items": []any{},
		}), "current-secret", now)
		if !pause {
			b.respondRaw(req.DeviceID, req.RequestID, response)
			return
		}
		armed.Store(true)
		go func() {
			defer close(callbackDone)
			b.respondRaw(req.DeviceID, req.RequestID, response)
		}()
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan error, 1)
	go func() {
		_, err := c.ActionLog(ctx, ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
		done <- err
	}()
	select {
	case <-entered:
	case <-time.After(5 * time.Second):
		t.Fatal("the response did not reach verification")
	}
	cancel()
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("export = %v, want cancelled", err)
	}
	close(release)
	<-callbackDone
	c.mu.Lock()
	pending := len(c.pending)
	c.mu.Unlock()
	if pending != 0 || c.verifier.ReplayEntries() != 0 {
		t.Fatalf("pending=%d, %d replay entries; a response for a cancelled export added state", pending, c.verifier.ReplayEntries())
	}

	pause = false
	if _, err := c.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}}); err != nil {
		t.Fatal(err)
	}
	if c.verifier.ReplayEntries() != 1 {
		t.Fatalf("%d replay entries after an answered export, want 1", c.verifier.ReplayEntries())
	}
}
