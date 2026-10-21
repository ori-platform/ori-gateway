// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
)

const testExportWait = 100 * time.Millisecond

func healthItems(deviceID, requestID string) map[string]any {
	return withEnvelope(map[string]any{
		"device_id": deviceID, "request_id": requestID, "export_type": "health", "complete": true,
		"items": []any{map[string]any{"device_id": deviceID}},
	})
}

func pendingCount(c *MQTTClient) int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return len(c.pending)
}

// TestAForgedExportResponseLeavesTheExchangePending answers a genuine export
// with an unsigned response, one signed under another secret, one signed for
// another device, and one signed for another request, each on the genuine
// response topic, and then with the verified response. Each forgery is
// dropped and logged, records no replay key, and leaves the request waiting;
// the verified response completes it.
func TestAForgedExportResponseLeavesTheExchangePending(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	b := newFakeBroker()
	logs := &lockedBuffer{}
	c, err := NewMQTTClient(b, WithRequestIDFunc(func() (string, error) { return "genuine", nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithLogger(slog.New(slog.NewTextHandler(logs, nil))), WithExportWait(5*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var replayAfterForgeries int
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		unsigned, _ := json.Marshal(healthItems(req.DeviceID, req.RequestID))
		otherDevice := healthItems("edge-2", req.RequestID)
		otherRequest := healthItems(req.DeviceID, "other-request")
		for _, forged := range [][]byte{
			unsigned,
			signedExportPayload(t, healthItems(req.DeviceID, req.RequestID), "another-secret", now),
			signedExportPayload(t, otherDevice, "current-secret", now),
			signedExportPayload(t, otherRequest, "current-secret", now),
		} {
			b.respondRaw(req.DeviceID, req.RequestID, forged)
		}
		replayAfterForgeries = c.verifier.ReplayEntries()
		if pendingCount(c) != 1 {
			t.Errorf("a forged response ended the exchange: %d pending", pendingCount(c))
		}
		b.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(t, healthItems(req.DeviceID, req.RequestID), "current-secret", now))
	}
	if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
		t.Fatalf("Health = %v, want completed by the verified response", err)
	}
	if replayAfterForgeries != 0 || c.verifier.ReplayEntries() != 1 {
		t.Fatalf("replay entries %d after forgeries, %d after the answer; want 0 then 1", replayAfterForgeries, c.verifier.ReplayEntries())
	}
	logged := logs.once(func(s string) bool { return strings.Count(s, "not verified; dropped") >= 4 })
	if got := strings.Count(logged, "not verified; dropped"); got != 4 {
		t.Fatalf("%d forgeries logged, want 4:\n%s", got, logged)
	}
}

// TestASilentRuntimeEndsTheExportAtItsBound waits on a runtime that never
// answers, with authentication enabled and disabled: the request ends as
// failed at its bound, is logged, and releases its pending state, though the
// caller's own context never ends.
func TestASilentRuntimeEndsTheExportAtItsBound(t *testing.T) {
	for _, auth := range []bool{true, false} {
		t.Run(fmt.Sprintf("auth=%v", auth), func(t *testing.T) {
			logs := &lockedBuffer{}
			opts := []MQTTClientOption{WithExportWait(testExportWait), WithLogger(slog.New(slog.NewTextHandler(logs, nil)))}
			if auth {
				opts = append(opts, WithMessageAuth("current-secret", "", time.Now))
			}
			c, err := NewMQTTClient(newFakeBroker(), opts...)
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, ErrExportTimeout) {
					t.Fatalf("Health = %v, want ErrExportTimeout", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a silent runtime held the caller past its bound")
			}
			if pendingCount(c) != 0 {
				t.Fatal("a timed-out request kept its pending state")
			}
			if logged := logs.once(func(s string) bool { return strings.Contains(s, "timed out") }); !strings.Contains(logged, "timed out") {
				t.Fatalf("the timeout was not logged:\n%s", logged)
			}
		})
	}
}

// stallingBroker blocks Subscribe or Publish until its context ends, as a
// broker that has stopped answering.
type stallingBroker struct {
	*fakeBroker
	stallSubscribe, stallPublish bool
	subscribes                   atomic.Int32
}

func (s *stallingBroker) Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error {
	s.subscribes.Add(1)
	if s.stallSubscribe {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.fakeBroker.Subscribe(ctx, topic, qos, handler)
}

func (s *stallingBroker) Publish(ctx context.Context, topic string, qos byte, retain bool, payload []byte) error {
	if s.stallPublish {
		<-ctx.Done()
		return ctx.Err()
	}
	return s.fakeBroker.Publish(ctx, topic, qos, retain, payload)
}

// TestAStalledSubscribeOrPublishEndsAtTheBound stalls the broker on
// subscribing, then on publishing: the export ends at its bound either way,
// though the caller's context never ends.
func TestAStalledSubscribeOrPublishEndsAtTheBound(t *testing.T) {
	for _, stall := range []string{"subscribe", "publish"} {
		t.Run(stall, func(t *testing.T) {
			b := &stallingBroker{fakeBroker: newFakeBroker(), stallSubscribe: stall == "subscribe", stallPublish: stall == "publish"}
			logs := &lockedBuffer{}
			c, err := NewMQTTClient(b, WithExportWait(testExportWait), WithLogger(slog.New(slog.NewTextHandler(logs, nil))))
			if err != nil {
				t.Fatal(err)
			}
			done := make(chan error, 1)
			go func() {
				_, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, ErrExportTimeout) {
					t.Fatalf("Health = %v, want ErrExportTimeout", err)
				}
				if logged := logs.once(func(s string) bool { return strings.Contains(s, "timed out") }); !strings.Contains(logged, "timed out") {
					t.Fatalf("the stalled %s was not logged:\n%s", stall, logged)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("a stalled broker held the caller past its bound")
			}
			if pendingCount(c) != 0 {
				t.Fatal("a stalled export kept its pending state")
			}
		})
	}
}

// TestRepeatedForgeriesNeverExtendTheBound keeps forging responses on the
// genuine topic for the whole wait: the request still ends at its bound.
func TestRepeatedForgeriesNeverExtendTheBound(t *testing.T) {
	b := newFakeBroker()
	c, err := NewMQTTClient(b, WithRequestIDFunc(func() (string, error) { return "flooded", nil }),
		WithMessageAuth("current-secret", "", time.Now), WithExportWait(testExportWait),
		WithLogger(slog.New(slog.DiscardHandler)))
	if err != nil {
		t.Fatal(err)
	}
	var stop atomic.Bool
	forging := make(chan struct{})
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		forged, _ := json.Marshal(healthItems(req.DeviceID, req.RequestID))
		go func() {
			defer close(forging)
			for !stop.Load() {
				b.respondRaw(req.DeviceID, req.RequestID, forged)
				time.Sleep(time.Millisecond)
			}
		}()
	}
	done := make(chan error, 1)
	go func() {
		_, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
		done <- err
	}()
	select {
	case err := <-done:
		if !errors.Is(err, ErrExportTimeout) {
			t.Fatalf("Health = %v, want ErrExportTimeout", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("forged responses held the request past its bound")
	}
	stop.Store(true)
	<-forging
}

// TestAnExportRequestsABoundedNumberOfPages answers every page as incomplete
// with a fresh token: the export ends at its page bound, so with each
// request's wait bounded the export as a whole is bounded.
func TestAnExportRequestsABoundedNumberOfPages(t *testing.T) {
	b := newFakeBroker()
	c, err := NewMQTTClient(b, WithExportWait(testExportWait), WithLogger(slog.New(slog.DiscardHandler)), instantPacing())
	if err != nil {
		t.Fatal(err)
	}
	pages := 0
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		pages++
		b.respond(req.DeviceID, req.RequestID, map[string]any{
			"export_type": "action_log", "complete": false, "items": []any{},
			"next_page_token": fmt.Sprintf("page-%d", pages),
		})
	}
	_, err = c.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
	if err == nil || !strings.Contains(err.Error(), "max page limit") || pages != maxExportPages {
		t.Fatalf("export = %v after %d pages, want refused at the %d-page bound", err, pages, maxExportPages)
	}
}

// TestAResponseOnAnotherDevicesTopicNeverAnswers delivers a response for the
// pending request on another device's response topic, with authentication
// enabled and disabled: it never answers the request, which ends at its
// bound.
func TestAResponseOnAnotherDevicesTopicNeverAnswers(t *testing.T) {
	for _, auth := range []bool{true, false} {
		t.Run(fmt.Sprintf("auth=%v", auth), func(t *testing.T) {
			now := time.Now()
			b := newFakeBroker()
			opts := []MQTTClientOption{WithRequestIDFunc(func() (string, error) { return "rq", nil }),
				WithExportWait(testExportWait), WithLogger(slog.New(slog.DiscardHandler))}
			if auth {
				opts = append(opts, WithMessageAuth("current-secret", "", func() time.Time { return now }))
			}
			c, err := NewMQTTClient(b, opts...)
			if err != nil {
				t.Fatal(err)
			}
			b.publishHook = func(_ string, payload []byte) {
				var req exportRequest
				if err := json.Unmarshal(payload, &req); err != nil {
					t.Error(err)
					return
				}
				response := signedExportPayload(t, healthItems(req.DeviceID, req.RequestID), "current-secret", now)
				b.mu.Lock()
				handler := b.handlers["ori/edge-1/export/response/+"]
				b.mu.Unlock()
				handler("ori/edge-2/export/response/"+req.RequestID, response)
			}
			done := make(chan error, 1)
			go func() {
				_, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
				done <- err
			}()
			select {
			case err := <-done:
				if !errors.Is(err, ErrExportTimeout) {
					t.Fatalf("Health = %v, want the request unanswered until its bound", err)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the request outlived its bound")
			}
		})
	}
}

// instantClock is a pacing clock that a wait advances at once, so a test that
// is not about pacing issues requests at the paced rate without spending real
// time.
func instantClock() (func() time.Time, func(context.Context, time.Duration) error) {
	var mu sync.Mutex
	at := time.Unix(1_787_000_000, 0)
	return func() time.Time { mu.Lock(); defer mu.Unlock(); return at },
		func(_ context.Context, d time.Duration) error { mu.Lock(); at = at.Add(d); mu.Unlock(); return nil }
}

// instantPacing gives a client the instant pacing clock.
func instantPacing() MQTTClientOption { return WithExportPacingClock(instantClock()) }

// productionPacingClock is the pacing clock production starts every pacer
// with, kept so a test can check it.
var productionPacingClock = pacingClock

// TestMain gives every client a test builds the instant pacing clock unless
// the test chooses another: only the pacing tests spend paced time.
func TestMain(m *testing.M) {
	productionPacingClock = pacingClock
	pacingClock = instantClock
	os.Exit(m.Run())
}
