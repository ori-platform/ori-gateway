// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"slices"
	"sync"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/mqttauth"
)

// latePublishBroker holds every publish until its wait has expired, then
// answers it with a correctly signed response before the publish returns, as
// a broker whose completion is delayed past the deadline would.
type latePublishBroker struct {
	*fakeBroker
	t   *testing.T
	now time.Time
}

func (b *latePublishBroker) Publish(ctx context.Context, _ string, _ byte, _ bool, payload []byte) error {
	var req exportRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		b.t.Error(err)
		return err
	}
	<-ctx.Done()
	b.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(b.t, healthItems(req.DeviceID, req.RequestID), "current-secret", b.now))
	return nil
}

// TestAResponseAfterTheDeadlineNeverCompletesTheExport holds the publish
// completion until the request's wait has expired and then delivers a
// correctly signed response, many times over: every request ends with
// ErrExportTimeout, and no response delivered after the deadline records a
// replay key (gateway-api/v2 Export Auth Envelope).
func TestAResponseAfterTheDeadlineNeverCompletesTheExport(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	b := &latePublishBroker{fakeBroker: newFakeBroker(), t: t, now: now}
	n := 0
	c, err := NewMQTTClient(b, WithRequestIDFunc(func() (string, error) { n++; return fmt.Sprintf("late-%d", n), nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(20*time.Millisecond), instantPacing())
	if err != nil {
		t.Fatal(err)
	}
	for i := range 40 {
		if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); !errors.Is(err, ErrExportTimeout) {
			t.Fatalf("exchange %d = %v, want ErrExportTimeout for a response after the deadline", i, err)
		}
	}
	if entries := c.verifier.ReplayEntries(); entries != 0 {
		t.Fatalf("%d replay keys recorded for responses after the deadline, want none", entries)
	}
}

// stalledSubscribeBroker never completes a subscribe until released, and
// ignores the context it is given, as a transport whose write window
// outlasts the caller would.
type stalledSubscribeBroker struct {
	*fakeBroker
	release  chan struct{}
	attempts sync.Mutex
	count    int
}

func (b *stalledSubscribeBroker) Subscribe(ctx context.Context, topic string, qos byte, handler broker.MessageHandler) error {
	b.attempts.Lock()
	b.count++
	b.attempts.Unlock()
	<-b.release
	return b.fakeBroker.Subscribe(ctx, topic, qos, handler)
}

// TestConcurrentSubscribesEndWithinTheBound starts many exports to one device
// while its response subscription stalls: each ends with ErrExportTimeout
// within its own bound, not queued behind another caller, and the stalled
// subscribe is made once; once it completes, the next export proceeds.
func TestConcurrentSubscribesEndWithinTheBound(t *testing.T) {
	const wait = 100 * time.Millisecond
	b := &stalledSubscribeBroker{fakeBroker: newFakeBroker(), release: make(chan struct{})}
	t.Cleanup(func() {
		select {
		case <-b.release:
		default:
			close(b.release)
		}
	})
	c, err := NewMQTTClient(b, WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(wait), instantPacing())
	if err != nil {
		t.Fatal(err)
	}
	var wg sync.WaitGroup
	took := make(chan time.Duration, 8)
	for range 8 {
		wg.Go(func() {
			began := time.Now()
			if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); !errors.Is(err, ErrExportTimeout) {
				t.Errorf("export during a stalled subscribe = %v, want ErrExportTimeout", err)
			}
			took <- time.Since(began)
		})
	}
	wg.Wait()
	close(took)
	for d := range took {
		if d > wait+150*time.Millisecond {
			t.Fatalf("an export waited %v on a stalled subscribe, over its bound %v", d, wait)
		}
	}
	b.attempts.Lock()
	attempts := b.count
	b.attempts.Unlock()
	if attempts != 1 {
		t.Fatalf("%d subscribes made for one topic, want one shared", attempts)
	}
	close(b.release)
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		b.respond(req.DeviceID, req.RequestID, healthItems(req.DeviceID, req.RequestID))
	}
	waitUntilSubscribed(t, c, "edge-1")
	if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
		t.Fatalf("export once the subscribe completed = %v", err)
	}
}

func waitUntilSubscribed(t *testing.T, c *MQTTClient, deviceID string) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for {
		c.mu.Lock()
		subscribed := c.subscribed["ori/"+deviceID+"/export/response/+"]
		c.mu.Unlock()
		if subscribed {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("the response subscription never completed")
		}
		time.Sleep(time.Millisecond)
	}
}

// contractEnvelope is gateway-api/v2's Runtime Export Response Payload
// envelope, taken from the contract's text, kept apart from the decoder's list
// so the two are compared rather than one copied from the other.
var contractEnvelope = []string{"request_id", "export_type", "device_id", "items", "next_page_token", "complete", "error"}

func TestTheEnforcedEnvelopeIsTheContractsEnvelope(t *testing.T) {
	got, want := slices.Sorted(slices.Values(exportEnvelope)), slices.Sorted(slices.Values(contractEnvelope))
	if !slices.Equal(got, want) {
		t.Fatalf("decoder requires %v, the contract's envelope is %v", got, want)
	}
}

// TestAnExportPageMissingAnEnvelopeMemberFailsTheExport answers, through
// requestPage, a page missing each envelope member in turn, signed and
// unsigned: no export is ever read as complete, empty or successful. A signed
// page without the request_id or device_id that bind it to its request is not
// verified, so it is dropped (GW-23) and the export fails at its bound with no
// replay key; every other missing member fails the export as malformed at
// once. The full envelope with "error": null succeeds.
func TestAnExportPageMissingAnEnvelopeMemberFailsTheExport(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	for _, signed := range []bool{true, false} {
		for _, missing := range append([]string{""}, contractEnvelope...) {
			name := fmt.Sprintf("signed %v, missing %q", signed, missing)
			t.Run(name, func(t *testing.T) {
				b := newFakeBroker()
				opts := []MQTTClientOption{WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(200 * time.Millisecond), instantPacing()}
				if signed {
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
					page := map[string]any{
						"request_id": req.RequestID, "export_type": "action_log", "device_id": req.DeviceID,
						"items": []any{map[string]any{"action_name": "relay_off"}}, "next_page_token": "", "complete": true, "error": nil,
					}
					delete(page, missing)
					if signed {
						// Signed under the request's own identifiers.
						auth, err := mqttauth.Sign(page, contracts.ExportResponseMessageType, req.DeviceID, req.RequestID, now.UnixMilli(), "current-secret")
						if err != nil {
							t.Error(err)
							return
						}
						page["auth"] = auth
					}
					encoded, err := json.Marshal(page)
					if err != nil {
						t.Error(err)
						return
					}
					b.respondRaw(req.DeviceID, req.RequestID, encoded)
				}
				rows, err := c.ActionLog(context.Background(), ActionLogRequest{BoundedWindow: BoundedWindow{DeviceID: "edge-1", SinceMS: 1, Limit: 10}})
				binding := missing == "request_id" || missing == "device_id"
				switch {
				case missing == "":
					if err != nil || len(rows) != 1 {
						t.Fatalf("a full envelope with a null error = %d rows, %v; want the row", len(rows), err)
					}
				case err == nil:
					t.Fatalf("a page missing %s was read as %d rows, want the export failed", missing, len(rows))
				case signed && binding:
					if !errors.Is(err, ErrExportTimeout) || c.verifier.ReplayEntries() != 0 {
						t.Fatalf("a signed page without %s = %v with %d replay keys, want dropped unverified and the export failed at its bound", missing, err, c.verifier.ReplayEntries())
					}
				case errors.Is(err, ErrExportTimeout):
					t.Fatalf("a page missing %s left the export waiting, want it failed as malformed", missing)
				}
			})
		}
	}
}

// TestExportsArePacedPerDeviceAtTheRuntimesRate issues more requests to one
// device than its runtime's burst, interleaved with requests to another: on
// the pacing clock, neither device ever exceeds 2 a second with bursts of 10,
// and one device's backlog never delays the other's.
func TestExportsArePacedPerDeviceAtTheRuntimesRate(t *testing.T) {
	var mu sync.Mutex
	clock := time.Unix(1_787_000_000, 0)
	sent := map[string][]time.Time{}
	b := newFakeBroker()
	c, err := NewMQTTClient(b, WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(time.Hour),
		WithExportPacingClock(
			func() time.Time { mu.Lock(); defer mu.Unlock(); return clock },
			func(_ context.Context, d time.Duration) error {
				mu.Lock()
				clock = clock.Add(d)
				mu.Unlock()
				return nil
			},
		))
	if err != nil {
		t.Fatal(err)
	}
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		mu.Lock()
		sent[req.DeviceID] = append(sent[req.DeviceID], clock)
		mu.Unlock()
		b.respond(req.DeviceID, req.RequestID, healthItems(req.DeviceID, req.RequestID))
	}
	for i := range 40 {
		if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
			t.Fatal(err)
		}
		if i%4 == 0 {
			mu.Lock()
			before := clock
			mu.Unlock()
			if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-2"}); err != nil {
				t.Fatal(err)
			}
			mu.Lock()
			waited := clock.Sub(before)
			mu.Unlock()
			if waited > 0 {
				t.Fatalf("edge-2's request waited %v behind edge-1's backlog", waited)
			}
		}
	}
	for device, times := range sent {
		if over := overRuntimeRate(times); over >= 0 {
			t.Fatalf("%s's request %d was sent beyond 2 a second, burst 10", device, over)
		}
	}
}

// overRuntimeRate replays send times through the runtime's own admission, a
// token bucket of 2 a second and burst 10, written apart from the client's,
// and returns the index of the first send it would drop, or -1.
func overRuntimeRate(times []time.Time) int {
	tokens := 10.0
	var last time.Time
	for i, at := range times {
		if i > 0 {
			tokens = min(10, tokens+at.Sub(last).Seconds()*2)
		}
		last = at
		if tokens < 1-1e-9 {
			return i
		}
		tokens--
	}
	return -1
}

// TestAPacingWaitPastTheBoundEndsTheExport exhausts a device's burst, then
// asks for one more request with a wait shorter than the pacing would take:
// it ends with ErrExportTimeout without being sent.
func TestAPacingWaitPastTheBoundEndsTheExport(t *testing.T) {
	b := newFakeBroker()
	c, err := NewMQTTClient(b, WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(50*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	published := 0
	b.publishHook = func(_ string, payload []byte) {
		published++
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		b.respond(req.DeviceID, req.RequestID, healthItems(req.DeviceID, req.RequestID))
	}
	for range exportBurst {
		if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); !errors.Is(err, ErrExportTimeout) {
		t.Fatalf("a request paced past its bound = %v, want ErrExportTimeout", err)
	}
	if published != exportBurst {
		t.Fatalf("%d requests sent, want the burst of %d and none past the bound", published, exportBurst)
	}
}

// pastDeadline is a wait whose deadline has passed but whose timer has not
// yet fired, as in the moment between the two.
type pastDeadline struct{ context.Context }

func (pastDeadline) Deadline() (time.Time, bool) { return time.Now().Add(-time.Millisecond), true }
func (pastDeadline) Err() error                  { return nil }

// TestAResponseIsRefusedOnceTheDeadlineHasPassedBeforeItsTimerFires hands a
// response to a request whose deadline has passed though its timer has not
// fired: it is not delivered, so it can complete nothing and record no key.
func TestAResponseIsRefusedOnceTheDeadlineHasPassedBeforeItsTimerFires(t *testing.T) {
	c, err := NewMQTTClient(newFakeBroker())
	if err != nil {
		t.Fatal(err)
	}
	ch := make(chan exportResult, 1)
	c.mu.Lock()
	c.pending["late"] = pendingExport{ch: ch, wait: pastDeadline{context.Background()}}
	c.mu.Unlock()
	if c.deliver("late", ch, exportResult{}) {
		t.Fatal("a response was delivered after its request's deadline")
	}
}

// TestASharedSubscribeThatExpiresEndsEveryWaiterAsATimeout starts one export,
// whose subscribe stalls until its own bound, then a second export later, so
// the shared subscribe's bound passes before the second caller's: both end
// with ErrExportTimeout, never a bare context error.
func TestASharedSubscribeThatExpiresEndsEveryWaiterAsATimeout(t *testing.T) {
	const wait = 100 * time.Millisecond
	b := &stallingBroker{fakeBroker: newFakeBroker(), stallSubscribe: true}
	c, err := NewMQTTClient(b, WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(wait), instantPacing())
	if err != nil {
		t.Fatal(err)
	}
	errs := make(chan error, 2)
	go func() {
		_, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
		errs <- err
	}()
	time.Sleep(wait / 2)
	go func() {
		_, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
		errs <- err
	}()
	for range 2 {
		if err := <-errs; !errors.Is(err, ErrExportTimeout) {
			t.Fatalf("a waiter on an expired shared subscribe = %v, want ErrExportTimeout", err)
		}
	}
}
