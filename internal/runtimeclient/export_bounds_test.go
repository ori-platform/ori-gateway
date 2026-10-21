// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package runtimeclient

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log/slog"
	"math/rand/v2"
	"slices"
	"strings"
	"sync"
	"sync/atomic"
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
			// At most its own bucket's interval; edge-1's backlog is many.
			if interval := time.Duration(float64(time.Second) / exportRatePerSecond); waited > interval {
				t.Fatalf("edge-2's request waited %v, over its own interval %v: it waited behind edge-1's backlog", waited, interval)
			}
		}
	}
	// After an idle spell refills both bursts, edge-1 spends its whole burst:
	// edge-2's next request is sent at once, from its own.
	mu.Lock()
	clock = clock.Add(10 * time.Second)
	mu.Unlock()
	for range 8 {
		if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
			t.Fatal(err)
		}
	}
	mu.Lock()
	before := clock
	mu.Unlock()
	if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-2"}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	waited := clock.Sub(before)
	mu.Unlock()
	if waited != 0 {
		t.Fatalf("edge-2's request waited %v after edge-1 spent its burst, want none: the devices share a bucket", waited)
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

// TestAPacingWaitPastTheBoundEndsTheExport asks a device whose bucket is still
// empty, as at a start, for a request with a wait shorter than the pacing
// would take: it ends with ErrExportTimeout without being sent.
func TestAPacingWaitPastTheBoundEndsTheExport(t *testing.T) {
	b := newFakeBroker()
	// On the real clock: the pacing wait itself is under test.
	c, err := NewMQTTClient(b, WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(50*time.Millisecond),
		WithExportPacingClock(time.Now, sleepContext))
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
	// A device's bucket starts empty, so its first request already waits
	// longer than this bound.
	if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); !errors.Is(err, ErrExportTimeout) {
		t.Fatalf("a request paced past its bound = %v, want ErrExportTimeout", err)
	}
	if published != 0 {
		t.Fatalf("%d requests sent past the pacing bound, want none", published)
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
	const wait = 400 * time.Millisecond
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
	time.Sleep(wait / 4)
	go func() {
		_, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
		errs <- err
	}()
	for range 2 {
		if err := <-errs; !errors.Is(err, ErrExportTimeout) {
			t.Fatalf("a waiter on an expired shared subscribe = %v, want ErrExportTimeout", err)
		}
	}
	// Both waited on the one subscribe, so the second really waited on a
	// shared attempt whose bound came before its own.
	if n := b.subscribes.Load(); n != 1 {
		t.Fatalf("%d subscribes made, want the one shared", n)
	}
}

// pacedSends drives n health exports to edge-1 through a client built with
// the given options and returns the pacing clock's reading at each publish,
// with idle gaps that let the burst refill every 30 requests, and the reading
// before the first.
func pacedSends(t *testing.T, n int, now func() time.Time, sleep func(context.Context, time.Duration) error, opts ...MQTTClientOption) (time.Time, []time.Time) {
	t.Helper()
	b := newFakeBroker()
	var sent []time.Time
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		sent = append(sent, now())
		b.respond(req.DeviceID, req.RequestID, map[string]any{"export_type": "health", "items": []any{map[string]any{"device_id": req.DeviceID}}})
	}
	ids := 0
	opts = append([]MQTTClientOption{WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(time.Hour),
		WithRequestIDFunc(func() (string, error) { ids++; return fmt.Sprintf("paced-%d", ids), nil })}, opts...)
	c, err := NewMQTTClient(b, opts...)
	if err != nil {
		t.Fatal(err)
	}
	start := now()
	for i := range n {
		if i%30 == 29 {
			_ = sleep(context.Background(), 10*time.Second)
		}
		if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
			t.Fatalf("export %d = %v", i, err)
		}
	}
	if len(sent) != n {
		t.Fatalf("%d requests published, want %d", len(sent), n)
	}
	return start, sent
}

// TestPacedExportsSurviveArrivalJitterAtTheRuntime sends a long run of
// exports through a client, with idle gaps that let the burst refill. Every
// send is within 1.6 a second and burst 8, starting empty. Replayed as the
// runtime receives them, each delayed by up to 100 ms, in order, into the
// runtime's own bucket of 2 a second and burst 10, starting empty as after a
// burst just before a gateway restart, none is dropped, since the runtime
// judges arrivals, not sends.
func TestPacedExportsSurviveArrivalJitterAtTheRuntime(t *testing.T) {
	now, sleep := instantClock()
	start, sent := pacedSends(t, 120, now, sleep, WithExportPacingClock(now, sleep))
	tokens, last := 0.0, start
	for i, at := range sent {
		tokens = min(8, tokens+at.Sub(last).Seconds()*1.6)
		last = at
		if tokens < 1-1e-9 {
			t.Fatalf("request %d was sent beyond 1.6 a second and burst 8", i)
		}
		tokens--
	}
	rng := rand.New(rand.NewPCG(7, 11))
	for run := range 500 {
		arrivals := make([]time.Time, len(sent))
		for i, at := range sent {
			arrivals[i] = at.Add(time.Duration(rng.IntN(100_000)) * time.Microsecond)
			if i > 0 && arrivals[i].Before(arrivals[i-1]) {
				arrivals[i] = arrivals[i-1]
			}
		}
		tokens := 0.0
		last := arrivals[0].Add(-time.Duration(float64(time.Second) / 1.6))
		for i, at := range arrivals {
			tokens = min(10, tokens+at.Sub(last).Seconds()*2)
			last = at
			if tokens < 1-1e-9 {
				t.Fatalf("run %d: request %d arrived beyond the runtime's admission and would be dropped", run, i)
			}
			tokens--
		}
	}
}

// TestProductionPacingWaitsOnTheRealClock builds a client with no pacing
// option on the clock production uses: the first request to a device, whose
// bucket starts empty, waits about one interval at 1.6 a second. The clock is
// monotonic and its wait blocks rather than spins.
func TestProductionPacingWaitsOnTheRealClock(t *testing.T) {
	now, sleep := productionPacingClock()
	if !strings.Contains(now().String(), " m=") {
		t.Fatalf("the production pacing clock reads %v, want a monotonic reading", now())
	}
	began := time.Now()
	cpu, measured := processCPU(t)
	if err := sleep(context.Background(), 100*time.Millisecond); err != nil {
		t.Fatal(err)
	}
	if slept := time.Since(began); slept < 100*time.Millisecond {
		t.Fatalf("the production pacing wait returned after %v, want 100ms", slept)
	}
	if after, _ := processCPU(t); measured && after-cpu > 50*time.Millisecond {
		t.Fatalf("the production pacing wait spent %v of CPU over 100ms, want it to block", after-cpu)
	}
	pacingClock = productionPacingClock
	t.Cleanup(func() { pacingClock = instantClock })
	start, sent := pacedSends(t, 1, time.Now, sleepContext)
	if waited := sent[0].Sub(start); waited < 600*time.Millisecond {
		t.Fatalf("the first request waited %v, want about 625ms on the real clock", waited)
	}
}

// inTimePublishBroker answers every publish at once with a signed response,
// then holds the publish's return until its wait has expired, as a broker
// whose completion is slow while the runtime is fast.
type inTimePublishBroker struct {
	*fakeBroker
	t   *testing.T
	now time.Time
}

func (b *inTimePublishBroker) Publish(ctx context.Context, _ string, _ byte, _ bool, payload []byte) error {
	var req exportRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		b.t.Error(err)
		return err
	}
	b.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(b.t, healthItems(req.DeviceID, req.RequestID), "current-secret", b.now))
	<-ctx.Done()
	return nil
}

// TestAResponseHandedOverInTimeCompletesTheExport delivers a verified response
// before the deadline while the publish only returns at it, many times over:
// every export succeeds, since the response was handed over in time, whichever
// the wait's select sees first.
func TestAResponseHandedOverInTimeCompletesTheExport(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	b := &inTimePublishBroker{fakeBroker: newFakeBroker(), t: t, now: now}
	n := 0
	c, err := NewMQTTClient(b, WithRequestIDFunc(func() (string, error) { n++; return fmt.Sprintf("in-time-%d", n), nil }),
		WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	for i := range 60 {
		if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
			t.Fatalf("exchange %d = %v, want the response handed over in time", i, err)
		}
	}
}

// TestPacingNeverSpinsOnAnInexactRate paces at rates whose arithmetic is not
// exact in binary on a clock that only a wait advances: every wait moves the
// clock on, so acquiring never spins.
func TestPacingNeverSpinsOnAnInexactRate(t *testing.T) {
	for _, rate := range []float64{exportRatePerSecond, 1.8, 2.2, 0.7, 1.3} {
		now, sleep := instantClock()
		zeroWaits := 0
		pacer := &exportPacer{now: now, rate: rate, burst: exportBurst, sleep: func(ctx context.Context, d time.Duration) error {
			if d <= 0 {
				zeroWaits++
				if zeroWaits > 1000 {
					return errors.New("the pacer is spinning on waits that never move the clock")
				}
			}
			return sleep(ctx, d)
		}, buckets: map[string]*paceBucket{}}
		for range 200 {
			if err := pacer.acquire(context.Background(), "edge-1"); err != nil {
				t.Fatalf("rate %v: %v", rate, err)
			}
		}
		if zeroWaits != 0 {
			t.Fatalf("rate %v: %d waits of no time, want every wait to move the clock", rate, zeroWaits)
		}
	}
}

// TestEndingTheLifetimeAbandonsAStalledSubscribe stalls a subscribe, with an
// export wait far longer than the test, then ends the client's lifetime, as
// the gateway's stop does: the shared subscribe ends at once rather than at
// its bound.
func TestEndingTheLifetimeAbandonsAStalledSubscribe(t *testing.T) {
	b := &stallingBroker{fakeBroker: newFakeBroker(), stallSubscribe: true}
	lifetime, end := context.WithCancel(context.Background())
	c, err := NewMQTTClient(b, WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(time.Hour), WithLifetime(lifetime))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancel()
	_, _ = c.Health(ctx, HealthRequest{DeviceID: "edge-1"})
	end()
	deadline := time.Now().Add(2 * time.Second)
	for {
		c.mu.Lock()
		inFlight := len(c.subscribing)
		c.mu.Unlock()
		if inFlight == 0 {
			return
		}
		if time.Now().After(deadline) {
			t.Fatal("a stalled subscribe outlived the client's lifetime")
		}
		time.Sleep(time.Millisecond)
	}
}

// failingPublishBroker answers every publish at once with a signed response,
// then reports the publish failed: after its wait expires, as a broker whose
// acknowledgement never came, or at once, as one whose connection dropped.
type failingPublishBroker struct {
	*inTimePublishBroker
	afterTheWait bool
}

func (b *failingPublishBroker) Publish(ctx context.Context, _ string, _ byte, _ bool, payload []byte) error {
	var req exportRequest
	if err := json.Unmarshal(payload, &req); err != nil {
		b.t.Error(err)
		return err
	}
	b.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(b.t, healthItems(req.DeviceID, req.RequestID), "current-secret", b.now))
	if b.afterTheWait {
		<-ctx.Done()
		return ctx.Err()
	}
	return errors.New("broker: publish: connection lost")
}

// TestAResponseHandedOverInTimeSurvivesAFailedPublish delivers a verified
// response before the deadline, then fails the publish that carried its
// request, by timing out or with another error: the export completes, since
// the runtime answered in time and its replay key is recorded.
func TestAResponseHandedOverInTimeSurvivesAFailedPublish(t *testing.T) {
	for _, afterTheWait := range []bool{true, false} {
		t.Run(fmt.Sprintf("after the wait %v", afterTheWait), func(t *testing.T) {
			now := time.UnixMilli(1787000003000)
			b := &failingPublishBroker{inTimePublishBroker: &inTimePublishBroker{fakeBroker: newFakeBroker(), t: t, now: now}, afterTheWait: afterTheWait}
			c, err := NewMQTTClient(b, WithMessageAuth("current-secret", "", func() time.Time { return now }),
				WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(30*time.Millisecond))
			if err != nil {
				t.Fatal(err)
			}
			if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); err != nil {
				t.Fatalf("Health = %v, want the response handed over in time", err)
			}
			if n := c.verifier.ReplayEntries(); n != 1 {
				t.Fatalf("%d replay keys recorded, want the one answer's", n)
			}
		})
	}
}

// blockingHandler is a logger that blocks every record until released.
type blockingHandler struct {
	entered, release chan struct{}
	once             sync.Once
}

func newBlockingHandler() *blockingHandler {
	return &blockingHandler{entered: make(chan struct{}), release: make(chan struct{})}
}

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *blockingHandler) Handle(context.Context, slog.Record) error {
	h.once.Do(func() { close(h.entered) })
	<-h.release
	return nil
}

func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler      { return h }

// TestATimeoutNeverWaitsOnTheLogger ends an export at its bound while pacing,
// subscribing and waiting for a response, behind a logger that blocks: each
// returns at its bound, and the record reaches the logger once it is
// released. A response refused in the broker callback returns at once too.
func TestATimeoutNeverWaitsOnTheLogger(t *testing.T) {
	const wait = 20 * time.Millisecond
	for _, phase := range []string{"pacing", "subscribe", "response"} {
		t.Run(phase, func(t *testing.T) {
			h := newBlockingHandler()
			b := &stallingBroker{fakeBroker: newFakeBroker(), stallSubscribe: phase == "subscribe"}
			opts := []MQTTClientOption{WithLogger(slog.New(h)), WithExportWait(wait)}
			if phase == "pacing" {
				opts = append(opts, WithExportPacingClock(productionPacingClock()))
			}
			c, err := NewMQTTClient(b, opts...)
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
			case <-time.After(500 * time.Millisecond):
				close(h.release)
				t.Fatalf("the export was held past its %v bound by a logger that blocks", wait)
			}
			if n := pendingCount(c); n != 0 {
				t.Fatalf("%d exports still pending behind the logger, want none", n)
			}
			select {
			case <-h.entered:
			case <-time.After(time.Second):
				t.Fatal("the timeout was never logged")
			}
			close(h.release)
		})
	}
	t.Run("refused response", func(t *testing.T) {
		h := newBlockingHandler()
		defer close(h.release)
		now := time.UnixMilli(1787000003000)
		b := newFakeBroker()
		c, err := NewMQTTClient(b, WithMessageAuth("current-secret", "", func() time.Time { return now }),
			WithLogger(slog.New(h)), WithExportWait(time.Second), WithRequestIDFunc(func() (string, error) { return "held-1", nil }))
		if err != nil {
			t.Fatal(err)
		}
		forged := make(chan struct{})
		b.publishHook = func(_ string, payload []byte) {
			var req exportRequest
			if err := json.Unmarshal(payload, &req); err != nil {
				t.Error(err)
				return
			}
			go func() {
				defer close(forged)
				for range 3 {
					b.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(t, healthItems(req.DeviceID, req.RequestID), "forged-secret", now))
				}
			}()
		}
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		go func() { _, _ = c.Health(ctx, HealthRequest{DeviceID: "edge-1"}) }()
		select {
		case <-forged:
		case <-time.After(500 * time.Millisecond):
			t.Fatal("the broker callback was held by a logger that blocks")
		}
		select {
		case <-h.entered:
		case <-time.After(time.Second):
			t.Fatal("the refused responses were never logged")
		}
		cancel()
	})
}

// recordingHandler blocks its first record until released, then records
// every message in the order written.
type recordingHandler struct {
	entered, release chan struct{}
	once             sync.Once
	mu               sync.Mutex
	messages         []string
	attrs            []string
}

func (h *recordingHandler) Enabled(context.Context, slog.Level) bool { return true }

func (h *recordingHandler) Handle(_ context.Context, r slog.Record) error {
	h.once.Do(func() { close(h.entered); <-h.release })
	h.mu.Lock()
	defer h.mu.Unlock()
	h.messages = append(h.messages, r.Message)
	r.Attrs(func(a slog.Attr) bool { h.attrs = append(h.attrs, a.String()); return true })
	return nil
}

func (h *recordingHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *recordingHandler) WithGroup(string) slog.Handler      { return h }

// TestClientLogLinesKeepTheirOrderAndAreBoundedBehindABlockedLogger queues 300
// records while the logger blocks on the first: the 64 queued behind it are
// written in the order they were decided, the other 235 are counted, and the
// count is written last.
func TestClientLogLinesKeepTheirOrderAndAreBoundedBehindABlockedLogger(t *testing.T) {
	h := &recordingHandler{entered: make(chan struct{}), release: make(chan struct{})}
	log := slog.New(&queuedHandler{inner: h, notes: &noteLog{}})
	log.Warn("line 0")
	<-h.entered
	for i := 1; i < 300; i++ {
		log.Warn(fmt.Sprintf("line %d", i))
	}
	close(h.release)
	deadline := time.Now().Add(2 * time.Second)
	for {
		h.mu.Lock()
		n := len(h.messages)
		h.mu.Unlock()
		if n >= 66 || time.Now().After(deadline) {
			break
		}
		time.Sleep(time.Millisecond)
	}
	h.mu.Lock()
	defer h.mu.Unlock()
	if len(h.messages) != 66 {
		t.Fatalf("%d records written, want the one being written, the 64 queued and the count", len(h.messages))
	}
	for i := range 65 {
		if want := fmt.Sprintf("line %d", i); h.messages[i] != want {
			t.Fatalf("record %d is %q, want %q: records were written out of order", i, h.messages[i], want)
		}
	}
	if !strings.Contains(h.messages[65], "dropped") || !slices.Contains(h.attrs, "dropped=235") {
		t.Fatalf("last record %q with %v, want the count of the 235 dropped", h.messages[65], h.attrs)
	}
}

// TestAResponseHandedOverAsItsRequestIsReleasedIsNeverDiscarded fails the
// publish that carries a request, and hands a verified response over at the
// moment the request is released, before the release takes the lock: it is
// handed over first and completes the export, never recorded and then
// discarded.
func TestAResponseHandedOverAsItsRequestIsReleasedIsNeverDiscarded(t *testing.T) {
	now := time.UnixMilli(1787000003000)
	b := newFakeBroker()
	failing := &stallingBroker{fakeBroker: b}
	c, err := NewMQTTClient(&publishFailsBroker{failing}, WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithLogger(slog.New(slog.DiscardHandler)), WithExportWait(time.Second),
		WithRequestIDFunc(func() (string, error) { return "released-1", nil }))
	if err != nil {
		t.Fatal(err)
	}
	var once sync.Once
	c.beforeRelease = func() {
		once.Do(func() {
			b.respondRaw("edge-1", "released-1", signedExportPayload(t, healthItems("edge-1", "released-1"), "current-secret", now))
		})
	}
	_, err = c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"})
	if err != nil {
		t.Fatalf("Health = %v with %d replay keys recorded, want the response handed over before the release", err, c.verifier.ReplayEntries())
	}
	if n := c.verifier.ReplayEntries(); n != 1 {
		t.Fatalf("%d replay keys recorded, want the one answer's", n)
	}
}

// publishFailsBroker reports every publish failed at once, as a broker whose
// connection dropped.
type publishFailsBroker struct{ *stallingBroker }

func (b *publishFailsBroker) Publish(context.Context, string, byte, bool, []byte) error {
	return errors.New("broker: publish: connection lost")
}

// TestAResponseRefusedBelowTheFloorNeverHoldsTheCallback answers an export
// with a response signed before the client's verifier started, behind a
// logger that blocks: the broker callback returns at once, and the floor
// warning is still logged.
func TestAResponseRefusedBelowTheFloorNeverHoldsTheCallback(t *testing.T) {
	h := newBlockingHandler()
	defer close(h.release)
	now := time.UnixMilli(1787000003000)
	b := newFakeBroker()
	c, err := NewMQTTClient(b, WithMessageAuth("current-secret", "", func() time.Time { return now }),
		WithLogger(slog.New(h)), WithExportWait(time.Second), WithRequestIDFunc(func() (string, error) { return "floor-1", nil }))
	if err != nil {
		t.Fatal(err)
	}
	answered := make(chan struct{})
	b.publishHook = func(_ string, payload []byte) {
		var req exportRequest
		if err := json.Unmarshal(payload, &req); err != nil {
			t.Error(err)
			return
		}
		go func() {
			defer close(answered)
			b.respondRaw(req.DeviceID, req.RequestID, signedExportPayload(t, healthItems(req.DeviceID, req.RequestID), "current-secret", now.Add(-2*time.Second)))
		}()
	}
	go func() { _, _ = c.Health(t.Context(), HealthRequest{DeviceID: "edge-1"}) }()
	select {
	case <-answered:
	case <-time.After(500 * time.Millisecond):
		t.Fatal("the broker callback was held by the verifier's floor warning behind a logger that blocks")
	}
	select {
	case <-h.entered:
	case <-time.After(time.Second):
		t.Fatal("the refusal below the floor was never logged")
	}
}

// serialHandler is a valid handler that holds one lock for its level check and
// for writing, and blocks writing its first record until released.
type serialHandler struct {
	mu               sync.Mutex
	entered, release chan struct{}
	once             sync.Once
	levels           atomic.Int32
}

func (h *serialHandler) Enabled(context.Context, slog.Level) bool {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.levels.Add(1)
	return true
}

func (h *serialHandler) Handle(context.Context, slog.Record) error {
	h.mu.Lock()
	defer h.mu.Unlock()
	h.once.Do(func() { close(h.entered) })
	<-h.release
	return nil
}

func (h *serialHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *serialHandler) WithGroup(string) slog.Handler      { return h }

// TestALoggerThatLocksItsLevelCheckNeverHoldsAnExport blocks a logger that
// holds one lock for its level check and its writing, then ends an export at
// its bound: the export returns at its bound, since the level is asked off
// the caller's path, by the drainer.
func TestALoggerThatLocksItsLevelCheckNeverHoldsAnExport(t *testing.T) {
	h := &serialHandler{entered: make(chan struct{}), release: make(chan struct{})}
	c, err := NewMQTTClient(newFakeBroker(), WithLogger(slog.New(h)), WithExportWait(20*time.Millisecond))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := c.Health(context.Background(), HealthRequest{DeviceID: "edge-1"}); !errors.Is(err, ErrExportTimeout) {
		t.Fatalf("Health = %v, want ErrExportTimeout", err)
	}
	<-h.entered
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
	case <-time.After(500 * time.Millisecond):
		close(h.release)
		t.Fatal("an export was held past its bound by the logger's level check")
	}
	close(h.release)
	waitFor := time.Now().Add(time.Second)
	for h.levels.Load() < 2 && time.Now().Before(waitFor) {
		time.Sleep(time.Millisecond)
	}
	if h.levels.Load() < 2 {
		t.Fatal("the drainer never asked the logger's level before writing")
	}
}

// errorOnlyHandler enables only errors, holds its first level check until
// released, and counts what it is asked and what it writes.
type errorOnlyHandler struct {
	entered, release chan struct{}
	once             sync.Once
	asked, written   atomic.Int32
}

func (h *errorOnlyHandler) Enabled(_ context.Context, level slog.Level) bool {
	h.once.Do(func() { close(h.entered); <-h.release })
	h.asked.Add(1)
	return level >= slog.LevelError
}

func (h *errorOnlyHandler) Handle(context.Context, slog.Record) error {
	h.written.Add(1)
	return nil
}

func (h *errorOnlyHandler) WithAttrs([]slog.Attr) slog.Handler { return h }
func (h *errorOnlyHandler) WithGroup(string) slog.Handler      { return h }

// TestALoggerThatEnablesOnlyErrorsIsWrittenNothing queues 300 warnings, more
// than wait, behind a logger that enables only errors: none is written, nor
// the count of those dropped, which is a warning too.
func TestALoggerThatEnablesOnlyErrorsIsWrittenNothing(t *testing.T) {
	h := &errorOnlyHandler{entered: make(chan struct{}), release: make(chan struct{})}
	log := slog.New(&queuedHandler{inner: h, notes: &noteLog{}})
	log.Warn("line 0")
	<-h.entered
	for i := 1; i < 300; i++ {
		log.Warn(fmt.Sprintf("line %d", i))
	}
	close(h.release)
	// The one being checked, the 64 queued and the count line.
	deadline := time.Now().Add(2 * time.Second)
	for h.asked.Load() < 66 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if n := h.asked.Load(); n != 66 {
		t.Fatalf("the logger was asked its level %d times, want 66", n)
	}
	if n := h.written.Load(); n != 0 {
		t.Fatalf("%d records written to a logger that enables only errors, want none", n)
	}
}
