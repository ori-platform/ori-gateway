// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"bytes"
	"context"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/runtimeclient"
)

func TestNextRunUsesSiteLocalSchedule(t *testing.T) {
	loc, err := time.LoadLocation("Africa/Lagos")
	if err != nil {
		t.Fatal(err)
	}
	schedule := Schedule{Weekday: time.Monday, Hour: 8, Minute: 0, Location: loc}
	cases := []struct {
		name string
		now  time.Time
		want string
	}{
		{name: "before same day", now: time.Date(2026, 6, 8, 7, 0, 0, 0, loc), want: "2026-06-08T08:00:00+01:00"},
		{name: "at exact time rolls forward", now: time.Date(2026, 6, 8, 8, 0, 0, 0, loc), want: "2026-06-15T08:00:00+01:00"},
		{name: "after same day rolls forward", now: time.Date(2026, 6, 8, 9, 0, 0, 0, loc), want: "2026-06-15T08:00:00+01:00"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			got := NextRun(tc.now, schedule).Format(time.RFC3339)
			if got != tc.want {
				t.Fatalf("got %s, want %s", got, tc.want)
			}
		})
	}
}

func TestWeeklyReportRunnerLogsFailureAndContinues(t *testing.T) {
	provider := &scriptedProvider{outcomes: []error{errors.New("provider unavailable"), nil}, calls: make(chan struct{}, 3)}
	spy := &spyDeliverer{delivered: make(chan WeeklyReportArtifact, 3)}
	h := startRunner(t, provider, spy)

	h.trigger <- h.now
	receive(t, provider.calls, "first provider call")
	h.trigger <- h.now
	receive(t, provider.calls, "second provider call")
	receive(t, spy.delivered, "delivery after the provider recovered")
	logs := h.stop(t)

	if got := strings.Count(logs, "weekly report generation failed"); got != 1 {
		t.Fatalf("generation failures logged = %d, want 1:\n%s", got, logs)
	}
	if !strings.Contains(logs, "provider unavailable") {
		t.Fatalf("generation failure logged without its error:\n%s", logs)
	}
	assertNoMore(t, provider.calls, spy.delivered)
}

func TestWeeklyReportRunnerCallsDeliverers(t *testing.T) {
	provider := &scriptedProvider{outcomes: []error{nil}, calls: make(chan struct{}, 3)}
	spy := &spyDeliverer{delivered: make(chan WeeklyReportArtifact, 3)}
	h := startRunner(t, provider, spy)

	h.trigger <- h.now
	artifact := receive(t, spy.delivered, "delivery")
	h.stop(t)

	if artifact.DeviceID != "edge-1" {
		t.Errorf("artifact DeviceID = %q, want edge-1", artifact.DeviceID)
	}
	if artifact.Text != "all good" {
		t.Errorf("artifact Text = %q, want the provider's report", artifact.Text)
	}
	<-provider.calls
	assertNoMore(t, provider.calls, spy.delivered)
}

func TestWeeklyReportRunnerContinuesAfterDeliveryFailure(t *testing.T) {
	provider := &scriptedProvider{outcomes: []error{nil, nil}, calls: make(chan struct{}, 3)}
	failing := &spyDeliverer{delivered: make(chan WeeklyReportArtifact, 3), err: errors.New("disk full")}
	h := startRunner(t, provider, failing)

	// Fire two ticks; runner must survive both even though delivery fails each time.
	h.trigger <- h.now
	receive(t, failing.delivered, "first delivery")
	h.trigger <- h.now
	receive(t, failing.delivered, "second delivery")
	logs := h.stop(t)

	if got := strings.Count(logs, "weekly report delivery failed"); got != 2 {
		t.Fatalf("delivery failures logged = %d, want 2:\n%s", got, logs)
	}
	<-provider.calls
	<-provider.calls
	assertNoMore(t, provider.calls, failing.delivered)
}

// runnerHarness runs a WeeklyReportRunner whose schedule fires only when the
// test sends on trigger. Its log is read only after Run has returned, which
// orders every write before the read.
type runnerHarness struct {
	trigger chan time.Time
	now     time.Time
	cancel  context.CancelFunc
	done    chan error
	logs    *bytes.Buffer
}

func startRunner(t *testing.T, provider *scriptedProvider, deliverer Deliverer) *runnerHarness {
	t.Helper()
	runtime := &runtimeclient.FakeClient{HealthSnapshot: runtimeclient.HealthSnapshot{DeviceID: "edge-1"}}
	generator, err := NewWeeklyReportGenerator(runtime, provider, WithNow(fixedNow))
	if err != nil {
		t.Fatal(err)
	}
	h := &runnerHarness{
		trigger: make(chan time.Time, 1),
		now:     time.Date(2026, 6, 8, 7, 0, 0, 0, time.UTC),
		done:    make(chan error, 1),
		logs:    &bytes.Buffer{},
	}
	runner, err := NewWeeklyReportRunner(
		generator,
		WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: []string{"current-main"}},
		Schedule{Weekday: time.Monday, Hour: 8, Minute: 0, Location: time.UTC},
		RunnerOptions{
			Logger:     slog.New(slog.NewTextHandler(h.logs, nil)),
			Now:        func() time.Time { return h.now },
			After:      func(time.Duration) <-chan time.Time { return h.trigger },
			Deliverers: []Deliverer{deliverer},
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	h.cancel = cancel
	go func() { h.done <- runner.Run(ctx) }()
	t.Cleanup(cancel)
	return h
}

// stop cancels the runner, which is then waiting on a trigger that never
// fires, and returns its log once Run has returned context.Canceled.
func (h *runnerHarness) stop(t *testing.T) string {
	t.Helper()
	h.cancel()
	if err := receive(t, h.done, "Run to return"); !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	return h.logs.String()
}

// receive waits for the event on ch. The bound only turns a hang into a
// failure; no assertion depends on how long the event took.
func receive[T any](t *testing.T, ch <-chan T, what string) T {
	t.Helper()
	select {
	case v := <-ch:
		return v
	case <-time.After(10 * time.Second):
		t.Fatalf("timed out waiting for %s", what)
		panic("unreachable")
	}
}

// assertNoMore checks, once Run has returned, that no call or delivery
// happened beyond those the test received.
func assertNoMore(t *testing.T, calls <-chan struct{}, delivered <-chan WeeklyReportArtifact) {
	t.Helper()
	if n := len(calls); n != 0 {
		t.Fatalf("%d unexpected provider calls", n)
	}
	if n := len(delivered); n != 0 {
		t.Fatalf("%d unexpected deliveries", n)
	}
}

// scriptedProvider answers its nth call with outcomes[n], a nil outcome being
// a report, and announces each call on calls. Only the runner's goroutine
// touches outcomes.
type scriptedProvider struct {
	outcomes []error
	calls    chan struct{}
}

func (p *scriptedProvider) GenerateWeeklyReport(context.Context, WeeklyReportInput) (ProviderReport, error) {
	if len(p.outcomes) == 0 {
		return ProviderReport{}, errors.New("scriptedProvider: unscripted call")
	}
	err := p.outcomes[0]
	p.outcomes = p.outcomes[1:]
	p.calls <- struct{}{}
	if err != nil {
		return ProviderReport{}, err
	}
	return ProviderReport{Text: "all good", Provider: "fake", Model: "m"}, nil
}

type spyDeliverer struct {
	delivered chan WeeklyReportArtifact
	err       error
}

func (s *spyDeliverer) Deliver(_ context.Context, a WeeklyReportArtifact) error {
	s.delivered <- a
	return s.err
}
