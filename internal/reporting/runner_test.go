// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"context"
	"errors"
	"log/slog"
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
	runtime := &runtimeclient.FakeClient{HealthSnapshot: runtimeclient.HealthSnapshot{DeviceID: "edge-1"}}
	provider := &fakeProvider{err: errors.New("provider unavailable")}
	generator := newGenerator(t, runtime, provider)
	trigger := make(chan time.Time, 3)
	now := time.Date(2026, 6, 8, 7, 0, 0, 0, time.UTC)
	runner, err := NewWeeklyReportRunner(
		generator,
		WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: []string{"current-main"}},
		Schedule{Weekday: time.Monday, Hour: 8, Minute: 0, Location: time.UTC},
		RunnerOptions{Logger: slog.New(slog.NewTextHandler(testDiscard{}, nil)), Now: func() time.Time { return now }, After: func(time.Duration) <-chan time.Time { return trigger }},
	)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()
	trigger <- now
	for provider.calls == 0 {
		time.Sleep(time.Millisecond)
	}
	provider.err = nil
	provider.report = ProviderReport{Text: "ok", Provider: "fake", Model: "m"}
	trigger <- now
	for provider.calls < 2 {
		time.Sleep(time.Millisecond)
	}
	cancel()
	trigger <- now
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
}

func TestWeeklyReportRunnerCallsDeliverers(t *testing.T) {
	runtime := &runtimeclient.FakeClient{HealthSnapshot: runtimeclient.HealthSnapshot{DeviceID: "edge-1"}}
	provider := &fakeProvider{report: ProviderReport{Text: "all good", Provider: "fake", Model: "m"}}
	generator := newGenerator(t, runtime, provider)

	var delivered []WeeklyReportArtifact
	spy := &spyDeliverer{fn: func(a WeeklyReportArtifact) { delivered = append(delivered, a) }}

	trigger := make(chan time.Time, 2)
	now := time.Date(2026, 6, 8, 7, 0, 0, 0, time.UTC)
	runner, err := NewWeeklyReportRunner(
		generator,
		WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: []string{"current-main"}},
		Schedule{Weekday: time.Monday, Hour: 8, Minute: 0, Location: time.UTC},
		RunnerOptions{
			Logger:     slog.New(slog.NewTextHandler(testDiscard{}, nil)),
			Now:        func() time.Time { return now },
			After:      func(time.Duration) <-chan time.Time { return trigger },
			Deliverers: []Deliverer{spy},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	trigger <- now
	deadline := time.Now().Add(2 * time.Second)
	for len(delivered) == 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	cancel()
	<-done

	if len(delivered) == 0 {
		t.Fatal("expected deliverer to be called at least once")
	}
	if delivered[0].DeviceID != "edge-1" {
		t.Errorf("artifact DeviceID = %q, want edge-1", delivered[0].DeviceID)
	}
}

func TestWeeklyReportRunnerContinuesAfterDeliveryFailure(t *testing.T) {
	runtime := &runtimeclient.FakeClient{HealthSnapshot: runtimeclient.HealthSnapshot{DeviceID: "edge-1"}}
	provider := &fakeProvider{report: ProviderReport{Text: "all good", Provider: "fake", Model: "m"}}
	generator := newGenerator(t, runtime, provider)

	callCount := 0
	failing := &spyDeliverer{fn: func(_ WeeklyReportArtifact) { callCount++ }, err: errors.New("disk full")}

	trigger := make(chan time.Time, 3)
	now := time.Date(2026, 6, 8, 7, 0, 0, 0, time.UTC)
	runner, err := NewWeeklyReportRunner(
		generator,
		WeeklyReportRequest{DeviceID: "edge-1", SensorIDs: []string{"current-main"}},
		Schedule{Weekday: time.Monday, Hour: 8, Minute: 0, Location: time.UTC},
		RunnerOptions{
			Logger:     slog.New(slog.NewTextHandler(testDiscard{}, nil)),
			Now:        func() time.Time { return now },
			After:      func(time.Duration) <-chan time.Time { return trigger },
			Deliverers: []Deliverer{failing},
		},
	)
	if err != nil {
		t.Fatal(err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 1)
	go func() { done <- runner.Run(ctx) }()

	// Fire two ticks; runner must survive both even though delivery fails each time.
	trigger <- now
	deadline := time.Now().Add(2 * time.Second)
	for callCount < 1 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	trigger <- now
	deadline = time.Now().Add(2 * time.Second)
	for callCount < 2 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}

	cancel()
	trigger <- now
	if err := <-done; !errors.Is(err, context.Canceled) {
		t.Fatalf("got %v, want context.Canceled", err)
	}
	if callCount < 2 {
		t.Errorf("deliverer called %d times, want at least 2", callCount)
	}
}

type spyDeliverer struct {
	fn  func(WeeklyReportArtifact)
	err error
}

func (s *spyDeliverer) Deliver(_ context.Context, a WeeklyReportArtifact) error {
	if s.fn != nil {
		s.fn(a)
	}
	return s.err
}

type testDiscard struct{}

func (testDiscard) Write(p []byte) (int, error) { return len(p), nil }
