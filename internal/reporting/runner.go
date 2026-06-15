// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package reporting

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"time"
)

// Schedule identifies the weekly local time for report generation.
type Schedule struct {
	Weekday  time.Weekday
	Hour     int
	Minute   int
	Location *time.Location
}

// NewSchedule validates and constructs a weekly report schedule.
func NewSchedule(day string, clock string, timezone string) (Schedule, error) {
	weekday, err := parseWeekday(day)
	if err != nil {
		return Schedule{}, err
	}
	parsedClock, err := time.Parse("15:04", strings.TrimSpace(clock))
	if err != nil {
		return Schedule{}, fmt.Errorf("reporting: weekly_report.time must use HH:MM 24-hour format")
	}
	loc, err := time.LoadLocation(strings.TrimSpace(timezone))
	if err != nil {
		return Schedule{}, fmt.Errorf("reporting: weekly_report.timezone %q is invalid: %w", timezone, err)
	}
	return Schedule{Weekday: weekday, Hour: parsedClock.Hour(), Minute: parsedClock.Minute(), Location: loc}, nil
}

// RunnerOptions configures WeeklyReportRunner.
type RunnerOptions struct {
	Logger     *slog.Logger
	Now        func() time.Time
	After      func(time.Duration) <-chan time.Time
	Deliverers []Deliverer
}

// WeeklyReportRunner supervises scheduled report generation.
type WeeklyReportRunner struct {
	generator  *WeeklyReportGenerator
	request    WeeklyReportRequest
	schedule   Schedule
	logger     *slog.Logger
	now        func() time.Time
	after      func(time.Duration) <-chan time.Time
	deliverers []Deliverer
}

// NewWeeklyReportRunner constructs a scheduled report runner.
func NewWeeklyReportRunner(generator *WeeklyReportGenerator, request WeeklyReportRequest, schedule Schedule, opts RunnerOptions) (*WeeklyReportRunner, error) {
	if generator == nil {
		return nil, fmt.Errorf("reporting: weekly report generator must not be nil")
	}
	if schedule.Location == nil {
		return nil, fmt.Errorf("reporting: weekly report schedule location must not be nil")
	}
	logger := opts.Logger
	if logger == nil {
		logger = slog.Default()
	}
	now := opts.Now
	if now == nil {
		now = time.Now
	}
	after := opts.After
	if after == nil {
		after = time.After
	}
	deliverers := opts.Deliverers
	if len(deliverers) == 0 {
		deliverers = []Deliverer{&LogDeliverer{Logger: logger}}
	}
	for i, d := range deliverers {
		if d == nil {
			return nil, fmt.Errorf("reporting: deliverer at index %d must not be nil", i)
		}
	}
	return &WeeklyReportRunner{
		generator:  generator,
		request:    request,
		schedule:   schedule,
		logger:     logger,
		now:        now,
		after:      after,
		deliverers: deliverers,
	}, nil
}

// Run generates reports on the configured weekly schedule until ctx is canceled.
func (r *WeeklyReportRunner) Run(ctx context.Context) error {
	for {
		now := r.now()
		next := NextRun(now, r.schedule)
		wait := next.Sub(now)
		if wait < 0 {
			wait = 0
		}
		r.logger.Info("weekly report scheduled", "next_run", next.Format(time.RFC3339), "device_id", r.request.DeviceID)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-r.after(wait):
		}

		artifact, err := r.generator.Generate(ctx, r.request)
		if err != nil {
			r.logger.Warn("weekly report generation failed", "device_id", r.request.DeviceID, "error", err)
			continue
		}
		for _, d := range r.deliverers {
			if err := d.Deliver(ctx, artifact); err != nil {
				r.logger.Warn("weekly report delivery failed", "device_id", artifact.DeviceID, "error", err)
			}
		}
	}
}

// NextRun returns the next scheduled instant strictly after now.
func NextRun(now time.Time, schedule Schedule) time.Time {
	local := now.In(schedule.Location)
	candidate := time.Date(local.Year(), local.Month(), local.Day(), schedule.Hour, schedule.Minute, 0, 0, schedule.Location)
	daysUntil := (int(schedule.Weekday) - int(local.Weekday()) + 7) % 7
	candidate = candidate.AddDate(0, 0, daysUntil)
	if !candidate.After(local) {
		candidate = candidate.AddDate(0, 0, 7)
	}
	return candidate
}

func parseWeekday(day string) (time.Weekday, error) {
	switch strings.ToLower(strings.TrimSpace(day)) {
	case "sunday":
		return time.Sunday, nil
	case "monday":
		return time.Monday, nil
	case "tuesday":
		return time.Tuesday, nil
	case "wednesday":
		return time.Wednesday, nil
	case "thursday":
		return time.Thursday, nil
	case "friday":
		return time.Friday, nil
	case "saturday":
		return time.Saturday, nil
	default:
		return time.Sunday, fmt.Errorf("reporting: weekly_report.day must be a weekday name")
	}
}
