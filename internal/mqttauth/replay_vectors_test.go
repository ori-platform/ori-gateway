// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"encoding/json"
	"errors"
	"slices"
	"testing"

	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

type replayCorpus struct {
	Reasons []string `json:"reasons"`
	Cases   []struct {
		Name    string `json:"name"`
		Context struct {
			SkewMS        int64  `json:"skew_ms"`
			ReplayTTLMS   *int64 `json:"replay_ttl_ms"`
			Stores        bool   `json:"stores"`
			StoredFloorMS *int64 `json:"stored_floor_ms"`
			StartClockMS  int64  `json:"start_clock_ms"`
			Held          []struct {
				Key          string `json:"key"`
				SignedAtMS   int64  `json:"signed_at_ms"`
				ReceivedAtMS int64  `json:"received_at_ms"`
			} `json:"held"`
		} `json:"context"`
		Events []struct {
			Event         string `json:"event"`
			Key           string `json:"key"`
			Handle        string `json:"handle"`
			SignedAtMS    int64  `json:"signed_at_ms"`
			T             int64  `json:"t"`
			SkewMS        *int64 `json:"skew_ms"`
			KeepsPrevious bool   `json:"keeps_previous"`
			Expected      struct {
				Reason  *string  `json:"reason"`
				FloorMS int64    `json:"floor_ms"`
				Held    []string `json:"held"`
			} `json:"expected"`
		} `json:"events"`
	} `json:"cases"`
}

var replayReasons = map[error]string{
	ErrStaleTimestamp:    "stale_timestamp",
	ErrFutureTimestamp:   "future_timestamp",
	ErrBeforeReplayFloor: "before_replay_floor",
	ErrReplayDetected:    "replay_detected",
}

func reasonOf(t *testing.T, err error) *string {
	t.Helper()
	if err == nil {
		return nil
	}
	for target, reason := range replayReasons {
		if errors.Is(err, target) {
			return &reason
		}
	}
	t.Fatalf("unnamed refusal %v", err)
	return nil
}

// TestReplayVectorCases drives every vendored case through the replay state
// the gateway's verifier uses, event by event, and compares each event's
// reason, floor and held keys.
func TestReplayVectorCases(t *testing.T) {
	raw, err := specvectors.Read("gateway-api/vectors/replay-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus replayCorpus
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, reason := range replayReasons {
		names = append(names, reason)
	}
	slices.Sort(names)
	want := slices.Clone(corpus.Reasons)
	slices.Sort(want)
	if !slices.Equal(names, want) {
		t.Fatalf("reasons %v, corpus %v", names, corpus.Reasons)
	}
	for _, c := range corpus.Cases {
		t.Run(c.Name, func(t *testing.T) {
			ctx := c.Context
			var ttl int64
			if ctx.ReplayTTLMS != nil {
				ttl = *ctx.ReplayTTLMS
			}
			s := newReplayState(ctx.SkewMS, ttl, ctx.Stores, 0, ctx.StartClockMS)
			if ctx.StoredFloorMS != nil {
				s.storedFloor, s.storedSet = *ctx.StoredFloorMS, true
				s.floor = max(s.floor, *ctx.StoredFloorMS)
			}
			for _, held := range ctx.Held {
				s.record(held.Key, held.SignedAtMS, held.ReceivedAtMS)
			}
			pending := map[string]struct {
				key                string
				signedAt, received int64
			}{}
			for i, e := range c.Events {
				var reason *string
				switch e.Event {
				case "receive":
					err := s.check(e.Key, e.SignedAtMS, e.T)
					if err == nil {
						s.record(e.Key, e.SignedAtMS, e.T)
					}
					reason = reasonOf(t, err)
				case "authenticate":
					err := s.check(e.Key, e.SignedAtMS, e.T)
					if err == nil {
						pending[e.Handle] = struct {
							key                string
							signedAt, received int64
						}{e.Key, e.SignedAtMS, e.T}
					}
					reason = reasonOf(t, err)
				case "commit":
					p, ok := pending[e.Handle]
					if !ok {
						t.Fatalf("event %d: commit of an unknown handle %q", i, e.Handle)
					}
					delete(pending, e.Handle)
					err := s.recheck(p.key, p.signedAt)
					if err == nil {
						s.record(p.key, p.signedAt, p.received)
					}
					reason = reasonOf(t, err)
				case "prune":
					s.prune(e.T)
				case "evict":
					s.evict()
				case "restart":
					skew := s.skew
					if e.SkewMS != nil {
						skew = *e.SkewMS
					}
					s.restart(e.T, skew)
					// A restart discards every authentication not yet recorded.
					clear(pending)
				case "rotate":
					s.rotate(e.KeepsPrevious, e.T)
					if !e.KeepsPrevious {
						clear(pending)
					}
				default:
					t.Fatalf("event %d: unknown event %q", i, e.Event)
				}
				if (reason == nil) != (e.Expected.Reason == nil) || (reason != nil && *reason != *e.Expected.Reason) {
					t.Fatalf("event %d %s: reason %v, want %v", i, e.Event, deref(reason), deref(e.Expected.Reason))
				}
				var held []string
				for key := range s.keys {
					held = append(held, key)
				}
				slices.Sort(held)
				wantHeld := slices.Clone(e.Expected.Held)
				slices.Sort(wantHeld)
				if s.floor != e.Expected.FloorMS || !slices.Equal(held, wantHeld) {
					t.Fatalf("event %d %s: floor %d held %v, want floor %d held %v", i, e.Event, s.floor, held, e.Expected.FloorMS, wantHeld)
				}
			}
		})
	}
}

func deref(s *string) string {
	if s == nil {
		return "accepted"
	}
	return *s
}
