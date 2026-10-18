// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"bytes"
	"encoding/json"
	"errors"
	"log/slog"
	"strings"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

type replayClock struct{ t time.Time }

func (c *replayClock) now() time.Time { return c.t }

func signedHeartbeat(t *testing.T, signedAtMS int64, status string) []byte {
	t.Helper()
	payload := map[string]any{"device_id": "dev-01", "status": status}
	auth, err := Sign(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "", signedAtMS, "site-local-secret")
	if err != nil {
		t.Fatal(err)
	}
	payload["auth"] = auth
	raw, err := json.Marshal(payload)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func replayVerifier(t *testing.T, clock *replayClock, logs *bytes.Buffer, maxKeys int) *Verifier {
	t.Helper()
	v, err := NewVerifier(Config{
		SharedSecret: "site-local-secret", MaxSkew: time.Minute, ReplayTTL: time.Second,
		Now: clock.now, MaxKeys: maxKeys, Logger: slog.New(slog.NewTextHandler(logs, nil)),
	})
	if err != nil {
		t.Fatal(err)
	}
	return v
}

func verifyAt(v *Verifier, payload []byte) error {
	_, err := v.VerifyJSON(payload, contracts.RuntimeHeartbeatMessageType, "dev-01", "")
	return err
}

// TestAKeyIsKeptUntilItsEnvelopeIsStale accepts an envelope signed at the
// future edge of the skew, then replays it just before it goes stale, long
// after a fixed retention from receipt would have dropped it: it is still
// refused as a replay.
func TestAKeyIsKeptUntilItsEnvelopeIsStale(t *testing.T) {
	start := time.UnixMilli(1_787_000_000_000)
	clock := &replayClock{t: start}
	v := replayVerifier(t, clock, &bytes.Buffer{}, 0)
	edge := start.Add(time.Minute).UnixMilli()
	payload := signedHeartbeat(t, edge, "healthy")
	if err := verifyAt(v, payload); err != nil {
		t.Fatalf("an envelope at the future edge was refused: %v", err)
	}
	clock.t = start.Add(2 * time.Minute) // still fresh: signed_at + skew is not past
	if err := verifyAt(v, payload); !errors.Is(err, ErrReplayDetected) {
		t.Fatalf("a replay %s after receipt = %v, want replay_detected", 2*time.Minute, err)
	}
	clock.t = start.Add(2*time.Minute + time.Millisecond)
	if err := verifyAt(v, payload); !errors.Is(err, ErrStaleTimestamp) {
		t.Fatalf("the same envelope once stale = %v, want stale_timestamp", err)
	}
}

// TestADroppedKeyRaisesTheFloor fills a verifier that holds one key, so the
// next accepted envelope evicts the first and raises the floor past it: the
// evicted envelope is then refused below the floor, though it is still fresh,
// and the refusal is logged with the floor and the clock.
func TestADroppedKeyRaisesTheFloor(t *testing.T) {
	start := time.UnixMilli(1_787_000_000_000)
	clock := &replayClock{t: start}
	logs := &bytes.Buffer{}
	v := replayVerifier(t, clock, logs, 1)
	first := signedHeartbeat(t, start.Add(30*time.Second).UnixMilli(), "healthy")
	if err := verifyAt(v, first); err != nil {
		t.Fatal(err)
	}
	if err := verifyAt(v, signedHeartbeat(t, start.Add(40*time.Second).UnixMilli(), "degraded")); err != nil {
		t.Fatal(err)
	}
	if err := verifyAt(v, first); !errors.Is(err, ErrBeforeReplayFloor) {
		t.Fatalf("the evicted envelope = %v, want before_replay_floor", err)
	}
	if !strings.Contains(logs.String(), "floor_ms=") || !strings.Contains(logs.String(), "t_ms=") {
		t.Fatalf("the refusal below a floor ahead of the clock was not logged:\n%s", logs.String())
	}
	if v.ReplayEntries() != 1 {
		t.Fatalf("%d keys held, want the cap of 1", v.ReplayEntries())
	}
}

// TestAStartFloorRefusesWhatWasSignedBefore starts a verifier, as a gateway
// start does, and offers an envelope signed before that start but within the
// skew: it is refused below the floor, not accepted.
func TestAStartFloorRefusesWhatWasSignedBefore(t *testing.T) {
	start := time.UnixMilli(1_787_000_000_000)
	v := replayVerifier(t, &replayClock{t: start}, &bytes.Buffer{}, 0)
	if err := verifyAt(v, signedHeartbeat(t, start.Add(-time.Second).UnixMilli(), "healthy")); !errors.Is(err, ErrBeforeReplayFloor) {
		t.Fatalf("an envelope signed before the start = %v, want before_replay_floor", err)
	}
}

// TestFreshnessReasonsAreNamedApart names a stale and a future envelope
// differently, and checks staleness before the floor and the floor before a
// replay.
func TestFreshnessReasonsAreNamedApart(t *testing.T) {
	start := time.UnixMilli(1_787_000_000_000)
	v := replayVerifier(t, &replayClock{t: start}, &bytes.Buffer{}, 0)
	cases := []struct {
		signedAt time.Time
		want     error
	}{
		{start.Add(-time.Minute - time.Millisecond), ErrStaleTimestamp},
		{start.Add(time.Minute + time.Millisecond), ErrFutureTimestamp},
		{start.Add(-time.Second), ErrBeforeReplayFloor},
	}
	for _, c := range cases {
		if err := verifyAt(v, signedHeartbeat(t, c.signedAt.UnixMilli(), "healthy")); !errors.Is(err, c.want) {
			t.Fatalf("signed at %s = %v, want %v", c.signedAt.Sub(start), err, c.want)
		}
	}
}
