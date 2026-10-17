// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package heartbeat

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
	"github.com/ori-platform/ori-gateway/internal/specvectors"
)

const topInt = 9007199254740991

// TestRequiredRepliesFitTheFloor builds the gateway heartbeat with the
// publisher's own encoder at the vendored corpus's bounds: the longest status
// and provider, every integer at its widest, signed. It is no longer than the
// corpus's escaped payload, which the floor admits.
func TestRequiredRepliesFitTheFloor(t *testing.T) {
	want := sizeReply(t, "gateway_heartbeat")
	now := time.UnixMilli(topInt - 1)
	publisher, err := NewPublisher(func(context.Context, []byte) error { return nil },
		stubProvider{name: "llama_cpp", healthy: true}, SIMStatus{}, Options{
			Interval: time.Second, StartedAt: now.Add(-1_000_000 * time.Hour), Now: func() time.Time { return now },
			Auth: AuthConfig{Enabled: true, SharedSecret: "site-local-secret"},
			WebhookBridge: func() *contracts.WebhookBridgePosture {
				return &contracts.WebhookBridgePosture{
					ProviderCIDRCount: topInt, BodyLimitBytes: topInt, RequestTimeoutMS: topInt,
				}
			},
		})
	if err != nil {
		t.Fatal(err)
	}
	payload, err := publisher.buildPayload(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if len(payload) > want.Length || want.Length > contracts.PayloadFloor || want.Verdict != "admitted" {
		t.Fatalf("heartbeat is %d bytes; the corpus bound is %d (%s) against the %d-byte floor", len(payload), want.Length, want.Verdict, contracts.PayloadFloor)
	}
}

type sizeReplyCase struct {
	Name              string `json:"name"`
	ConfiguredMaximum int    `json:"configured_maximum"`
	Length            int    `json:"length"`
	Verdict           string `json:"verdict"`
}

func sizeReply(t *testing.T, name string) sizeReplyCase {
	t.Helper()
	raw, err := specvectors.Read("gateway-api/vectors/payload-size-v2.json")
	if err != nil {
		t.Fatal(err)
	}
	var corpus struct {
		Replies []sizeReplyCase `json:"replies"`
	}
	if err := json.Unmarshal(raw, &corpus); err != nil {
		t.Fatal(err)
	}
	for _, c := range corpus.Replies {
		if c.Name == name {
			return c
		}
	}
	t.Fatalf("the corpus has no %s reply", name)
	return sizeReplyCase{}
}
