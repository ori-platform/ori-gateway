// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package session

import (
	"testing"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

func TestNextAttemptPreservesRequestID(t *testing.T) {
	req := contracts.ReasoningRequest{RequestID: "req-1"}
	sess := New(req, RetryPolicy{TimeoutMS: 1000, MaxRetries: 1}, 0)
	attempt, ok := sess.NextAttempt(10)
	if !ok {
		t.Fatal("expected attempt")
	}
	if attempt.RequestID != req.RequestID {
		t.Fatalf("request id changed: %q", attempt.RequestID)
	}
}

func TestRegistryCorrelatesAndRemovesSession(t *testing.T) {
	reg := NewRegistry()
	req := contracts.ReasoningRequest{RequestID: "req-1"}
	reg.Register(New(req, RetryPolicy{TimeoutMS: 1000}, 0))

	sess, ok := reg.Correlate(contracts.ReasoningResponse{RequestID: "req-1"})
	if !ok {
		t.Fatal("expected correlation")
	}
	if sess.Request.RequestID != "req-1" {
		t.Fatalf("unexpected session: %s", sess.Request.RequestID)
	}
	if _, ok := reg.Correlate(contracts.ReasoningResponse{RequestID: "req-1"}); ok {
		t.Fatal("expected correlated session to be removed")
	}
}

func TestEvictTimedOut(t *testing.T) {
	reg := NewRegistry()
	reg.Register(New(contracts.ReasoningRequest{RequestID: "req-1"}, RetryPolicy{TimeoutMS: 100}, 0))
	evicted := reg.EvictTimedOut(100)
	if len(evicted) != 1 {
		t.Fatalf("expected one eviction, got %d", len(evicted))
	}
}
