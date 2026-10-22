// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"bytes"
	"context"
	"log/slog"
	"testing"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// blockingHandler blocks every write, and every level check, until the test
// ends: a log destination that has stopped accepting writes.
type blockingHandler struct{ gate chan struct{} }

func newBlockingHandler(t *testing.T) *blockingHandler {
	h := &blockingHandler{gate: make(chan struct{})}
	t.Cleanup(func() { close(h.gate) })
	return h
}

func (h *blockingHandler) Enabled(context.Context, slog.Level) bool  { <-h.gate; return true }
func (h *blockingHandler) Handle(context.Context, slog.Record) error { <-h.gate; return nil }
func (h *blockingHandler) WithAttrs([]slog.Attr) slog.Handler        { return h }
func (h *blockingHandler) WithGroup(string) slog.Handler             { return h }

// The client's own records on paho's routing goroutine, a retained message
// refused, a handler panic, an ingress refusal, are written off it: a logger
// that blocks never holds the message behind the one being logged.
func TestALoggerThatBlocksNeverHoldsTheClientsDelivery(t *testing.T) {
	c, err := New(Options{BrokerURL: "tcp://127.0.0.1:1", ClientID: "blocked-logger", Logger: slog.New(newBlockingHandler(t))})
	if err != nil {
		t.Fatal(err)
	}
	topic := "ori/dev-01/reasoning/request"
	gate, err := newIngressGate(topic, nil, c.ingress, time.Now)
	if err != nil {
		t.Fatal(err)
	}
	handled := make(chan struct{}, 8)
	panicking := true
	wrapped := c.wrapHandler(subscription{qos: QoSReasoning, gate: gate, handler: func(string, []byte) {
		if panicking {
			panicking = false
			panic("handler panic")
		}
		handled <- struct{}{}
	}})
	done := make(chan struct{})
	go func() {
		defer close(done)
		wrapped(nil, retainedTestMessage{topic: topic, payload: []byte(`{}`), retained: true})
		wrapped(nil, ingressMessage{topic: topic, payload: bytes.Repeat([]byte("a"), contracts.PayloadMaxima[contracts.FamilyReasoningRequest]+1)})
		wrapped(nil, ingressMessage{topic: topic, payload: []byte(`{}`)})
		wrapped(nil, ingressMessage{topic: topic, payload: []byte(`{}`)})
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("a logger that blocks held the client's delivery")
	}
	if len(handled) != 1 {
		t.Fatalf("%d messages reached the handler after the panic, want 1", len(handled))
	}
}
