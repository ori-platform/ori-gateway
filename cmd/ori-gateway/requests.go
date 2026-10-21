// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"log/slog"
	"strings"
	"sync"

	"github.com/ori-platform/ori-gateway/internal/broker"
)

// requestPool runs a family's handler off the broker callback, so a provider
// call or a reply's publish never holds the client's ordered message
// delivery. A reply waits for its PUBACK, which paho cannot read while the
// callback that published it still holds delivery; every later message on
// the client, runtime heartbeats included, would wait with it.
//
// Requests held are bounded per device as the ingress rate is: each
// configured device has its own allowance, the family's burst, and every
// other device segment shares one, so no sender can take a configured
// device's. A request over the bound is dropped unanswered and counted as a
// volume drop (gateway-api/v2 Payload Volume); its runtime times out and
// falls back, or keeps its bytes and carries them again. Each request is
// admitted through the callback gate as it is accepted, so none is accepted
// once the stop is decided and shutdown joins every one accepted (GW-15).
//
// An unordered pool runs each request on its own goroutine. An ordered pool
// runs each device's requests one at a time in the order they arrived, on a
// goroutine that lives while that device has requests waiting, so a family
// whose handling must keep arrival order per device keeps it.
type requestPool struct {
	family     string
	limit      int
	ordered    bool
	gate       *callbackGate
	ingress    *broker.IngressStats
	logger     *slog.Logger
	configured map[string]bool

	mu       sync.Mutex
	held     map[string]int
	queued   map[string][]pendingRequest
	draining map[string]bool
}

type pendingRequest struct {
	topic   string
	payload []byte
	release func()
}

func newRequestPool(family string, devices []string, gate *callbackGate, ingress *broker.IngressStats, logger *slog.Logger) *requestPool {
	configured := make(map[string]bool, len(devices))
	for _, deviceID := range devices {
		configured[deviceID] = true
	}
	return &requestPool{
		family:     family,
		limit:      broker.IngressRates[family].Burst,
		gate:       gate,
		ingress:    ingress,
		logger:     logger,
		configured: configured,
		held:       make(map[string]int),
		queued:     make(map[string][]pendingRequest),
		draining:   make(map[string]bool),
	}
}

// newOrderedRequestPool is newRequestPool for a family whose requests from
// one device are handled one at a time, in arrival order.
func newOrderedRequestPool(family string, devices []string, gate *callbackGate, ingress *broker.IngressStats, logger *slog.Logger) *requestPool {
	p := newRequestPool(family, devices, gate, ingress, logger)
	p.ordered = true
	return p
}

// handler returns a broker handler that runs handle for each request off the
// callback, within the pool's bound.
func (p *requestPool) handler(handle broker.MessageHandler) broker.MessageHandler {
	return func(topic string, payload []byte) {
		key := requestDeviceSegment(topic)
		if !p.configured[key] {
			key = ""
		}
		p.mu.Lock()
		if p.held[key] >= p.limit {
			p.mu.Unlock()
			p.ingress.Drop(p.family, len(payload))
			return
		}
		release, admitted := p.gate.admit()
		if !admitted {
			p.mu.Unlock()
			return
		}
		p.held[key]++
		req := pendingRequest{topic: topic, payload: payload, release: release}
		if !p.ordered {
			p.mu.Unlock()
			go p.run(key, req, handle)
			return
		}
		p.queued[key] = append(p.queued[key], req)
		start := !p.draining[key]
		p.draining[key] = true
		p.mu.Unlock()
		if start {
			go p.drain(key, handle)
		}
	}
}

// drain runs key's queued requests in order until none is left.
func (p *requestPool) drain(key string, handle broker.MessageHandler) {
	for {
		p.mu.Lock()
		req := p.queued[key][0]
		p.queued[key] = p.queued[key][1:]
		p.mu.Unlock()
		if last := p.run(key, req, handle); last {
			return
		}
	}
}

// run handles req and reports whether it was the last one an ordered pool
// held queued for key. Releasing req's admission is the last thing it does,
// so nothing touches the pool once the gate has joined every request.
func (p *requestPool) run(key string, req pendingRequest, handle broker.MessageHandler) (last bool) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Warn("mqtt message handler panic", "topic", req.topic, "recover", r)
		}
		p.mu.Lock()
		p.held[key]--
		if p.held[key] == 0 {
			delete(p.held, key)
		}
		if p.ordered && len(p.queued[key]) == 0 {
			delete(p.queued, key)
			delete(p.draining, key)
			last = true
		}
		p.mu.Unlock()
		req.release()
	}()
	handle(req.topic, req.payload)
	return false
}

// requestDeviceSegment is the device segment of an ori/{device_id}/... topic.
func requestDeviceSegment(topic string) string {
	rest, ok := strings.CutPrefix(topic, "ori/")
	if !ok {
		return ""
	}
	deviceID, _, _ := strings.Cut(rest, "/")
	return deviceID
}
