// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package main

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"

	"github.com/ori-platform/ori-gateway/internal/broker"
	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// requestPool runs the handlers of the families it serves off the broker
// callback, so a provider call or a reply's publish never holds the client's
// ordered message delivery. A reply waits for its PUBACK, which paho cannot
// read while the callback that published it still holds delivery; every
// later message on the client, runtime heartbeats included, would wait with
// it.
//
// What a device holds of each family is bounded as the ingress rate is: each
// configured device has its own allowance, the family's burst, and every
// other device segment shares one, so no sender can take a configured
// device's. A message over the bound is dropped unanswered and counted as a
// volume drop (gateway-api/v2 Payload Volume); its runtime times out and
// falls back, or keeps its bytes and carries them again. Each message is
// admitted through the callback gate as it is accepted, so none is accepted
// once the stop is decided and shutdown joins every one accepted (GW-15);
// one still waiting when the run stops is released without being handled.
//
// An unordered pool runs each message on its own goroutine. An ordered pool
// runs each device's messages, of every family it serves, one at a time in
// the order they arrived, on a goroutine that lives while that device has
// messages waiting.
type requestPool struct {
	ordered    bool
	limits     map[string]int
	gate       *callbackGate
	stopped    <-chan struct{}
	ingress    *broker.IngressStats
	logger     *slog.Logger
	configured map[string]bool

	mu       sync.Mutex
	held     map[heldKey]int
	queued   map[string][]pendingRequest
	draining map[string]bool
}

type heldKey struct {
	device string
	family string
}

type pendingRequest struct {
	topic   string
	payload []byte
	family  string
	handle  broker.MessageHandler
	release func()
}

// newRequestPool returns an unordered pool for families. stopped is the
// run's end: a message still waiting then is not handled.
func newRequestPool(devices []string, gate *callbackGate, stopped <-chan struct{}, ingress *broker.IngressStats, logger *slog.Logger, families ...string) *requestPool {
	configured := make(map[string]bool, len(devices))
	for _, deviceID := range devices {
		configured[deviceID] = true
	}
	limits := make(map[string]int, len(families))
	for _, family := range families {
		limits[family] = broker.IngressRates[family].Burst
	}
	return &requestPool{
		limits:     limits,
		gate:       gate,
		stopped:    stopped,
		ingress:    ingress,
		logger:     logger,
		configured: configured,
		held:       make(map[heldKey]int),
		queued:     make(map[string][]pendingRequest),
		draining:   make(map[string]bool),
	}
}

// newOrderedRequestPool is newRequestPool for families whose messages from
// one device are handled one at a time, in arrival order across them all.
func newOrderedRequestPool(devices []string, gate *callbackGate, stopped <-chan struct{}, ingress *broker.IngressStats, logger *slog.Logger, families ...string) *requestPool {
	p := newRequestPool(devices, gate, stopped, ingress, logger, families...)
	p.ordered = true
	return p
}

// subscribe subscribes topic on client with handle run through the pool. A
// topic outside the families the pool serves is refused.
func (p *requestPool) subscribe(ctx context.Context, client brokerClient, topic string, qos byte, handle broker.MessageHandler) error {
	family, ok := contracts.PayloadFamily(topic)
	if _, served := p.limits[family]; !ok || !served {
		return fmt.Errorf("request pool does not serve the family of %q", topic)
	}
	return client.Subscribe(ctx, topic, qos, p.handler(family, handle))
}

func (p *requestPool) handler(family string, handle broker.MessageHandler) broker.MessageHandler {
	return func(topic string, payload []byte) {
		device := requestDeviceSegment(topic)
		if !p.configured[device] {
			device = ""
		}
		key := heldKey{device: device, family: family}
		p.mu.Lock()
		release, admitted := p.gate.admit()
		if !admitted {
			p.mu.Unlock()
			return
		}
		if p.held[key] >= p.limits[family] {
			p.mu.Unlock()
			release()
			p.ingress.Drop(family, len(payload))
			return
		}
		p.held[key]++
		req := pendingRequest{topic: topic, payload: payload, family: family, handle: handle, release: release}
		if !p.ordered {
			p.mu.Unlock()
			go p.run(device, req)
			return
		}
		p.queued[device] = append(p.queued[device], req)
		start := !p.draining[device]
		p.draining[device] = true
		p.mu.Unlock()
		if start {
			go p.drain(device)
		}
	}
}

// drain runs device's queued messages in order until none is left.
func (p *requestPool) drain(device string) {
	for {
		p.mu.Lock()
		req := p.queued[device][0]
		p.queued[device] = p.queued[device][1:]
		p.mu.Unlock()
		if last := p.run(device, req); last {
			return
		}
	}
}

// run handles req, unless the run has stopped, and reports whether it was
// the last message an ordered pool held queued for device. Releasing req's
// admission is the last thing it does, so nothing touches the pool once the
// gate has joined every message.
func (p *requestPool) run(device string, req pendingRequest) (last bool) {
	defer func() {
		if r := recover(); r != nil {
			p.logger.Warn("mqtt message handler panic", "topic", req.topic, "recover", r)
		}
		key := heldKey{device: device, family: req.family}
		p.mu.Lock()
		p.held[key]--
		if p.held[key] == 0 {
			delete(p.held, key)
		}
		if p.ordered && len(p.queued[device]) == 0 {
			delete(p.queued, device)
			delete(p.draining, device)
			last = true
		}
		p.mu.Unlock()
		req.release()
	}()
	select {
	case <-p.stopped:
		return false
	default:
	}
	req.handle(req.topic, req.payload)
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
