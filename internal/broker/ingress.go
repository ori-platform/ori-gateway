// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
	"errors"
	"fmt"
	"log/slog"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/ori-platform/ori-gateway/internal/contracts"
)

// Refusal reasons a subscription records before its handler runs
// (gateway-api/v2 Payload Size and Payload Volume).
const (
	RefusalPayloadTooLarge = "payload_too_large"
	RefusalVolumeDropped   = "volume_dropped"
)

// ingressLogInterval paces the refusal log: one line per family and reason at
// most each interval, counting every refusal since the last, so a flood
// cannot grow the log.
const ingressLogInterval = time.Second

// IngressRate is the rate at which a subscription processes messages:
// PerSecond messages sustained and Burst at once, and BytesPerSecond payload
// bytes sustained and BurstBytes at once. The byte budget bounds the parse
// work the member rule does before authentication, which grows with a
// payload's length; BurstBytes is at least the family maximum, so one
// payload at the maximum is always within it.
type IngressRate struct {
	PerSecond      float64
	Burst          int
	BytesPerSecond float64
	BurstBytes     int
}

// IngressRates are the gateway's own rates for each family it receives,
// chosen for a Pi-class host. Each applies per device: a per-device
// subscription has one bucket, and a wildcard subscription has one for each
// configured device and one shared by every other device segment, so no
// sender can starve a configured device or add state. No payload, envelope or
// configuration the runtime controls changes one.
var IngressRates = map[string]IngressRate{
	// A runtime heartbeats every few seconds at most.
	contracts.FamilyRuntimeHeartbeat: {PerSecond: 1, Burst: 5, BytesPerSecond: 128 << 10, BurstBytes: 256 << 10},
	// A runtime sends one request per gateway escalation.
	contracts.FamilyReasoningRequest: {PerSecond: 10, Burst: 20, BytesPerSecond: 512 << 10, BurstBytes: 1 << 20},
	// An enrichment request follows an operator's Tier C proposal.
	contracts.FamilyEnrichmentRequest: {PerSecond: 2, Burst: 10, BytesPerSecond: 256 << 10, BurstBytes: 512 << 10},
	// Export responses answer the gateway's own paged requests, one page each.
	// The runtime never resends a page and serves at most four requests at
	// once, so the burst holds four pages at the maximum.
	contracts.FamilyExportResponse: {PerSecond: 20, Burst: 64, BytesPerSecond: 4 << 20, BurstBytes: 16 << 20},
	// One runtime drain pass publishes up to about 151 carriages back to back
	// (50 envelopes, a checkpoint, 50 registrations, 50 re-offers); a dropped
	// carriage is retried by its runtime, which keeps its bytes.
	contracts.FamilyEvidenceOutbound: {PerSecond: 20, Burst: 160, BytesPerSecond: 2 << 20, BurstBytes: 4 << 20},
	// An inbound acknowledgement follows a carriage the gateway sent; a
	// dropped one leaves the artifact queued and carried again.
	contracts.FamilyEvidenceInboundAck: {PerSecond: 20, Burst: 64, BytesPerSecond: 64 << 10, BurstBytes: 256 << 10},
}

// CheckSubscription reports whether topic may be subscribed: it must belong to
// a family with a payload maximum and an ingress rate.
func CheckSubscription(topic string) error {
	family, ok := contracts.PayloadFamily(topic)
	if !ok {
		return fmt.Errorf("%w: %q", ErrNoPayloadMaximum, topic)
	}
	if _, ok := IngressRates[family]; !ok {
		return fmt.Errorf("%w: %q has no ingress rate", ErrNoPayloadMaximum, topic)
	}
	return nil
}

// IngressCount is one family's size and volume refusals since the process
// started: the counts site health projects. A member rule refusal is logged
// with its reason, not counted here.
type IngressCount struct {
	PayloadTooLarge uint64
	VolumeDropped   uint64
}

// IngressStats counts and logs every subscription's refusals by family. One
// value is shared by every broker client of a process. Nothing of a refused
// payload is recorded but its family and size class.
type IngressStats struct {
	mu       sync.Mutex
	log      *slog.Logger
	now      func() time.Time
	counts   map[string]*IngressCount
	unlogged map[ingressLogKey]*ingressPending
}

type ingressLogKey struct {
	family string
	reason string
}

type ingressPending struct {
	count    uint64
	classes  map[int]struct{}
	loggedAt time.Time
	flushing bool
}

// NewIngressStats returns empty counts that log through logger.
func NewIngressStats(logger *slog.Logger) *IngressStats {
	if logger == nil {
		logger = slog.Default()
	}
	return &IngressStats{
		log:      logger,
		now:      time.Now,
		counts:   make(map[string]*IngressCount),
		unlogged: make(map[ingressLogKey]*ingressPending),
	}
}

// Snapshot returns every family's counts.
func (s *IngressStats) Snapshot() map[string]IngressCount {
	s.mu.Lock()
	defer s.mu.Unlock()
	out := make(map[string]IngressCount, len(s.counts))
	for family, count := range s.counts {
		out[family] = *count
	}
	return out
}

// Drop counts and logs a message its handler dropped over the subscription's
// own bound on requests in flight, as a volume drop: the rate at which a
// receiver processes messages is bounded there too (gateway-api/v2 Payload
// Volume).
func (s *IngressStats) Drop(family string, length int) {
	s.refuse(family, RefusalVolumeDropped, length)
}

func (s *IngressStats) refuse(family, reason string, length int) {
	s.mu.Lock()
	count := s.counts[family]
	if count == nil {
		count = &IngressCount{}
		s.counts[family] = count
	}
	switch reason {
	case RefusalPayloadTooLarge:
		count.PayloadTooLarge++
	case RefusalVolumeDropped:
		count.VolumeDropped++
	}
	key := ingressLogKey{family: family, reason: reason}
	pending := s.unlogged[key]
	if pending == nil {
		pending = &ingressPending{classes: make(map[int]struct{})}
		s.unlogged[key] = pending
	}
	pending.count++
	if reason == RefusalPayloadTooLarge {
		pending.classes[contracts.SizeClass(length)] = struct{}{}
	}
	if pending.flushing {
		s.mu.Unlock()
		return
	}
	if wait := pending.loggedAt.Add(ingressLogInterval).Sub(s.now()); wait > 0 {
		pending.flushing = true
		s.mu.Unlock()
		time.AfterFunc(wait, func() { s.flush(key) })
		return
	}
	line := s.takeLocked(key)
	s.mu.Unlock()
	s.write(key, line)
}

func (s *IngressStats) flush(key ingressLogKey) {
	s.mu.Lock()
	s.unlogged[key].flushing = false
	line := s.takeLocked(key)
	s.mu.Unlock()
	s.write(key, line)
}

type ingressLine struct {
	count   uint64
	classes []int
}

func (s *IngressStats) takeLocked(key ingressLogKey) ingressLine {
	pending := s.unlogged[key]
	line := ingressLine{count: pending.count}
	for class := range pending.classes {
		line.classes = append(line.classes, class)
	}
	sort.Ints(line.classes)
	pending.count = 0
	pending.classes = make(map[int]struct{})
	pending.loggedAt = s.now()
	return line
}

func (s *IngressStats) write(key ingressLogKey, line ingressLine) {
	if line.count == 0 {
		return
	}
	if key.reason == RefusalPayloadTooLarge {
		s.log.Warn("mqtt ingress refused payloads over their topic maximum",
			"family", key.family, "refused", line.count, "size_classes", line.classes,
			"maximum", contracts.PayloadMaxima[key.family])
		return
	}
	if key.reason == RefusalVolumeDropped {
		s.log.Warn("mqtt ingress dropped messages over the family rate",
			"family", key.family, "dropped", line.count)
		return
	}
	s.log.Warn("mqtt ingress refused payloads under the payload member rule",
		"family", key.family, "reason", key.reason, "refused", line.count)
}

// ingressGate admits one subscription's messages: size first, then the rate
// of the sending device. It belongs to the subscription, so a reconnect keeps
// its buckets.
type ingressGate struct {
	family  string
	maximum int
	rate    IngressRate
	stats   *IngressStats
	now     func() time.Time
	// perDevice is set for a wildcard device segment: the bucket is chosen by
	// the topic's device segment rather than shared.
	perDevice bool

	mu sync.Mutex
	// buckets holds one bucket per configured device, fixed at subscription;
	// overflow serves every other device segment and a per-device
	// subscription.
	buckets  map[string]*ingressBucket
	overflow *ingressBucket
}

type ingressBucket struct {
	tokens float64
	bytes  float64
	last   time.Time
}

func newIngressBucket(rate IngressRate, now time.Time) *ingressBucket {
	return &ingressBucket{tokens: float64(rate.Burst), bytes: float64(rate.BurstBytes), last: now}
}

func newIngressGate(topic string, devices []string, stats *IngressStats, now func() time.Time) (*ingressGate, error) {
	if err := CheckSubscription(topic); err != nil {
		return nil, err
	}
	family, _ := contracts.PayloadFamily(topic)
	rate := IngressRates[family]
	start := now()
	g := &ingressGate{
		family:    family,
		maximum:   contracts.PayloadMaxima[family],
		rate:      rate,
		stats:     stats,
		now:       now,
		perDevice: deviceSegment(topic) == "+",
		buckets:   make(map[string]*ingressBucket),
		overflow:  newIngressBucket(rate, start),
	}
	if g.perDevice {
		for _, device := range devices {
			g.buckets[device] = newIngressBucket(rate, start)
		}
	}
	return g, nil
}

// deviceSegment is the {device_id} segment of a topic in the table.
func deviceSegment(topic string) string {
	segments := strings.SplitN(topic, "/", 3)
	if len(segments) < 3 {
		return ""
	}
	return segments[1]
}

// admitMembers applies the Payload Member Rule to a payload size and rate
// have admitted. The broker's wrapper calls admitVolume, discards a retained
// message, then calls this.
func (g *ingressGate) admitMembers(payload []byte) bool {
	var refused *contracts.MemberRuleError
	if err := contracts.AdmitPayload(g.family, payload); errors.As(err, &refused) {
		g.stats.refuse(g.family, refused.Reason, len(payload))
		return false
	} else if err != nil {
		g.stats.refuse(g.family, contracts.ReasonMalformedJSON, len(payload))
		return false
	}
	return true
}

// admitVolume applies size, then the sending device's message and byte rate.
// It reads nothing of the payload but its length, and nothing of the topic but
// its device segment.
func (g *ingressGate) admitVolume(topic string, length int) bool {
	if length > g.maximum {
		g.stats.refuse(g.family, RefusalPayloadTooLarge, length)
		return false
	}
	g.mu.Lock()
	now := g.now()
	b := g.overflow
	if g.perDevice {
		if own, ok := g.buckets[deviceSegment(topic)]; ok {
			b = own
		}
	}
	if elapsed := now.Sub(b.last).Seconds(); elapsed > 0 {
		b.tokens = min(float64(g.rate.Burst), b.tokens+elapsed*g.rate.PerSecond)
		b.bytes = min(float64(g.rate.BurstBytes), b.bytes+elapsed*g.rate.BytesPerSecond)
	}
	b.last = now
	admitted := b.tokens >= 1 && b.bytes >= float64(length)
	if admitted {
		b.tokens--
		b.bytes -= float64(length)
	}
	g.mu.Unlock()
	if !admitted {
		g.stats.refuse(g.family, RefusalVolumeDropped, length)
	}
	return admitted
}
