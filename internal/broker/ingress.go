// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package broker

import (
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
// PerSecond sustained, Burst at once.
type IngressRate struct {
	PerSecond float64
	Burst     int
}

// IngressRates are the gateway's own rates for each family it receives,
// chosen for a Pi-class host. Each applies per device: a per-device
// subscription has one bucket, and a wildcard subscription has one for each
// configured device and one shared by every other device segment, so no
// sender can starve a configured device or add state. No payload, envelope or
// configuration the runtime controls changes one.
var IngressRates = map[string]IngressRate{
	// A runtime heartbeats every few seconds at most.
	contracts.FamilyRuntimeHeartbeat: {PerSecond: 1, Burst: 5},
	// A runtime sends one request per gateway escalation.
	contracts.FamilyReasoningRequest: {PerSecond: 10, Burst: 20},
	// An enrichment request follows an operator's Tier C proposal.
	contracts.FamilyEnrichmentRequest: {PerSecond: 2, Burst: 10},
	// Export responses answer the gateway's own paged requests.
	contracts.FamilyExportResponse: {PerSecond: 20, Burst: 64},
	// One runtime drain pass publishes up to about 151 carriages back to back
	// (50 envelopes, a checkpoint, 50 registrations, 50 re-offers); a dropped
	// carriage is retried by its runtime, which keeps its bytes.
	contracts.FamilyEvidenceOutbound: {PerSecond: 20, Burst: 160},
	// An inbound acknowledgement follows a carriage the gateway sent; a
	// dropped one leaves the artifact queued and carried again.
	contracts.FamilyEvidenceInboundAck: {PerSecond: 20, Burst: 64},
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

// IngressCount is one family's refusals since the process started.
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

func (s *IngressStats) refuse(family, reason string, length int) {
	s.mu.Lock()
	count := s.counts[family]
	if count == nil {
		count = &IngressCount{}
		s.counts[family] = count
	}
	if reason == RefusalPayloadTooLarge {
		count.PayloadTooLarge++
	} else {
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
	s.log.Warn("mqtt ingress dropped messages over the family rate",
		"family", key.family, "dropped", line.count)
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
	last   time.Time
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
		overflow:  &ingressBucket{tokens: float64(rate.Burst), last: start},
	}
	if g.perDevice {
		for _, device := range devices {
			g.buckets[device] = &ingressBucket{tokens: float64(rate.Burst), last: start}
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

// admit reads nothing of the payload but its length, and nothing of the
// topic but its device segment.
func (g *ingressGate) admit(topic string, length int) bool {
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
		b.tokens += elapsed * g.rate.PerSecond
		if burst := float64(g.rate.Burst); b.tokens > burst {
			b.tokens = burst
		}
	}
	b.last = now
	admitted := b.tokens >= 1
	if admitted {
		b.tokens--
	}
	g.mu.Unlock()
	if !admitted {
		g.stats.refuse(g.family, RefusalVolumeDropped, length)
	}
	return admitted
}
