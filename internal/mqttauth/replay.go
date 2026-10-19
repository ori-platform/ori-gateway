// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package mqttauth

import (
	"container/heap"
	"errors"
)

// The refusals of gateway-api/v2 Envelope Freshness and Replay, in the order
// its checks run. Each names a transport-authentication refusal, not proof of
// an attack: before_replay_floor most often means a restart or a clock step.
var (
	ErrStaleTimestamp    = errors.New("mqtt auth stale_timestamp")
	ErrFutureTimestamp   = errors.New("mqtt auth future_timestamp")
	ErrBeforeReplayFloor = errors.New("mqtt auth before_replay_floor")
	ErrReplayDetected    = errors.New("mqtt auth replay_detected")
)

// heldKey is an accepted envelope's key with its signing and receipt times,
// in milliseconds on the receiver's clock.
type heldKey struct {
	key        string
	signedAt   int64
	receivedAt int64
	// dropAt is the first time its retention has ended; index and
	// dropIndex place it in the two heaps.
	dropAt    int64
	index     int
	dropIndex int
}

// replayState is the receiver's freshness and replay state: the floor, the
// held keys, and the rules of gateway-api/v2 Envelope Freshness and Replay,
// each taking the receiver's clock as an argument. It is not safe for
// concurrent use; the Verifier holds its lock around every call.
type replayState struct {
	skew int64
	// ttl is gateway.auth.replay_ttl_ms, a lower bound on retention from
	// receipt; 0 sets none.
	ttl int64
	// stores is whether the floor and held keys survive a restart.
	stores bool

	floor int64
	// storedFloor is the floor raised by dropped keys, the only floor ever
	// stored; storedSet is false until a drop raises it.
	storedFloor int64
	storedSet   bool

	keys    map[string]*heldKey
	bySign  signHeap
	byDrop  dropHeap
	maxKeys int
}

func newReplayState(skew, ttl int64, stores bool, maxKeys int, startClock int64) *replayState {
	return &replayState{
		skew: skew, ttl: ttl, stores: stores, maxKeys: maxKeys,
		floor: startClock, keys: make(map[string]*heldKey),
	}
}

// check makes the four checks in order, for an envelope signed at signedAt and
// verified at t. Every bound is inclusive.
func (s *replayState) check(key string, signedAt, t int64) error {
	switch {
	case signedAt < t-s.skew:
		return ErrStaleTimestamp
	case signedAt > t+s.skew:
		return ErrFutureTimestamp
	}
	return s.recheck(key, signedAt)
}

// recheck makes checks 3 and 4, as a family that records only once the
// envelope answers something must make them again in one step with recording.
func (s *replayState) recheck(key string, signedAt int64) error {
	if signedAt < s.floor {
		return ErrBeforeReplayFloor
	}
	if _, held := s.keys[key]; held {
		return ErrReplayDetected
	}
	return nil
}

// record holds an accepted key from receivedAt. A full receiver first drops
// its key with the smallest signing time, raising the floor past it.
func (s *replayState) record(key string, signedAt, receivedAt int64) {
	if s.maxKeys > 0 && len(s.keys) >= s.maxKeys {
		s.evict()
	}
	held := &heldKey{key: key, signedAt: signedAt, receivedAt: receivedAt}
	held.dropAt = s.dropAt(held)
	s.keys[key] = held
	heap.Push(&s.bySign, held)
	heap.Push(&s.byDrop, held)
}

// dropAt is the first time a held key's retention has ended: its envelope is
// stale, and replay_ttl_ms has passed since its receipt where one is set.
func (s *replayState) dropAt(held *heldKey) int64 {
	at := held.signedAt + s.skew + 1
	if s.ttl > 0 {
		at = max(at, held.receivedAt+s.ttl)
	}
	return at
}

// drop removes a held key and raises the floor to at least one past its
// signing time, in the same step.
func (s *replayState) drop(held *heldKey) {
	delete(s.keys, held.key)
	heap.Remove(&s.bySign, held.index)
	heap.Remove(&s.byDrop, held.dropIndex)
	if raised := held.signedAt + 1; raised > s.floor {
		s.floor = raised
	}
	if raised := held.signedAt + 1; !s.storedSet || raised > s.storedFloor {
		s.storedFloor, s.storedSet = raised, true
	}
}

// prune drops every key whose retention has ended at t: its envelope is
// stale, and replay_ttl_ms has passed since its receipt where one is set. It
// pops only the keys due, in order of their drop times.
func (s *replayState) prune(t int64) {
	for len(s.byDrop) > 0 && s.byDrop[0].dropAt <= t {
		s.drop(s.byDrop[0])
	}
}

// evict drops the held key with the smallest signing time.
func (s *replayState) evict() {
	if len(s.bySign) > 0 {
		s.drop(s.bySign[0])
	}
}

// restart starts the receiver again at clock t, under skew. A storing
// receiver keeps its stored floor and held keys; one that stores nothing
// keeps neither. Either way the floor becomes the greater of the stored floor
// and the clock at the start, and the start floor itself is never stored.
func (s *replayState) restart(t, skew int64) {
	s.skew = skew
	if !s.stores {
		s.keys, s.bySign, s.byDrop, s.storedSet, s.storedFloor = make(map[string]*heldKey), nil, nil, false, 0
	}
	// A new skew moves every held key's drop time.
	for _, held := range s.keys {
		held.dropAt = s.dropAt(held)
	}
	heap.Init(&s.byDrop)
	s.floor = t
	if s.storedSet && s.storedFloor > s.floor {
		s.floor = s.storedFloor
	}
}

// rotate switches the secret generation at clock t. One that keeps the
// previous secret keeps the floor and every held key; one under new key
// material in which no earlier secret verifies may drop both, leaving the
// floor at the clock.
func (s *replayState) rotate(keepsPrevious bool, t int64) {
	if keepsPrevious {
		return
	}
	s.keys, s.bySign, s.byDrop, s.storedSet, s.storedFloor = make(map[string]*heldKey), nil, nil, false, 0
	s.floor = t
}

// signHeap orders held keys by signing time, for eviction.
type signHeap []*heldKey

func (h signHeap) Len() int           { return len(h) }
func (h signHeap) Less(i, j int) bool { return h[i].signedAt < h[j].signedAt }
func (h signHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].index, h[j].index = i, j
}
func (h *signHeap) Push(x any) {
	held := x.(*heldKey)
	held.index = len(*h)
	*h = append(*h, held)
}
func (h *signHeap) Pop() any {
	old := *h
	held := old[len(old)-1]
	*h = old[:len(old)-1]
	return held
}

// dropHeap orders held keys by drop time, for pruning.
type dropHeap []*heldKey

func (h dropHeap) Len() int           { return len(h) }
func (h dropHeap) Less(i, j int) bool { return h[i].dropAt < h[j].dropAt }
func (h dropHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
	h[i].dropIndex, h[j].dropIndex = i, j
}
func (h *dropHeap) Push(x any) {
	held := x.(*heldKey)
	held.dropIndex = len(*h)
	*h = append(*h, held)
}
func (h *dropHeap) Pop() any {
	old := *h
	held := old[len(old)-1]
	*h = old[:len(old)-1]
	return held
}
