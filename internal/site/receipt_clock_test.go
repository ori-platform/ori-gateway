// Copyright 2026 Ori Nexus Systems LTD
// SPDX-License-Identifier: Apache-2.0

package site

import (
	"sync"
	"time"
)

// fakeReceiptClock is a receipt clock a test moves. advance moves both of its
// readings, as time passing does; stepWall moves the wall reading alone, as
// a clock set by NTP or by hand does.
type fakeReceiptClock struct {
	mu   sync.Mutex
	wall time.Time
	mono time.Duration
}

func newFakeReceiptClock(wallMS int64) *fakeReceiptClock {
	return &fakeReceiptClock{wall: time.UnixMilli(wallMS)}
}

func (c *fakeReceiptClock) Wall() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.wall
}

func (c *fakeReceiptClock) Mono() time.Duration {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.mono
}

func (c *fakeReceiptClock) advance(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall = c.wall.Add(d)
	c.mono += d
}

// moveTo advances both readings until the wall clock reads wallMS.
func (c *fakeReceiptClock) moveTo(wallMS int64) {
	c.advance(time.UnixMilli(wallMS).Sub(c.Wall()))
}

func (c *fakeReceiptClock) stepWall(d time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.wall = c.wall.Add(d)
}

func (c *fakeReceiptClock) registry() *Registry {
	return NewRegistryWithClock(c.Wall, c.Mono)
}

// freshRegistry receives every heartbeat at the start of the wall clock, so no
// report reads older than its node's own last_seen_ms says: for tests that are
// not about a report's age.
func freshRegistry() *Registry {
	return newFakeReceiptClock(0).registry()
}
