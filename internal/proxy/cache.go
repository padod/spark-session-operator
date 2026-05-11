/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package proxy

import (
	"sync"
	"time"
)

// sessionCacheTTL is the lifetime for entries in the session and endpoint
// caches. Without expiry the maps would grow unbounded (memory leak) and a
// stale endpoint entry could route to a dead backend after a session moves.
// Five minutes is a compromise: long enough to absorb bursts of repeated
// lookups, short enough that a moved/deleted backend recovers quickly.
const sessionCacheTTL = 5 * time.Minute

// ttlCacheSweepThreshold is the soft size at which set() opportunistically
// sweeps expired entries. Sweeping is amortized — at most one full scan per
// TTL/2 — so the cost is bounded even under heavy churn.
const ttlCacheSweepThreshold = 1024

// ttlEntry pairs a cached value with its expiry timestamp.
type ttlEntry struct {
	value     string
	expiresAt time.Time
}

// ttlMap is a string→string cache with per-entry TTL. Reads evict expired
// entries lazily; writes opportunistically sweep when the map grows past
// ttlCacheSweepThreshold. All operations are safe for concurrent callers.
type ttlMap struct {
	mu        sync.Mutex
	ttl       time.Duration
	items     map[string]ttlEntry
	lastSweep time.Time
}

// newTTLMap returns a cache where entries expire ttl after they are written.
func newTTLMap(ttl time.Duration) *ttlMap {
	return &ttlMap{
		ttl:       ttl,
		items:     map[string]ttlEntry{},
		lastSweep: time.Now(),
	}
}

// get returns the cached value for key if present and not expired.
func (m *ttlMap) get(key string) (string, bool) {
	m.mu.Lock()
	defer m.mu.Unlock()

	e, ok := m.items[key]
	if !ok {
		return "", false
	}
	if !time.Now().Before(e.expiresAt) {
		delete(m.items, key)
		return "", false
	}
	return e.value, true
}

// set writes value under key with a fresh TTL.
func (m *ttlMap) set(key, value string) {
	m.mu.Lock()
	defer m.mu.Unlock()

	now := time.Now()
	if len(m.items) >= ttlCacheSweepThreshold && now.Sub(m.lastSweep) > m.ttl/2 {
		m.sweepLocked(now)
	}
	m.items[key] = ttlEntry{value: value, expiresAt: now.Add(m.ttl)}
}

// delete removes key from the cache. Safe to call for missing keys.
func (m *ttlMap) delete(key string) {
	m.mu.Lock()
	delete(m.items, key)
	m.mu.Unlock()
}

// len returns the current number of stored entries (including not-yet-evicted
// expired ones). Intended for tests and metrics only.
func (m *ttlMap) len() int {
	m.mu.Lock()
	defer m.mu.Unlock()
	return len(m.items)
}

// sweepLocked removes expired entries. Caller must hold mu.
func (m *ttlMap) sweepLocked(now time.Time) {
	for k, e := range m.items {
		if !now.Before(e.expiresAt) {
			delete(m.items, k)
		}
	}
	m.lastSweep = now
}
