/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package proxy

import (
	"testing"
	"time"
)

// TestTTLMap_Operations covers the session/endpoint cache lifecycle:
// fresh writes are readable, expired entries are evicted on access, and
// explicit delete removes entries. Table-driven so each behaviour failing
// is reported independently.
func TestTTLMap_Operations(t *testing.T) {
	cases := []struct {
		name       string
		ttl        time.Duration
		seed       map[string]string
		sleep      time.Duration
		op         func(m *ttlMap) (string, bool)
		wantValue  string
		wantExists bool
		wantLen    int
	}{
		{
			name:       "fresh entry is readable",
			ttl:        time.Minute,
			seed:       map[string]string{"k": "v"},
			op:         func(m *ttlMap) (string, bool) { return m.get("k") },
			wantValue:  "v",
			wantExists: true,
			wantLen:    1,
		},
		{
			name:       "missing key is not found",
			ttl:        time.Minute,
			seed:       map[string]string{"k": "v"},
			op:         func(m *ttlMap) (string, bool) { return m.get("absent") },
			wantExists: false,
			wantLen:    1,
		},
		{
			name:       "expired entry is evicted on read",
			ttl:        10 * time.Millisecond,
			seed:       map[string]string{"k": "v"},
			sleep:      30 * time.Millisecond,
			op:         func(m *ttlMap) (string, bool) { return m.get("k") },
			wantExists: false,
			wantLen:    0,
		},
		{
			name:       "delete removes entry",
			ttl:        time.Minute,
			seed:       map[string]string{"k": "v"},
			op:         func(m *ttlMap) (string, bool) { m.delete("k"); return m.get("k") },
			wantExists: false,
			wantLen:    0,
		},
		{
			name: "set refreshes ttl",
			ttl:  40 * time.Millisecond,
			seed: map[string]string{"k": "v"},
			// Sleep less than ttl, then re-set to extend, then sleep less than ttl again.
			op: func(m *ttlMap) (string, bool) {
				time.Sleep(25 * time.Millisecond)
				m.set("k", "v2")
				time.Sleep(25 * time.Millisecond)
				return m.get("k")
			},
			wantValue:  "v2",
			wantExists: true,
			wantLen:    1,
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			m := newTTLMap(tc.ttl)
			for k, v := range tc.seed {
				m.set(k, v)
			}
			if tc.sleep > 0 {
				time.Sleep(tc.sleep)
			}

			gotValue, gotOK := tc.op(m)
			if gotOK != tc.wantExists {
				t.Fatalf("exists: got %v, want %v", gotOK, tc.wantExists)
			}
			if gotOK && gotValue != tc.wantValue {
				t.Fatalf("value: got %q, want %q", gotValue, tc.wantValue)
			}
			if got := m.len(); got != tc.wantLen {
				t.Fatalf("len: got %d, want %d", got, tc.wantLen)
			}
		})
	}
}

// TestTTLMap_SweepUnderPressure verifies the opportunistic sweep on set()
// reclaims expired entries once the map grows past ttlCacheSweepThreshold.
// Without this guard the cache would grow unbounded between accesses.
func TestTTLMap_SweepUnderPressure(t *testing.T) {
	m := newTTLMap(10 * time.Millisecond)
	// Fill past the sweep threshold with entries that will expire immediately.
	for i := 0; i < ttlCacheSweepThreshold+10; i++ {
		m.set(itoa(i), "v")
	}
	// Force m.lastSweep to be old enough that the next set() triggers a sweep.
	m.mu.Lock()
	m.lastSweep = time.Now().Add(-time.Hour)
	m.mu.Unlock()

	// Wait for entries to expire.
	time.Sleep(30 * time.Millisecond)

	// One more write should trigger sweep and reclaim every prior entry.
	m.set("trigger", "v")

	// Only the just-written "trigger" entry should remain.
	if got := m.len(); got != 1 {
		t.Fatalf("after sweep len = %d, want 1", got)
	}
}

// itoa avoids strconv import bloat for the single test that needs it.
func itoa(i int) string {
	if i == 0 {
		return "0"
	}
	neg := i < 0
	if neg {
		i = -i
	}
	var buf [20]byte
	pos := len(buf)
	for i > 0 {
		pos--
		buf[pos] = byte('0' + i%10)
		i /= 10
	}
	if neg {
		pos--
		buf[pos] = '-'
	}
	return string(buf[pos:])
}
