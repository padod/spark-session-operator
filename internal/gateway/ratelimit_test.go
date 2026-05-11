/*
Copyright 2026.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0
*/

package gateway

import (
	"net/http"
	"testing"
	"time"

	"golang.org/x/time/rate"
)

// TestIPRateLimiter_Allow exercises the per-IP token bucket: requests
// within burst pass, the (burst+1)th from the same IP is rejected, and
// a different IP gets its own independent budget.
func TestIPRateLimiter_Allow(t *testing.T) {
	cases := []struct {
		name      string
		limit     rate.Limit
		burst     int
		hits      []string
		wantAllow []bool
	}{
		{
			name:      "burst fully consumed then blocked",
			limit:     rate.Every(time.Hour), // refill effectively disabled
			burst:     3,
			hits:      []string{"10.0.0.1", "10.0.0.1", "10.0.0.1", "10.0.0.1"},
			wantAllow: []bool{true, true, true, false},
		},
		{
			name:      "per-ip isolation",
			limit:     rate.Every(time.Hour),
			burst:     1,
			hits:      []string{"10.0.0.1", "10.0.0.1", "10.0.0.2"},
			wantAllow: []bool{true, false, true},
		},
		{
			name:      "burst of one only ever passes one",
			limit:     rate.Every(time.Hour),
			burst:     1,
			hits:      []string{"10.0.0.5", "10.0.0.5", "10.0.0.5"},
			wantAllow: []bool{true, false, false},
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			l := newIPRateLimiter(tc.limit, tc.burst, time.Minute)
			for i, ip := range tc.hits {
				if got := l.allow(ip); got != tc.wantAllow[i] {
					t.Fatalf("hit %d (ip=%s): allow=%v want %v", i, ip, got, tc.wantAllow[i])
				}
			}
		})
	}
}

// TestIPRateLimiter_Refill verifies the bucket refills over time so a
// previously blocked client recovers without a manual reset.
func TestIPRateLimiter_Refill(t *testing.T) {
	l := newIPRateLimiter(rate.Every(20*time.Millisecond), 1, time.Minute)

	if !l.allow("ip") {
		t.Fatal("first hit should pass")
	}
	if l.allow("ip") {
		t.Fatal("second hit should be blocked (burst exhausted)")
	}
	time.Sleep(40 * time.Millisecond)
	if !l.allow("ip") {
		t.Fatal("after refill window the next hit should pass")
	}
}

// TestIPRateLimiter_SweepReclaimsIdleBuckets ensures the idle-bucket
// sweep actually deletes stale entries once the map grows past
// sweepThreshold, bounding memory under churn from many distinct IPs.
func TestIPRateLimiter_SweepReclaimsIdleBuckets(t *testing.T) {
	prevThreshold := sweepThreshold
	sweepThreshold = 4
	t.Cleanup(func() { sweepThreshold = prevThreshold })

	l := newIPRateLimiter(rate.Every(time.Hour), 1, 5*time.Millisecond)

	// Seed the map with stale entries.
	for _, ip := range []string{"a", "b", "c", "d"} {
		l.allow(ip)
	}
	// Wait past idleTTL so all are eligible for reclamation.
	time.Sleep(15 * time.Millisecond)
	// Force lastSweep to be old enough to trigger the sweep on next allow().
	l.mu.Lock()
	l.lastSweep = time.Now().Add(-time.Hour)
	l.mu.Unlock()

	l.allow("trigger")

	l.mu.Lock()
	defer l.mu.Unlock()
	if got := len(l.buckets); got != 1 {
		t.Fatalf("after sweep buckets = %d, want 1", got)
	}
	if _, ok := l.buckets["trigger"]; !ok {
		t.Fatal("trigger bucket should remain")
	}
}

// TestClientIP confirms we key on the immediate TCP peer (host portion of
// RemoteAddr) and never on attacker-controllable headers such as
// X-Forwarded-For — that would let a single client cycle IPs.
func TestClientIP(t *testing.T) {
	cases := []struct {
		name       string
		remoteAddr string
		forwarded  string
		want       string
	}{
		{
			name:       "host port split",
			remoteAddr: "10.0.0.1:54321",
			want:       "10.0.0.1",
		},
		{
			name:       "ipv6 with port",
			remoteAddr: "[::1]:1234",
			want:       "::1",
		},
		{
			name:       "malformed remoteaddr falls back to raw value",
			remoteAddr: "weird",
			want:       "weird",
		},
		{
			name:       "x-forwarded-for is ignored",
			remoteAddr: "10.0.0.1:54321",
			forwarded:  "1.2.3.4",
			want:       "10.0.0.1",
		},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			r := &http.Request{RemoteAddr: tc.remoteAddr, Header: http.Header{}}
			if tc.forwarded != "" {
				r.Header.Set("X-Forwarded-For", tc.forwarded)
			}
			if got := clientIP(r); got != tc.want {
				t.Fatalf("got %q want %q", got, tc.want)
			}
		})
	}
}
