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

package gateway

import (
	"net"
	"net/http"
	"sync"
	"time"

	"golang.org/x/time/rate"
)

// defaultRateLimit is the per-IP token-bucket configuration applied to
// auth-protected gateway endpoints: 1 token per second with a burst of 60,
// i.e. ~60 requests per minute sustained, brief bursts allowed.
var (
	defaultRateLimit = rate.Every(time.Second)
	defaultRateBurst = 60
	defaultIdleTTL   = 10 * time.Minute
	sweepThreshold   = 1024
)

// ipRateLimiter keeps a token-bucket limiter per remote IP. Idle buckets are
// reclaimed lazily when the map grows past sweepThreshold to bound memory.
type ipRateLimiter struct {
	mu        sync.Mutex
	buckets   map[string]*rateBucket
	limit     rate.Limit
	burst     int
	idleTTL   time.Duration
	lastSweep time.Time
}

type rateBucket struct {
	limiter  *rate.Limiter
	lastSeen time.Time
}

func newIPRateLimiter(limit rate.Limit, burst int, idleTTL time.Duration) *ipRateLimiter {
	return &ipRateLimiter{
		buckets: map[string]*rateBucket{},
		limit:   limit,
		burst:   burst,
		idleTTL: idleTTL,
	}
}

// allow consumes a token for ip and reports whether the request may proceed.
func (l *ipRateLimiter) allow(ip string) bool {
	l.mu.Lock()
	defer l.mu.Unlock()

	now := time.Now()
	if len(l.buckets) >= sweepThreshold && now.Sub(l.lastSweep) > l.idleTTL/2 {
		l.sweepLocked(now)
	}

	b, ok := l.buckets[ip]
	if !ok {
		b = &rateBucket{limiter: rate.NewLimiter(l.limit, l.burst)}
		l.buckets[ip] = b
	}
	b.lastSeen = now
	return b.limiter.Allow()
}

func (l *ipRateLimiter) sweepLocked(now time.Time) {
	for k, v := range l.buckets {
		if now.Sub(v.lastSeen) > l.idleTTL {
			delete(l.buckets, k)
		}
	}
	l.lastSweep = now
}

// clientIP extracts the host portion of r.RemoteAddr. X-Forwarded-For is
// intentionally not honored: rate limiting must key on a trustworthy source
// (the immediate peer) — header-spoofing would let attackers cycle IPs.
func clientIP(r *http.Request) string {
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}
