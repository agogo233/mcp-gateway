package main

import (
	"sync"
	"time"
)

// maxTrackedIPs caps the failure map. Beyond it, records whose window and
// block have both expired are swept lazily, so a flood of spoofed sources
// cannot grow memory without bound.
const maxTrackedIPs = 4096

// authLimiter throttles failed authentication attempts per source IP. It is
// in-memory only: counters and blocks reset on process restart. /mcp/* and
// /api/* each get their own instance so exhausting one credential list does
// not lock out the other. The settings are read from the live configuration
// on every call, so hot reloads apply immediately.
type authLimiter struct {
	mu    sync.Mutex
	fails map[string]*failRecord
}

type failRecord struct {
	count        int
	windowStart  time.Time
	blockedUntil time.Time
}

func newAuthLimiter() *authLimiter {
	return &authLimiter{fails: make(map[string]*failRecord)}
}

// blocked reports whether the source IP is currently blocked, plus the
// remaining whole seconds for the Retry-After header.
func (l *authLimiter) blocked(ip string, rl AuthRateLimitConfig, now time.Time) (bool, int) {
	if rl.Disabled {
		return false, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	r, ok := l.fails[ip]
	if !ok || !r.blockedUntil.After(now) {
		return false, 0
	}
	return true, int(r.blockedUntil.Sub(now).Seconds()) + 1
}

// recordFailure counts one failed authentication attempt from ip. It returns
// whether the source is already past the limit (the caller answers 429), whether
// the 401 should be logged (first failure of a window and every 10th after
// that, so an attacker cannot flood the log), and the failure total within
// the current window. The attempt that trips the limit still answers 401 —
// it is a genuine auth failure; the block starts with the next request.
func (l *authLimiter) recordFailure(ip string, rl AuthRateLimitConfig, now time.Time) (blocked, shouldLog bool, total int) {
	if rl.Disabled {
		return false, false, 0
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	r := l.fails[ip]
	if r == nil {
		if len(l.fails) >= maxTrackedIPs {
			l.sweepLocked(rl, now)
		}
		if len(l.fails) >= maxTrackedIPs {
			// Hard cap: a distributed flood with more distinct sources than
			// the cap must not grow memory without bound. New sources still
			// get their plain 401, they are just not tracked until room
			// frees up.
			return false, false, 0
		}
		r = &failRecord{}
		l.fails[ip] = r
	}
	if r.blockedUntil.After(now) {
		return true, false, r.count
	}
	// A source that just came off a block starts from a clean window, so a
	// blockMs shorter than the window cannot chain new blocks from residual
	// counts.
	if !r.blockedUntil.IsZero() && !r.blockedUntil.After(now) {
		r.count = 0
		r.windowStart = now
	}
	window := time.Duration(rl.WindowMs) * time.Millisecond
	if r.windowStart.IsZero() || now.Sub(r.windowStart) > window {
		r.windowStart = now
		r.count = 0
	}
	r.count++
	total = r.count
	if r.count >= rl.MaxFails {
		r.blockedUntil = now.Add(time.Duration(rl.BlockMs) * time.Millisecond)
		return false, true, total
	}
	return false, total == 1 || total%10 == 0, total
}

// sweepLocked drops records whose counting window has expired and that are
// not blocked. Called only once the map reached maxTrackedIPs entries.
func (l *authLimiter) sweepLocked(rl AuthRateLimitConfig, now time.Time) {
	window := time.Duration(rl.WindowMs) * time.Millisecond
	for ip, r := range l.fails {
		if now.Sub(r.windowStart) > window && !r.blockedUntil.After(now) {
			delete(l.fails, ip)
		}
	}
}
