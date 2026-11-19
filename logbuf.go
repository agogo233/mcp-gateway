package main

import (
	"sync"
	"time"
	"unicode/utf8"
)

// recentLogCapacity caps the in-memory activity timeline. It is large enough
// to review a useful window of calls and small enough to stay negligible in
// memory.
const recentLogCapacity = 500

// maxDetailLen bounds stored error texts / event summaries so a chatty
// upstream error cannot bloat memory or the console.
const maxDetailLen = 200

// logEntry is one record in the console "recent activity" timeline: either a
// forwarded MCP call (Kind tool|prompt|resource) or a lifecycle event
// (Kind event with Event up|down|reload). Entries never contain call
// arguments or full tokens.
type logEntry struct {
	Time     time.Time `json:"time"`
	Kind     string    `json:"kind"` // tool|prompt|resource|event
	Event    string    `json:"event,omitempty"`
	Caller   string    `json:"caller,omitempty"`
	SrcIP    string    `json:"srcIP,omitempty"`
	Name     string    `json:"name,omitempty"`
	Upstream string    `json:"upstream,omitempty"`
	OK       bool      `json:"ok"`
	Detail   string    `json:"detail,omitempty"`
	DurMs    int64     `json:"durMs,omitempty"`
}

// recentLog is a fixed-capacity in-memory ring buffer backing the console
// activity view. It never touches disk: entries live for the lifetime of the
// process and are lost on restart. Long-term retention stays with the
// standard container log (docker logs), which already records every call.
type recentLog struct {
	mu   sync.Mutex
	buf  []logEntry
	next int // next write position
	full bool
}

func newRecentLog() *recentLog {
	return &recentLog{buf: make([]logEntry, recentLogCapacity)}
}

// add appends one entry, overwriting the oldest once full. Safe for
// concurrent use; the buffer has its own lock and never calls back into
// gateway or upstream locks.
func (l *recentLog) add(e logEntry) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.buf[l.next] = e
	l.next++
	if l.next == recentLogCapacity {
		l.next = 0
		l.full = true
	}
}

// snapshot returns up to limit newest entries in reverse chronological
// order. kind optionally filters: "call" keeps tool/prompt/resource entries,
// "event" keeps lifecycle events; any other value keeps everything.
func (l *recentLog) snapshot(limit int, kind string) []logEntry {
	l.mu.Lock()
	defer l.mu.Unlock()
	n := recentLogCapacity
	if !l.full {
		n = l.next
	}
	if limit > n {
		limit = n
	}
	out := make([]logEntry, 0, limit)
	for i := 0; i < n && len(out) < limit; i++ {
		idx := (l.next - 1 - i + recentLogCapacity) % recentLogCapacity
		e := l.buf[idx]
		switch kind {
		case "call":
			if e.Kind == "event" {
				continue
			}
		case "event":
			if e.Kind != "event" {
				continue
			}
		}
		out = append(out, e)
	}
	return out
}

// truncate shortens s to at most max bytes on a rune boundary.
func truncate(s string, max int) string {
	if len(s) <= max {
		return s
	}
	cut := s[:max]
	for len(cut) > 0 && !utf8.ValidString(cut) {
		cut = cut[:len(cut)-1]
	}
	return cut
}
