package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// gateway owns the upstreams and the HTTP surface: one MCP endpoint per
// upstream (/mcp/<name>), an optional aggregate endpoint (/mcp/<aggregate>),
// a /healthz status endpoint and the embedded management console (/ and
// /api/*). The upstream set can change at runtime via applyConfig (hot
// reload), so routing is resolved per request.
type gateway struct {
	version    string
	startTime  time.Time
	rootCtx    context.Context
	configPath string // location of gateway.json, used by reload and console saves

	cfg atomic.Pointer[Config]

	// reloadMu serializes configuration reloads.
	reloadMu sync.Mutex

	// logs is the in-memory activity timeline shown in the console.
	logs *recentLog

	// authFails throttles failed /mcp/* authentications per source IP;
	// consoleFails does the same for /api/* console authentications. Both
	// share the authRateLimit settings but count independently.
	authFails    *authLimiter
	consoleFails *authLimiter

	// listen is the address the HTTP server actually bound to at startup.
	// It is surfaced via /api/health so the console can show drift between
	// the configured listen value and the effective one (listen changes
	// need a restart).
	listen atomic.Pointer[string]

	mu        sync.RWMutex
	upstreams map[string]*upstream
	aggregate *aggregate
	aggCancel context.CancelFunc

	wg sync.WaitGroup
}

func newGateway(cfg *Config, version string) *gateway {
	cfg.applyDefaults()
	g := &gateway{
		version:      version,
		startTime:    time.Now(),
		upstreams:    make(map[string]*upstream, len(cfg.Upstreams)),
		logs:         newRecentLog(),
		authFails:    newAuthLimiter(),
		consoleFails: newAuthLimiter(),
	}
	g.cfg.Store(cfg)
	if cfg.Aggregate != "" {
		g.aggregate = newAggregate(cfg.Aggregate, g)
	}
	return g
}

func (g *gateway) config() *Config { return g.cfg.Load() }

// setListen records the address the HTTP server actually bound to.
func (g *gateway) setListen(addr string) { g.listen.Store(&addr) }

// effectiveListen returns the bound address, or "" when unknown (tests).
func (g *gateway) effectiveListen() string {
	if a := g.listen.Load(); a != nil {
		return *a
	}
	return ""
}

// start launches one probe loop per enabled upstream plus the aggregate sync
// loop.
func (g *gateway) start(ctx context.Context) {
	g.rootCtx = ctx
	for _, uc := range g.config().Upstreams {
		if !uc.isEnabled() {
			log.Printf("upstream %q is disabled by config; not started", uc.Name)
			continue
		}
		g.addUpstream(uc)
	}
	if g.aggregate != nil {
		aggCtx, cancel := context.WithCancel(ctx)
		g.aggCancel = cancel
		g.wg.Add(1)
		go func() {
			defer g.wg.Done()
			g.aggregate.run(aggCtx)
		}()
	}
}

func (g *gateway) wait() { g.wg.Wait() }

func (g *gateway) addUpstream(uc UpstreamConfig) {
	u := newUpstream(uc, g)
	ctx, cancel := context.WithCancel(g.rootCtx)
	u.stop = cancel
	g.mu.Lock()
	g.upstreams[uc.Name] = u
	g.mu.Unlock()
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		u.run(ctx)
	}()
	log.Printf("upstream %q added (%s)", uc.Name, uc.URL)
}

func (g *gateway) removeUpstream(name string) {
	g.mu.Lock()
	u := g.upstreams[name]
	delete(g.upstreams, name)
	g.mu.Unlock()
	if u == nil {
		return
	}
	u.stop()
	select {
	case <-u.done:
	case <-time.After(3 * time.Second):
		log.Printf("warning: upstream %q probe loop did not exit in time", name)
	}
	log.Printf("upstream %q removed", name)
}

func (g *gateway) upstream(name string) *upstream {
	g.mu.RLock()
	defer g.mu.RUnlock()
	return g.upstreams[name]
}

func (g *gateway) allUpstreams() []*upstream {
	g.mu.RLock()
	defer g.mu.RUnlock()
	list := make([]*upstream, 0, len(g.upstreams))
	for _, u := range g.upstreams {
		list = append(list, u)
	}
	return list
}

func (g *gateway) upstreamNames() map[string]bool {
	g.mu.RLock()
	defer g.mu.RUnlock()
	names := make(map[string]bool, len(g.upstreams))
	for name := range g.upstreams {
		names[name] = true
	}
	return names
}

// aggregateName returns the configured aggregate path segment or "" when
// the aggregate endpoint is disabled.
func (g *gateway) aggregateName() string {
	g.mu.RLock()
	defer g.mu.RUnlock()
	if g.aggregate == nil {
		return ""
	}
	return g.aggregate.name
}

// kickAggregate nudges the aggregate sync loop after any upstream state or
// catalog change. Safe to call anytime; non-blocking.
func (g *gateway) kickAggregate() {
	g.mu.RLock()
	a := g.aggregate
	g.mu.RUnlock()
	if a != nil {
		a.kick()
	}
}

// applyConfig hot-swaps the configuration: upstreams are diffed (kept, added,
// removed or rebuilt when their settings changed), global parameters take
// effect for subsequent probes and the aggregate endpoint is rebuilt if its
// name changed.
func (g *gateway) applyConfig(cfg *Config) {
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()

	old := g.config()
	newByName := make(map[string]UpstreamConfig, len(cfg.Upstreams))
	for _, uc := range cfg.Upstreams {
		newByName[uc.Name] = uc
	}

	var toRemove []string
	var toAdd []UpstreamConfig
	for name := range g.upstreamNames() {
		if _, ok := newByName[name]; !ok {
			toRemove = append(toRemove, name)
		}
	}
	// Iterate the config slice (not the map) so the add order — and with it
	// the aggregate's registration order — is deterministic across reloads.
	for _, uc := range cfg.Upstreams {
		if !uc.isEnabled() {
			// Disabled upstreams have no runner; stop one if it is running.
			// Re-enabling later flips equal(), which rebuilds the runner.
			if g.upstream(uc.Name) != nil {
				toRemove = append(toRemove, uc.Name)
			}
			continue
		}
		if cur := g.upstream(uc.Name); cur != nil {
			if cur.cfg.equal(uc) {
				continue
			}
			toRemove = append(toRemove, uc.Name) // changed: rebuild from scratch
		}
		toAdd = append(toAdd, uc)
	}

	g.cfg.Store(cfg)

	for _, name := range toRemove {
		g.removeUpstream(name)
	}
	for _, uc := range toAdd {
		g.addUpstream(uc)
	}

	if cfg.Aggregate != old.Aggregate {
		g.rebuildAggregate(cfg.Aggregate)
	}
	log.Printf("config reloaded: %d upstream(s), aggregate=%q", len(cfg.Upstreams), cfg.Aggregate)
	g.logs.add(logEntry{
		Time:   time.Now(),
		Kind:   "event",
		Event:  "reload",
		Detail: truncate(fmt.Sprintf("%d upstream(s), aggregate=%q", len(cfg.Upstreams), cfg.Aggregate), maxDetailLen),
	})
}

// reloadFrom loads and applies a config file, keeping the current
// configuration on any error.
func (g *gateway) reloadFrom(path string) {
	if err := g.reloadFromFile(path); err != nil {
		log.Printf("config reload skipped, keeping current config: %v", err)
	}
}

// reloadFromFile loads and validates a config file and applies it, returning
// the validation error (if any) so callers such as the console API can report
// it. On error the current configuration is kept.
func (g *gateway) reloadFromFile(path string) error {
	cfg, err := loadConfig(path)
	if err != nil {
		return err
	}
	g.applyConfig(cfg)
	return nil
}

func (g *gateway) rebuildAggregate(name string) {
	g.mu.Lock()
	if g.aggCancel != nil {
		g.aggCancel()
	}
	if name == "" {
		g.aggregate = nil
		g.aggCancel = nil
		g.mu.Unlock()
		log.Printf("aggregate endpoint disabled")
		return
	}
	g.aggregate = newAggregate(name, g)
	var aggCtx context.Context
	aggCtx, g.aggCancel = context.WithCancel(g.rootCtx)
	agg := g.aggregate
	g.mu.Unlock()
	g.wg.Add(1)
	go func() {
		defer g.wg.Done()
		agg.run(aggCtx)
	}()
	log.Printf("aggregate endpoint /mcp/%s enabled", name)
}

// shutdown drains HTTP connections and waits for background loops to exit.
func (g *gateway) shutdown(srv *http.Server) {
	timeout := g.config().shutdownTimeout()
	sctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()
	_ = srv.Shutdown(sctx)
	// srv.Shutdown drains in-flight handlers (bounded by the timeout),
	// including console API calls running applyConfig. Hold reloadMu across
	// the wait below so no hot reload can call wg.Add concurrently with
	// wg.Wait ("Add after Wait" is undefined). A reload already in flight
	// always finishes first: applyConfig is bounded (removeUpstream waits at
	// most 3s), and upstreams added after ctx cancellation exit their probe
	// loop immediately.
	g.reloadMu.Lock()
	defer g.reloadMu.Unlock()
	done := make(chan struct{})
	go func() {
		g.wg.Wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(timeout):
		log.Printf("warning: background loops did not exit within %s", timeout)
	}
	log.Printf("stopped")
}

func (g *gateway) handler() http.Handler {
	return http.HandlerFunc(g.serveHTTP)
}

// bearerToken extracts the raw bearer token from the Authorization header.
func bearerToken(r *http.Request) string {
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return ""
	}
	return auth[len(prefix):]
}

// authorizeAuth enforces the downstream credential list (config "auth") that
// guards /mcp/* endpoints. When auth is not configured every caller is
// accepted with an empty principal.
func (g *gateway) authorizeAuth(r *http.Request) (principal, bool) {
	cfg := g.config()
	if !cfg.authEnabled() {
		return principal{}, true
	}
	if p, ok := cfg.Auth.match(bearerToken(r)); ok {
		return p, true
	}
	return principal{}, false
}

// principalInfo carries the authenticated identity and client IP through the
// MCP handler chain so forwarded calls can be attributed in the console log.
type principalInfo struct {
	principal principal
	srcIP     string
}

type ctxKey int

const principalKey ctxKey = 1

// withPrincipal attaches the authenticated identity to the request context.
func withPrincipal(r *http.Request, p principal, srcIP string) *http.Request {
	return r.WithContext(context.WithValue(r.Context(), principalKey, principalInfo{principal: p, srcIP: srcIP}))
}

// requestPrincipal returns the identity attached by withPrincipal, if any.
func requestPrincipal(ctx context.Context) (principal, string) {
	if v, ok := ctx.Value(principalKey).(principalInfo); ok {
		return v.principal, v.srcIP
	}
	return principal{}, ""
}

// clientIP extracts the bare IP (no port) from the request's remote address.
func clientIP(r *http.Request) string {
	if host, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
		return host
	}
	return r.RemoteAddr
}

func (g *gateway) serveHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("X-Content-Type-Options", "nosniff")
	w.Header().Set("X-Frame-Options", "DENY")
	w.Header().Set("Referrer-Policy", "no-referrer")
	// Inline styles are used by the console markup; scripts stay same-origin
	// only (no inline script exists). object/base/frame sources are closed
	// off as defense in depth.
	w.Header().Set("Content-Security-Policy", "default-src 'self'; style-src 'self' 'unsafe-inline'; object-src 'none'; base-uri 'none'; frame-ancestors 'none'")

	if r.URL.Path == "/healthz" {
		g.handleHealth(w, r)
		return
	}
	if strings.HasPrefix(r.URL.Path, "/api/") {
		g.serveAPI(w, r)
		return
	}
	// Console static assets are served without authentication: the page
	// itself contains no secrets, all data comes from authenticated /api/*
	// calls. Unknown paths fall through to auth/404 handling below.
	if r.Method == http.MethodGet || r.Method == http.MethodHead {
		if r.URL.Path == "/" || staticExists(strings.TrimPrefix(r.URL.Path, "/")) {
			g.serveStatic(w, r)
			return
		}
	}
	// Without downstream auth there is nothing to brute-force: accept all
	// (unknown paths 404 below), exactly as before the throttle existed.
	if !g.config().authEnabled() {
		if rest, has := strings.CutPrefix(r.URL.Path, "/mcp/"); has {
			g.serveMCP(w, withPrincipal(r, principal{}, clientIP(r)), rest)
			return
		}
		http.NotFound(w, r)
		return
	}
	// Throttle blocked sources before any token comparison, so a blocked
	// caller cannot use the 401/429/200 difference to verify a guessed
	// token. Rejection logs are paced per source IP.
	ip := clientIP(r)
	rl := g.config().authRateLimit()
	if b, retry := g.authFails.blocked(ip, rl, time.Now()); b {
		writeTooManyRequests(w, retry)
		return
	}
	if p, ok := g.authorizeAuth(r); ok {
		if rest, has := strings.CutPrefix(r.URL.Path, "/mcp/"); has {
			g.serveMCP(w, withPrincipal(r, p, clientIP(r)), rest)
			return
		}
		http.NotFound(w, r)
		return
	}
	b, shouldLog, total := g.authFails.recordFailure(ip, rl, time.Now())
	if b {
		// Raced into the block window between the two checks.
		writeTooManyRequests(w, retryAfter(rl))
		return
	}
	if shouldLog {
		log.Printf("auth: reject %s %s from %s (failures=%d/%d)", r.Method, r.URL.Path, r.RemoteAddr, total, rl.MaxFails)
	}
	w.Header().Set("WWW-Authenticate", `Bearer realm="mcp-gateway"`)
	http.Error(w, "unauthorized", http.StatusUnauthorized)
}

// retryAfter converts the block duration to whole seconds for Retry-After.
func retryAfter(rl AuthRateLimitConfig) int {
	return int(time.Duration(rl.BlockMs)*time.Millisecond/time.Second) + 1
}

// writeTooManyRequests answers an authentication attempt from a source that
// exceeded the failure limit. No log line here on purpose: blocked sources
// may retry indefinitely and must not flood the log via the 429 path either.
func writeTooManyRequests(w http.ResponseWriter, retryAfterSeconds int) {
	w.Header().Set("Retry-After", strconv.Itoa(retryAfterSeconds))
	http.Error(w, "too many requests", http.StatusTooManyRequests)
}

// serveMCP routes /mcp/<name> to the matching upstream (or aggregate)
// handler. Routing is resolved per request because the upstream set can
// change via hot reload.
func (g *gateway) serveMCP(w http.ResponseWriter, r *http.Request, rest string) {
	g.mu.RLock()
	u := g.upstreams[rest]
	var agg *aggregate
	if u == nil && g.aggregate != nil && rest == g.aggregate.name {
		agg = g.aggregate
	}
	g.mu.RUnlock()
	if u != nil {
		u.handler.ServeHTTP(w, r)
		return
	}
	if agg != nil {
		agg.handler.ServeHTTP(w, r)
		return
	}
	http.NotFound(w, r)
}

func (g *gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	_, authorized := g.authorizeAuth(r)
	_ = json.NewEncoder(w).Encode(g.healthDocument(authorized))
}

// healthDocument assembles the health status. Unauthenticated callers get a
// liveness-only answer: the process is up, but internal topology (upstream
// URLs, failure counters, versions) is not disclosed.
func (g *gateway) healthDocument(authorized bool) healthResponse {
	health := healthResponse{
		Status:    "ok",
		Version:   g.version,
		Uptime:    time.Since(g.startTime).Round(time.Second).String(),
		Time:      time.Now().UTC().Format(time.RFC3339),
		Listen:    g.effectiveListen(),
		Upstreams: []upstreamHealth{},
	}
	if !authorized {
		return healthResponse{Status: health.Status, Uptime: health.Uptime, Time: health.Time}
	}
	g.mu.RLock()
	agg := g.aggregate
	g.mu.RUnlock()
	if agg != nil {
		h := agg.health()
		health.Aggregate = &h
	}
	runners := make(map[string]bool, len(g.upstreams))
	for _, u := range g.allUpstreams() {
		h := u.health()
		runners[h.Name] = true
		health.Upstreams = append(health.Upstreams, h)
	}
	// Disabled upstreams have no runner; surface them so the console can
	// show (and flip) the enabled state.
	for _, uc := range g.config().Upstreams {
		if uc.isEnabled() || runners[uc.Name] {
			continue
		}
		health.Upstreams = append(health.Upstreams, upstreamHealth{
			Name:    uc.Name,
			URL:     uc.URL,
			State:   "disabled",
			Enabled: false,
		})
	}
	return health
}

type healthResponse struct {
	Status    string           `json:"status"`
	Version   string           `json:"version,omitempty"`
	Uptime    string           `json:"uptime"`
	Listen    string           `json:"listen,omitempty"`
	Aggregate *aggregateHealth `json:"aggregate,omitempty"`
	Upstreams []upstreamHealth `json:"upstreams,omitempty"`
	Time      string           `json:"time"`
}

// watchConfigFile polls the config file's mtime and reports the path on every
// change. SIGHUP works regardless; this makes "edit the mounted file" enough.
func watchConfigFile(ctx context.Context, path string, changes chan<- string) {
	var last time.Time
	if fi, err := os.Stat(path); err == nil {
		last = fi.ModTime()
	}
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fi, err := os.Stat(path)
			if err != nil || fi.ModTime().Equal(last) {
				continue
			}
			// Only advance `last` once the consumer accepted the report;
			// otherwise a change signaled while the consumer is busy with a
			// previous reload would be lost.
			select {
			case changes <- path:
				last = fi.ModTime()
			default:
			}
		}
	}
}
