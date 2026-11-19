package main

import (
	"context"
	"crypto/sha256"
	"crypto/subtle"
	"encoding/json"
	"log"
	"net/http"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// gateway owns the upstreams and the HTTP surface: one MCP endpoint per
// upstream (/mcp/<name>), an optional aggregate endpoint (/mcp/<aggregate>)
// and a /healthz status endpoint. The upstream set can change at runtime via
// applyConfig (hot reload), so routing is resolved per request.
type gateway struct {
	version   string
	startTime time.Time
	rootCtx   context.Context

	cfg atomic.Pointer[Config]

	// reloadMu serializes configuration reloads.
	reloadMu sync.Mutex

	mu        sync.RWMutex
	upstreams map[string]*upstream
	aggregate *aggregate
	aggCancel context.CancelFunc

	wg sync.WaitGroup
}

func newGateway(cfg *Config, version string) *gateway {
	cfg.applyDefaults()
	g := &gateway{
		version:   version,
		startTime: time.Now(),
		upstreams: make(map[string]*upstream, len(cfg.Upstreams)),
	}
	g.cfg.Store(cfg)
	if cfg.Aggregate != "" {
		g.aggregate = newAggregate(cfg.Aggregate, g)
	}
	return g
}

func (g *gateway) config() *Config { return g.cfg.Load() }

// start launches one probe loop per upstream plus the aggregate sync loop.
func (g *gateway) start(ctx context.Context) {
	g.rootCtx = ctx
	for _, uc := range g.config().Upstreams {
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
}

// reloadFrom loads and validates a config file and applies it; on any error
// the current configuration is kept.
func (g *gateway) reloadFrom(path string) {
	cfg, err := loadConfig(path)
	if err != nil {
		log.Printf("config reload skipped, keeping current config: %v", err)
		return
	}
	g.applyConfig(cfg)
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

// authorize enforces the optional gateway-level bearer auth (config "auth").
// /healthz always answers, but only authorized callers get the detailed
// health document. Comparison is constant-time over SHA-256 digests so
// response timing does not leak token content or length.
func (g *gateway) authorize(r *http.Request) bool {
	cfg := g.config()
	if !cfg.authEnabled() {
		return true
	}
	const prefix = "Bearer "
	auth := r.Header.Get("Authorization")
	if len(auth) <= len(prefix) || !strings.EqualFold(auth[:len(prefix)], prefix) {
		return false
	}
	sum := sha256.Sum256([]byte(auth[len(prefix):]))
	for _, want := range cfg.tokenHashes() {
		if subtle.ConstantTimeCompare(sum[:], want[:]) == 1 {
			return true
		}
	}
	return false
}

func (g *gateway) serveHTTP(w http.ResponseWriter, r *http.Request) {
	if r.URL.Path == "/healthz" {
		g.handleHealth(w, r)
		return
	}
	if !g.authorize(r) {
		log.Printf("auth: reject %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		w.Header().Set("WWW-Authenticate", `Bearer realm="mcp-gateway"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	if rest, ok := strings.CutPrefix(r.URL.Path, "/mcp/"); ok {
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
	}
	http.NotFound(w, r)
}

func (g *gateway) handleHealth(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	health := healthResponse{
		Status:    "ok",
		Version:   g.version,
		Uptime:    time.Since(g.startTime).Round(time.Second).String(),
		Time:      time.Now().UTC().Format(time.RFC3339),
		Upstreams: []upstreamHealth{},
	}
	if !g.authorize(r) {
		// Unauthenticated callers (container healthchecks, port scanners) get
		// a liveness-only answer: the process is up, but internal topology
		// (upstream URLs, failure counters, versions) is not disclosed.
		_ = json.NewEncoder(w).Encode(healthResponse{
			Status: health.Status,
			Uptime: health.Uptime,
			Time:   health.Time,
		})
		return
	}
	g.mu.RLock()
	agg := g.aggregate
	g.mu.RUnlock()
	if agg != nil {
		h := agg.health()
		health.Aggregate = &h
	}
	for _, u := range g.allUpstreams() {
		health.Upstreams = append(health.Upstreams, u.health())
	}
	_ = json.NewEncoder(w).Encode(health)
}

type healthResponse struct {
	Status    string           `json:"status"`
	Version   string           `json:"version,omitempty"`
	Uptime    string           `json:"uptime"`
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
