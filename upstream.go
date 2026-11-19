package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// Pagination caps for catalog listing. A broken or malicious upstream that
// keeps returning a non-empty cursor (or huge pages) must not pin the probe
// goroutine forever: exceeding either cap clears the section instead.
const (
	maxListPages = 100
	maxListItems = 100000
)

type upstreamState int

const (
	stateUnknown upstreamState = iota
	stateUp
	stateDown
)

func (s upstreamState) String() string {
	switch s {
	case stateUp:
		return "up"
	case stateDown:
		return "down"
	default:
		return "unknown"
	}
}

// upstream manages one MCP upstream: a probe loop keeps a client session
// alive, mirrors the upstream catalog into a local mcp.Server, and routes
// calls to the live session. The upstream is marked down only after
// FailureThreshold consecutive failed probes, so transient errors and
// long-running in-flight calls are not punished by a single hiccup.
type upstream struct {
	cfg        UpstreamConfig
	gw         *gateway
	httpClient *http.Client

	server  *mcp.Server
	handler http.Handler

	// stop cancels this upstream's probe loop; done is closed when the loop
	// has fully exited (sessions closed). Both are managed by the gateway.
	stop func()
	done chan struct{}
	// wake nudges the probe loop (e.g. after clearing a quarantine) without
	// waiting for the next tick; buffered so senders never block.
	wake chan struct{}

	mu             sync.Mutex
	session        *mcp.ClientSession
	state          upstreamState
	failures       int
	quarantined    bool
	lastErr        string
	lastTransition time.Time
	toolDefs       map[string]*mcp.Tool
	promptDefs     map[string]*mcp.Prompt
	resourceDefs   map[string]*mcp.Resource // keyed by URI
}

func newUpstream(cfg UpstreamConfig, gw *gateway) *upstream {
	u := &upstream{
		cfg:          cfg,
		gw:           gw,
		httpClient:   newHTTPClient(cfg.Headers),
		done:         make(chan struct{}),
		wake:         make(chan struct{}, 1),
		toolDefs:     map[string]*mcp.Tool{},
		promptDefs:   map[string]*mcp.Prompt{},
		resourceDefs: map[string]*mcp.Resource{},
	}
	// Capabilities are advertised unconditionally: opencode skips listing
	// prompts/resources entirely when the capability is missing, so an empty
	// catalog must still advertise all three sections.
	u.server = mcp.NewServer(
		&mcp.Implementation{Name: "mcp-gateway/" + cfg.Name, Version: gw.version},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				Tools:     &mcp.ToolCapabilities{ListChanged: true},
				Prompts:   &mcp.PromptCapabilities{ListChanged: true},
				Resources: &mcp.ResourceCapabilities{ListChanged: true},
			},
		},
	)
	u.handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return u.server }, nil)
	return u
}

func newHTTPClient(headers map[string]string) *http.Client {
	if len(headers) == 0 {
		return http.DefaultClient
	}
	return &http.Client{Transport: &headerTransport{base: http.DefaultTransport, headers: headers}}
}

type headerTransport struct {
	base    http.RoundTripper
	headers map[string]string
}

func (t *headerTransport) RoundTrip(req *http.Request) (*http.Response, error) {
	r := req.Clone(req.Context())
	for k, v := range t.headers {
		r.Header.Set(k, v)
	}
	return t.base.RoundTrip(r)
}

func (u *upstream) run(ctx context.Context) {
	defer close(u.done)
	u.probeOnce(ctx)
	ticker := time.NewTicker(u.nextProbeInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			u.closeSession()
			return
		case <-ticker.C:
			u.probeOnce(ctx)
			// Re-read the configured interval so hot reloads of
			// probeIntervalMs apply to existing upstreams too. A quarantined
			// upstream stops ticking entirely until it is re-enabled or its
			// config changes (both rebuild the runner).
			if u.isQuarantined() {
				stopTicker(ticker)
			} else {
				ticker.Reset(u.nextProbeInterval())
			}
		case <-u.wake:
			// Nudged (e.g. quarantine cleared): probe immediately and re-arm
			// the ticker (it may have been stopped while quarantined).
			u.probeOnce(ctx)
			if u.isQuarantined() {
				stopTicker(ticker)
			} else {
				ticker.Reset(u.nextProbeInterval())
			}
		}
	}
}

// stopTicker stops the ticker and drains a possibly buffered tick so a
// quarantined upstream is not probed one extra time after the decision to
// pause.
func stopTicker(ticker *time.Ticker) {
	ticker.Stop()
	for {
		select {
		case <-ticker.C:
			continue
		default:
		}
		break
	}
}

// probeIntervalFor computes the probe interval for an upstream with the given
// consecutive-failure count: the base interval doubled per failure beyond the
// threshold, capped at max. Pure so the schedule is unit-testable.
func probeIntervalFor(base time.Duration, failures, threshold int, max time.Duration) time.Duration {
	if max <= 0 {
		max = defaultMaxProbeIntervalMs * time.Millisecond
	}
	if base <= 0 {
		base = 15 * time.Second
	}
	if base > max {
		base = max
	}
	if failures <= threshold {
		return base
	}
	interval := base
	for i := threshold; i < failures; i++ {
		interval *= 2
		if interval >= max {
			return max
		}
	}
	if interval > max {
		interval = max
	}
	return interval
}

// nextProbeInterval is the delay until this upstream's next probe.
func (u *upstream) nextProbeInterval() time.Duration {
	u.mu.Lock()
	failures := u.failures
	u.mu.Unlock()
	return probeIntervalFor(
		u.cfg.probeInterval(u.gw.config().probeInterval()),
		failures,
		u.gw.config().FailureThreshold,
		u.gw.config().maxProbeInterval(),
	)
}

func (u *upstream) isQuarantined() bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.quarantined
}

// resume clears the runtime quarantine and nudges the probe loop so a
// quarantined upstream starts probing again immediately. It is a no-op for
// fresh runners (never quarantined) apart from one extra probe.
func (u *upstream) resume() {
	u.mu.Lock()
	u.quarantined = false
	u.failures = 0
	u.mu.Unlock()
	select {
	case u.wake <- struct{}{}:
	default:
	}
}

func (u *upstream) probeOnce(ctx context.Context) {
	defer u.gw.kickAggregate()
	if s := u.currentSession(); s != nil {
		pctx, cancel := context.WithTimeout(ctx, u.gw.config().listTimeout())
		err := u.livenessProbe(pctx, s)
		cancel()
		if err == nil {
			u.recordSuccess()
			u.syncCatalog(ctx, s)
			return
		}
		// Keep the session on the first failure so in-flight calls survive;
		// only retire it once the failure threshold is reached.
		if !u.recordFailure(err) {
			return
		}
		u.dropSession(s)
	}
	s, err := u.connect(ctx)
	if err != nil {
		u.recordFailure(err)
		return
	}
	u.setSession(s)
	u.recordSuccess()
	u.syncCatalog(ctx, s)
}

// mcpProtocolVersion20260728 is the protocol revision (SEP-2575) that removed
// the ping method and made liveness a transport concern. Mirrors go-sdk's
// usesNewProtocol threshold.
const mcpProtocolVersion20260728 = "2026-07-28"

// livenessProbe sends a cheap request the upstream's negotiated protocol
// still supports. The 2026-07-28 revision removed ping and strict transports
// (e.g. mark3labs/mcp-go based servers such as go-github-mcp) reject it with
// 400 before dispatch; go-sdk v1.8.0's Ping also omits the mandatory _meta
// protocol-version stamp. Probe with the first catalog list the server
// declares a capability for instead. A session that declares no capabilities
// at all is treated as alive (the initialize handshake succeeded and
// syncCatalog re-checks the sections on every probe).
func (u *upstream) livenessProbe(ctx context.Context, s *mcp.ClientSession) error {
	if res := s.InitializeResult(); res != nil && res.ProtocolVersion >= mcpProtocolVersion20260728 {
		caps := res.Capabilities
		switch {
		case caps != nil && caps.Tools != nil:
			_, err := s.ListTools(ctx, &mcp.ListToolsParams{})
			return err
		case caps != nil && caps.Prompts != nil:
			_, err := s.ListPrompts(ctx, &mcp.ListPromptsParams{})
			return err
		case caps != nil && caps.Resources != nil:
			_, err := s.ListResources(ctx, &mcp.ListResourcesParams{})
			return err
		default:
			return nil
		}
	}
	return s.Ping(ctx, &mcp.PingParams{})
}

func (u *upstream) connect(ctx context.Context) (*mcp.ClientSession, error) {
	cctx, cancel := context.WithTimeout(ctx, u.gw.config().connectTimeout())
	defer cancel()
	client := mcp.NewClient(&mcp.Implementation{Name: "mcp-gateway", Version: u.gw.version}, nil)
	transport := &mcp.StreamableClientTransport{
		Endpoint:   u.cfg.URL,
		HTTPClient: u.httpClient,
		MaxRetries: -1,
	}
	return client.Connect(cctx, transport, nil)
}

// listPaginated fetches one catalog section page by page. Each request gets
// its own listTimeout budget, so large catalogs are not truncated by a shared
// deadline. A failing page aborts the section (ok=false) without affecting
// the other sections.
func listPaginated[T any](u *upstream, ctx context.Context, section string, fetch func(ctx context.Context, cursor string) ([]T, string, error)) ([]T, bool) {
	var out []T
	cursor := ""
	pages := 0
	for {
		if pages >= maxListPages {
			log.Printf("upstream %q: list %s exceeded %d pages, clearing %s section", u.cfg.Name, section, maxListPages, section)
			return nil, false
		}
		pages++
		cctx, cancel := context.WithTimeout(ctx, u.gw.config().listTimeout())
		items, next, err := fetch(cctx, cursor)
		cancel()
		if err != nil {
			log.Printf("upstream %q: list %s failed, clearing %s section: %v", u.cfg.Name, section, section, err)
			return nil, false
		}
		out = append(out, items...)
		if len(out) > maxListItems {
			log.Printf("upstream %q: list %s exceeded %d items, clearing %s section", u.cfg.Name, section, maxListItems, section)
			return nil, false
		}
		if next == "" {
			return out, true
		}
		cursor = next
	}
}

// syncCatalog lists the upstream catalog (only sections the upstream
// declares) and mirrors it into the local server. A failing section is
// cleared on its own; it does not affect the other sections.
func (u *upstream) syncCatalog(ctx context.Context, s *mcp.ClientSession) {
	caps := s.InitializeResult().Capabilities

	tools := map[string]*mcp.Tool{}
	if caps != nil && caps.Tools != nil {
		items, ok := listPaginated(u, ctx, "tools", func(ctx context.Context, cursor string) ([]*mcp.Tool, string, error) {
			res, err := s.ListTools(ctx, &mcp.ListToolsParams{Cursor: cursor})
			if err != nil {
				return nil, "", err
			}
			return res.Tools, res.NextCursor, nil
		})
		if ok {
			for _, t := range items {
				tt := *t
				if tt.InputSchema == nil {
					tt.InputSchema = map[string]any{"type": "object"}
				}
				tools[tt.Name] = &tt
			}
		}
	}

	prompts := map[string]*mcp.Prompt{}
	if caps != nil && caps.Prompts != nil {
		items, ok := listPaginated(u, ctx, "prompts", func(ctx context.Context, cursor string) ([]*mcp.Prompt, string, error) {
			res, err := s.ListPrompts(ctx, &mcp.ListPromptsParams{Cursor: cursor})
			if err != nil {
				return nil, "", err
			}
			return res.Prompts, res.NextCursor, nil
		})
		if ok {
			for _, p := range items {
				pp := *p
				prompts[pp.Name] = &pp
			}
		}
	}

	resources := map[string]*mcp.Resource{}
	if caps != nil && caps.Resources != nil {
		items, ok := listPaginated(u, ctx, "resources", func(ctx context.Context, cursor string) ([]*mcp.Resource, string, error) {
			res, err := s.ListResources(ctx, &mcp.ListResourcesParams{Cursor: cursor})
			if err != nil {
				return nil, "", err
			}
			return res.Resources, res.NextCursor, nil
		})
		if ok {
			for _, r := range items {
				rr := *r
				resources[rr.URI] = &rr
			}
		}
	}

	u.reconcile(tools, prompts, resources)
}

// reconcile applies the desired catalog to the local server, registering,
// updating and unregistering only what changed so clients are not spammed
// with list-changed notifications on every probe.
func (u *upstream) reconcile(tools map[string]*mcp.Tool, prompts map[string]*mcp.Prompt, resources map[string]*mcp.Resource) {
	u.mu.Lock()
	defer u.mu.Unlock()

	for name := range u.toolDefs {
		if _, ok := tools[name]; !ok {
			u.server.RemoveTools(name)
			delete(u.toolDefs, name)
		}
	}
	for name, t := range tools {
		if cur, ok := u.toolDefs[name]; ok {
			if defJSONEqual(cur, t) {
				continue
			}
			// Same name but changed definition (schema, description, ...):
			// replace it so clients always see the upstream's current contract.
			u.server.RemoveTools(name)
			delete(u.toolDefs, name)
		}
		if !safeAddTool(u.server, t, u.handleTool) {
			continue
		}
		u.toolDefs[name] = t
	}

	for name := range u.promptDefs {
		if _, ok := prompts[name]; !ok {
			u.server.RemovePrompts(name)
			delete(u.promptDefs, name)
		}
	}
	for name, p := range prompts {
		if cur, ok := u.promptDefs[name]; ok {
			if defJSONEqual(cur, p) {
				continue
			}
			u.server.RemovePrompts(name)
			delete(u.promptDefs, name)
		}
		if !safeAddPrompt(u.server, p, u.handlePrompt) {
			continue
		}
		u.promptDefs[name] = p
	}

	for uri := range u.resourceDefs {
		if _, ok := resources[uri]; !ok {
			u.server.RemoveResources(uri)
			delete(u.resourceDefs, uri)
		}
	}
	for uri, r := range resources {
		if cur, ok := u.resourceDefs[uri]; ok {
			if defJSONEqual(cur, r) {
				continue
			}
			u.server.RemoveResources(uri)
			delete(u.resourceDefs, uri)
		}
		if !safeAddResource(u.server, r, u.handleResource) {
			continue
		}
		u.resourceDefs[uri] = r
	}
}

// defJSONEqual reports whether two definitions are identical via canonical
// JSON comparison (encoding/json sorts map keys, so this is stable). On a
// marshal error it conservatively reports "equal" to avoid remove/re-add
// churn on definitions that cannot be compared.
func defJSONEqual(a, b any) bool {
	if a == nil || b == nil {
		return a == nil && b == nil
	}
	aj, err := json.Marshal(a)
	if err != nil {
		return true
	}
	bj, err := json.Marshal(b)
	if err != nil {
		return true
	}
	return string(aj) == string(bj)
}

// The SDK's Server.AddXXX panic on malformed definitions (e.g. a tool whose
// input schema is missing or not of type "object"). A buggy upstream must not
// crash the whole gateway, so registrations are guarded.
func safeAddTool(srv *mcp.Server, t *mcp.Tool, h mcp.ToolHandler) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("add tool %q failed: %v", t.Name, r)
			ok = false
		}
	}()
	srv.AddTool(t, h)
	return true
}

func safeAddPrompt(srv *mcp.Server, p *mcp.Prompt, h mcp.PromptHandler) (ok bool) {
	defer func() {
		if r := recover(); r != nil {
			log.Printf("add prompt %q failed: %v", p.Name, r)
			ok = false
		}
	}()
	srv.AddPrompt(p, h)
	return true
}

func safeAddResource(srv *mcp.Server, r *mcp.Resource, h mcp.ResourceHandler) (ok bool) {
	defer func() {
		if rr := recover(); rr != nil {
			log.Printf("add resource %q failed: %v", r.URI, rr)
			ok = false
		}
	}()
	srv.AddResource(r, h)
	return true
}

// clearCatalogLocked drops every mirrored feature. Callers must hold u.mu.
func (u *upstream) clearCatalogLocked() {
	for name := range u.toolDefs {
		u.server.RemoveTools(name)
	}
	u.toolDefs = map[string]*mcp.Tool{}
	for name := range u.promptDefs {
		u.server.RemovePrompts(name)
	}
	u.promptDefs = map[string]*mcp.Prompt{}
	for uri := range u.resourceDefs {
		u.server.RemoveResources(uri)
	}
	u.resourceDefs = map[string]*mcp.Resource{}
}

// catalogSnapshot is a point-in-time copy of an upstream's mirrored catalog
// and health state, used by the aggregate endpoint.
type catalogSnapshot struct {
	state     upstreamState
	tools     map[string]*mcp.Tool
	prompts   map[string]*mcp.Prompt
	resources map[string]*mcp.Resource
}

func (u *upstream) snapshot() catalogSnapshot {
	u.mu.Lock()
	defer u.mu.Unlock()
	s := catalogSnapshot{
		state:     u.state,
		tools:     make(map[string]*mcp.Tool, len(u.toolDefs)),
		prompts:   make(map[string]*mcp.Prompt, len(u.promptDefs)),
		resources: make(map[string]*mcp.Resource, len(u.resourceDefs)),
	}
	for k, v := range u.toolDefs {
		s.tools[k] = v
	}
	for k, v := range u.promptDefs {
		s.prompts[k] = v
	}
	for k, v := range u.resourceDefs {
		s.resources[k] = v
	}
	return s
}

// recordSuccess resets the failure counter and marks the upstream up.
func (u *upstream) recordSuccess() {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.failures = 0
	u.quarantined = false
	u.lastErr = ""
	if u.state != stateUp {
		log.Printf("upstream %q is up", u.cfg.Name)
		u.state = stateUp
		u.lastTransition = time.Now()
		u.gw.logs.add(logEntry{Time: time.Now(), Kind: "event", Event: "up", Upstream: u.cfg.Name})
	}
}

// recordFailure bumps the failure counter; it returns true once the
// consecutive-failure threshold is reached, in which case the upstream is
// marked down and its mirrored catalog is cleared. When autoDisableAfter is
// configured and reached, the upstream is additionally quarantined: probing
// pauses entirely (runtime-only state, cleared by re-enabling it via the
// console, changing its config or restarting the process).
func (u *upstream) recordFailure(err error) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.failures++
	u.lastErr = err.Error()
	threshold := u.gw.config().FailureThreshold
	if limit := u.gw.config().autoDisableLimit(); limit > 0 && u.failures >= limit && !u.quarantined {
		u.quarantined = true
		log.Printf("upstream %q quarantined after %d consecutive failures; probing paused (resume via console toggle or config change)", u.cfg.Name, u.failures)
		u.gw.logs.add(logEntry{
			Time: time.Now(), Kind: "event", Event: "quarantined",
			Upstream: u.cfg.Name, Detail: truncate(err.Error(), maxDetailLen),
		})
	}
	if u.failures < threshold {
		log.Printf("upstream %q probe failed (%d/%d): %v", u.cfg.Name, u.failures, threshold, err)
		return false
	}
	if u.state != stateDown {
		log.Printf("upstream %q is down: %v (%d consecutive failures)", u.cfg.Name, err, u.failures)
		u.state = stateDown
		u.lastTransition = time.Now()
		u.clearCatalogLocked()
		u.gw.logs.add(logEntry{
			Time: time.Now(), Kind: "event", Event: "down",
			Upstream: u.cfg.Name, Detail: truncate(err.Error(), maxDetailLen),
		})
		return true
	}
	if u.failures%10 == 0 {
		log.Printf("upstream %q still down after %d consecutive failures: %v", u.cfg.Name, u.failures, err)
	}
	return true
}

func (u *upstream) currentSession() *mcp.ClientSession {
	u.mu.Lock()
	defer u.mu.Unlock()
	return u.session
}

func (u *upstream) setSession(s *mcp.ClientSession) {
	u.mu.Lock()
	defer u.mu.Unlock()
	if u.session != nil && u.session != s {
		u.session.Close()
	}
	u.session = s
}

func (u *upstream) dropSession(s *mcp.ClientSession) {
	u.mu.Lock()
	if u.session == s {
		u.session = nil
	}
	u.mu.Unlock()
	s.Close()
}

func (u *upstream) closeSession() {
	u.mu.Lock()
	s := u.session
	u.session = nil
	u.mu.Unlock()
	if s != nil {
		s.Close()
	}
}

func errText(err error) string {
	if err == nil {
		return ""
	}
	return err.Error()
}

// logCall records one forwarded call's outcome in the console activity log
// (with caller attribution) and in the standard container log, without ever
// logging arguments (they may contain sensitive content).
func (u *upstream) logCall(ctx context.Context, kind, name string, start time.Time, ok bool, detail string) {
	dur := time.Since(start).Round(time.Millisecond)
	p, srcIP := requestPrincipal(ctx)
	caller := p.Fingerprint
	if p.Name != "" {
		caller = p.Name
	}
	u.gw.logs.add(logEntry{
		Time:     time.Now(),
		Kind:     kind,
		Caller:   caller,
		SrcIP:    srcIP,
		Name:     name,
		Upstream: u.cfg.Name,
		OK:       ok,
		Detail:   truncate(detail, maxDetailLen),
		DurMs:    dur.Milliseconds(),
	})
	if ok {
		log.Printf("call %s %q upstream %q ok dur=%s", kind, name, u.cfg.Name, dur)
		return
	}
	log.Printf("call %s %q upstream %q failed dur=%s: %s", kind, name, u.cfg.Name, dur, detail)
}

// handleTool forwards a tools/call to the upstream session. When the upstream
// is down the call fails fast with an error result instead of hanging.
func (u *upstream) handleTool(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	start := time.Now()
	s := u.currentSession()
	if s == nil {
		u.logCall(ctx, "tool", req.Params.Name, start, false, "upstream unavailable")
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("upstream %q is unavailable", u.cfg.Name)}},
		}, nil
	}
	cctx, cancel := context.WithTimeout(ctx, u.cfg.callTimeout())
	defer cancel()
	res, err := s.CallTool(cctx, &mcp.CallToolParams{
		Name:      req.Params.Name,
		Arguments: req.Params.Arguments,
	})
	if err != nil {
		u.logCall(ctx, "tool", req.Params.Name, start, false, err.Error())
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("upstream %q call %q failed: %v", u.cfg.Name, req.Params.Name, err)}},
		}, nil
	}
	u.logCall(ctx, "tool", req.Params.Name, start, !res.IsError, "error result")
	return res, nil
}

func (u *upstream) handlePrompt(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	start := time.Now()
	s := u.currentSession()
	if s == nil {
		u.logCall(ctx, "prompt", req.Params.Name, start, false, "upstream unavailable")
		return nil, fmt.Errorf("upstream %q is unavailable", u.cfg.Name)
	}
	cctx, cancel := context.WithTimeout(ctx, u.cfg.callTimeout())
	defer cancel()
	res, err := s.GetPrompt(cctx, &mcp.GetPromptParams{
		Name:      req.Params.Name,
		Arguments: req.Params.Arguments,
	})
	u.logCall(ctx, "prompt", req.Params.Name, start, err == nil, errText(err))
	return res, err
}

func (u *upstream) handleResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	start := time.Now()
	s := u.currentSession()
	if s == nil {
		u.logCall(ctx, "resource", req.Params.URI, start, false, "upstream unavailable")
		return nil, fmt.Errorf("upstream %q is unavailable", u.cfg.Name)
	}
	cctx, cancel := context.WithTimeout(ctx, u.cfg.callTimeout())
	defer cancel()
	res, err := s.ReadResource(cctx, &mcp.ReadResourceParams{URI: req.Params.URI})
	u.logCall(ctx, "resource", req.Params.URI, start, err == nil, errText(err))
	return res, err
}

type upstreamHealth struct {
	Name           string `json:"name"`
	URL            string `json:"url"`
	State          string `json:"state"`
	Enabled        bool   `json:"enabled"`
	Quarantined    bool   `json:"quarantined,omitempty"`
	NextProbeMs    int64  `json:"nextProbeMs,omitempty"`
	Failures       int    `json:"failures"`
	LastError      string `json:"lastError,omitempty"`
	LastTransition string `json:"lastTransition,omitempty"`
	Tools          int    `json:"tools"`
	Prompts        int    `json:"prompts"`
	Resources      int    `json:"resources"`
}

func (u *upstream) health() upstreamHealth {
	u.mu.Lock()
	h := upstreamHealth{
		Name:        u.cfg.Name,
		URL:         u.cfg.URL,
		State:       u.state.String(),
		Enabled:     true, // runners only exist for enabled upstreams
		Failures:    u.failures,
		Quarantined: u.quarantined,
		LastError:   u.lastErr,
		Tools:       len(u.toolDefs),
		Prompts:     len(u.promptDefs),
		Resources:   len(u.resourceDefs),
	}
	lastTransition := u.lastTransition
	u.mu.Unlock()
	if !lastTransition.IsZero() {
		h.LastTransition = lastTransition.UTC().Format(time.RFC3339)
	}
	h.NextProbeMs = u.nextProbeInterval().Milliseconds()
	return h
}
