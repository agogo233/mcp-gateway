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

	mu             sync.Mutex
	session        *mcp.ClientSession
	state          upstreamState
	failures       int
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
	ticker := time.NewTicker(u.gw.config().probeInterval())
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			u.closeSession()
			return
		case <-ticker.C:
			u.probeOnce(ctx)
			// Re-read the configured interval so hot reloads of
			// probeIntervalMs apply to existing upstreams too.
			ticker.Reset(u.gw.config().probeInterval())
		}
	}
}

func (u *upstream) probeOnce(ctx context.Context) {
	defer u.gw.kickAggregate()
	if s := u.currentSession(); s != nil {
		pctx, cancel := context.WithTimeout(ctx, u.gw.config().listTimeout())
		err := s.Ping(pctx, &mcp.PingParams{})
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
	u.lastErr = ""
	if u.state != stateUp {
		log.Printf("upstream %q is up", u.cfg.Name)
		u.state = stateUp
		u.lastTransition = time.Now()
	}
}

// recordFailure bumps the failure counter; it returns true once the
// consecutive-failure threshold is reached, in which case the upstream is
// marked down and its mirrored catalog is cleared.
func (u *upstream) recordFailure(err error) bool {
	u.mu.Lock()
	defer u.mu.Unlock()
	u.failures++
	u.lastErr = err.Error()
	threshold := u.gw.config().FailureThreshold
	if u.failures < threshold {
		log.Printf("upstream %q probe failed (%d/%d): %v", u.cfg.Name, u.failures, threshold, err)
		return false
	}
	if u.state != stateDown {
		log.Printf("upstream %q is down: %v (%d consecutive failures)", u.cfg.Name, err, u.failures)
		u.state = stateDown
		u.lastTransition = time.Now()
		u.clearCatalogLocked()
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

// logCall records one forwarded call's outcome without ever logging
// arguments (they may contain sensitive content).
func (u *upstream) logCall(kind, name string, start time.Time, ok bool, detail string) {
	dur := time.Since(start).Round(time.Millisecond)
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
		u.logCall("tool", req.Params.Name, start, false, "upstream unavailable")
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
		u.logCall("tool", req.Params.Name, start, false, err.Error())
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("upstream %q call %q failed: %v", u.cfg.Name, req.Params.Name, err)}},
		}, nil
	}
	u.logCall("tool", req.Params.Name, start, !res.IsError, "error result")
	return res, nil
}

func (u *upstream) handlePrompt(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	start := time.Now()
	s := u.currentSession()
	if s == nil {
		u.logCall("prompt", req.Params.Name, start, false, "upstream unavailable")
		return nil, fmt.Errorf("upstream %q is unavailable", u.cfg.Name)
	}
	cctx, cancel := context.WithTimeout(ctx, u.cfg.callTimeout())
	defer cancel()
	res, err := s.GetPrompt(cctx, &mcp.GetPromptParams{
		Name:      req.Params.Name,
		Arguments: req.Params.Arguments,
	})
	u.logCall("prompt", req.Params.Name, start, err == nil, errText(err))
	return res, err
}

func (u *upstream) handleResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	start := time.Now()
	s := u.currentSession()
	if s == nil {
		u.logCall("resource", req.Params.URI, start, false, "upstream unavailable")
		return nil, fmt.Errorf("upstream %q is unavailable", u.cfg.Name)
	}
	cctx, cancel := context.WithTimeout(ctx, u.cfg.callTimeout())
	defer cancel()
	res, err := s.ReadResource(cctx, &mcp.ReadResourceParams{URI: req.Params.URI})
	u.logCall("resource", req.Params.URI, start, err == nil, errText(err))
	return res, err
}

type upstreamHealth struct {
	Name           string `json:"name"`
	URL            string `json:"url"`
	State          string `json:"state"`
	Failures       int    `json:"failures"`
	LastError      string `json:"lastError,omitempty"`
	LastTransition string `json:"lastTransition,omitempty"`
	Tools          int    `json:"tools"`
	Prompts        int    `json:"prompts"`
	Resources      int    `json:"resources"`
}

func (u *upstream) health() upstreamHealth {
	u.mu.Lock()
	defer u.mu.Unlock()
	h := upstreamHealth{
		Name:      u.cfg.Name,
		URL:       u.cfg.URL,
		State:     u.state.String(),
		Failures:  u.failures,
		LastError: u.lastErr,
		Tools:     len(u.toolDefs),
		Prompts:   len(u.promptDefs),
		Resources: len(u.resourceDefs),
	}
	if !u.lastTransition.IsZero() {
		h.LastTransition = u.lastTransition.UTC().Format(time.RFC3339)
	}
	return h
}
