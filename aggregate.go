package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"sort"
	"strings"
	"sync"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// aggregate exposes a single MCP endpoint (/mcp/<name>) that merges the
// catalogs of every healthy upstream under prefixed names: tool "echo" on
// upstream "ida" becomes "ida_echo"; resource "file:///doc.txt" on "ida"
// becomes URI "ida##file:///doc.txt". Calls are routed back by longest
// matching upstream-name prefix (resources by the "##" separator). Upstreams
// that are down contribute nothing, so one broken upstream never blanks the
// whole aggregate catalog.
type aggregate struct {
	name    string
	gw      *gateway
	server  *mcp.Server
	handler http.Handler

	// dirty is signaled (non-blocking) whenever an upstream probe completes,
	// so catalog changes surface quickly without lock coupling.
	dirty chan struct{}

	mu        sync.Mutex
	tools     map[string]string // exposed name -> owner upstream
	prompts   map[string]string // exposed name -> owner upstream
	resources map[string]string // exposed URI -> owner upstream

	// Current mirrored definitions, used to detect same-name definition
	// changes from upstreams and replace them on the merged server.
	toolDefs     map[string]*mcp.Tool
	promptDefs   map[string]*mcp.Prompt
	resourceDefs map[string]*mcp.Resource

	// warned deduplicates conflict log lines across sync rounds.
	warned map[string]bool
}

func newAggregate(name string, gw *gateway) *aggregate {
	a := &aggregate{
		name:         name,
		gw:           gw,
		dirty:        make(chan struct{}, 1),
		tools:        map[string]string{},
		prompts:      map[string]string{},
		resources:    map[string]string{},
		toolDefs:     map[string]*mcp.Tool{},
		promptDefs:   map[string]*mcp.Prompt{},
		resourceDefs: map[string]*mcp.Resource{},
		warned:       map[string]bool{},
	}
	// Capabilities are advertised unconditionally so clients keep listing
	// even while every upstream is down (empty catalog).
	a.server = mcp.NewServer(
		&mcp.Implementation{Name: "mcp-gateway-aggregate/" + name, Version: gw.version},
		&mcp.ServerOptions{
			Capabilities: &mcp.ServerCapabilities{
				Tools:     &mcp.ToolCapabilities{ListChanged: true},
				Prompts:   &mcp.PromptCapabilities{ListChanged: true},
				Resources: &mcp.ResourceCapabilities{ListChanged: true},
			},
		},
	)
	a.handler = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return a.server }, nil)
	return a
}

func (a *aggregate) kick() {
	select {
	case a.dirty <- struct{}{}:
	default:
	}
}

// run keeps the merged catalog in sync: immediately on a dirty signal and
// periodically as a safety net.
func (a *aggregate) run(ctx context.Context) {
	ticker := time.NewTicker(2 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-a.dirty:
		case <-ticker.C:
		}
		a.sync()
	}
}

// sync rebuilds the desired merged catalog from every healthy upstream's
// snapshot and applies only the difference to the local server.
func (a *aggregate) sync() {
	desiredTools := map[string]*mcp.Tool{}
	desiredPrompts := map[string]*mcp.Prompt{}
	desiredResources := map[string]*mcp.Resource{}
	toolOwner := map[string]string{}
	promptOwner := map[string]string{}
	resOwner := map[string]string{}
	claim := func(seen map[string]string, exposed, owner string) bool {
		if prev, ok := seen[exposed]; ok {
			if prev != owner && !a.warned[exposed] {
				a.warned[exposed] = true
				log.Printf("aggregate %q: skipping %q from upstream %q, already provided by %q", a.name, exposed, owner, prev)
			}
			return false
		}
		seen[exposed] = owner
		return true
	}

	// Visit upstreams in a deterministic order (config-independent, by name)
	// so the conflict winner is reproducible across sync rounds.
	ups := a.gw.allUpstreams()
	sort.Slice(ups, func(i, j int) bool { return ups[i].cfg.Name < ups[j].cfg.Name })
	for _, u := range ups {
		snap := u.snapshot()
		if snap.state != stateUp {
			continue
		}
		for name, t := range snap.tools {
			exposed := u.cfg.Name + "_" + name
			if !claim(toolOwner, exposed, u.cfg.Name) {
				continue
			}
			tt := *t
			tt.Name = exposed
			desiredTools[exposed] = &tt
		}
		for name, p := range snap.prompts {
			exposed := u.cfg.Name + "_" + name
			if !claim(promptOwner, exposed, u.cfg.Name) {
				continue
			}
			pp := *p
			pp.Name = exposed
			desiredPrompts[exposed] = &pp
		}
		for uri, r := range snap.resources {
			exposed := u.cfg.Name + "##" + uri
			if !claim(resOwner, exposed, u.cfg.Name) {
				continue
			}
			rr := *r
			rr.URI = exposed
			rr.Name = u.cfg.Name + "_" + r.Name
			desiredResources[exposed] = &rr
		}
	}

	a.mu.Lock()
	defer a.mu.Unlock()

	for name := range a.tools {
		if _, ok := desiredTools[name]; !ok {
			a.server.RemoveTools(name)
			delete(a.tools, name)
			delete(a.toolDefs, name)
		}
	}
	for name, t := range desiredTools {
		if cur, ok := a.toolDefs[name]; ok {
			if defJSONEqual(cur, t) {
				continue
			}
			// Same exposed name but changed definition: replace it.
			a.server.RemoveTools(name)
			delete(a.tools, name)
			delete(a.toolDefs, name)
		}
		if safeAddTool(a.server, t, a.handleTool) {
			a.tools[name] = toolOwner[name]
			a.toolDefs[name] = t
		}
	}

	for name := range a.prompts {
		if _, ok := desiredPrompts[name]; !ok {
			a.server.RemovePrompts(name)
			delete(a.prompts, name)
			delete(a.promptDefs, name)
		}
	}
	for name, p := range desiredPrompts {
		if cur, ok := a.promptDefs[name]; ok {
			if defJSONEqual(cur, p) {
				continue
			}
			a.server.RemovePrompts(name)
			delete(a.prompts, name)
			delete(a.promptDefs, name)
		}
		if safeAddPrompt(a.server, p, a.handlePrompt) {
			a.prompts[name] = promptOwner[name]
			a.promptDefs[name] = p
		}
	}

	for uri := range a.resources {
		if _, ok := desiredResources[uri]; !ok {
			a.server.RemoveResources(uri)
			delete(a.resources, uri)
			delete(a.resourceDefs, uri)
		}
	}
	for uri, r := range desiredResources {
		if cur, ok := a.resourceDefs[uri]; ok {
			if defJSONEqual(cur, r) {
				continue
			}
			a.server.RemoveResources(uri)
			delete(a.resources, uri)
			delete(a.resourceDefs, uri)
		}
		if safeAddResource(a.server, r, a.handleResource) {
			a.resources[uri] = resOwner[uri]
			a.resourceDefs[uri] = r
		}
	}
}

// split resolves an exposed name to its owner upstream and the original name
// using the longest matching upstream-name prefix. It is only a fallback for
// names that are not currently registered; normal routing goes through the
// exact ownership maps (routeTool/routePrompt).
func (a *aggregate) split(exposed string) (owner, original string, ok bool) {
	best := ""
	for up := range a.gw.upstreamNames() {
		if strings.HasPrefix(exposed, up+"_") && len(up) > len(best) {
			best = up
		}
	}
	if best == "" {
		return "", "", false
	}
	return best, exposed[len(best)+1:], true
}

// routeTool resolves an exposed tool name to its owner upstream via the
// registered ownership map (exact match). Routing by owner — instead of
// inferring the longest upstream-name prefix — keeps calls correct when
// upstream names are prefixes of each other (e.g. "a" and "a_b" both map a
// tool to the exposed name "a_b_c").
func (a *aggregate) routeTool(exposed string) (owner, original string, ok bool) {
	a.mu.Lock()
	owner, ok = a.tools[exposed]
	a.mu.Unlock()
	if !ok {
		return a.split(exposed)
	}
	return owner, exposed[len(owner)+1:], true
}

func (a *aggregate) routePrompt(exposed string) (owner, original string, ok bool) {
	a.mu.Lock()
	owner, ok = a.prompts[exposed]
	a.mu.Unlock()
	if !ok {
		return a.split(exposed)
	}
	return owner, exposed[len(owner)+1:], true
}

func (a *aggregate) handleTool(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
	owner, original, ok := a.routeTool(req.Params.Name)
	if !ok {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("unknown tool %q", req.Params.Name)}},
		}, nil
	}
	u := a.gw.upstream(owner)
	if u == nil {
		return &mcp.CallToolResult{
			IsError: true,
			Content: []mcp.Content{&mcp.TextContent{Text: fmt.Sprintf("upstream %q is unavailable", owner)}},
		}, nil
	}
	return u.handleTool(ctx, &mcp.CallToolRequest{Params: &mcp.CallToolParamsRaw{
		Name:      original,
		Arguments: req.Params.Arguments,
	}})
}

func (a *aggregate) handlePrompt(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
	owner, original, ok := a.routePrompt(req.Params.Name)
	if !ok {
		return nil, fmt.Errorf("unknown prompt %q", req.Params.Name)
	}
	u := a.gw.upstream(owner)
	if u == nil {
		return nil, fmt.Errorf("upstream %q is unavailable", owner)
	}
	return u.handlePrompt(ctx, &mcp.GetPromptRequest{Params: &mcp.GetPromptParams{
		Name:      original,
		Arguments: req.Params.Arguments,
	}})
}

func (a *aggregate) handleResource(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
	owner, original, ok := strings.Cut(req.Params.URI, "##")
	if !ok {
		return nil, fmt.Errorf("unknown resource %q", req.Params.URI)
	}
	u := a.gw.upstream(owner)
	if u == nil {
		return nil, fmt.Errorf("upstream %q is unavailable", owner)
	}
	return u.handleResource(ctx, &mcp.ReadResourceRequest{Params: &mcp.ReadResourceParams{URI: original}})
}

type aggregateHealth struct {
	Name      string `json:"name"`
	Tools     int    `json:"tools"`
	Prompts   int    `json:"prompts"`
	Resources int    `json:"resources"`
}

func (a *aggregate) health() aggregateHealth {
	a.mu.Lock()
	defer a.mu.Unlock()
	return aggregateHealth{
		Name:      a.name,
		Tools:     len(a.tools),
		Prompts:   len(a.prompts),
		Resources: len(a.resources),
	}
}
