package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

// fakeUpstream is an in-process MCP server used as a stand-in for ida/x64dbg.
// The gate switch makes the whole endpoint return 503 to simulate an outage.
type fakeUpstream struct {
	ts       *httptest.Server
	gate     atomic.Bool
	inner    http.Handler
	lastAuth atomic.Value // last non-empty Authorization header seen
}

func newFakeUpstream(t *testing.T, withPromptsAndResources bool) *fakeUpstream {
	t.Helper()
	f := &fakeUpstream{}
	f.gate.Store(true)
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "0.0.1"}, nil)
	srv.AddTool(&mcp.Tool{
		Name:        "echo",
		Description: "echoes its input text",
		InputSchema: map[string]any{"type": "object"},
	}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{
			Content: []mcp.Content{&mcp.TextContent{Text: "echo:" + string(req.Params.Arguments)},
				&mcp.TextContent{Text: "echo:" + req.Params.Name}},
		}, nil
	})
	if withPromptsAndResources {
		srv.AddPrompt(&mcp.Prompt{Name: "greet", Description: "greeting prompt"},
			func(ctx context.Context, req *mcp.GetPromptRequest) (*mcp.GetPromptResult, error) {
				return &mcp.GetPromptResult{Messages: []*mcp.PromptMessage{{
					Role:    mcp.Role("assistant"),
					Content: &mcp.TextContent{Text: "hello"},
				}}}, nil
			})
		srv.AddResource(&mcp.Resource{Name: "doc", URI: "file:///doc.txt", MIMEType: "text/plain", Description: "a doc"},
			func(ctx context.Context, req *mcp.ReadResourceRequest) (*mcp.ReadResourceResult, error) {
				return &mcp.ReadResourceResult{Contents: []*mcp.ResourceContents{{
					URI:      req.Params.URI,
					MIMEType: "text/plain",
					Text:     "doc-body",
				}}}, nil
			})
	}
	f.inner = mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil)
	mux := http.NewServeMux()
	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		if a := r.Header.Get("Authorization"); a != "" {
			f.lastAuth.Store(a)
		}
		if !f.gate.Load() {
			http.Error(w, "unavailable", http.StatusServiceUnavailable)
			return
		}
		f.inner.ServeHTTP(w, r)
	})
	f.ts = httptest.NewServer(mux)
	t.Cleanup(f.ts.Close)
	return f
}

func testConfig(url string) *Config {
	return &Config{
		Listen:           ":0",
		ProbeIntervalMs:  50,
		ConnectTimeoutMs: 500,
		ListTimeoutMs:    500,
		FailureThreshold: 2,
		Upstreams:        []UpstreamConfig{{Name: "test", URL: url}},
	}
}

func startGateway(t *testing.T, cfg *Config) *httptest.Server {
	t.Helper()
	g := newGateway(cfg, "test")
	ctx, cancel := context.WithCancel(context.Background())
	g.start(ctx)
	ts := httptest.NewServer(g.handler())
	t.Cleanup(func() {
		ts.Close()
		cancel()
		g.wait()
	})
	return ts
}

func connectClient(t *testing.T, endpoint string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatalf("client connect to %s: %v", endpoint, err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

// waitForState polls the health endpoint until the named upstream reports
// the wanted state. When auth is enabled on the gateway, pass a valid token
// so the detailed health document (with upstream states) is returned.
func waitForState(t *testing.T, healthURL, name, want string, d time.Duration, token ...string) {
	t.Helper()
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		req, err := http.NewRequest(http.MethodGet, healthURL, nil)
		if err != nil {
			t.Fatal(err)
		}
		if len(token) > 0 && token[0] != "" {
			req.Header.Set("Authorization", "Bearer "+token[0])
		}
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			var h healthResponse
			err = json.NewDecoder(resp.Body).Decode(&h)
			resp.Body.Close()
			if err == nil {
				for _, u := range h.Upstreams {
					if u.Name == name && u.State == want {
						return
					}
				}
			}
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("upstream %q never reached state %q within %v", name, want, d)
}

// waitForAggregateTools polls the aggregate endpoint until exactly the wanted
// tool names are listed.
func waitForAggregateTools(t *testing.T, endpoint string, want []string, d time.Duration) {
	t.Helper()
	wantSet := map[string]bool{}
	for _, w := range want {
		wantSet[w] = true
	}
	deadline := time.Now().Add(d)
	for time.Now().Before(deadline) {
		cs := connectClient(t, endpoint)
		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		res, err := cs.ListTools(ctx, nil)
		cancel()
		cs.Close()
		if err == nil {
			got := map[string]bool{}
			for _, tl := range res.Tools {
				got[tl.Name] = true
			}
			if len(got) == len(wantSet) {
				match := true
				for k := range wantSet {
					if !got[k] {
						match = false
						break
					}
				}
				if match {
					return
				}
			}
		}
		time.Sleep(50 * time.Millisecond)
	}
	t.Fatalf("aggregate %s never listed %v within %v", endpoint, want, d)
}

// TestEndToEnd covers a healthy upstream: catalog mirroring with metadata
// intact, tool calls, prompt fetch and resource read through the gateway.
func TestEndToEnd(t *testing.T) {
	fake := newFakeUpstream(t, true)
	gw := startGateway(t, testConfig(fake.ts.URL))
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tools, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools: %v", err)
	}
	if len(tools.Tools) != 1 || tools.Tools[0].Name != "echo" {
		t.Fatalf("expected [echo], got %+v", tools.Tools)
	}
	if tools.Tools[0].Description != "echoes its input text" {
		t.Fatalf("tool description not preserved: %q", tools.Tools[0].Description)
	}
	if tools.Tools[0].InputSchema == nil {
		t.Fatal("tool input schema not preserved")
	}

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": "hi"}})
	if err != nil {
		t.Fatalf("call tool: %v", err)
	}
	if res.IsError || len(res.Content) == 0 {
		t.Fatalf("bad call result: %+v", res)
	}
	if tc, ok := res.Content[0].(*mcp.TextContent); !ok || !strings.HasPrefix(tc.Text, "echo:") {
		t.Fatalf("unexpected content: %+v", res.Content)
	}

	prompts, err := cs.ListPrompts(ctx, nil)
	if err != nil || len(prompts.Prompts) != 1 || prompts.Prompts[0].Name != "greet" {
		t.Fatalf("prompts list: %v %+v", err, prompts)
	}
	pr, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{Name: "greet"})
	if err != nil {
		t.Fatalf("get prompt: %v", err)
	}
	if len(pr.Messages) != 1 {
		t.Fatalf("expected 1 message, got %+v", pr.Messages)
	}
	if tc, ok := pr.Messages[0].Content.(*mcp.TextContent); !ok || tc.Text != "hello" {
		t.Fatalf("unexpected prompt content: %+v", pr.Messages[0].Content)
	}

	resources, err := cs.ListResources(ctx, nil)
	if err != nil || len(resources.Resources) != 1 || resources.Resources[0].URI != "file:///doc.txt" {
		t.Fatalf("resources list: %v %+v", err, resources)
	}
	rr, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: "file:///doc.txt"})
	if err != nil {
		t.Fatalf("read resource: %v", err)
	}
	if len(rr.Contents) != 1 || rr.Contents[0].Text != "doc-body" {
		t.Fatalf("unexpected resource contents: %+v", rr.Contents)
	}
}

// TestUnreachableUpstreamIsFastAndEmpty: an upstream that cannot be reached
// must not make the gateway hang; lists return instantly (empty) and calls
// fail fast with an error result.
func TestUnreachableUpstreamIsFastAndEmpty(t *testing.T) {
	cfg := testConfig("http://127.0.0.1:1/mcp")
	gw := startGateway(t, cfg)
	waitForState(t, gw.URL+"/healthz", "test", "down", 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	start := time.Now()
	tools, err := cs.ListTools(ctx, nil)
	elapsed := time.Since(start)
	if err != nil {
		t.Fatalf("list tools through gateway must work even when upstream is down: %v", err)
	}
	if len(tools.Tools) != 0 {
		t.Fatalf("expected empty tool list, got %+v", tools.Tools)
	}
	if elapsed > 2*time.Second {
		t.Fatalf("list took too long with unreachable upstream: %v", elapsed)
	}

	cr, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"text": "hi"}})
	// The SDK may reject the call as "unknown tool" (protocol error) since the
	// down upstream's catalog is empty; both that and an isError result are
	// acceptable, as long as it fails fast instead of hanging.
	if err == nil && !cr.IsError {
		t.Fatalf("expected failure for unavailable upstream: %+v", cr)
	}
	if time.Since(start) > 2*time.Second {
		t.Fatalf("call took too long with unreachable upstream: %v", time.Since(start))
	}
}

// TestRecovery: after the upstream comes back, the gateway must re-mirror its
// catalog within a probe cycle or two.
func TestRecovery(t *testing.T) {
	fake := newFakeUpstream(t, false)
	fake.gate.Store(false)
	gw := startGateway(t, testConfig(fake.ts.URL))
	waitForState(t, gw.URL+"/healthz", "test", "down", 5*time.Second)

	fake.gate.Store(true)
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 1 || tools.Tools[0].Name != "echo" {
		t.Fatalf("tools should be back after recovery: %v %+v", err, tools)
	}
}

// TestToolsOnlyUpstream: an upstream that only declares tools must be listed
// without prompts/resources sections, and must not be marked down because of
// the missing sections.
func TestToolsOnlyUpstream(t *testing.T) {
	fake := newFakeUpstream(t, false)
	gw := startGateway(t, testConfig(fake.ts.URL))
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 1 {
		t.Fatalf("tools list: %v %+v", err, tools)
	}
	prompts, err := cs.ListPrompts(ctx, nil)
	if err != nil {
		t.Fatalf("prompts list must return empty (not error): %v", err)
	}
	if len(prompts.Prompts) != 0 {
		t.Fatalf("expected no prompts, got %+v", prompts.Prompts)
	}

	resp, err := http.Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	var h healthResponse
	_ = json.NewDecoder(resp.Body).Decode(&h)
	resp.Body.Close()
	for _, u := range h.Upstreams {
		if u.Name == "test" && (u.Prompts != 0 || u.Resources != 0) {
			t.Fatalf("tools-only upstream should mirror no prompts/resources: %+v", u)
		}
	}
}

// TestSafeAddToolGuardsPanic: a malformed definition (e.g. a broken upstream
// sending a non-object input schema) must be skipped instead of panicking.
func TestSafeAddToolGuardsPanic(t *testing.T) {
	srv := mcp.NewServer(&mcp.Implementation{Name: "s", Version: "0"}, nil)
	ok := safeAddTool(srv, &mcp.Tool{
		Name:        "bad",
		Description: "schema is not an object",
		InputSchema: map[string]any{"type": "string"},
	}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
		return &mcp.CallToolResult{}, nil
	})
	if ok {
		t.Fatal("expected safeAddTool to report failure for malformed schema")
	}
}

// TestHeaderInjection: configured upstream headers (e.g. Bearer token) must
// reach the upstream with every request.
func TestHeaderInjection(t *testing.T) {
	fake := newFakeUpstream(t, false)
	cfg := testConfig(fake.ts.URL)
	cfg.Upstreams[0].Headers = map[string]string{"Authorization": "Bearer tok123"}
	gw := startGateway(t, cfg)
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second)

	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if v, _ := fake.lastAuth.Load().(string); v == "Bearer tok123" {
			return
		}
		time.Sleep(25 * time.Millisecond)
	}
	t.Fatalf("Authorization header was not forwarded to upstream (saw %v)", fake.lastAuth.Load())
}

// TestListPagination: the catalog lister must accumulate every page, give
// each page its own timeout budget and clear the section on page errors.
func TestListPagination(t *testing.T) {
	cfg := testConfig("http://127.0.0.1:1/mcp")
	g := newGateway(cfg, "test")
	u := newUpstream(cfg.Upstreams[0], g)

	calls := 0
	fetch := func(ctx context.Context, cursor string) ([]*mcp.Tool, string, error) {
		calls++
		switch cursor {
		case "":
			return []*mcp.Tool{{Name: "a"}, {Name: "b"}}, "page2", nil
		case "page2":
			return []*mcp.Tool{{Name: "c"}}, "", nil
		default:
			return nil, "", fmt.Errorf("unexpected cursor %q", cursor)
		}
	}
	items, ok := listPaginated(u, context.Background(), "tools", fetch)
	if !ok {
		t.Fatal("expected successful pagination")
	}
	if calls != 2 {
		t.Fatalf("expected 2 page fetches, got %d", calls)
	}
	if len(items) != 3 || items[0].Name != "a" || items[2].Name != "c" {
		t.Fatalf("expected accumulated pages [a b c], got %+v", items)
	}

	// A failing page clears the whole section.
	failing := func(ctx context.Context, cursor string) ([]*mcp.Tool, string, error) {
		if cursor == "" {
			return []*mcp.Tool{{Name: "a"}}, "page2", nil
		}
		return nil, "", fmt.Errorf("upstream exploded mid-pagination")
	}
	items, ok = listPaginated(u, context.Background(), "tools", failing)
	if ok || items != nil {
		t.Fatalf("expected section failure, got ok=%v items=%v", ok, items)
	}

	// An upstream that never runs out of cursors must be cut off by the page
	// cap instead of pinning the probe goroutine forever.
	count := 0
	endless := func(ctx context.Context, cursor string) ([]*mcp.Tool, string, error) {
		count++
		return []*mcp.Tool{{Name: "x"}}, "more", nil
	}
	items, ok = listPaginated(u, context.Background(), "tools", endless)
	if ok || items != nil {
		t.Fatalf("expected pagination cap to fail the section, got ok=%v items=%d", ok, len(items))
	}
	if count != maxListPages {
		t.Fatalf("expected %d page fetches before the cap, got %d", maxListPages, count)
	}

	// A single oversized page is rejected too.
	huge := func(ctx context.Context, cursor string) ([]*mcp.Tool, string, error) {
		if cursor == "" {
			page := make([]*mcp.Tool, maxListItems+1)
			for i := range page {
				page[i] = &mcp.Tool{Name: "t"}
			}
			return page, "", nil
		}
		return nil, "", fmt.Errorf("unreachable")
	}
	items, ok = listPaginated(u, context.Background(), "tools", huge)
	if ok || items != nil {
		t.Fatalf("expected item cap to fail the section, got ok=%v items=%d", ok, len(items))
	}
}

// TestAggregateEndpoint: the merged endpoint exposes prefixed names from all
// healthy upstreams, routes calls back, and drops a down upstream's items
// while keeping the rest.
func TestAggregateEndpoint(t *testing.T) {
	f1 := newFakeUpstream(t, false) // tools only
	f2 := newFakeUpstream(t, true)  // tools + prompts + resources
	cfg := &Config{
		Listen:           ":0",
		ProbeIntervalMs:  50,
		ConnectTimeoutMs: 500,
		ListTimeoutMs:    500,
		FailureThreshold: 2,
		Aggregate:        "all",
		Upstreams: []UpstreamConfig{
			{Name: "up1", URL: f1.ts.URL},
			{Name: "up2", URL: f2.ts.URL},
		},
	}
	gw := startGateway(t, cfg)
	waitForState(t, gw.URL+"/healthz", "up1", "up", 5*time.Second)
	waitForState(t, gw.URL+"/healthz", "up2", "up", 5*time.Second)
	waitForAggregateTools(t, gw.URL+"/mcp/all", []string{"up1_echo", "up2_echo"}, 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/all")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 2 {
		t.Fatalf("aggregate tools list: %v %+v", err, tools)
	}
	for _, tl := range tools.Tools {
		if tl.Description != "echoes its input text" {
			t.Fatalf("metadata not preserved through aggregate: %+v", tl)
		}
	}

	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "up1_echo", Arguments: map[string]any{"text": "hi"}})
	if err != nil || res.IsError {
		t.Fatalf("aggregate call up1_echo: %v %+v", err, res)
	}
	if tc, ok := res.Content[0].(*mcp.TextContent); !ok || !strings.HasPrefix(tc.Text, "echo:") {
		t.Fatalf("unexpected up1_echo content: %+v", res.Content)
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "up2_echo", Arguments: map[string]any{"text": "yo"}}); err != nil {
		t.Fatalf("aggregate call up2_echo: %v", err)
	}

	prompts, err := cs.ListPrompts(ctx, nil)
	if err != nil || len(prompts.Prompts) != 1 || prompts.Prompts[0].Name != "up2_greet" {
		t.Fatalf("aggregate prompts list: %v %+v", err, prompts)
	}
	pr, err := cs.GetPrompt(ctx, &mcp.GetPromptParams{Name: "up2_greet"})
	if err != nil || len(pr.Messages) != 1 {
		t.Fatalf("aggregate get prompt: %v %+v", err, pr)
	}
	resources, err := cs.ListResources(ctx, nil)
	if err != nil || len(resources.Resources) != 1 || resources.Resources[0].URI != "up2##file:///doc.txt" {
		t.Fatalf("aggregate resources list: %v %+v", err, resources)
	}
	rr, err := cs.ReadResource(ctx, &mcp.ReadResourceParams{URI: "up2##file:///doc.txt"})
	if err != nil || len(rr.Contents) != 1 || rr.Contents[0].Text != "doc-body" {
		t.Fatalf("aggregate read resource: %v %+v", err, rr)
	}

	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "missing_tool"}); err == nil {
		t.Fatal("expected unknown aggregate tool to fail")
	}

	// Take up1 down: its tools disappear from the aggregate, up2 stays.
	f1.gate.Store(false)
	waitForState(t, gw.URL+"/healthz", "up1", "down", 5*time.Second)
	waitForAggregateTools(t, gw.URL+"/mcp/all", []string{"up2_echo"}, 5*time.Second)
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "up1_echo"}); err == nil {
		t.Fatal("expected up1_echo to fail while up1 is down")
	}
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "up2_echo", Arguments: map[string]any{"text": "x"}}); err != nil {
		t.Fatalf("up2_echo must keep working while up1 is down: %v", err)
	}

	// Recovery brings it back.
	f1.gate.Store(true)
	waitForState(t, gw.URL+"/healthz", "up1", "up", 5*time.Second)
	waitForAggregateTools(t, gw.URL+"/mcp/all", []string{"up1_echo", "up2_echo"}, 5*time.Second)
}

// listToolsAt connects a fresh client to the endpoint and returns the tool
// names it lists.
func listToolsAt(t *testing.T, endpoint string) []string {
	t.Helper()
	cs := connectClient(t, endpoint)
	defer cs.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	res, err := cs.ListTools(ctx, nil)
	if err != nil {
		t.Fatalf("list tools at %s: %v", endpoint, err)
	}
	names := []string{}
	for _, tl := range res.Tools {
		names = append(names, tl.Name)
	}
	return names
}

// TestHotReload: applyConfig adds, removes and rebuilds upstreams without
// stopping the gateway, and swaps global parameters.
func TestHotReload(t *testing.T) {
	f1 := newFakeUpstream(t, false)
	f2 := newFakeUpstream(t, false)
	dir := t.TempDir()
	cfgPath := filepath.Join(dir, "gateway.json")

	writeCfg := func(upstreams []UpstreamConfig, probeInterval int) {
		t.Helper()
		cfg := Config{
			Listen:           ":0",
			ProbeIntervalMs:  probeInterval,
			ConnectTimeoutMs: 500,
			ListTimeoutMs:    500,
			FailureThreshold: 2,
			Upstreams:        upstreams,
		}
		data, err := json.Marshal(cfg)
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(cfgPath, data, 0o644); err != nil {
			t.Fatal(err)
		}
	}

	writeCfg([]UpstreamConfig{{Name: "a", URL: f1.ts.URL}}, 50)
	g := newGateway(mustLoad(t, cfgPath), "test")
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	g.start(ctx)
	ts := httptest.NewServer(g.handler())
	t.Cleanup(func() {
		ts.Close()
		g.wait()
	})
	waitForState(t, ts.URL+"/healthz", "a", "up", 5*time.Second)
	if got := listToolsAt(t, ts.URL+"/mcp/a"); len(got) != 1 {
		t.Fatalf("upstream a tools: %v", got)
	}

	// Add upstream b.
	writeCfg([]UpstreamConfig{{Name: "a", URL: f1.ts.URL}, {Name: "b", URL: f2.ts.URL}}, 50)
	g.reloadFrom(cfgPath)
	waitForState(t, ts.URL+"/healthz", "b", "up", 5*time.Second)
	if got := listToolsAt(t, ts.URL+"/mcp/b"); len(got) != 1 {
		t.Fatalf("upstream b tools after hot add: %v", got)
	}

	// Remove upstream a: /mcp/a disappears, b keeps working.
	writeCfg([]UpstreamConfig{{Name: "b", URL: f2.ts.URL}}, 50)
	g.reloadFrom(cfgPath)
	if resp, err := http.Get(ts.URL + "/mcp/a"); err != nil || resp.StatusCode != http.StatusNotFound {
		t.Fatalf("expected 404 for removed upstream a, got %v %v", err, resp)
	}
	if got := listToolsAt(t, ts.URL+"/mcp/b"); len(got) != 1 {
		t.Fatalf("upstream b must keep working after removing a: %v", got)
	}

	// Changing b's settings rebuilds it (same URL, different call timeout).
	writeCfg([]UpstreamConfig{{Name: "b", URL: f2.ts.URL, CallTimeoutMs: 12345}}, 50)
	g.reloadFrom(cfgPath)
	waitForState(t, ts.URL+"/healthz", "b", "up", 5*time.Second)
	if got := listToolsAt(t, ts.URL+"/mcp/b"); len(got) != 1 {
		t.Fatalf("upstream b must work after rebuild: %v", got)
	}

	// Global parameters are hot-swapped.
	if got := g.config().ProbeIntervalMs; got != 50 {
		t.Fatalf("expected initial probe interval 50, got %d", got)
	}
	writeCfg([]UpstreamConfig{{Name: "b", URL: f2.ts.URL, CallTimeoutMs: 12345}}, 1234)
	g.reloadFrom(cfgPath)
	if got := g.config().ProbeIntervalMs; got != 1234 {
		t.Fatalf("expected hot-swapped probe interval 1234, got %d", got)
	}

	// A broken config is rejected and the current one is kept.
	if err := os.WriteFile(cfgPath, []byte("{not json"), 0o644); err != nil {
		t.Fatal(err)
	}
	g.reloadFrom(cfgPath)
	if got := g.config().ProbeIntervalMs; got != 1234 {
		t.Fatalf("broken config must be ignored, probe interval changed to %d", got)
	}
	if g.upstream("b") == nil {
		t.Fatal("upstream b must survive a rejected reload")
	}
}

func mustLoad(t *testing.T, path string) *Config {
	t.Helper()
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg
}

// TestWatchConfigFile: the mtime poller reports the path after the file
// changes.
func TestWatchConfigFile(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.json")
	if err := os.WriteFile(path, []byte("{}"), 0o644); err != nil {
		t.Fatal(err)
	}
	changes := make(chan string, 1)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go watchConfigFile(ctx, path, changes)
	time.Sleep(200 * time.Millisecond) // let the watcher capture the initial mtime

	future := time.Now().Add(time.Hour)
	if err := os.Chtimes(path, future, future); err != nil {
		t.Fatal(err)
	}
	select {
	case got := <-changes:
		if got != path {
			t.Fatalf("expected %q, got %q", path, got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("watcher did not report the config change")
	}
}

// TestConcurrentCalls: one gateway client session must sustain many
// concurrent forwarded calls.
func TestConcurrentCalls(t *testing.T) {
	fake := newFakeUpstream(t, false)
	gw := startGateway(t, testConfig(fake.ts.URL))
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/test")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	const n = 20
	var wg sync.WaitGroup
	errCh := make(chan error, n)
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{"i": i}})
			if err != nil {
				errCh <- err
				return
			}
			if res.IsError {
				errCh <- fmt.Errorf("call %d returned isError", i)
			}
		}(i)
	}
	wg.Wait()
	close(errCh)
	for err := range errCh {
		t.Errorf("concurrent call failed: %v", err)
	}
}

// TestGracefulShutdown: cancelling the root context must stop every probe
// loop promptly (sessions closed), so process shutdown never hangs.
func TestGracefulShutdown(t *testing.T) {
	fake := newFakeUpstream(t, false)
	g := newGateway(testConfig(fake.ts.URL), "test")
	ctx, cancel := context.WithCancel(context.Background())
	g.start(ctx)
	ts := httptest.NewServer(g.handler())
	defer ts.Close()
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second)

	cancel()
	done := make(chan struct{})
	go func() {
		g.wait()
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("probe loops did not exit after context cancellation")
	}
}

// TestConfigValidation: aggregate names and upstream names must not collide
// or contain routing-reserved characters.
func TestConfigValidation(t *testing.T) {
	dir := t.TempDir()
	write := func(content string) error {
		return os.WriteFile(filepath.Join(dir, "gateway.json"), []byte(content), 0o644)
	}

	base := `"upstreams":[{"name":"ida","url":"http://x"}]`
	if err := write(`{"aggregate":"all",` + base + `}`); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(filepath.Join(dir, "gateway.json")); err != nil {
		t.Fatalf("valid aggregate config rejected: %v", err)
	}

	for _, bad := range []string{
		`{"aggregate":"all","upstreams":[{"name":"all","url":"http://x"}]}`,
		`{"upstreams":[{"name":"a/b","url":"http://x"}]}`,
		`{"upstreams":[{"name":"a##b","url":"http://x"}]}`,
		`{"aggregate":"all","upstreams":[{"name":"ida","url":"http://x"},{"name":"ida","url":"http://y"}]}`,
		`{"upstreams":[]}`,
		`{"auth":{"tokens":["short"]},"upstreams":[{"name":"ida","url":"http://x"}]}`,
		`{"auth":{"tokens":["aaaaaaaaaaaaaaaa","aaaaaaaaaaaaaaaa"]},"upstreams":[{"name":"ida","url":"http://x"}]}`,
		`{"auth":{"tokens":["   "]},"upstreams":[{"name":"ida","url":"http://x"}]}`,
		`{"auth":{"tokens":[]},"upstreams":[{"name":"ida","url":"http://x"}]}`,
		`{"auth":{},"upstreams":[{"name":"ida","url":"http://x"}]}`,
		`{"upstreams":[{"name":"ida","url":"not-a-url"}]}`,
		`{"upstreams":[{"name":"ida","url":"ftp://x/mcp"}]}`,
		`{"upstreams":[{"name":"ida","url":"http://"}]}`,
	} {
		if err := write(bad); err != nil {
			t.Fatal(err)
		}
		if _, err := loadConfig(filepath.Join(dir, "gateway.json")); err == nil {
			t.Fatalf("expected rejection for %s", bad)
		}
	}

	if err := write(`{"auth":{"tokens":["tok-0123456789abcdef"]},` + base + `}`); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(filepath.Join(dir, "gateway.json")); err != nil {
		t.Fatalf("valid auth config rejected: %v", err)
	}
}

// authTestConfig returns a config with bearer auth enabled.
func authTestConfig(url string, tokens ...string) *Config {
	cfg := testConfig(url)
	cfg.Auth = &AuthConfig{Tokens: tokens}
	return cfg
}

// authRoundTripper injects an Authorization header into every request the MCP
// client sends (initialize, posts and the SSE stream).
type authRoundTripper struct{ token string }

func (a authRoundTripper) RoundTrip(r *http.Request) (*http.Response, error) {
	r = r.Clone(r.Context())
	if a.token != "" {
		r.Header.Set("Authorization", "Bearer "+a.token)
	}
	return http.DefaultTransport.RoundTrip(r)
}

func connectClientAs(t *testing.T, endpoint, token string) *mcp.ClientSession {
	t.Helper()
	client := mcp.NewClient(&mcp.Implementation{Name: "test-client", Version: "0.0.1"}, nil)
	hc := &http.Client{Transport: authRoundTripper{token: token}}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	cs, err := client.Connect(ctx, &mcp.StreamableClientTransport{Endpoint: endpoint, HTTPClient: hc, MaxRetries: -1}, nil)
	if err != nil {
		t.Fatalf("client connect to %s: %v", endpoint, err)
	}
	t.Cleanup(func() { cs.Close() })
	return cs
}

// postStatus sends a minimal JSON-RPC ping to an endpoint and returns the
// HTTP status, used to assert auth rejections before any MCP handling. It
// uses a timeout client so an unexpected server hang fails the test instead
// of blocking the suite.
func postStatus(t *testing.T, url, token string) int {
	t.Helper()
	req, err := http.NewRequest(http.MethodPost, url, strings.NewReader(`{"jsonrpc":"2.0","id":1,"method":"ping"}`))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Accept", "application/json, text/event-stream")
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	client := &http.Client{Timeout: 5 * time.Second}
	resp, err := client.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	return resp.StatusCode
}

const (
	authTokenA = "token-alpha-0123456789"
	authTokenB = "token-beta-0123456789012"
)

// TestAuthBearerRequired: with auth configured, /mcp/* rejects missing or
// wrong tokens with 401 (+ WWW-Authenticate) and accepts the right one;
// /healthz stays open for probes.
func TestAuthBearerRequired(t *testing.T) {
	fake := newFakeUpstream(t, false)
	gw := startGateway(t, authTestConfig(fake.ts.URL, authTokenA))
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second, authTokenA)

	resp, err := http.Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz must stay open without a token, got %d", resp.StatusCode)
	}

	if got := postStatus(t, gw.URL+"/mcp/test", ""); got != http.StatusUnauthorized {
		t.Fatalf("missing token: expected 401, got %d", got)
	}
	if got := postStatus(t, gw.URL+"/mcp/test", "bogus-token-0123456789"); got != http.StatusUnauthorized {
		t.Fatalf("wrong token: expected 401, got %d", got)
	}

	cs := connectClientAs(t, gw.URL+"/mcp/test", authTokenA)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 1 || tools.Tools[0].Name != "echo" {
		t.Fatalf("authorized list tools: %v %+v", err, tools)
	}
}

// TestAuthUnknownPathAlsoProtected: auth runs before routing, so probing
// random paths reveals nothing without a token.
func TestAuthUnknownPathAlsoProtected(t *testing.T) {
	fake := newFakeUpstream(t, false)
	gw := startGateway(t, authTestConfig(fake.ts.URL, authTokenA))
	if got := postStatus(t, gw.URL+"/mcp/not-registered", ""); got != http.StatusUnauthorized {
		t.Fatalf("unknown path without token: expected 401, got %d", got)
	}
}

// TestAuthAggregateProtected: the aggregate endpoint requires the token too.
func TestAuthAggregateProtected(t *testing.T) {
	fake := newFakeUpstream(t, false)
	cfg := authTestConfig(fake.ts.URL, authTokenA)
	cfg.Aggregate = "all"
	gw := startGateway(t, cfg)
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second, authTokenA)

	if got := postStatus(t, gw.URL+"/mcp/all", ""); got != http.StatusUnauthorized {
		t.Fatalf("aggregate without token: expected 401, got %d", got)
	}
	cs := connectClientAs(t, gw.URL+"/mcp/all", authTokenA)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cs.ListTools(ctx, nil); err != nil {
		t.Fatalf("authorized aggregate list tools: %v", err)
	}
}

// TestAuthHotReloadRotate: token lists hot-reload; after a rotation the old
// token is rejected immediately and the new one accepted, without restart.
func TestAuthHotReloadRotate(t *testing.T) {
	fake := newFakeUpstream(t, false)
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.json")
	write := func(tokens ...string) {
		t.Helper()
		quoted := make([]string, len(tokens))
		for i, tok := range tokens {
			quoted[i] = fmt.Sprintf("%q", tok)
		}
		body := fmt.Sprintf(`{"listen":":0","probeIntervalMs":50,"connectTimeoutMs":500,"listTimeoutMs":500,"failureThreshold":2,"watchConfig":false,"auth":{"tokens":[%s]},"upstreams":[{"name":"test","url":%q}]}`, strings.Join(quoted, ","), fake.ts.URL)
		if err := os.WriteFile(path, []byte(body), 0o644); err != nil {
			t.Fatal(err)
		}
	}
	write(authTokenA)
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	g := newGateway(cfg, "test")
	ctx, cancel := context.WithCancel(context.Background())
	g.start(ctx)
	ts := httptest.NewServer(g.handler())
	// Same teardown order as startGateway: registered before the client's
	// cleanup, so the client session (and its SSE stream) closes first and
	// ts.Close does not wait on a still-open forwarded stream.
	t.Cleanup(func() {
		ts.Close()
		cancel()
		g.wait()
	})
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second, authTokenA)

	if got := postStatus(t, ts.URL+"/mcp/test", authTokenA); got == http.StatusUnauthorized {
		t.Fatal("token A must be accepted before rotation")
	}

	write(authTokenB)
	g.reloadFrom(path)

	if got := postStatus(t, ts.URL+"/mcp/test", authTokenA); got != http.StatusUnauthorized {
		t.Fatalf("old token must be rejected after rotation, got %d", got)
	}
	cs := connectClientAs(t, ts.URL+"/mcp/test", authTokenB)
	rctx, rcancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer rcancel()
	if _, err := cs.ListTools(rctx, nil); err != nil {
		t.Fatalf("new token must be accepted after rotation: %v", err)
	}
}

// TestHealthzHidesTopologyWithoutAuth: an unauthenticated /healthz request
// answers liveness only (no upstream URLs, counters or versions); the full
// health document requires a valid bearer token.
func TestHealthzHidesTopologyWithoutAuth(t *testing.T) {
	fake := newFakeUpstream(t, false)
	gw := startGateway(t, authTestConfig(fake.ts.URL, authTokenA))
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second, authTokenA)

	resp, err := http.Get(gw.URL + "/healthz")
	if err != nil {
		t.Fatal(err)
	}
	body, err := io.ReadAll(resp.Body)
	resp.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("healthz must stay open without a token, got %d", resp.StatusCode)
	}
	if strings.Contains(string(body), "upstreams") || strings.Contains(string(body), fake.ts.URL) {
		t.Fatalf("healthz leaked topology without auth: %s", body)
	}

	req, err := http.NewRequest(http.MethodGet, gw.URL+"/healthz", nil)
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+authTokenA)
	resp2, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	var h healthResponse
	err = json.NewDecoder(resp2.Body).Decode(&h)
	resp2.Body.Close()
	if err != nil {
		t.Fatal(err)
	}
	if len(h.Upstreams) != 1 || h.Upstreams[0].URL != fake.ts.URL {
		t.Fatalf("authenticated healthz must include upstream topology, got %+v", h.Upstreams)
	}
}

// TestDefinitionChangeSync: an upstream that changes a tool's definition
// under the same name must have the new definition mirrored, instead of the
// gateway keeping the stale one forever.
func TestDefinitionChangeSync(t *testing.T) {
	srv := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "0.0.1"}, nil)
	ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
	t.Cleanup(ts.Close)

	echo := func(desc string) {
		t.Helper()
		srv.AddTool(&mcp.Tool{
			Name:        "echo",
			Description: desc,
			InputSchema: map[string]any{"type": "object"},
		}, func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
			return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: desc}}}, nil
		})
	}
	echo("description-v1")

	gw := startGateway(t, testConfig(ts.URL))
	waitForState(t, gw.URL+"/healthz", "test", "up", 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/test")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	waitDesc := func(want string) {
		t.Helper()
		deadline := time.Now().Add(5 * time.Second)
		for time.Now().Before(deadline) {
			res, err := cs.ListTools(ctx, nil)
			if err == nil && len(res.Tools) == 1 && res.Tools[0].Description == want {
				return
			}
			time.Sleep(50 * time.Millisecond)
		}
		t.Fatalf("tool description never became %q", want)
	}
	waitDesc("description-v1")

	srv.RemoveTools("echo")
	echo("description-v2")
	waitDesc("description-v2")
}

// TestAggregatePrefixConflictRouting: upstreams "a" and "a_b" both map a tool
// onto the exposed name "a_b_c". The call must follow the registered owner
// (upstream "a", tool "b_c") instead of the longest-prefix guess, which would
// execute upstream "a_b"'s tool "c".
func TestAggregatePrefixConflictRouting(t *testing.T) {
	newFake := func(tool, marker string) *httptest.Server {
		srv := mcp.NewServer(&mcp.Implementation{Name: "fake", Version: "0.0.1"}, nil)
		srv.AddTool(&mcp.Tool{Name: tool, InputSchema: map[string]any{"type": "object"}},
			func(ctx context.Context, req *mcp.CallToolRequest) (*mcp.CallToolResult, error) {
				return &mcp.CallToolResult{Content: []mcp.Content{&mcp.TextContent{Text: marker}}}, nil
			})
		ts := httptest.NewServer(mcp.NewStreamableHTTPHandler(func(*http.Request) *mcp.Server { return srv }, nil))
		t.Cleanup(ts.Close)
		return ts
	}
	tsA := newFake("b_c", "from:a:b_c")
	tsAB := newFake("c", "from:a_b:c")

	cfg := &Config{
		Listen:           ":0",
		ProbeIntervalMs:  50,
		ConnectTimeoutMs: 500,
		ListTimeoutMs:    500,
		FailureThreshold: 2,
		Aggregate:        "all",
		Upstreams: []UpstreamConfig{
			{Name: "a", URL: tsA.URL},
			{Name: "a_b", URL: tsAB.URL},
		},
	}
	gw := startGateway(t, cfg)
	waitForState(t, gw.URL+"/healthz", "a", "up", 5*time.Second)
	waitForState(t, gw.URL+"/healthz", "a_b", "up", 5*time.Second)
	// Both upstreams produce the exposed name "a_b_c"; the claim goes to the
	// name-sorted first upstream ("a"), the other is skipped with a warning.
	waitForAggregateTools(t, gw.URL+"/mcp/all", []string{"a_b_c"}, 5*time.Second)

	cs := connectClient(t, gw.URL+"/mcp/all")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	tools, err := cs.ListTools(ctx, nil)
	if err != nil || len(tools.Tools) != 1 || tools.Tools[0].Name != "a_b_c" {
		t.Fatalf("aggregate tools: %v %+v", err, tools)
	}
	res, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "a_b_c"})
	if err != nil || res.IsError {
		t.Fatalf("call a_b_c: %v %+v", err, res)
	}
	tc, ok := res.Content[0].(*mcp.TextContent)
	if !ok || tc.Text != "from:a:b_c" {
		t.Fatalf("a_b_c must route to its owner upstream a, got %+v", res.Content)
	}
}
