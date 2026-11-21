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
	"testing"
	"time"

	"github.com/modelcontextprotocol/go-sdk/mcp"
)

const consoleToken = "console-token-0123456789"

// startConsoleGateway starts a gateway with console credentials enabled and
// its config file materialized in a temp dir, so console saves and reloads
// hit a real file. The optional mutate hook adjusts the config before start.
func startConsoleGateway(t *testing.T, mutate func(cfg *Config)) (*gateway, *httptest.Server, string) {
	t.Helper()
	fake := newFakeUpstream(t, true)
	cfg := testConfig(fake.ts.URL)
	cfg.Console = &AuthConfig{Tokens: []AuthClient{{Token: consoleToken}}}
	if mutate != nil {
		mutate(cfg)
	}
	path := filepath.Join(t.TempDir(), "gateway.json")
	data, err := json.Marshal(cfg)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, data, 0o600); err != nil {
		t.Fatal(err)
	}
	g := newGateway(cfg, "test")
	g.configPath = path
	ctx, cancel := context.WithCancel(context.Background())
	g.start(ctx)
	ts := httptest.NewServer(g.handler())
	t.Cleanup(func() {
		ts.Close()
		cancel()
		g.wait()
	})
	return g, ts, path
}

func apiRequest(t *testing.T, method, url, token string, body string) (*http.Response, string) {
	t.Helper()
	var reader *strings.Reader
	if body == "" {
		reader = strings.NewReader("")
	} else {
		reader = strings.NewReader(body)
	}
	req, err := http.NewRequest(method, url, reader)
	if err != nil {
		t.Fatal(err)
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	if body != "" {
		req.Header.Set("Content-Type", "application/json")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	data, err := io.ReadAll(resp.Body)
	if err != nil {
		t.Fatal(err)
	}
	return resp, string(data)
}

func TestConsoleDisabledWithoutTokens(t *testing.T) {
	_, ts, _ := startConsoleGateway(t, func(cfg *Config) { cfg.Console = nil })
	resp, _ := apiRequest(t, http.MethodGet, ts.URL+"/api/health", consoleToken, "")
	if resp.StatusCode != http.StatusForbidden {
		t.Fatalf("console without tokens must be 403, got %d", resp.StatusCode)
	}
}

func TestConsoleAuthRequired(t *testing.T) {
	_, ts, _ := startConsoleGateway(t, nil)
	if resp, _ := apiRequest(t, http.MethodGet, ts.URL+"/api/health", "", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing console token: expected 401, got %d", resp.StatusCode)
	}
	if resp, _ := apiRequest(t, http.MethodGet, ts.URL+"/api/health", "bogus-token-0123456789", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("wrong console token: expected 401, got %d", resp.StatusCode)
	}
	if resp, _ := apiRequest(t, http.MethodGet, ts.URL+"/api/health", consoleToken, ""); resp.StatusCode != http.StatusOK {
		t.Fatalf("valid console token: expected 200, got %d", resp.StatusCode)
	}
	// The downstream auth list must not open the console.
	_, ts2, _ := startConsoleGateway(t, func(cfg *Config) {
		cfg.Auth = &AuthConfig{Tokens: []AuthClient{{Name: "opencode", Token: "downstream-token-0123456"}}}
	})
	if resp, _ := apiRequest(t, http.MethodGet, ts2.URL+"/api/health", "downstream-token-0123456", ""); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("downstream token must not open the console, got %d", resp.StatusCode)
	}
}

// TestConsoleRateLimit: failed console logins trip their own per-IP block
// (429), and the console throttle never locks the /mcp/* endpoints (and vice
// versa) because the two credential lists count independently.
func TestConsoleRateLimit(t *testing.T) {
	const downstreamToken = "downstream-token-0123456"
	_, ts, _ := startConsoleGateway(t, func(cfg *Config) {
		cfg.AuthRateLimit = &AuthRateLimitConfig{WindowMs: 60000, MaxFails: 3, BlockMs: 60000}
		cfg.Auth = &AuthConfig{Tokens: []AuthClient{{Name: "opencode", Token: downstreamToken}}}
	})
	url := ts.URL + "/api/health"
	for i := 1; i <= 3; i++ {
		if resp, _ := apiRequest(t, http.MethodGet, url, "bogus-token-0123456789", ""); resp.StatusCode != http.StatusUnauthorized {
			t.Fatalf("failure %d: expected 401, got %d", i, resp.StatusCode)
		}
	}
	if resp, _ := apiRequest(t, http.MethodGet, url, consoleToken, ""); resp.StatusCode != http.StatusTooManyRequests {
		t.Fatalf("expected 429 for the valid console token while blocked, got %d", resp.StatusCode)
	}
	// The console block must not throttle /mcp/* authentications.
	if got := postStatus(t, ts.URL+"/mcp/test", "bogus-token-0123456789"); got != http.StatusUnauthorized {
		t.Fatalf("expected plain 401 on /mcp while the console is blocked, got %d", got)
	}
	if got := postStatus(t, ts.URL+"/mcp/test", downstreamToken); got != http.StatusOK {
		t.Fatalf("expected the valid downstream token to pass while the console is blocked, got %d", got)
	}
}

func TestConsoleStaticServedWithoutAuth(t *testing.T) {
	_, ts, _ := startConsoleGateway(t, nil)
	for _, path := range []string{"/", "/style.css", "/app.js"} {
		resp, body := apiRequest(t, http.MethodGet, ts.URL+path, "", "")
		if resp.StatusCode != http.StatusOK {
			t.Fatalf("static %s must be served without auth, got %d", path, resp.StatusCode)
		}
		if strings.TrimSpace(body) == "" {
			t.Fatalf("static %s returned an empty body", path)
		}
	}
}

func TestConsoleConfigRedacted(t *testing.T) {
	_, ts, _ := startConsoleGateway(t, func(cfg *Config) {
		cfg.Auth = &AuthConfig{Tokens: []AuthClient{{Name: "opencode", Token: "downstream-token-0123456"}}}
		cfg.Upstreams[0].Headers = map[string]string{"Authorization": "Bearer upstream-secret"}
	})
	resp, body := apiRequest(t, http.MethodGet, ts.URL+"/api/config", consoleToken, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("config get: expected 200, got %d", resp.StatusCode)
	}
	if strings.Contains(body, "downstream-token-0123456") || strings.Contains(body, "upstream-secret") {
		t.Fatalf("config get leaked secrets: %s", body)
	}
	if !strings.Contains(body, redactedPlaceholder) || !strings.Contains(body, "sha256:") {
		t.Fatalf("config get missing redactions: %s", body)
	}
	var view configView
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatal(err)
	}
	if len(view.Auth.Tokens) != 1 || view.Auth.Tokens[0].Name != "opencode" || !strings.HasPrefix(view.Auth.Tokens[0].Fingerprint, "sha256:") {
		t.Fatalf("auth token fingerprint view wrong: %+v", view.Auth)
	}
	if view.Upstreams[0].Headers["Authorization"] != redactedPlaceholder {
		t.Fatalf("header not redacted: %+v", view.Upstreams[0])
	}
}

func TestConsoleConfigPutHotReload(t *testing.T) {
	fake := newFakeUpstream(t, false)
	_, ts, path := startConsoleGateway(t, func(cfg *Config) {
		cfg.Upstreams = append(cfg.Upstreams, UpstreamConfig{Name: "second", URL: fake.ts.URL})
	})
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second)

	edit := map[string]any{
		"probeIntervalMs": 12345,
		"upstreams": []map[string]any{
			{"name": "test", "url": "http://10.255.255.1:1/mcp"},
			{"name": "second", "url": fake.ts.URL},
		},
	}
	body, _ := json.Marshal(edit)
	resp, respBody := apiRequest(t, http.MethodPut, ts.URL+"/api/config", consoleToken, string(body))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("config put: expected 200, got %d: %s", resp.StatusCode, respBody)
	}
	if !strings.Contains(respBody, `"persisted":true`) {
		t.Fatalf("expected persisted true: %s", respBody)
	}

	// File was rewritten and the in-memory config applied.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), "12345") {
		t.Fatalf("gateway.json not updated: %s", string(data))
	}
	deadline := time.Now().Add(5 * time.Second)
	var h healthResponse
	for {
		req, _ := http.NewRequest(http.MethodGet, ts.URL+"/healthz", nil)
		req.Header.Set("Authorization", "Bearer "+consoleToken)
		resp, err := http.DefaultClient.Do(req)
		if err == nil {
			_ = json.NewDecoder(resp.Body).Decode(&h)
			resp.Body.Close()
			if len(h.Upstreams) == 2 {
				break
			}
		}
		if time.Now().After(deadline) {
			t.Fatalf("second upstream never appeared: %+v", h.Upstreams)
		}
		time.Sleep(25 * time.Millisecond)
	}
	if pi := findGatewayProbeInterval(t, path); pi != 12345 {
		t.Fatalf("probeIntervalMs not applied: %d", pi)
	}
}

func findGatewayProbeInterval(t *testing.T, path string) int {
	t.Helper()
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	return cfg.ProbeIntervalMs
}

func TestConsoleConfigPutInvalidKeepsCurrent(t *testing.T) {
	g, ts, path := startConsoleGateway(t, nil)
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	bad := map[string]any{
		"upstreams": []map[string]any{
			{"name": "test", "url": "http://x"},
			{"name": "test", "url": "http://y"},
		},
	}
	body, _ := json.Marshal(bad)
	resp, _ := apiRequest(t, http.MethodPut, ts.URL+"/api/config", consoleToken, string(body))
	if resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("invalid config: expected 400, got %d", resp.StatusCode)
	}
	after, _ := os.ReadFile(path)
	if string(before) != string(after) {
		t.Fatal("invalid config must not touch gateway.json")
	}
	if g.config().Upstreams[0].URL == "http://x" {
		t.Fatal("invalid config must not be applied")
	}
}

func TestConsoleConfigPutPersistFailed(t *testing.T) {
	g, ts, path := startConsoleGateway(t, nil)
	g.configPath = filepath.Join(filepath.Dir(path), "missing-dir", "gateway.json")
	edit := map[string]any{"probeIntervalMs": 9999}
	body, _ := json.Marshal(edit)
	resp, respBody := apiRequest(t, http.MethodPut, ts.URL+"/api/config", consoleToken, string(body))
	if resp.StatusCode != http.StatusInternalServerError {
		t.Fatalf("unwritable config: expected 500, got %d: %s", resp.StatusCode, respBody)
	}
	if !strings.Contains(respBody, `"persisted":false`) {
		t.Fatalf("expected persisted false: %s", respBody)
	}
	if g.config().ProbeIntervalMs == 9999 {
		t.Fatal("failed persist must not apply the config")
	}

	// persist=false applies in memory only.
	g.configPath = path
	resp, respBody = apiRequest(t, http.MethodPut, ts.URL+"/api/config?persist=false", consoleToken, string(body))
	if resp.StatusCode != http.StatusOK || !strings.Contains(respBody, `"persisted":false`) {
		t.Fatalf("persist=false: expected 200/persisted false, got %d: %s", resp.StatusCode, respBody)
	}
	if g.config().ProbeIntervalMs != 9999 {
		t.Fatal("persist=false must still apply in memory")
	}
}

func TestConsoleConfigPutRedactionsKeepSecrets(t *testing.T) {
	_, ts, path := startConsoleGateway(t, func(cfg *Config) {
		cfg.Upstreams[0].Headers = map[string]string{"Authorization": "Bearer upstream-secret"}
		cfg.Auth = &AuthConfig{Tokens: []AuthClient{{Name: "opencode", Token: "downstream-token-0123456"}}}
	})
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second, "downstream-token-0123456")

	// Fetch fingerprints.
	_, body := apiRequest(t, http.MethodGet, ts.URL+"/api/config", consoleToken, "")
	var view configView
	if err := json.Unmarshal([]byte(body), &view); err != nil {
		t.Fatal(err)
	}
	fp := view.Auth.Tokens[0].Fingerprint

	edit := map[string]any{
		"auth": map[string]any{
			"tokens": []map[string]any{{"name": "renamed", "fingerprint": fp}},
		},
		"upstreams": []map[string]any{
			{"name": "test", "url": view.Upstreams[0].URL, "headers": map[string]string{"Authorization": redactedPlaceholder}},
		},
	}
	rawEdit, _ := json.Marshal(edit)
	resp, respBody := apiRequest(t, http.MethodPut, ts.URL+"/api/config", consoleToken, string(rawEdit))
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("redacted put: expected 200, got %d: %s", resp.StatusCode, respBody)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	if cfg.Auth.Tokens[0].Token != "downstream-token-0123456" || cfg.Auth.Tokens[0].Name != "renamed" {
		t.Fatalf("token rename must keep the token value: %+v", cfg.Auth.Tokens)
	}
	if cfg.Upstreams[0].Headers["Authorization"] != "Bearer upstream-secret" {
		t.Fatalf("redacted header must keep the stored value: %+v", cfg.Upstreams[0].Headers)
	}
}

func TestConsoleCatalog(t *testing.T) {
	_, ts, _ := startConsoleGateway(t, nil)
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second)

	resp, body := apiRequest(t, http.MethodGet, ts.URL+"/api/catalog?upstream=test&section=tools", consoleToken, "")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("catalog: expected 200, got %d: %s", resp.StatusCode, body)
	}
	var out struct {
		Total int `json:"total"`
		Items []struct {
			Name        string `json:"name"`
			Description string `json:"description"`
		} `json:"items"`
	}
	if err := json.Unmarshal([]byte(body), &out); err != nil {
		t.Fatal(err)
	}
	if out.Total != 1 || len(out.Items) != 1 || out.Items[0].Name != "echo" {
		t.Fatalf("catalog tools wrong: %+v", out)
	}

	if resp, _ = apiRequest(t, http.MethodGet, ts.URL+"/api/catalog?upstream=nope&section=tools", consoleToken, ""); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown upstream: expected 404, got %d", resp.StatusCode)
	}
	if resp, _ = apiRequest(t, http.MethodGet, ts.URL+"/api/catalog?upstream=test&section=bad", consoleToken, ""); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("bad section: expected 400, got %d", resp.StatusCode)
	}
	// Filtered query matches nothing.
	resp, body = apiRequest(t, http.MethodGet, ts.URL+"/api/catalog?upstream=test&section=tools&q=zzz", consoleToken, "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `"total":0`) {
		t.Fatalf("filtered catalog: expected empty, got %d: %s", resp.StatusCode, body)
	}
}

func TestConsoleLogsAndAttribution(t *testing.T) {
	g, ts, _ := startConsoleGateway(t, func(cfg *Config) {
		cfg.Auth = &AuthConfig{Tokens: []AuthClient{{Name: "opencode", Token: "downstream-token-0123456"}}}
	})
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second, "downstream-token-0123456")

	cs := connectClientAs(t, ts.URL+"/mcp/test", "downstream-token-0123456")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cs.CallTool(ctx, &mcp.CallToolParams{Name: "echo", Arguments: map[string]any{}}); err != nil {
		t.Fatalf("call tool: %v", err)
	}

	deadline := time.Now().Add(5 * time.Second)
	for {
		entries := g.logs.snapshot(recentLogCapacity, "")
		found := false
		for _, e := range entries {
			if e.Kind == "tool" && e.Caller == "opencode" && e.OK && strings.Contains(e.SrcIP, "127.0.0.1") {
				found = true
			}
		}
		if found {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("call attribution missing in logs: %+v", entries)
		}
		time.Sleep(25 * time.Millisecond)
	}

	// Mark the upstream down; the transition must appear as an event.
	u := g.upstream("test")
	if u == nil {
		t.Fatal("upstream test missing")
	}
	u.recordFailure(fmt.Errorf("boom"))
	u.recordFailure(fmt.Errorf("boom"))
	entries := g.logs.snapshot(recentLogCapacity, "event")
	foundDown := false
	for _, e := range entries {
		if e.Event == "down" && e.Upstream == "test" {
			foundDown = true
		}
	}
	if !foundDown {
		t.Fatalf("down event missing: %+v", entries)
	}

	// The logs API serves the same buffer with type filtering.
	resp, body := apiRequest(t, http.MethodGet, ts.URL+"/api/logs?type=event&limit=10", consoleToken, "")
	if resp.StatusCode != http.StatusOK || !strings.Contains(body, `"event":"down"`) {
		t.Fatalf("logs api: %d %s", resp.StatusCode, body)
	}
}

func TestAuthClientFormatsAndReuse(t *testing.T) {
	dir := t.TempDir()
	path := filepath.Join(dir, "gateway.json")
	write := func(content string) error {
		return os.WriteFile(path, []byte(content), 0o600)
	}
	// Legacy string list still parses.
	if err := write(`{"auth":{"tokens":["tok-0123456789abcdef"]},"upstreams":[{"name":"ida","url":"http://x"}]}`); err != nil {
		t.Fatal(err)
	}
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatalf("legacy tokens: %v", err)
	}
	if len(cfg.Auth.Tokens) != 1 || cfg.Auth.Tokens[0].Token != "tok-0123456789abcdef" || cfg.Auth.Tokens[0].Name != "" {
		t.Fatalf("legacy tokens parse: %+v", cfg.Auth.Tokens)
	}
	// Named objects parse.
	if err := write(`{"auth":{"tokens":[{"name":"opencode","token":"tok-0123456789abcdef"}]},"console":{"tokens":["console-0123456789abcd"]},"upstreams":[{"name":"ida","url":"http://x"}]}`); err != nil {
		t.Fatal(err)
	}
	cfg, err = loadConfig(path)
	if err != nil {
		t.Fatalf("named tokens: %v", err)
	}
	if cfg.Auth.Tokens[0].Name != "opencode" || !cfg.consoleEnabled() {
		t.Fatalf("named tokens parse: %+v", cfg.Auth)
	}
	// The same token must not serve both roles.
	if err := write(`{"auth":{"tokens":["same-token-0123456789"]},"console":{"tokens":["same-token-0123456789"]},"upstreams":[{"name":"ida","url":"http://x"}]}`); err != nil {
		t.Fatal(err)
	}
	if _, err := loadConfig(path); err == nil {
		t.Fatal("identical console/auth token must be rejected")
	}
}

func TestConfigSaveRewritesNamedTokens(t *testing.T) {
	// A console save must write the normalized named format back to disk.
	_, ts, path := startConsoleGateway(t, func(cfg *Config) {
		cfg.Auth = &AuthConfig{Tokens: []AuthClient{{Name: "opencode", Token: "downstream-token-0123456"}}}
	})
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second, "downstream-token-0123456")

	resp, respBody := apiRequest(t, http.MethodPut, ts.URL+"/api/config", consoleToken, "{}")
	if resp.StatusCode != http.StatusOK {
		t.Fatalf("empty edit: expected 200, got %d: %s", resp.StatusCode, respBody)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	var raw struct {
		Auth *struct {
			Tokens []map[string]any `json:"tokens"`
		} `json:"auth"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		t.Fatal(err)
	}
	if raw.Auth == nil || len(raw.Auth.Tokens) != 1 ||
		raw.Auth.Tokens[0]["token"] != "downstream-token-0123456" || raw.Auth.Tokens[0]["name"] != "opencode" {
		t.Fatalf("named token format not persisted: %s", string(data))
	}
}

// toggleRequest is a small helper around the upstream toggle API.
func toggleRequest(t *testing.T, url, token, name string, enabled bool) (*http.Response, string) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"name": name, "enabled": enabled})
	if err != nil {
		t.Fatal(err)
	}
	return apiRequest(t, http.MethodPost, url+"/api/upstream/toggle", token, string(body))
}

func TestUpstreamToggleLifecycle(t *testing.T) {
	_, ts, path := startConsoleGateway(t, func(cfg *Config) {
		cfg.Upstreams = append(cfg.Upstreams, UpstreamConfig{Name: "test2", URL: cfg.Upstreams[0].URL})
	})
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second)
	waitForState(t, ts.URL+"/healthz", "test2", "up", 5*time.Second)

	// Disable test2: it must lose its runner, drop out of /mcp/* and show up
	// in the health document as disabled.
	if resp, body := toggleRequest(t, ts.URL, consoleToken, "test2", false); resp.StatusCode != http.StatusOK {
		t.Fatalf("toggle off: expected 200, got %d: %s", resp.StatusCode, body)
	}
	waitForState(t, ts.URL+"/healthz", "test2", "disabled", 5*time.Second)
	if resp, _ := apiRequest(t, http.MethodPost, ts.URL+"/mcp/test2", "", "{}"); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("disabled upstream endpoint: expected 404, got %d", resp.StatusCode)
	}
	if g := newGatewayForTest(t, path); g.upstream("test2") != nil {
		t.Fatal("disabled upstream must not have a runner")
	}

	// The persisted file must carry enabled:false for test2 only.
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(data), `"enabled": false`) || strings.Count(string(data), "enabled") != 1 {
		t.Fatalf("enabled flag not persisted correctly: %s", string(data))
	}

	// Re-enable: the runner comes back and the endpoint serves again.
	if resp, body := toggleRequest(t, ts.URL, consoleToken, "test2", true); resp.StatusCode != http.StatusOK {
		t.Fatalf("toggle on: expected 200, got %d: %s", resp.StatusCode, body)
	}
	waitForState(t, ts.URL+"/healthz", "test2", "up", 5*time.Second)
	cs := connectClient(t, ts.URL+"/mcp/test2")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cs.ListTools(ctx, nil); err != nil {
		t.Fatalf("re-enabled upstream must serve tools: %v", err)
	}
}

// TestUpstreamToggleClearsQuarantine: the console toggle is the resume path
// for an auto-disabled (quarantined) upstream. Quarantine is runtime-only —
// the persisted config still says enabled — so toggling to enabled must
// clear the flag, reset failures and bring the endpoint back.
func TestUpstreamToggleClearsQuarantine(t *testing.T) {
	g, ts, path := startConsoleGateway(t, nil)
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second)
	u := g.upstream("test")
	if u == nil {
		t.Fatal("runner missing")
	}
	u.mu.Lock()
	u.quarantined = true
	u.failures = 99
	u.mu.Unlock()

	if resp, body := toggleRequest(t, ts.URL, consoleToken, "test", true); resp.StatusCode != http.StatusOK {
		t.Fatalf("toggle on: expected 200, got %d: %s", resp.StatusCode, body)
	}
	if u.isQuarantined() {
		t.Fatal("quarantine must be cleared after re-enable")
	}
	u.mu.Lock()
	failures := u.failures
	u.mu.Unlock()
	if failures != 0 {
		t.Fatalf("failures must reset on resume, got %d", failures)
	}
	// Quarantine is runtime-only: re-enabling must never persist
	// enabled:false for the upstream (the toggle writes enabled:true).
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(data), `"enabled": false`) {
		t.Fatalf("re-enable must not persist enabled:false: %s", string(data))
	}
	waitForState(t, ts.URL+"/healthz", "test", "up", 5*time.Second)
	cs := connectClient(t, ts.URL+"/mcp/test")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if _, err := cs.ListTools(ctx, nil); err != nil {
		t.Fatalf("resumed upstream must serve tools: %v", err)
	}
}

func TestUpstreamToggleAuthAndValidation(t *testing.T) {
	_, ts, _ := startConsoleGateway(t, func(cfg *Config) {
		cfg.Auth = &AuthConfig{Tokens: []AuthClient{{Name: "opencode", Token: "downstream-token-0123456"}}}
	})
	// Missing console token → 401.
	if resp, _ := toggleRequest(t, ts.URL, "", "test", false); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("missing console token: expected 401, got %d", resp.StatusCode)
	}
	// A downstream auth token must not open the console API.
	if resp, _ := toggleRequest(t, ts.URL, "downstream-token-0123456", "test", false); resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("downstream token: expected 401, got %d", resp.StatusCode)
	}
	// Unknown upstream → 404.
	if resp, _ := toggleRequest(t, ts.URL, consoleToken, "nope", false); resp.StatusCode != http.StatusNotFound {
		t.Fatalf("unknown upstream: expected 404, got %d", resp.StatusCode)
	}
	// Missing enabled field → 400.
	if resp, _ := apiRequest(t, http.MethodPost, ts.URL+"/api/upstream/toggle", consoleToken, `{"name":"test"}`); resp.StatusCode != http.StatusBadRequest {
		t.Fatalf("missing enabled: expected 400, got %d", resp.StatusCode)
	}
}

// newGatewayForTest re-loads the config file from disk into a fresh gateway,
// proving the persisted file (not just memory) carries the change.
func newGatewayForTest(t *testing.T, path string) *gateway {
	t.Helper()
	cfg, err := loadConfig(path)
	if err != nil {
		t.Fatal(err)
	}
	return newGateway(cfg, "test")
}
