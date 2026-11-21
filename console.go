package main

import (
	"embed"
	"encoding/json"
	"fmt"
	"io/fs"
	"log"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"sync"
)

// webFiles holds the embedded management console; webRoot is the subtree
// served at "/" (index.html, style.css, app.js).
//
//go:embed all:web
var webFiles embed.FS

var webRoot = mustWebSub()

func mustWebSub() fs.FS {
	sub, err := fs.Sub(webFiles, "web")
	if err != nil {
		panic(err)
	}
	return sub
}

// staticExists reports whether an embedded console asset with the given
// (slash-separated, cleaned) name exists.
func staticExists(name string) bool {
	if name == "" {
		return false
	}
	f, err := webRoot.Open(name)
	if err != nil {
		return false
	}
	_ = f.Close()
	return true
}

// serveStatic serves the embedded console. Responses are no-store so an
// updated binary is picked up immediately.
func (g *gateway) serveStatic(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	http.FileServerFS(webRoot).ServeHTTP(w, r)
}

// apiWriteMu serializes console config saves end to end (read current config,
// resolve redactions, validate, persist) so two concurrent saves cannot
// interleave. It deliberately never wraps applyConfig, which takes reloadMu
// itself; the file is the source of truth and applyConfig is idempotent, so
// a concurrent file watcher reload is harmless.
var apiWriteMu sync.Mutex

// redactedPlaceholder replaces every secret value in console GET responses.
// A PUT that sends this placeholder (or omits the value) keeps the stored
// value unchanged.
const redactedPlaceholder = "***"

// maxConfigBody bounds the size of a console config PUT.
const maxConfigBody = 1 << 20

// authorizeConsole enforces the management credential list (config
// "console.tokens") that guards /api/*. It is fully independent of the
// downstream auth token list.
func (g *gateway) authorizeConsole(r *http.Request) (principal, bool) {
	cfg := g.config()
	if !cfg.consoleEnabled() {
		return principal{}, false
	}
	return cfg.Console.match(bearerToken(r))
}

func (g *gateway) serveAPI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Cache-Control", "no-store")
	if !g.config().consoleEnabled() {
		http.Error(w, "console api is disabled: configure console.tokens in gateway.json", http.StatusForbidden)
		return
	}
	if _, ok := g.authorizeConsole(r); !ok {
		log.Printf("console: unauthorized %s %s from %s", r.Method, r.URL.Path, r.RemoteAddr)
		w.Header().Set("WWW-Authenticate", `Bearer realm="mcp-gateway console"`)
		http.Error(w, "unauthorized", http.StatusUnauthorized)
		return
	}
	rest := strings.TrimPrefix(r.URL.Path, "/api")
	switch {
	case rest == "/health" && r.Method == http.MethodGet:
		g.writeJSON(w, http.StatusOK, g.healthDocument(true))
	case rest == "/catalog" && r.Method == http.MethodGet:
		g.handleAPICatalog(w, r)
	case rest == "/config" && r.Method == http.MethodGet:
		g.handleAPIConfigGet(w, r)
	case rest == "/config" && r.Method == http.MethodPut:
		g.handleAPIConfigPut(w, r)
	case rest == "/logs" && r.Method == http.MethodGet:
		g.handleAPILogs(w, r)
	case rest == "/reload" && r.Method == http.MethodPost:
		g.handleAPIReload(w, r)
	case rest == "/upstream/toggle" && r.Method == http.MethodPost:
		g.handleAPIUpstreamToggle(w, r)
	default:
		http.Error(w, "not found", http.StatusNotFound)
	}
}

func (g *gateway) writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

// handleAPICatalog serves one section of one upstream's mirrored catalog
// (tools, prompts or resources) with optional name/description filtering and
// pagination so very large catalogs cannot overwhelm the console.
func (g *gateway) handleAPICatalog(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	name := q.Get("upstream")
	section := q.Get("section")
	u := g.upstream(name)
	if u == nil {
		http.Error(w, "unknown upstream", http.StatusNotFound)
		return
	}
	snap := u.snapshot()

	type catalogItem struct {
		Name        string `json:"name,omitempty"`
		URI         string `json:"uri,omitempty"`
		Description string `json:"description,omitempty"`
		MIMEType    string `json:"mimeType,omitempty"`
		InputSchema any    `json:"inputSchema,omitempty"`
		Arguments   any    `json:"arguments,omitempty"`
	}
	items := []catalogItem{}
	switch section {
	case "tools":
		for _, t := range snap.tools {
			items = append(items, catalogItem{Name: t.Name, Description: t.Description, InputSchema: t.InputSchema})
		}
	case "prompts":
		for _, p := range snap.prompts {
			items = append(items, catalogItem{Name: p.Name, Description: p.Description, Arguments: p.Arguments})
		}
	case "resources":
		for _, res := range snap.resources {
			items = append(items, catalogItem{Name: res.Name, URI: res.URI, Description: res.Description, MIMEType: res.MIMEType})
		}
	default:
		http.Error(w, "section must be tools, prompts or resources", http.StatusBadRequest)
		return
	}

	sort.Slice(items, func(i, j int) bool {
		a, b := items[i].Name, items[j].Name
		if section == "resources" {
			a, b = items[i].URI, items[j].URI
		}
		return a < b
	})

	if needle := strings.ToLower(q.Get("q")); needle != "" {
		filtered := items[:0]
		for _, it := range items {
			hay := strings.ToLower(it.Name + " " + it.URI + " " + it.Description)
			if strings.Contains(hay, needle) {
				filtered = append(filtered, it)
			}
		}
		items = filtered
	}

	offset := 0
	if v := q.Get("offset"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			offset = n
		}
	}
	limit := 100
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > 1000 {
		limit = 1000
	}
	page := []catalogItem{}
	if offset < len(items) {
		end := offset + limit
		if end > len(items) {
			end = len(items)
		}
		page = items[offset:end]
	}
	g.writeJSON(w, http.StatusOK, map[string]any{"total": len(items), "items": page})
}

// tokenView is the redacted form of a credential: only an optional label and
// an irreversible fingerprint. Token values are never echoed back.
type tokenView struct {
	Name        string `json:"name,omitempty"`
	Fingerprint string `json:"fingerprint"`
}

type authView struct {
	Tokens []tokenView `json:"tokens"`
}

type upstreamView struct {
	Name            string            `json:"name"`
	URL             string            `json:"url"`
	CallTimeoutMs   int               `json:"callTimeoutMs,omitempty"`
	ProbeIntervalMs int               `json:"probeIntervalMs,omitempty"`
	Headers         map[string]string `json:"headers,omitempty"`
	Enabled         bool              `json:"enabled"`
}

type configView struct {
	Listen             string         `json:"listen"`
	ProbeIntervalMs    int            `json:"probeIntervalMs"`
	MaxProbeIntervalMs int            `json:"maxProbeIntervalMs"`
	AutoDisableAfter   int            `json:"autoDisableAfter"`
	ConnectTimeoutMs   int            `json:"connectTimeoutMs"`
	ListTimeoutMs      int            `json:"listTimeoutMs"`
	FailureThreshold   int            `json:"failureThreshold"`
	ShutdownTimeoutMs  int            `json:"shutdownTimeoutMs"`
	Aggregate          string         `json:"aggregate,omitempty"`
	WatchConfig        bool           `json:"watchConfig"`
	Auth               *authView      `json:"auth,omitempty"`
	Console            *authView      `json:"console,omitempty"`
	Upstreams          []upstreamView `json:"upstreams"`
}

func authViewOf(a *AuthConfig) *authView {
	if a == nil {
		return nil
	}
	v := &authView{Tokens: []tokenView{}}
	for _, c := range a.Tokens {
		v.Tokens = append(v.Tokens, tokenView{Name: c.Name, Fingerprint: tokenFingerprint(c.Token)})
	}
	return v
}

// handleAPIConfigGet returns the effective configuration with all secrets
// redacted: upstream header values become "***" and tokens become
// fingerprints (name plus sha256 prefix).
func (g *gateway) handleAPIConfigGet(w http.ResponseWriter, r *http.Request) {
	cfg := g.config()
	view := configView{
		Listen:             cfg.Listen,
		ProbeIntervalMs:    cfg.ProbeIntervalMs,
		MaxProbeIntervalMs: cfg.MaxProbeIntervalMs,
		AutoDisableAfter:   cfg.AutoDisableAfter,
		ConnectTimeoutMs:   cfg.ConnectTimeoutMs,
		ListTimeoutMs:      cfg.ListTimeoutMs,
		FailureThreshold:   cfg.FailureThreshold,
		ShutdownTimeoutMs:  cfg.ShutdownTimeoutMs,
		Aggregate:          cfg.Aggregate,
		WatchConfig:        cfg.watchEnabled(),
		Auth:               authViewOf(cfg.Auth),
		Console:            authViewOf(cfg.Console),
		Upstreams:          []upstreamView{},
	}
	for _, u := range cfg.Upstreams {
		uv := upstreamView{Name: u.Name, URL: u.URL, CallTimeoutMs: u.CallTimeoutMs, ProbeIntervalMs: u.ProbeIntervalMs, Enabled: u.isEnabled()}
		if len(u.Headers) > 0 {
			uv.Headers = make(map[string]string, len(u.Headers))
			for k := range u.Headers {
				uv.Headers[k] = redactedPlaceholder
			}
		}
		view.Upstreams = append(view.Upstreams, uv)
	}
	g.writeJSON(w, http.StatusOK, view)
}

// tokenEdit is one entry of a console token list update. Token is either a
// new plaintext value, or empty/"***" together with Fingerprint to keep the
// existing token (optionally renaming it).
type tokenEdit struct {
	Name        string `json:"name,omitempty"`
	Token       string `json:"token,omitempty"`
	Fingerprint string `json:"fingerprint,omitempty"`
}

type configEdit struct {
	Listen             *string `json:"listen"`
	ProbeIntervalMs    *int    `json:"probeIntervalMs"`
	MaxProbeIntervalMs *int    `json:"maxProbeIntervalMs"`
	AutoDisableAfter   *int    `json:"autoDisableAfter"`
	ConnectTimeoutMs   *int    `json:"connectTimeoutMs"`
	ListTimeoutMs      *int    `json:"listTimeoutMs"`
	FailureThreshold   *int    `json:"failureThreshold"`
	ShutdownTimeoutMs  *int    `json:"shutdownTimeoutMs"`
	Aggregate          *string `json:"aggregate"`
	WatchConfig        *bool   `json:"watchConfig"`
	// AuthRaw / ConsoleRaw distinguish "absent" (nil: keep current),
	// "null" (delete the section) and an object with a tokens list.
	AuthRaw    *json.RawMessage `json:"auth"`
	ConsoleRaw *json.RawMessage `json:"console"`
	Upstreams  []upstreamEdit   `json:"upstreams"`
}

type upstreamEdit struct {
	Name          string            `json:"name"`
	URL           string            `json:"url"`
	CallTimeoutMs int               `json:"callTimeoutMs,omitempty"`
	Headers       map[string]string `json:"headers,omitempty"`
	// ProbeIntervalMs nil keeps the current per-upstream override (or the
	// global interval for new upstreams).
	ProbeIntervalMs *int `json:"probeIntervalMs,omitempty"`
	// Enabled nil keeps the current state (or defaults to enabled for new
	// upstreams); true/false sets it explicitly.
	Enabled *bool `json:"enabled,omitempty"`
}

// handleAPIConfigPut validates and applies a configuration edit. With
// persist (default) the new configuration is atomically written back to
// gateway.json before the hot reload; a failed write leaves the running
// configuration untouched. Secrets sent as "***" are resolved against the
// current configuration so the console never needs to handle plaintext
// values it does not know.
func (g *gateway) handleAPIConfigPut(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, maxConfigBody)
	var e configEdit
	if err := json.NewDecoder(r.Body).Decode(&e); err != nil {
		http.Error(w, "invalid json: "+err.Error(), http.StatusBadRequest)
		return
	}
	apiWriteMu.Lock()
	defer apiWriteMu.Unlock()

	cur := g.config()
	nc, warnings, err := buildEditedConfig(cur, &e)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	persist := r.URL.Query().Get("persist") != "false"
	if persist {
		if err := saveConfigFile(g.configPath, nc); err != nil {
			g.writeJSON(w, http.StatusInternalServerError, map[string]any{
				"error":     fmt.Sprintf("persist failed, keeping current configuration: %v", err),
				"persisted": false,
			})
			return
		}
	}
	g.applyConfig(nc)
	if warnings == nil {
		warnings = []string{}
	}
	g.writeJSON(w, http.StatusOK, map[string]any{"persisted": persist, "warnings": warnings})
}

// buildEditedConfig merges the edit onto the current configuration, resolves
// redacted placeholders, validates the result and derives reload warnings.
func buildEditedConfig(cur *Config, e *configEdit) (*Config, []string, error) {
	nc := &Config{
		Listen:             cur.Listen,
		ProbeIntervalMs:    cur.ProbeIntervalMs,
		MaxProbeIntervalMs: cur.MaxProbeIntervalMs,
		AutoDisableAfter:   cur.AutoDisableAfter,
		ConnectTimeoutMs:   cur.ConnectTimeoutMs,
		ListTimeoutMs:      cur.ListTimeoutMs,
		FailureThreshold:   cur.FailureThreshold,
		ShutdownTimeoutMs:  cur.ShutdownTimeoutMs,
		Aggregate:          cur.Aggregate,
		WatchConfig:        cur.WatchConfig,
		Auth:               cloneAuth(cur.Auth),
		Console:            cloneAuth(cur.Console),
		Upstreams:          cloneUpstreams(cur.Upstreams),
	}
	if e.Listen != nil {
		nc.Listen = *e.Listen
	}
	if e.ProbeIntervalMs != nil {
		nc.ProbeIntervalMs = *e.ProbeIntervalMs
	}
	if e.MaxProbeIntervalMs != nil {
		nc.MaxProbeIntervalMs = *e.MaxProbeIntervalMs
	}
	if e.AutoDisableAfter != nil {
		nc.AutoDisableAfter = *e.AutoDisableAfter
	}
	if e.ConnectTimeoutMs != nil {
		nc.ConnectTimeoutMs = *e.ConnectTimeoutMs
	}
	if e.ListTimeoutMs != nil {
		nc.ListTimeoutMs = *e.ListTimeoutMs
	}
	if e.FailureThreshold != nil {
		nc.FailureThreshold = *e.FailureThreshold
	}
	if e.ShutdownTimeoutMs != nil {
		nc.ShutdownTimeoutMs = *e.ShutdownTimeoutMs
	}
	if e.Aggregate != nil {
		nc.Aggregate = *e.Aggregate
	}
	if e.WatchConfig != nil {
		nc.WatchConfig = e.WatchConfig
	}

	if e.AuthRaw != nil {
		tokens, err := applyAuthEdit(cur.Auth, *e.AuthRaw)
		if err != nil {
			return nil, nil, fmt.Errorf("auth: %w", err)
		}
		nc.Auth = tokens
	}
	if e.ConsoleRaw != nil {
		tokens, err := applyAuthEdit(cur.Console, *e.ConsoleRaw)
		if err != nil {
			return nil, nil, fmt.Errorf("console: %w", err)
		}
		nc.Console = tokens
	}

	if e.Upstreams != nil {
		ups := make([]UpstreamConfig, 0, len(e.Upstreams))
		for _, ue := range e.Upstreams {
			uc := UpstreamConfig{Name: ue.Name, URL: ue.URL, CallTimeoutMs: ue.CallTimeoutMs}
			if ue.ProbeIntervalMs != nil {
				uc.ProbeIntervalMs = *ue.ProbeIntervalMs
			} else if prev, ok := findUpstream(cur.Upstreams, ue.Name); ok {
				// Omitted override keeps the upstream's current value.
				uc.ProbeIntervalMs = prev.ProbeIntervalMs
			}
			if ue.Enabled != nil {
				uc.Enabled = ue.Enabled
			} else if prev, ok := findUpstream(cur.Upstreams, ue.Name); ok {
				// Omitted enabled flag keeps the upstream's current state.
				uc.Enabled = prev.Enabled
			}
			if len(ue.Headers) > 0 {
				uc.Headers = make(map[string]string, len(ue.Headers))
				for k, v := range ue.Headers {
					if v == redactedPlaceholder {
						orig, ok := existingHeader(cur.Upstreams, ue.Name, k)
						if !ok {
							return nil, nil, fmt.Errorf("upstream %q: header %q is redacted but not present in the current configuration", ue.Name, k)
						}
						uc.Headers[k] = orig
						continue
					}
					uc.Headers[k] = v
				}
			}
			ups = append(ups, uc)
		}
		nc.Upstreams = ups
	}

	nc.applyDefaults()
	if err := nc.validate(); err != nil {
		return nil, nil, err
	}
	return nc, reloadWarnings(cur, nc), nil
}

// applyAuthEdit interprets a raw JSON auth/console section from a PUT: null
// removes the section, an object replaces the token list (with redacted
// placeholders resolved against cur).
func applyAuthEdit(cur *AuthConfig, raw json.RawMessage) (*AuthConfig, error) {
	if string(raw) == "null" {
		return nil, nil
	}
	var parsed struct {
		Tokens []tokenEdit `json:"tokens"`
	}
	if err := json.Unmarshal(raw, &parsed); err != nil {
		return nil, err
	}
	out := make([]AuthClient, 0, len(parsed.Tokens))
	for i, te := range parsed.Tokens {
		tok := strings.TrimSpace(te.Token)
		if tok != "" && tok != redactedPlaceholder {
			out = append(out, AuthClient{Name: strings.TrimSpace(te.Name), Token: tok})
			continue
		}
		kept := false
		if cur != nil {
			for _, c := range cur.Tokens {
				if tokenFingerprint(c.Token) == te.Fingerprint {
					client := AuthClient{Name: c.Name, Token: c.Token}
					if name := strings.TrimSpace(te.Name); name != "" {
						client.Name = name
					}
					out = append(out, client)
					kept = true
					break
				}
			}
		}
		if !kept {
			return nil, fmt.Errorf("tokens[%d]: provide a token value or the fingerprint of the existing token to keep", i)
		}
	}
	return &AuthConfig{Tokens: out}, nil
}

// existingHeader looks up a stored header value for redaction resolution.
func existingHeader(upstreams []UpstreamConfig, name, key string) (string, bool) {
	for _, u := range upstreams {
		if u.Name != name {
			continue
		}
		v, ok := u.Headers[key]
		return v, ok
	}
	return "", false
}

// findUpstream looks up an upstream config by name.
func findUpstream(upstreams []UpstreamConfig, name string) (UpstreamConfig, bool) {
	for _, u := range upstreams {
		if u.Name == name {
			return u, true
		}
	}
	return UpstreamConfig{}, false
}

func cloneAuth(a *AuthConfig) *AuthConfig {
	if a == nil {
		return nil
	}
	c := &AuthConfig{Tokens: make([]AuthClient, len(a.Tokens))}
	copy(c.Tokens, a.Tokens)
	return c
}

// cloneUpstreams deep-copies the upstream list. Headers maps are duplicated
// too: callers mutate the copy (e.g. toggling enabled) and must never share
// the header map with the live configuration.
func cloneUpstreams(list []UpstreamConfig) []UpstreamConfig {
	out := make([]UpstreamConfig, len(list))
	for i, u := range list {
		out[i] = u
		if u.Headers != nil {
			out[i].Headers = make(map[string]string, len(u.Headers))
			for k, v := range u.Headers {
				out[i].Headers[k] = v
			}
		}
	}
	return out
}

// reloadWarnings compares the current and the new configuration and returns
// human-readable hints shown in the console after a save.
func reloadWarnings(cur, nc *Config) []string {
	var warnings []string
	if nc.Listen != cur.Listen {
		warnings = append(warnings, "listen 变更需要重启进程才能生效")
	}
	curByName := make(map[string]UpstreamConfig, len(cur.Upstreams))
	for _, u := range cur.Upstreams {
		curByName[u.Name] = u
	}
	newNames := make(map[string]bool, len(nc.Upstreams))
	for _, u := range nc.Upstreams {
		newNames[u.Name] = true
		if old, ok := curByName[u.Name]; !ok {
			warnings = append(warnings, fmt.Sprintf("上游 %q 将新增并开始探测", u.Name))
		} else {
			if !old.equal(u) {
				warnings = append(warnings, fmt.Sprintf("上游 %q 配置变更将重建连接，进行中的调用可能失败", u.Name))
			}
			if old.isEnabled() && !u.isEnabled() {
				warnings = append(warnings, fmt.Sprintf("上游 %q 将停用：停止探测并从目录与聚合端点移除", u.Name))
			}
			if !old.isEnabled() && u.isEnabled() {
				warnings = append(warnings, fmt.Sprintf("上游 %q 将启用并开始探测", u.Name))
			}
		}
	}
	for _, u := range cur.Upstreams {
		if !newNames[u.Name] {
			warnings = append(warnings, fmt.Sprintf("上游 %q 将被移除", u.Name))
		}
	}
	if nc.Aggregate != cur.Aggregate {
		warnings = append(warnings, "聚合端点配置变更将重建聚合目录")
	}
	return warnings
}

// saveConfigFile atomically writes the configuration file (temp file in the
// same directory + rename) with restrictive permissions, since it contains
// plaintext tokens.
func saveConfigFile(path string, cfg *Config) error {
	if path == "" {
		return fmt.Errorf("config path unknown (not set at startup)")
	}
	data, err := json.MarshalIndent(cfg, "", "  ")
	if err != nil {
		return err
	}
	data = append(data, '\n')
	dir := filepath.Dir(path)
	tmp, err := os.CreateTemp(dir, ".gateway-*.json")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	defer func() {
		if tmpName != "" {
			_ = os.Remove(tmpName)
		}
	}()
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Chmod(tmpName, 0o600); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	tmpName = ""
	return nil
}

// handleAPIUpstreamToggle flips one upstream's enabled flag. It persists the
// change to gateway.json (like a config save) and hot-reloads, so a disabled
// upstream stops being probed/served immediately and stays off across
// restarts until re-enabled. Quarantine (auto-disable) is runtime-only and
// unaffected; enabling an upstream clears it via the rebuild below.
func (g *gateway) handleAPIUpstreamToggle(w http.ResponseWriter, r *http.Request) {
	r.Body = http.MaxBytesReader(w, r.Body, 4096)
	var req struct {
		Name    string `json:"name"`
		Enabled *bool  `json:"enabled"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil || req.Name == "" || req.Enabled == nil {
		http.Error(w, "invalid json: need {\"name\": string, \"enabled\": bool}", http.StatusBadRequest)
		return
	}
	apiWriteMu.Lock()
	defer apiWriteMu.Unlock()

	cur := g.config()
	idx := -1
	for i := range cur.Upstreams {
		if cur.Upstreams[i].Name == req.Name {
			idx = i
			break
		}
	}
	if idx < 0 {
		http.Error(w, "unknown upstream", http.StatusNotFound)
		return
	}
	nc := *cur
	nc.Upstreams = cloneUpstreams(cur.Upstreams)
	nc.Upstreams[idx].Enabled = req.Enabled
	if err := saveConfigFile(g.configPath, &nc); err != nil {
		g.writeJSON(w, http.StatusInternalServerError, map[string]any{
			"error":     fmt.Sprintf("persist failed, keeping current configuration: %v", err),
			"persisted": false,
		})
		return
	}
	g.applyConfig(&nc)
	// Re-enabling must also clear a runtime quarantine: equal() treats nil
	// and true as the same enabled state, so no rebuild happens and the old
	// runner (with its stopped ticker) would keep sitting idle.
	if *req.Enabled {
		if u := g.upstream(req.Name); u != nil {
			u.resume()
		}
	}
	g.writeJSON(w, http.StatusOK, map[string]any{"ok": true, "name": req.Name, "enabled": *req.Enabled})
}

// handleAPILogs returns the newest activity entries (calls and lifecycle
// events), newest first. type=call or type=event filters the timeline.
func (g *gateway) handleAPILogs(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	limit := 100
	if v := q.Get("limit"); v != "" {
		if n, err := strconv.Atoi(v); err == nil && n > 0 {
			limit = n
		}
	}
	if limit > recentLogCapacity {
		limit = recentLogCapacity
	}
	g.writeJSON(w, http.StatusOK, map[string]any{"entries": g.logs.snapshot(limit, q.Get("type"))})
}

// handleAPIReload reloads the configuration from gateway.json (the same
// effect as SIGHUP) and reports success or the validation error.
func (g *gateway) handleAPIReload(w http.ResponseWriter, r *http.Request) {
	if err := g.reloadFromFile(g.configPath); err != nil {
		g.writeJSON(w, http.StatusBadRequest, map[string]any{"error": err.Error(), "reloaded": false})
		return
	}
	g.writeJSON(w, http.StatusOK, map[string]any{"reloaded": true})
}
