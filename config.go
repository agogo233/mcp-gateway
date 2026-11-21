package main

import (
	"crypto/sha256"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

// UpstreamConfig describes one MCP upstream and the endpoint that exposes it.
// The gateway serves /mcp/<Name> for each upstream, so opencode keeps its
// original config keys (e.g. "ida", "x64dbg") and only the URL changes.
type UpstreamConfig struct {
	Name string `json:"name"`
	URL  string `json:"url"`
	// CallTimeoutMs bounds a single tools/call, prompts/get or resources/read
	// forwarded to this upstream. Zero means 60s.
	CallTimeoutMs int `json:"callTimeoutMs,omitempty"`
	// Headers are sent with every request made to this upstream
	// (e.g. {"Authorization": "Bearer ..."}).
	Headers map[string]string `json:"headers,omitempty"`
	// ProbeIntervalMs overrides the global probe interval for this upstream
	// (e.g. heavier catalogs can be probed less often). Zero means the
	// global probeIntervalMs applies.
	ProbeIntervalMs int `json:"probeIntervalMs,omitempty"`
	// Enabled gates this upstream on/off. Nil or true means enabled; false
	// stops the probe loop, drops the session and excludes the upstream from
	// the per-upstream and aggregate endpoints until re-enabled.
	Enabled *bool `json:"enabled,omitempty"`
}

// probeInterval resolves this upstream's effective base probe interval given
// the global one.
func (u UpstreamConfig) probeInterval(global time.Duration) time.Duration {
	if u.ProbeIntervalMs <= 0 {
		return global
	}
	return time.Duration(u.ProbeIntervalMs) * time.Millisecond
}

// isEnabled reports whether the upstream should be served. Absent (nil) and
// explicit true both mean enabled, so existing config files keep working.
func (u UpstreamConfig) isEnabled() bool {
	return u.Enabled == nil || *u.Enabled
}

// enabledPtr is a small helper for building configs in tests and code.
func enabledPtr(v bool) *bool { return &v }

// equal reports whether two upstream configs are semantically identical;
// used by hot reload to decide between keep and rebuild.
func (u UpstreamConfig) equal(o UpstreamConfig) bool {
	if u.Name != o.Name || u.URL != o.URL || u.CallTimeoutMs != o.CallTimeoutMs || u.ProbeIntervalMs != o.ProbeIntervalMs || len(u.Headers) != len(o.Headers) {
		return false
	}
	if u.isEnabled() != o.isEnabled() {
		return false
	}
	for k, v := range u.Headers {
		if o.Headers[k] != v {
			return false
		}
	}
	return true
}

// AuthClient is one downstream credential: an optional display name plus the
// bearer token value. The name only labels the client in the console UI and
// in call-attribution logs; it never participates in authentication.
type AuthClient struct {
	Name  string `json:"name,omitempty"`
	Token string `json:"token"`
}

// AuthConfig enables gateway-level client authentication. When set, every
// /mcp/* request must carry "Authorization: Bearer <token>" matching one of
// the configured clients; /healthz stays open for probes. When Auth is nil
// (the default) the gateway accepts unauthenticated clients, exactly as
// before.
type AuthConfig struct {
	// Tokens is the list of accepted bearer credentials. Each entry is
	// either a plain token string (legacy format) or an object with an
	// optional "name" label and the "token" value. Rotate without downtime
	// by adding the new token, reloading the config, then removing the old
	// one. Tokens must be at least 16 characters and are compared in
	// constant time.
	Tokens []AuthClient `json:"tokens"`
}

// UnmarshalJSON accepts both the legacy format ("tokens": ["s1", "s2"]) and
// the named format ("tokens": [{"name": "opencode", "token": "s1"}]) so
// existing config files keep working unchanged.
func (a *AuthConfig) UnmarshalJSON(data []byte) error {
	var raw struct {
		Tokens []json.RawMessage `json:"tokens"`
	}
	if err := json.Unmarshal(data, &raw); err != nil {
		return err
	}
	clients := make([]AuthClient, 0, len(raw.Tokens))
	for i, r := range raw.Tokens {
		var s string
		if err := json.Unmarshal(r, &s); err == nil {
			clients = append(clients, AuthClient{Token: s})
			continue
		}
		var c AuthClient
		if err := json.Unmarshal(r, &c); err != nil {
			return fmt.Errorf("tokens[%d]: expected a token string or a {\"name\",\"token\"} object", i)
		}
		clients = append(clients, c)
	}
	a.Tokens = clients
	return nil
}

// Default settings for the per-IP authentication failure throttle.
const (
	defaultFailWindowMs = 60000 // counting window: 1 minute
	defaultMaxFails     = 10    // failures per window before a block
	defaultFailBlockMs  = 60000 // block duration once tripped
)

// AuthRateLimitConfig tunes throttling of failed authentication attempts per
// source IP. One setting guards both /mcp/* and /api/*; each endpoint keeps
// its own failure counters. Absent or zero fields mean the defaults.
type AuthRateLimitConfig struct {
	// Disabled turns the throttle off. Authentication itself stays on.
	Disabled bool `json:"disabled,omitempty"`
	// WindowMs is the length of the failure-counting window. Zero means 60000.
	WindowMs int `json:"windowMs,omitempty"`
	// MaxFails is the number of failed attempts within one window that
	// blocks the source. Zero means 10.
	MaxFails int `json:"maxFails,omitempty"`
	// BlockMs is how long a tripped source stays blocked (429). Zero means
	// 60000.
	BlockMs int `json:"blockMs,omitempty"`
}

// authRateLimit returns the effective throttle settings with defaults
// applied; non-positive values fall back to the defaults as well.
func (c *Config) authRateLimit() AuthRateLimitConfig {
	r := AuthRateLimitConfig{
		WindowMs: defaultFailWindowMs,
		MaxFails: defaultMaxFails,
		BlockMs:  defaultFailBlockMs,
	}
	if c.AuthRateLimit != nil {
		if c.AuthRateLimit.WindowMs > 0 {
			r.WindowMs = c.AuthRateLimit.WindowMs
		}
		if c.AuthRateLimit.MaxFails > 0 {
			r.MaxFails = c.AuthRateLimit.MaxFails
		}
		if c.AuthRateLimit.BlockMs > 0 {
			r.BlockMs = c.AuthRateLimit.BlockMs
		}
		r.Disabled = c.AuthRateLimit.Disabled
	}
	return r
}

// principal identifies the credential that authenticated a request: the
// configured display name for named tokens, or only a fingerprint for
// anonymous ones. Fingerprints are short irreversible digest prefixes used
// for log attribution and for in-place token edits via the console API.
type principal struct {
	Name        string
	Fingerprint string
}

// tokenFingerprint returns a short irreversible prefix of the token digest;
// it is safe to expose and to log.
func tokenFingerprint(token string) string {
	sum := sha256.Sum256([]byte(token))
	return "sha256:" + hex.EncodeToString(sum[:])[:12]
}

// match reports whether the presented bearer token equals one of the
// configured tokens. Comparison runs over SHA-256 digests in constant time so
// response timing does not leak token content or length.
func (a *AuthConfig) match(presented string) (principal, bool) {
	if presented == "" {
		return principal{}, false
	}
	sum := sha256.Sum256([]byte(presented))
	for _, c := range a.Tokens {
		want := sha256.Sum256([]byte(c.Token))
		if subtle.ConstantTimeCompare(sum[:], want[:]) == 1 {
			return principal{Name: c.Name, Fingerprint: tokenFingerprint(c.Token)}, true
		}
	}
	return principal{}, false
}

// Config is the gateway configuration file format.
type Config struct {
	Listen string `json:"listen,omitempty"`
	// ProbeIntervalMs is the period between upstream health probes.
	ProbeIntervalMs int `json:"probeIntervalMs,omitempty"`
	// ConnectTimeoutMs bounds the MCP initialize handshake with an upstream.
	ConnectTimeoutMs int `json:"connectTimeoutMs,omitempty"`
	// ListTimeoutMs bounds each ping / tools / prompts / resources list
	// request. It is applied per request, not per catalog sync.
	ListTimeoutMs int `json:"listTimeoutMs,omitempty"`
	// SyncBudgetMs bounds one full catalog sync (all sections together) per
	// probe. When the budget expires the previous catalog is kept and a
	// warning is logged, so a huge catalog cannot stall the probe loop and
	// delay down-detection. Zero means 30000.
	SyncBudgetMs int `json:"syncBudgetMs,omitempty"`
	// FailureThreshold is the number of consecutive failed probes before an
	// upstream is marked down and its session is retired.
	FailureThreshold int `json:"failureThreshold,omitempty"`
	// MaxProbeIntervalMs caps the exponential probe backoff: once an upstream
	// is down, the probe interval doubles per additional failure up to this
	// ceiling. Zero means 300000 (5 minutes).
	MaxProbeIntervalMs int `json:"maxProbeIntervalMs,omitempty"`
	// AutoDisableAfter quarantines an upstream after this many consecutive
	// failed probes: probing pauses entirely until it is re-enabled via the
	// console, its config changes or the process restarts. Zero (default)
	// disables quarantining; when set it is clamped to at least
	// FailureThreshold. The state is runtime-only and never written to the
	// config file.
	AutoDisableAfter int `json:"autoDisableAfter,omitempty"`
	// ShutdownTimeoutMs bounds graceful shutdown: draining HTTP connections
	// and waiting for background loops to exit. Zero means 5000.
	ShutdownTimeoutMs int `json:"shutdownTimeoutMs,omitempty"`
	// Aggregate is the path segment of the aggregate endpoint /mcp/<Aggregate>
	// which merges every healthy upstream catalog under prefixed names
	// (<upstream>_<name>, resources <upstream>##<uri>). Empty (default)
	// disables the aggregate endpoint.
	Aggregate string `json:"aggregate,omitempty"`
	// WatchConfig enables mtime-polling hot reload of the config file
	// (SIGHUP works regardless). Default is enabled.
	WatchConfig *bool `json:"watchConfig,omitempty"`
	// Auth enables gateway-level bearer authentication for /mcp/* clients
	// when non-nil.
	Auth *AuthConfig `json:"auth,omitempty"`
	// AuthRateLimit throttles failed authentication attempts per source IP
	// on /mcp/* and /api/* (see AuthRateLimitConfig). Optional; when nil the
	// defaults apply. Set "disabled": true to turn the throttle off.
	AuthRateLimit *AuthRateLimitConfig `json:"authRateLimit,omitempty"`
	// Console enables the management console API (/api/*) with its own,
	// independent credential list when non-nil and non-empty. Console tokens
	// must differ from every auth token; reuse is rejected during validation.
	Console   *AuthConfig      `json:"console,omitempty"`
	Upstreams []UpstreamConfig `json:"upstreams"`
}

var aggregateNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

// defaultMaxProbeIntervalMs is the backoff ceiling applied when
// maxProbeIntervalMs is not configured.
const defaultMaxProbeIntervalMs = 300000

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":9200"
	}
	if c.ProbeIntervalMs <= 0 {
		c.ProbeIntervalMs = 15000
	}
	if c.MaxProbeIntervalMs <= 0 {
		c.MaxProbeIntervalMs = defaultMaxProbeIntervalMs
	}
	if c.AutoDisableAfter < 0 {
		c.AutoDisableAfter = 0
	}
	if c.ConnectTimeoutMs <= 0 {
		c.ConnectTimeoutMs = 3000
	}
	if c.ListTimeoutMs <= 0 {
		c.ListTimeoutMs = 3000
	}
	if c.SyncBudgetMs <= 0 {
		c.SyncBudgetMs = 30000
	}
	if c.FailureThreshold <= 0 {
		c.FailureThreshold = 2
	}
	if c.ShutdownTimeoutMs <= 0 {
		c.ShutdownTimeoutMs = 5000
	}
}

func (c *Config) probeInterval() time.Duration {
	return time.Duration(c.ProbeIntervalMs) * time.Millisecond
}

func (c *Config) maxProbeInterval() time.Duration {
	return time.Duration(c.MaxProbeIntervalMs) * time.Millisecond
}

// autoDisableLimit returns the effective quarantine threshold: zero when
// disabled, otherwise at least the failure threshold so an upstream always
// transitions to down before it can be quarantined.
func (c *Config) autoDisableLimit() int {
	if c.AutoDisableAfter <= 0 {
		return 0
	}
	if c.AutoDisableAfter < c.FailureThreshold {
		return c.FailureThreshold
	}
	return c.AutoDisableAfter
}

func (c *Config) connectTimeout() time.Duration {
	return time.Duration(c.ConnectTimeoutMs) * time.Millisecond
}

func (c *Config) listTimeout() time.Duration {
	return time.Duration(c.ListTimeoutMs) * time.Millisecond
}

func (c *Config) syncBudget() time.Duration {
	return time.Duration(c.SyncBudgetMs) * time.Millisecond
}

func (c *Config) shutdownTimeout() time.Duration {
	return time.Duration(c.ShutdownTimeoutMs) * time.Millisecond
}

func (c *Config) watchEnabled() bool {
	return c.WatchConfig == nil || *c.WatchConfig
}

func (c *Config) authEnabled() bool {
	return c.Auth != nil && len(c.Auth.Tokens) > 0
}

// consoleEnabled reports whether the management console API has credentials
// configured. An absent or empty console section disables /api/* entirely.
func (c *Config) consoleEnabled() bool {
	return c.Console != nil && len(c.Console.Tokens) > 0
}

func (u UpstreamConfig) callTimeout() time.Duration {
	if u.CallTimeoutMs <= 0 {
		return 60 * time.Second
	}
	return time.Duration(u.CallTimeoutMs) * time.Millisecond
}

func loadConfig(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var cfg Config
	if err := json.Unmarshal(data, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	cfg.applyDefaults()
	if err := cfg.validate(); err != nil {
		return nil, err
	}
	return &cfg, nil
}

// validate checks the semantic correctness of a configuration. It is shared
// by file loading and by console config saves so both paths enforce exactly
// the same rules.
func (c *Config) validate() error {
	// Reject a malformed listen address at save/load time; otherwise the
	// process would only fail on the next restart.
	if c.Listen == "" {
		return fmt.Errorf("listen is required")
	}
	if _, port, err := net.SplitHostPort(c.Listen); err != nil {
		return fmt.Errorf("listen %q must be host:port (e.g. \":9200\")", c.Listen)
	} else if n, perr := strconv.Atoi(port); perr != nil || n < 1 || n > 65535 {
		return fmt.Errorf("listen %q: port must be an integer between 1 and 65535", c.Listen)
	}
	if c.Aggregate != "" && !aggregateNameRE.MatchString(c.Aggregate) {
		return fmt.Errorf("aggregate %q may only contain letters, digits, '_' and '-'", c.Aggregate)
	}
	seen := map[string]bool{}
	names := []string{}
	for _, u := range c.Upstreams {
		if u.Name == "" || u.URL == "" {
			return fmt.Errorf("every upstream needs a name and a url")
		}
		if pu, perr := url.Parse(u.URL); perr != nil || pu.Host == "" || (pu.Scheme != "http" && pu.Scheme != "https") {
			return fmt.Errorf("upstream %q: url %q must be an absolute http(s) URL", u.Name, u.URL)
		}
		if strings.Contains(u.Name, "/") || strings.Contains(u.Name, "##") {
			return fmt.Errorf("upstream name %q must not contain %q or %q", u.Name, "/", "##")
		}
		if c.Aggregate != "" && u.Name == c.Aggregate {
			return fmt.Errorf("upstream name %q conflicts with the aggregate endpoint name", u.Name)
		}
		if seen[u.Name] {
			return fmt.Errorf("duplicate upstream name %q", u.Name)
		}
		seen[u.Name] = true
		names = append(names, u.Name)
	}
	if len(c.Upstreams) == 0 {
		return fmt.Errorf("no upstreams configured")
	}
	if c.Auth != nil {
		if err := validateTokenList("auth.tokens", c.Auth.Tokens, true); err != nil {
			return err
		}
	}
	if c.Console != nil {
		// An empty console section legitimately disables the console API, so
		// unlike auth it is not required to be non-empty here.
		if err := validateTokenList("console.tokens", c.Console.Tokens, false); err != nil {
			return err
		}
	}
	if c.Console != nil && c.Auth != nil {
		authSet := map[string]bool{}
		for _, cl := range c.Auth.Tokens {
			authSet[cl.Token] = true
		}
		for i, cl := range c.Console.Tokens {
			if authSet[cl.Token] {
				return fmt.Errorf("console.tokens[%d]: the same token must not be reused as a downstream auth token", i)
			}
		}
	}
	if c.Aggregate != "" {
		for i, a := range names {
			for j, b := range names {
				if i != j && strings.HasPrefix(a, b) {
					log.Printf("warning: upstream %q is a prefix of upstream %q; aggregate names may be ambiguous (longest prefix wins)", b, a)
				}
			}
		}
	}
	return nil
}

// validateTokenList checks length and uniqueness of a credential list and
// trims whitespace in place. requireNonEmpty rejects an empty list (used for
// auth, where an empty section is a configuration mistake; the console may be
// empty to disable the API).
func validateTokenList(field string, tokens []AuthClient, requireNonEmpty bool) error {
	if requireNonEmpty && len(tokens) == 0 {
		return fmt.Errorf("%s is configured but empty; add at least one token or remove the section", field)
	}
	seen := map[string]bool{}
	for i, c := range tokens {
		tok := strings.TrimSpace(c.Token)
		if len(tok) < 16 {
			return fmt.Errorf("%s[%d]: token too short (min 16 characters)", field, i)
		}
		if seen[tok] {
			return fmt.Errorf("%s[%d]: duplicate token", field, i)
		}
		seen[tok] = true
		tokens[i].Token = tok
	}
	return nil
}
