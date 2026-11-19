package main

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"log"
	"net/url"
	"os"
	"regexp"
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
}

// equal reports whether two upstream configs are semantically identical;
// used by hot reload to decide between keep and rebuild.
func (u UpstreamConfig) equal(o UpstreamConfig) bool {
	if u.Name != o.Name || u.URL != o.URL || u.CallTimeoutMs != o.CallTimeoutMs || len(u.Headers) != len(o.Headers) {
		return false
	}
	for k, v := range u.Headers {
		if o.Headers[k] != v {
			return false
		}
	}
	return true
}

// AuthConfig enables gateway-level client authentication. When set, every
// /mcp/* request must carry "Authorization: Bearer <token>" with a token from
// Tokens; /healthz stays open for probes. When Auth is nil (the default) the
// gateway accepts unauthenticated clients, exactly as before.
type AuthConfig struct {
	// Tokens is the list of accepted bearer tokens. Rotate without downtime
	// by adding the new token, reloading the config, then removing the old
	// one. Tokens must be at least 16 characters and are compared in
	// constant time.
	Tokens []string `json:"tokens"`
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
	// FailureThreshold is the number of consecutive failed probes before an
	// upstream is marked down and its session is retired.
	FailureThreshold int `json:"failureThreshold,omitempty"`
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
	// Auth enables gateway-level bearer authentication when non-nil.
	Auth      *AuthConfig      `json:"auth,omitempty"`
	Upstreams []UpstreamConfig `json:"upstreams"`
}

var aggregateNameRE = regexp.MustCompile(`^[A-Za-z0-9_-]+$`)

func (c *Config) applyDefaults() {
	if c.Listen == "" {
		c.Listen = ":9200"
	}
	if c.ProbeIntervalMs <= 0 {
		c.ProbeIntervalMs = 15000
	}
	if c.ConnectTimeoutMs <= 0 {
		c.ConnectTimeoutMs = 3000
	}
	if c.ListTimeoutMs <= 0 {
		c.ListTimeoutMs = 3000
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

func (c *Config) connectTimeout() time.Duration {
	return time.Duration(c.ConnectTimeoutMs) * time.Millisecond
}

func (c *Config) listTimeout() time.Duration {
	return time.Duration(c.ListTimeoutMs) * time.Millisecond
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

// tokenHashes returns SHA-256 digests of the configured tokens so that
// presented credentials can be compared in constant time without leaking
// token length or content.
func (c *Config) tokenHashes() [][sha256.Size]byte {
	if !c.authEnabled() {
		return nil
	}
	hashes := make([][sha256.Size]byte, 0, len(c.Auth.Tokens))
	for _, tok := range c.Auth.Tokens {
		hashes = append(hashes, sha256.Sum256([]byte(tok)))
	}
	return hashes
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
	if cfg.Aggregate != "" && !aggregateNameRE.MatchString(cfg.Aggregate) {
		return nil, fmt.Errorf("aggregate %q may only contain letters, digits, '_' and '-'", cfg.Aggregate)
	}
	seen := map[string]bool{}
	names := []string{}
	for _, u := range cfg.Upstreams {
		if u.Name == "" || u.URL == "" {
			return nil, fmt.Errorf("every upstream needs a name and a url")
		}
		if pu, perr := url.Parse(u.URL); perr != nil || pu.Host == "" || (pu.Scheme != "http" && pu.Scheme != "https") {
			return nil, fmt.Errorf("upstream %q: url %q must be an absolute http(s) URL", u.Name, u.URL)
		}
		if strings.Contains(u.Name, "/") || strings.Contains(u.Name, "##") {
			return nil, fmt.Errorf("upstream name %q must not contain %q or %q", u.Name, "/", "##")
		}
		if cfg.Aggregate != "" && u.Name == cfg.Aggregate {
			return nil, fmt.Errorf("upstream name %q conflicts with the aggregate endpoint name", u.Name)
		}
		if seen[u.Name] {
			return nil, fmt.Errorf("duplicate upstream name %q", u.Name)
		}
		seen[u.Name] = true
		names = append(names, u.Name)
	}
	if len(cfg.Upstreams) == 0 {
		return nil, fmt.Errorf("no upstreams configured")
	}
	if cfg.Auth != nil {
		if len(cfg.Auth.Tokens) == 0 {
			return nil, fmt.Errorf("auth is configured but auth.tokens is empty; add at least one token or remove the auth section")
		}
		seenTokens := map[string]bool{}
		for i, tok := range cfg.Auth.Tokens {
			tok = strings.TrimSpace(tok)
			if len(tok) < 16 {
				return nil, fmt.Errorf("auth.tokens[%d]: token too short (min 16 characters)", i)
			}
			if seenTokens[tok] {
				return nil, fmt.Errorf("auth.tokens[%d]: duplicate token", i)
			}
			seenTokens[tok] = true
			cfg.Auth.Tokens[i] = tok
		}
	}
	if cfg.Aggregate != "" {
		for i, a := range names {
			for j, b := range names {
				if i != j && strings.HasPrefix(a, b) {
					log.Printf("warning: upstream %q is a prefix of upstream %q; aggregate names may be ambiguous (longest prefix wins)", b, a)
				}
			}
		}
	}
	return &cfg, nil
}
