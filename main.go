package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

var version = "0.4.0"

func main() {
	log.SetFlags(log.LstdFlags | log.Lmsgprefix)
	log.SetPrefix("mcp-gateway: ")
	cfgPath := flag.String("config", "/etc/mcp-gateway/gateway.json", "path to gateway.json")
	flag.Parse()

	cfg, err := loadConfig(*cfgPath)
	if err != nil {
		log.Fatalf("load config: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	g := newGateway(cfg, version)
	g.configPath = *cfgPath
	g.start(ctx)

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           g.handler(),
		// ReadTimeout bounds slow-loris style request bodies. There is
		// deliberately no WriteTimeout: the MCP SDK streams responses (SSE)
		// and a write deadline would kill long-lived streams.
		ReadTimeout:       30 * time.Second,
		ReadHeaderTimeout: 10 * time.Second,
		IdleTimeout:       90 * time.Second,
	}
	errCh := make(chan error, 1)
	g.setListen(cfg.Listen) // actual bound address, surfaced via /api/health
	go func() { errCh <- srv.ListenAndServe() }()
	endpoint := ""
	if name := g.aggregateName(); name != "" {
		endpoint = ", aggregate=/mcp/" + name
	}
	log.Printf("%s listening on %s with %d upstream(s)%s", version, cfg.Listen, len(cfg.Upstreams), endpoint)

	hupCh := make(chan os.Signal, 1)
	signal.Notify(hupCh, syscall.SIGHUP)
	defer signal.Stop(hupCh)

	watchCh := make(chan string, 1)
	if cfg.watchEnabled() {
		go watchConfigFile(ctx, *cfgPath, watchCh)
	}

	for {
		select {
		case <-ctx.Done():
			g.shutdown(srv)
			return
		case err := <-errCh:
			if err != nil && err != http.ErrServerClosed {
				stop()
				g.shutdown(srv)
				log.Fatalf("http server: %v", err)
			}
			g.shutdown(srv)
			return
		case <-hupCh:
			log.Printf("SIGHUP received, reloading config")
			g.reloadFrom(*cfgPath)
		case p := <-watchCh:
			log.Printf("config file changed, reloading")
			g.reloadFrom(p)
		}
	}
}
