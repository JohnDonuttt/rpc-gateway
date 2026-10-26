package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/JohnDonuttt/rpc-gateway/internal/cache"
	"github.com/JohnDonuttt/rpc-gateway/internal/config"
	"github.com/JohnDonuttt/rpc-gateway/internal/gateway"
	"github.com/JohnDonuttt/rpc-gateway/internal/ratelimit"
	"github.com/JohnDonuttt/rpc-gateway/internal/upstream"
)

func main() {
	configPath := flag.String("config", "gateway.json", "path to the JSON config file")
	flag.Parse()

	log := slog.New(slog.NewTextHandler(os.Stderr, nil))

	cfg, err := config.Load(*configPath)
	if err != nil {
		log.Error("could not load config", "err", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client := &http.Client{} // per-attempt timeouts come from contexts, not the client
	ups := make([]*upstream.Upstream, len(cfg.Upstreams))
	for i, u := range cfg.Upstreams {
		ups[i] = upstream.New(u.Name, u.URL, client)
	}
	pool := upstream.NewPool(ups, cfg.UnhealthyAfter, cfg.MaxLagBlocks, cfg.AttemptTimeout.Std())
	go pool.Run(ctx, cfg.HealthInterval.Std())

	gw := gateway.New(gateway.Options{
		Pool:              pool,
		Cache:             cache.New(cfg.CacheEntries),
		Limiter:           ratelimit.New(cfg.RateLimit.RequestsPerSecond, cfg.RateLimit.Burst),
		AttemptTimeout:    cfg.AttemptTimeout.Std(),
		MaxAttempts:       cfg.MaxAttempts,
		TrustProxyHeaders: cfg.TrustProxyHeaders,
		Logger:            log,
	})

	srv := &http.Server{
		Addr:              cfg.Listen,
		Handler:           gw,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		<-ctx.Done()
		log.Info("shutting down")
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		srv.Shutdown(shutdownCtx)
	}()

	log.Info("listening", "addr", cfg.Listen, "upstreams", len(ups))
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Error("server stopped", "err", err)
		os.Exit(1)
	}
}
