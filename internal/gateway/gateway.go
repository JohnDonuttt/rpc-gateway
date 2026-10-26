// Package gateway is the HTTP handler: it takes JSON-RPC requests, answers from cache when it
// safely can, and otherwise forwards them to upstreams with failover.
package gateway

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net"
	"net/http"
	"strings"
	"sync/atomic"
	"time"

	"github.com/JohnDonuttt/rpc-gateway/internal/cache"
	"github.com/JohnDonuttt/rpc-gateway/internal/jsonrpc"
	"github.com/JohnDonuttt/rpc-gateway/internal/ratelimit"
	"github.com/JohnDonuttt/rpc-gateway/internal/upstream"
)

// Only methods whose answer can't change (or can only change through a reorg) are cached.
// Anything that takes a block tag like "latest" is deliberately absent.
var cacheTTL = map[string]time.Duration{
	"eth_chainId":               time.Hour,
	"net_version":               time.Hour,
	"eth_getBlockByHash":        time.Minute,
	"eth_getTransactionByHash":  30 * time.Second,
	"eth_getTransactionReceipt": 30 * time.Second,
}

type Options struct {
	Pool    *upstream.Pool
	Cache   *cache.Cache
	Limiter *ratelimit.Limiter

	// AttemptTimeout bounds each try against one upstream.
	AttemptTimeout time.Duration
	// MaxAttempts caps how many upstreams a single request is tried against.
	MaxAttempts  int
	MaxBodyBytes int64
	// TrustProxyHeaders makes the limiter key on X-Forwarded-For instead of the socket address.
	TrustProxyHeaders bool
	Logger            *slog.Logger
}

type Gateway struct {
	opts Options

	cacheHits   atomic.Uint64
	cacheMisses atomic.Uint64
	rateLimited atomic.Uint64
}

func New(opts Options) *Gateway {
	if opts.MaxAttempts < 1 {
		opts.MaxAttempts = 3
	}
	if opts.AttemptTimeout <= 0 {
		opts.AttemptTimeout = 10 * time.Second
	}
	if opts.MaxBodyBytes <= 0 {
		opts.MaxBodyBytes = 1 << 20
	}
	if opts.Logger == nil {
		opts.Logger = slog.New(slog.NewTextHandler(io.Discard, nil))
	}
	return &Gateway{opts: opts}
}

func (g *Gateway) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	switch r.URL.Path {
	case "/healthz":
		g.healthz(w)
		return
	case "/metrics":
		g.metrics(w)
		return
	case "/":
	default:
		http.NotFound(w, r)
		return
	}

	if r.Method != http.MethodPost {
		w.Header().Set("Allow", http.MethodPost)
		http.Error(w, "use POST", http.StatusMethodNotAllowed)
		return
	}

	if g.opts.Limiter != nil && !g.opts.Limiter.Allow(g.clientKey(r)) {
		g.rateLimited.Add(1)
		writeJSON(w, http.StatusTooManyRequests, jsonrpc.ErrorBody(nil, jsonrpc.CodeLimitExceeded, "rate limit exceeded"))
		return
	}

	body, err := io.ReadAll(http.MaxBytesReader(w, r.Body, g.opts.MaxBodyBytes))
	if err != nil {
		var tooBig *http.MaxBytesError
		if errors.As(err, &tooBig) {
			writeJSON(w, http.StatusRequestEntityTooLarge, jsonrpc.ErrorBody(nil, jsonrpc.CodeInvalidRequest, "request too large"))
			return
		}
		writeJSON(w, http.StatusBadRequest, jsonrpc.ErrorBody(nil, jsonrpc.CodeParseError, "could not read request"))
		return
	}

	reqs, batch, err := jsonrpc.Parse(body)
	if err != nil {
		writeJSON(w, http.StatusBadRequest, jsonrpc.ErrorBody(nil, jsonrpc.CodeParseError, err.Error()))
		return
	}

	var (
		key       string
		ttl       time.Duration
		cacheable bool
	)
	if !batch {
		key, ttl, cacheable = cacheKey(reqs[0])
	}

	if cacheable {
		if result, ok := g.opts.Cache.Get(key); ok {
			g.cacheHits.Add(1)
			w.Header().Set("X-Cache", "HIT")
			writeJSON(w, http.StatusOK, jsonrpc.ResultBody(reqs[0].ID, result))
			return
		}
		g.cacheMisses.Add(1)
	}

	resp, from, err := g.forward(r.Context(), body, batch)
	if err != nil {
		g.opts.Logger.Warn("all upstreams failed", "method", reqs[0].Method, "err", err)
		writeJSON(w, http.StatusBadGateway, jsonrpc.ErrorBody(reqs[0].ID, jsonrpc.CodeInternalError, "all upstreams failed"))
		return
	}

	if cacheable {
		g.maybeCache(key, ttl, resp)
		w.Header().Set("X-Cache", "MISS")
	}
	w.Header().Set("X-Upstream", from)
	writeJSON(w, http.StatusOK, resp)
}

// forward tries upstreams in order until one gives a usable answer.
func (g *Gateway) forward(ctx context.Context, body []byte, batch bool) ([]byte, string, error) {
	candidates := g.opts.Pool.Candidates()
	if len(candidates) > g.opts.MaxAttempts {
		candidates = candidates[:g.opts.MaxAttempts]
	}

	var lastErr error
	for i, u := range candidates {
		attemptCtx, cancel := context.WithTimeout(ctx, g.opts.AttemptTimeout)
		data, err := u.Do(attemptCtx, body)
		cancel()

		if err != nil {
			// the client hung up: stop, and don't blame the upstream
			if ctx.Err() != nil {
				return nil, "", ctx.Err()
			}
			g.opts.Pool.RecordFailureOn(u)
			lastErr = err
			g.opts.Logger.Info("upstream failed, trying next", "upstream", u.Name, "err", err)
			continue
		}

		// providers also report rate limits inside a 200 response; another upstream may have headroom
		hasMore := i < len(candidates)-1
		if !batch && hasMore && isRateLimited(data) {
			lastErr = fmt.Errorf("%s: rate limited", u.Name)
			g.opts.Logger.Info("upstream rate limited, trying next", "upstream", u.Name)
			continue
		}

		u.RecordSuccess()
		return data, u.Name, nil
	}

	if lastErr == nil {
		lastErr = errors.New("no upstreams configured")
	}
	return nil, "", lastErr
}

func (g *Gateway) maybeCache(key string, ttl time.Duration, respBody []byte) {
	var resp jsonrpc.Response
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return
	}
	// errors and null results (a receipt that isn't there yet) can change a moment later
	if resp.Error != nil || jsonrpc.IsNull(resp.Result) {
		return
	}
	g.opts.Cache.Set(key, resp.Result, ttl)
}

func cacheKey(req jsonrpc.Request) (key string, ttl time.Duration, ok bool) {
	ttl, ok = cacheTTL[req.Method]
	if !ok {
		return "", 0, false
	}

	var params bytes.Buffer
	if len(req.Params) > 0 {
		if err := json.Compact(&params, req.Params); err != nil {
			return "", 0, false
		}
	}
	return req.Method + "|" + params.String(), ttl, true
}

func isRateLimited(respBody []byte) bool {
	var resp jsonrpc.Response
	if err := json.Unmarshal(respBody, &resp); err != nil {
		return false // a batch response, or garbage; pass it through
	}
	return resp.Error != nil && resp.Error.Code == jsonrpc.CodeLimitExceeded
}

func (g *Gateway) clientKey(r *http.Request) string {
	if g.opts.TrustProxyHeaders {
		if xff := r.Header.Get("X-Forwarded-For"); xff != "" {
			first, _, _ := strings.Cut(xff, ",")
			return strings.TrimSpace(first)
		}
	}
	host, _, err := net.SplitHostPort(r.RemoteAddr)
	if err != nil {
		return r.RemoteAddr
	}
	return host
}

func (g *Gateway) healthz(w http.ResponseWriter) {
	healthy := 0
	for _, u := range g.opts.Pool.Upstreams() {
		if u.Healthy() {
			healthy++
		}
	}

	status := http.StatusOK
	if healthy == 0 {
		status = http.StatusServiceUnavailable
	}
	writeJSON(w, status, []byte(fmt.Sprintf(`{"healthyUpstreams":%d,"totalUpstreams":%d}`, healthy, len(g.opts.Pool.Upstreams()))))
}

// metrics writes Prometheus text format by hand to avoid pulling in a dependency.
func (g *Gateway) metrics(w http.ResponseWriter) {
	var b strings.Builder
	fmt.Fprintf(&b, "# TYPE rpc_cache_hits_total counter\nrpc_cache_hits_total %d\n", g.cacheHits.Load())
	fmt.Fprintf(&b, "# TYPE rpc_cache_misses_total counter\nrpc_cache_misses_total %d\n", g.cacheMisses.Load())
	fmt.Fprintf(&b, "# TYPE rpc_rate_limited_total counter\nrpc_rate_limited_total %d\n", g.rateLimited.Load())

	b.WriteString("# TYPE rpc_upstream_requests_total counter\n")
	for _, u := range g.opts.Pool.Upstreams() {
		fmt.Fprintf(&b, "rpc_upstream_requests_total{upstream=%q} %d\n", u.Name, u.Requests.Load())
	}
	b.WriteString("# TYPE rpc_upstream_failures_total counter\n")
	for _, u := range g.opts.Pool.Upstreams() {
		fmt.Fprintf(&b, "rpc_upstream_failures_total{upstream=%q} %d\n", u.Name, u.Failures.Load())
	}
	b.WriteString("# TYPE rpc_upstream_healthy gauge\n")
	for _, u := range g.opts.Pool.Upstreams() {
		v := 0
		if u.Healthy() {
			v = 1
		}
		fmt.Fprintf(&b, "rpc_upstream_healthy{upstream=%q} %d\n", u.Name, v)
	}
	b.WriteString("# TYPE rpc_upstream_block_number gauge\n")
	for _, u := range g.opts.Pool.Upstreams() {
		fmt.Fprintf(&b, "rpc_upstream_block_number{upstream=%q} %d\n", u.Name, u.BlockNumber())
	}

	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	io.WriteString(w, b.String())
}

func writeJSON(w http.ResponseWriter, status int, body []byte) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	w.Write(body)
}
