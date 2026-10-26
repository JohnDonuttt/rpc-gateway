// Package upstream talks to individual JSON-RPC nodes and keeps track of which ones are usable.
package upstream

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"github.com/JohnDonuttt/rpc-gateway/internal/jsonrpc"
)

const maxResponseBytes = 32 << 20

// RetryableError means the request never got a usable answer (network error, timeout, non-2xx),
// so it is safe to try another upstream.
type RetryableError struct {
	Status int
	Err    error
}

func (e *RetryableError) Error() string { return e.Err.Error() }
func (e *RetryableError) Unwrap() error { return e.Err }

type Upstream struct {
	Name string
	URL  string

	client  *http.Client
	healthy atomic.Bool
	fails   atomic.Int32
	block   atomic.Uint64

	Requests atomic.Uint64
	Failures atomic.Uint64
}

func New(name, url string, client *http.Client) *Upstream {
	if client == nil {
		client = http.DefaultClient
	}
	u := &Upstream{Name: name, URL: url, client: client}
	u.healthy.Store(true) // optimistic until the first probe says otherwise
	return u
}

func (u *Upstream) Healthy() bool       { return u.healthy.Load() }
func (u *Upstream) BlockNumber() uint64 { return u.block.Load() }

// Do sends a raw JSON-RPC body and returns the raw response body.
func (u *Upstream) Do(ctx context.Context, body []byte) ([]byte, error) {
	u.Requests.Add(1)
	data, err := u.roundTrip(ctx, body)
	// a caller that gave up (client disconnected) isn't the upstream's fault
	if err != nil && !errors.Is(err, context.Canceled) {
		u.Failures.Add(1)
	}
	return data, err
}

func (u *Upstream) roundTrip(ctx context.Context, body []byte) ([]byte, error) {
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, u.URL, bytes.NewReader(body))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Content-Type", "application/json")

	resp, err := u.client.Do(req)
	if err != nil {
		return nil, &RetryableError{Err: fmt.Errorf("%s: %w", u.Name, err)}
	}
	defer resp.Body.Close()

	data, err := io.ReadAll(io.LimitReader(resp.Body, maxResponseBytes))
	if err != nil {
		return nil, &RetryableError{Err: fmt.Errorf("%s: reading response: %w", u.Name, err)}
	}

	// providers signal rate limits, bad keys and outages with HTTP status, so anything that
	// isn't a 200 is a reason to try someone else
	if resp.StatusCode != http.StatusOK {
		return nil, &RetryableError{
			Status: resp.StatusCode,
			Err:    fmt.Errorf("%s: http %d", u.Name, resp.StatusCode),
		}
	}
	return data, nil
}

// RecordFailure counts a failed request or probe and marks the upstream down once it
// has failed `threshold` times in a row.
func (u *Upstream) RecordFailure(threshold int32) {
	if u.fails.Add(1) >= threshold {
		u.healthy.Store(false)
	}
}

// RecordSuccess clears the failure streak. It doesn't mark an unhealthy upstream healthy:
// only a health probe can do that, since a node can answer requests while far behind.
func (u *Upstream) RecordSuccess() { u.fails.Store(0) }

type Pool struct {
	upstreams      []*Upstream
	unhealthyAfter int32
	maxLag         uint64
	probeTimeout   time.Duration
}

// NewPool takes upstreams in priority order. maxLag is how many blocks behind the best
// upstream a node may be before it stops receiving traffic.
func NewPool(ups []*Upstream, unhealthyAfter int, maxLag uint64, probeTimeout time.Duration) *Pool {
	if unhealthyAfter < 1 {
		unhealthyAfter = 1
	}
	return &Pool{upstreams: ups, unhealthyAfter: int32(unhealthyAfter), maxLag: maxLag, probeTimeout: probeTimeout}
}

func (p *Pool) Upstreams() []*Upstream { return p.upstreams }

// RecordFailureOn lets the request path feed failures into the same health rules as the probes.
func (p *Pool) RecordFailureOn(u *Upstream) { u.RecordFailure(p.unhealthyAfter) }

// Candidates lists upstreams in the order they should be tried: healthy ones by priority,
// then unhealthy ones as a last resort, so a fully-degraded pool still attempts something.
func (p *Pool) Candidates() []*Upstream {
	out := make([]*Upstream, 0, len(p.upstreams))
	for _, u := range p.upstreams {
		if u.Healthy() {
			out = append(out, u)
		}
	}
	for _, u := range p.upstreams {
		if !u.Healthy() {
			out = append(out, u)
		}
	}
	return out
}

type probeResult struct {
	block uint64
	err   error
}

// CheckAll probes every upstream concurrently, then applies failure and lag rules.
func (p *Pool) CheckAll(ctx context.Context) {
	results := make([]probeResult, len(p.upstreams))

	var wg sync.WaitGroup
	for i, u := range p.upstreams {
		wg.Add(1)
		go func() {
			defer wg.Done()
			results[i].block, results[i].err = p.probe(ctx, u)
		}()
	}
	wg.Wait()

	var best uint64
	for _, r := range results {
		if r.err == nil && r.block > best {
			best = r.block
		}
	}

	for i, u := range p.upstreams {
		r := results[i]
		if r.err != nil {
			u.RecordFailure(p.unhealthyAfter)
			continue
		}

		u.block.Store(r.block)
		u.RecordSuccess()
		lagging := best > r.block && best-r.block > p.maxLag
		u.healthy.Store(!lagging)
	}
}

// Run probes once straight away and then on every tick until ctx is cancelled.
func (p *Pool) Run(ctx context.Context, interval time.Duration) {
	p.CheckAll(ctx)

	t := time.NewTicker(interval)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			p.CheckAll(ctx)
		}
	}
}

var blockNumberRequest = []byte(`{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`)

func (p *Pool) probe(ctx context.Context, u *Upstream) (uint64, error) {
	ctx, cancel := context.WithTimeout(ctx, p.probeTimeout)
	defer cancel()

	data, err := u.roundTrip(ctx, blockNumberRequest)
	if err != nil {
		return 0, err
	}

	var resp jsonrpc.Response
	if err := json.Unmarshal(data, &resp); err != nil {
		return 0, fmt.Errorf("%s: bad probe response: %w", u.Name, err)
	}
	if resp.Error != nil {
		return 0, fmt.Errorf("%s: probe error %d: %s", u.Name, resp.Error.Code, resp.Error.Message)
	}

	var hex string
	if err := json.Unmarshal(resp.Result, &hex); err != nil {
		return 0, fmt.Errorf("%s: probe result is not a string: %w", u.Name, err)
	}
	n, err := strconv.ParseUint(strings.TrimPrefix(hex, "0x"), 16, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: probe result %q is not a block number: %w", u.Name, hex, err)
	}
	return n, nil
}
