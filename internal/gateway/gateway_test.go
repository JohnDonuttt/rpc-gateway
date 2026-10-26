package gateway

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/JohnDonuttt/rpc-gateway/internal/cache"
	"github.com/JohnDonuttt/rpc-gateway/internal/jsonrpc"
	"github.com/JohnDonuttt/rpc-gateway/internal/ratelimit"
	"github.com/JohnDonuttt/rpc-gateway/internal/upstream"
)

// fakeNode is a JSON-RPC server whose behaviour each test can script.
type fakeNode struct {
	*httptest.Server
	calls atomic.Int32
}

// newNode starts a node. handle gets the raw request body and returns the raw response body;
// returning a non-200 status or sleeping lets tests simulate outages and slow nodes.
func newNode(t *testing.T, handle func(body []byte) (status int, resp string)) *fakeNode {
	t.Helper()
	n := &fakeNode{}
	n.Server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		n.calls.Add(1)
		body, _ := io.ReadAll(r.Body)
		status, resp := handle(body)
		w.WriteHeader(status)
		io.WriteString(w, resp)
	}))
	t.Cleanup(n.Close)
	return n
}

// echoResult answers every request with the given result, copying the request id.
func echoResult(result string) func([]byte) (int, string) {
	return func(body []byte) (int, string) {
		var req jsonrpc.Request
		json.Unmarshal(body, &req)
		return 200, fmt.Sprintf(`{"jsonrpc":"2.0","id":%s,"result":%s}`, req.ID, result)
	}
}

func fixedBody(status int, resp string) func([]byte) (int, string) {
	return func([]byte) (int, string) { return status, resp }
}

type harness struct {
	gw    *Gateway
	srv   *httptest.Server
	nodes []*fakeNode
	pool  *upstream.Pool
}

func newHarness(t *testing.T, mod func(*Options), nodes ...*fakeNode) *harness {
	t.Helper()
	ups := make([]*upstream.Upstream, len(nodes))
	for i, n := range nodes {
		ups[i] = upstream.New(fmt.Sprintf("node%d", i), n.URL, n.Client())
	}
	pool := upstream.NewPool(ups, 2, 5, time.Second)

	opts := Options{Pool: pool, Cache: cache.New(100), AttemptTimeout: time.Second, MaxAttempts: 3}
	if mod != nil {
		mod(&opts)
	}
	gw := New(opts)
	srv := httptest.NewServer(gw)
	t.Cleanup(srv.Close)
	return &harness{gw: gw, srv: srv, nodes: nodes, pool: pool}
}

func (h *harness) post(t *testing.T, body string) (*http.Response, string) {
	t.Helper()
	resp, err := http.Post(h.srv.URL, "application/json", strings.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	return resp, string(b)
}

const blockNumberReq = `{"jsonrpc":"2.0","id":1,"method":"eth_blockNumber","params":[]}`

func TestForwardsToFirstHealthyUpstream(t *testing.T) {
	a := newNode(t, echoResult(`"0x10"`))
	b := newNode(t, echoResult(`"0x20"`))
	h := newHarness(t, nil, a, b)

	resp, body := h.post(t, blockNumberReq)

	if resp.StatusCode != 200 || resp.Header.Get("X-Upstream") != "node0" {
		t.Fatalf("status=%d upstream=%q", resp.StatusCode, resp.Header.Get("X-Upstream"))
	}
	if !strings.Contains(body, `"0x10"`) {
		t.Fatalf("body = %s", body)
	}
	if b.calls.Load() != 0 {
		t.Fatal("the second upstream shouldn't be touched when the first works")
	}
}

func TestFailsOverOnServerError(t *testing.T) {
	a := newNode(t, fixedBody(500, "boom"))
	b := newNode(t, echoResult(`"0x20"`))
	h := newHarness(t, nil, a, b)

	resp, body := h.post(t, blockNumberReq)

	if resp.StatusCode != 200 || resp.Header.Get("X-Upstream") != "node1" || !strings.Contains(body, "0x20") {
		t.Fatalf("status=%d upstream=%q body=%s", resp.StatusCode, resp.Header.Get("X-Upstream"), body)
	}
}

func TestFailsOverOnTimeout(t *testing.T) {
	slow := newNode(t, func(body []byte) (int, string) {
		time.Sleep(300 * time.Millisecond)
		return echoResult(`"0x1"`)(body)
	})
	fast := newNode(t, echoResult(`"0x2"`))
	h := newHarness(t, func(o *Options) { o.AttemptTimeout = 50 * time.Millisecond }, slow, fast)

	resp, _ := h.post(t, blockNumberReq)

	if resp.Header.Get("X-Upstream") != "node1" {
		t.Fatalf("upstream = %q, want node1", resp.Header.Get("X-Upstream"))
	}
}

func TestFailsOverOnRateLimitInsideA200(t *testing.T) {
	limited := newNode(t, fixedBody(200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"limit exceeded"}}`))
	ok := newNode(t, echoResult(`"0x2"`))
	h := newHarness(t, nil, limited, ok)

	resp, _ := h.post(t, blockNumberReq)

	if resp.Header.Get("X-Upstream") != "node1" {
		t.Fatalf("upstream = %q, want node1", resp.Header.Get("X-Upstream"))
	}
}

func TestReturnsRateLimitErrorWhenNoOtherUpstreamLeft(t *testing.T) {
	limited := newNode(t, fixedBody(200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32005,"message":"limit exceeded"}}`))
	h := newHarness(t, nil, limited)

	resp, body := h.post(t, blockNumberReq)

	// the only upstream's answer is the best we have, so the client sees it as is
	if resp.StatusCode != 200 || !strings.Contains(body, "-32005") {
		t.Fatalf("status=%d body=%s", resp.StatusCode, body)
	}
}

func TestDoesNotRetryOrdinaryRPCErrors(t *testing.T) {
	revert := `{"jsonrpc":"2.0","id":1,"error":{"code":3,"message":"execution reverted"}}`
	a := newNode(t, fixedBody(200, revert))
	b := newNode(t, echoResult(`"0x1"`))
	h := newHarness(t, nil, a, b)

	_, body := h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_call","params":[{},"latest"]}`)

	if !strings.Contains(body, "execution reverted") {
		t.Fatalf("body = %s", body)
	}
	if b.calls.Load() != 0 {
		t.Fatal("a revert is a real answer; asking another node would be wrong")
	}
}

func TestAllUpstreamsFailingReturns502WithJSONRPCError(t *testing.T) {
	a := newNode(t, fixedBody(500, "x"))
	b := newNode(t, fixedBody(503, "y"))
	h := newHarness(t, nil, a, b)

	resp, body := h.post(t, `{"jsonrpc":"2.0","id":"req-7","method":"eth_blockNumber"}`)

	if resp.StatusCode != http.StatusBadGateway {
		t.Fatalf("status = %d", resp.StatusCode)
	}
	var out jsonrpc.Response
	if err := json.Unmarshal([]byte(body), &out); err != nil || out.Error == nil || string(out.ID) != `"req-7"` {
		t.Fatalf("body = %s (err %v)", body, err)
	}
}

func TestRepeatedFailuresTakeAnUpstreamOutOfRotation(t *testing.T) {
	bad := newNode(t, fixedBody(500, "x"))
	good := newNode(t, echoResult(`"0x2"`))
	h := newHarness(t, nil, bad, good)

	h.post(t, blockNumberReq)
	h.post(t, blockNumberReq) // threshold is 2

	before := bad.calls.Load()
	h.post(t, blockNumberReq)

	if bad.calls.Load() != before {
		t.Fatal("an upstream marked down should be tried last, not first")
	}
}

func TestCachesChainIDAndRewritesRequestID(t *testing.T) {
	node := newNode(t, echoResult(`"0x1"`))
	h := newHarness(t, nil, node)

	first, _ := h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`)
	second, body := h.post(t, `{"jsonrpc":"2.0","id":"second","method":"eth_chainId","params":[]}`)

	if first.Header.Get("X-Cache") != "MISS" || second.Header.Get("X-Cache") != "HIT" {
		t.Fatalf("cache headers: %q then %q", first.Header.Get("X-Cache"), second.Header.Get("X-Cache"))
	}
	if node.calls.Load() != 1 {
		t.Fatalf("upstream calls = %d, want 1", node.calls.Load())
	}
	if body != `{"jsonrpc":"2.0","result":"0x1","id":"second"}` {
		t.Fatalf("cached answer must carry the new request's id, got %s", body)
	}
}

func TestParamsAreAPartOfTheCacheKey(t *testing.T) {
	node := newNode(t, echoResult(`{"n":1}`))
	h := newHarness(t, nil, node)

	h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["0xaa",false]}`)
	h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":["0xaa",true]}`)
	h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_getBlockByHash","params":[ "0xaa" , false ]}`) // same as the first, different spacing

	if node.calls.Load() != 2 {
		t.Fatalf("upstream calls = %d, want 2 (full-tx flag differs; spacing doesn't)", node.calls.Load())
	}
}

func TestDoesNotCacheNullResults(t *testing.T) {
	node := newNode(t, echoResult(`null`))
	h := newHarness(t, nil, node)
	req := `{"jsonrpc":"2.0","id":1,"method":"eth_getTransactionReceipt","params":["0xabc"]}`

	h.post(t, req)
	h.post(t, req)

	if node.calls.Load() != 2 {
		t.Fatalf("a pending tx has a null receipt that will change; calls = %d, want 2", node.calls.Load())
	}
}

func TestDoesNotCacheErrors(t *testing.T) {
	node := newNode(t, fixedBody(200, `{"jsonrpc":"2.0","id":1,"error":{"code":-32000,"message":"nope"}}`))
	h := newHarness(t, nil, node)

	h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`)
	h.post(t, `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`)

	if node.calls.Load() != 2 {
		t.Fatalf("calls = %d, want 2", node.calls.Load())
	}
}

func TestDoesNotCacheMethodsThatChange(t *testing.T) {
	node := newNode(t, echoResult(`"0x1"`))
	h := newHarness(t, nil, node)

	h.post(t, blockNumberReq)
	h.post(t, blockNumberReq)

	if node.calls.Load() != 2 {
		t.Fatalf("eth_blockNumber must never be cached; calls = %d", node.calls.Load())
	}
}

func TestBatchesAreForwardedWholeAndNotCached(t *testing.T) {
	var got atomic.Value
	node := newNode(t, func(body []byte) (int, string) {
		got.Store(string(body))
		return 200, `[{"jsonrpc":"2.0","id":1,"result":"0x1"},{"jsonrpc":"2.0","id":2,"result":"0x2"}]`
	})
	h := newHarness(t, nil, node)
	batch := `[{"jsonrpc":"2.0","id":1,"method":"eth_chainId"},{"jsonrpc":"2.0","id":2,"method":"eth_blockNumber"}]`

	_, body1 := h.post(t, batch)
	h.post(t, batch)

	if got.Load().(string) != batch {
		t.Fatalf("upstream saw %s", got.Load())
	}
	if !strings.HasPrefix(body1, "[") || node.calls.Load() != 2 {
		t.Fatalf("body=%s calls=%d", body1, node.calls.Load())
	}
}

func TestRateLimitsPerClient(t *testing.T) {
	node := newNode(t, echoResult(`"0x1"`))
	h := newHarness(t, func(o *Options) { o.Limiter = ratelimit.New(0.001, 2) }, node)

	var codes []int
	for i := 0; i < 4; i++ {
		resp, _ := h.post(t, blockNumberReq)
		codes = append(codes, resp.StatusCode)
	}

	if fmt.Sprint(codes) != "[200 200 429 429]" {
		t.Fatalf("status codes = %v", codes)
	}
	if node.calls.Load() != 2 {
		t.Fatalf("limited requests must not reach the upstream, calls = %d", node.calls.Load())
	}
}

func TestForwardedForOnlyTrustedWhenEnabled(t *testing.T) {
	node := newNode(t, echoResult(`"0x1"`))

	send := func(h *harness, ip string) int {
		req, _ := http.NewRequest(http.MethodPost, h.srv.URL, strings.NewReader(blockNumberReq))
		req.Header.Set("X-Forwarded-For", ip)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		resp.Body.Close()
		return resp.StatusCode
	}

	trusted := newHarness(t, func(o *Options) { o.Limiter = ratelimit.New(0.001, 1); o.TrustProxyHeaders = true }, node)
	if send(trusted, "1.1.1.1") != 200 || send(trusted, "2.2.2.2") != 200 {
		t.Fatal("different forwarded IPs should get separate buckets when trusted")
	}

	untrusted := newHarness(t, func(o *Options) { o.Limiter = ratelimit.New(0.001, 1) }, node)
	if send(untrusted, "1.1.1.1") != 200 || send(untrusted, "2.2.2.2") != 429 {
		t.Fatal("a spoofed header must not dodge the limiter when proxy headers aren't trusted")
	}
}

func TestRejectsBadInput(t *testing.T) {
	node := newNode(t, echoResult(`"0x1"`))
	h := newHarness(t, func(o *Options) { o.MaxBodyBytes = 200 }, node)

	cases := []struct {
		name string
		body string
		want int
	}{
		{"not json", `hello`, 400},
		{"empty", ``, 400},
		{"no method", `{"id":1}`, 400},
		{"too large", `{"jsonrpc":"2.0","id":1,"method":"m","params":["` + strings.Repeat("a", 300) + `"]}`, 413},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, _ := h.post(t, tc.body)
			if resp.StatusCode != tc.want {
				t.Fatalf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
	if node.calls.Load() != 0 {
		t.Fatal("invalid requests must not reach upstreams")
	}
}

func TestOnlyPostIsAllowed(t *testing.T) {
	h := newHarness(t, nil, newNode(t, echoResult(`"0x1"`)))

	resp, err := http.Get(h.srv.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusMethodNotAllowed {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

func TestHealthzReflectsPoolState(t *testing.T) {
	h := newHarness(t, nil, newNode(t, echoResult(`"0x1"`)))

	get := func() (int, string) {
		resp, err := http.Get(h.srv.URL + "/healthz")
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()
		b, _ := io.ReadAll(resp.Body)
		return resp.StatusCode, string(b)
	}

	if code, body := get(); code != 200 || body != `{"healthyUpstreams":1,"totalUpstreams":1}` {
		t.Fatalf("healthy: %d %s", code, body)
	}

	h.pool.RecordFailureOn(h.pool.Upstreams()[0])
	h.pool.RecordFailureOn(h.pool.Upstreams()[0])
	if code, _ := get(); code != http.StatusServiceUnavailable {
		t.Fatalf("with no healthy upstream want 503, got %d", code)
	}
}

func TestMetricsExposeCountersAndUpstreamState(t *testing.T) {
	node := newNode(t, echoResult(`"0x1"`))
	h := newHarness(t, nil, node)
	chainID := `{"jsonrpc":"2.0","id":1,"method":"eth_chainId"}`
	h.post(t, chainID)
	h.post(t, chainID)

	resp, err := http.Get(h.srv.URL + "/metrics")
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	b, _ := io.ReadAll(resp.Body)
	text := string(b)

	for _, want := range []string{
		"rpc_cache_hits_total 1",
		"rpc_cache_misses_total 1",
		`rpc_upstream_requests_total{upstream="node0"} 1`,
		`rpc_upstream_healthy{upstream="node0"} 1`,
	} {
		if !bytes.Contains(b, []byte(want)) {
			t.Errorf("metrics missing %q in:\n%s", want, text)
		}
	}
}

func TestUnknownPathIs404(t *testing.T) {
	h := newHarness(t, nil, newNode(t, echoResult(`"0x1"`)))
	resp, err := http.Get(h.srv.URL + "/nope")
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusNotFound {
		t.Fatalf("status = %d", resp.StatusCode)
	}
}

// Run with -race: many clients hitting a flaky pool with a shared cache and limiter.
func TestConcurrentTrafficIsRaceFree(t *testing.T) {
	var n atomic.Int32
	flaky := newNode(t, func(body []byte) (int, string) {
		if n.Add(1)%3 == 0 {
			return 500, "x"
		}
		return echoResult(`"0x1"`)(body)
	})
	steady := newNode(t, echoResult(`"0x2"`))
	h := newHarness(t, func(o *Options) { o.Limiter = ratelimit.New(100000, 100000) }, flaky, steady)

	methods := []string{"eth_chainId", "eth_blockNumber", "net_version", "eth_getBlockByHash"}
	var wg sync.WaitGroup
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			for j := 0; j < 25; j++ {
				body := fmt.Sprintf(`{"jsonrpc":"2.0","id":%d,"method":%q,"params":["0x%x",false]}`, j, methods[(i+j)%len(methods)], j%5)
				resp, err := http.Post(h.srv.URL, "application/json", strings.NewReader(body))
				if err != nil {
					t.Error(err)
					return
				}
				io.Copy(io.Discard, resp.Body)
				resp.Body.Close()
				if resp.StatusCode != 200 {
					t.Errorf("status %d", resp.StatusCode)
				}
			}
		}(i)
	}
	wg.Wait()
}
