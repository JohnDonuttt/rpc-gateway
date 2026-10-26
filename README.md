# rpc-gateway

A small JSON-RPC proxy that sits in front of several Ethereum-style nodes (Alchemy, Infura, your own node) and makes them look like one reliable endpoint. If a provider is down, slow, rate-limited or behind on blocks, requests go to the next one.

Point your app, `cast`, or `viem` at the gateway instead of at a single provider:

```
your app  ->  rpc-gateway  ->  provider A  (primary)
                           ->  provider B  (used if A fails)
                           ->  your own node
```

Go standard library only. There are no third-party dependencies.

## Run it

Needs Go 1.22+.

```sh
cp examples/gateway.json gateway.json   # put your real upstream URLs in
make run
```

```sh
cast block-number --rpc-url http://127.0.0.1:8545
```

## What it does

- **Failover.** Upstreams are tried in priority order. A request that fails on a network error, a timeout, or a non-200 status moves to the next upstream, up to `maxAttempts`.
- **Rate-limit aware.** If a provider returns JSON-RPC error `-32005` inside a 200 response, the request is retried elsewhere. If it's the only upstream left, the client gets that error as is.
- **Real answers are not retried.** A revert or any other ordinary JSON-RPC error goes straight back to the client, because asking another node would not change it.
- **Health checks.** Every upstream is probed with `eth_blockNumber` on an interval. It is taken out of rotation after `unhealthyAfter` failures in a row, and brought back by the next good probe. Passive failures from live traffic count toward the same threshold.
- **Lag detection.** A node more than `maxLagBlocks` behind the best upstream stops receiving traffic until it catches up. A node can answer requests happily while serving stale data, so this matters.
- **Last resort.** If every upstream is marked down, they are still tried (in priority order) rather than failing instantly.
- **Caching.** Only answers that cannot change are cached: `eth_chainId`, `net_version`, `eth_getBlockByHash`, `eth_getTransactionByHash` and `eth_getTransactionReceipt`. Nulls and errors are never cached, since a pending transaction's receipt will change. Anything that takes `latest` is never cached. Cached replies carry the new request's `id`.
- **Rate limiting.** A token bucket per client IP. Set `trustProxyHeaders` only if you run behind a proxy you control, otherwise `X-Forwarded-For` could be spoofed to dodge the limit.
- **Batches** are forwarded to one upstream as a unit.
- **Graceful shutdown** on SIGINT and SIGTERM.

## Config

| Field | Default | Meaning |
| --- | --- | --- |
| `listen` | `:8545` | Address to serve on |
| `upstreams` | required | List of `{ "name", "url" }` in priority order |
| `attemptTimeout` | `10s` | Time allowed per upstream attempt |
| `maxAttempts` | `3` | Upstreams tried per request |
| `healthInterval` | `5s` | Probe interval |
| `unhealthyAfter` | `2` | Consecutive failures before an upstream is marked down |
| `maxLagBlocks` | `5` | How far behind the best node an upstream may be |
| `cacheEntries` | `10000` | LRU size. `0` turns caching off |
| `rateLimit` | off | `{ "requestsPerSecond", "burst" }` per client IP |
| `trustProxyHeaders` | `false` | Key rate limits on `X-Forwarded-For` |

Unknown keys are an error, so a typo like `"upstream"` fails loudly instead of being ignored.

## Endpoints

| Path | |
| --- | --- |
| `POST /` | JSON-RPC (single or batch) |
| `GET /healthz` | `200` if at least one upstream is healthy, else `503` |
| `GET /metrics` | Prometheus text: cache hits and misses, rate-limited count, per-upstream requests, failures, health and block height |

Responses carry `X-Upstream` (who answered) and `X-Cache` (`HIT` or `MISS`) headers.

## Layout

```
cmd/rpc-gateway/       entrypoint and wiring
internal/gateway/      HTTP handler: caching, failover, limits, metrics
internal/upstream/     one node + the pool with health checks and lag rules
internal/cache/        LRU with expiry
internal/ratelimit/    per-key token bucket
internal/jsonrpc/      request/response types and parsing
internal/config/       config loading and validation
```

## Testing

```sh
make test      # go test -race ./...
```

Tests run against in-process fake nodes (`httptest`) that can fail, stall, rate-limit and fall behind on demand. They cover failover on 500s, timeouts and in-body rate limits, not retrying reverts, cache correctness (ids, params, nulls, errors), batches, rate limiting, input validation, health transitions and lag, and 500 concurrent requests under the race detector.

I also ran the built binary against a live anvil node with a dead primary: requests were answered by the backup, repeated calls hit the cache, `cast` worked through it, and `/healthz` went to 503 when the last node was stopped.

## Limitations

- Only the listed read-only-safe methods are cached, and batches are never cached.
- No websockets (`eth_subscribe`).
- No per-method routing, such as sending archive queries to one particular node.
- Failover for writes is risky in general: `eth_sendRawTransaction` retried on another node is idempotent for the same signed transaction, but you may see "already known" errors from the second node.
- Rate limiting is in memory and per instance. Behind several gateway instances each one counts separately.
- On an older Go toolchain with a very new macOS you may hit a linker error when running tests. `GOFLAGS="-ldflags=-linkmode=external" go test ./...` works around it.

## License

MIT
