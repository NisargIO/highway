# Highway

Highway is a Gleam/OTP service for authorized live streams over WebSockets. Application backends issue signed JWT capabilities for exact tenant/stream actions. One owner orders each stream, and live delivery does not wait for storage.

**Status: milestones 1–6 implemented and verified locally.** Authenticated live streams now support explicit `development_memory` and `s3_async` storage. S3 writes immutable batches/indexes, conditionally commits the authoritative head, and allocates fresh durable epochs on recovery. Only confirmed head commits produce `persisted`; `accepted` remains admission to bounded memory and a crash can lose its pending tail. Authorized earliest/after replay joins committed history, pending memory, and later live events on one subscription, including interrupted epochs. Production packaging, bounded SIGTERM drain, storage/admission readiness, Prometheus metrics, and a Node.js/browser SDK are implemented. Real AWS conformance remains an opt-in release gate and has not been run here.

## Run the demonstration

Use the versions in `.tool-versions` (Gleam 1.19.2, Erlang/OTP 29.1.2, rebar3 3.27.3), Python 3, `patch`, and Node 22+.

```sh
make setup
make check
make smoke
make smoke-s3
make sdk-check
make smoke-release
# With Docker available:
make smoke-container
```

The [two-client smoke example](examples/smoke.mjs) generates an ephemeral signing key in memory, writes only its public JWKS to a temporary directory, starts an isolated server on `127.0.0.1:4401`, verifies ordered live delivery and failure cases, then stops the server process group and removes temporary files. Override `SMOKE_PORT` if necessary. No AWS account or third-party Node packages are needed.

The [S3 network smoke](examples/s3-smoke.mjs) uses a loopback Python contract server plus the real signing/HTTP adapter. It checks live delivery while head commits are paused, no early persistence notice, lost head responses, crash recovery, exclusive lost-tail resume, finite snapshots, ordered replay/live handoff while publishing and evicting cache, subscribe-only archive continuation, and stale writer fencing. It runs without Docker or AWS. The fixture checks signing metadata but does not validate the signature cryptographically; use the opt-in AWS check before release.

For a persistent local object store, run `make compose-up` and `make compose-smoke`. [Docker Compose with MinIO](docs/local-compose.md) builds pinned source, initializes a private bucket and tests real SigV4/conditional writes, live delivery during an outage, graceful drain, both-service restart, and ordered SDK replay. The stack stays running at `ws://127.0.0.1:4000/v1/ws`, with the MinIO console at `http://127.0.0.1:9001`. `make compose-token` writes a local JWT for the examples; `make compose-down` stops the stack while preserving history.

## Cluster mode: one endpoint, many regions

Set `HIGHWAY_CLUSTER_NODE`, `HIGHWAY_REGION` and a shared `HIGHWAY_CLUSTER_COOKIE(_FILE)` on every node, and point them all at the same bucket and prefix. Behind one latency-routed hostname, a stream is created on the node its first publisher reaches. Clients on other nodes are served by a local mirror, and their publishes are forwarded to the owner, which alone assigns cursors. Owners hold a lease in head v3 and stop admitting before it lapses. A crashed owner's streams move after lease plus skew; drained or idle (sleeping) streams wake wherever they are touched next. Clients and the SDK are unchanged. `make smoke-cluster` runs two nodes as `us-east-1` and `eu-west-1`. See [Decision 006](docs/decisions/006-multi-region-ownership.md) and [cluster deployment](docs/deployment.md#cluster-mode).

## Deploy and use clients

`make release` exports a native OTP shipment. `make image` builds a digest-pinned non-root runtime container; `make smoke-container` verifies read-only execution and signal handling. See [deployment instructions](docs/deployment.md) for secrets, private S3 IAM, TLS/Caddy, readiness, metrics, graceful drain and stop-then-start upgrades. In standalone mode, exactly one server per prefix remains required.

The [JavaScript SDK](sdk/javascript/README.md) supports Node.js 22+ and browsers, TypeScript declarations, token refresh, bounded async iteration, processed-cursor resume, and separate accepted/persisted events. It is implemented locally and has not been published to npm. Try [the Node example](examples/node-client.mjs) or serve the repository locally with `python3 -m http.server 3000 --bind 127.0.0.1` and open `http://localhost:3000/examples/browser/` for the [browser example](examples/browser/index.html). Obtain short-lived JWTs from your backend; no signing key belongs in either client. Create the stream with an authorized publish before subscribing.

`make smoke-release` runs the real native shipment and SDK through drain/restart/replay and writes `build/benchmark.json`. [Local measurements and their limits](docs/benchmarks.md) include a deliberately small 64-record pending cap; defaults are not production throughput promises.

## Independent stream tokens

The SDK can share a socket across independently authorized subscriptions:

```js
const client = new HighwayClient({ url: 'wss://streams.example.com/v1/ws' });
const sub = await client.subscribe({
  stream: 'events',
  getToken: () => getStreamJwt('events'), // your backend, fresh on renewal
  from: 'earliest',
  follow: true,
  window: 16,
});
const other = await client.subscribe({
  stream: 'notifications',
  getToken: () => getStreamJwt('notifications'),
  from: 'latest',
});
```

Use `sub.refresh()` to refresh one view explicitly; automatic refresh and expiry are independent. All tokens on the socket must share subject/tenant. Earliest/after need subscribe and history grants. Publish similarly takes its own `getToken` callback. This is the explicit [protocol v2 extension](docs/decisions/005-scoped-stream-auth.md), implemented in SDK 0.2.0; constructor-level `getToken` still selects the compatible v1 connection-auth API. `make smoke-scoped` reproduces multi-stream JWT isolation, expiry/refresh, live-before-head and ordered restart/resume; `make compose-smoke` uses v2 with real MinIO.

## Replay and continue live

After authentication, subscribe with history permission:

```json
{"type":"subscribe","id":"s1","subscription":"history","stream":"events","from":"earliest","follow":true,"window":128}
```

Use `"from":{"after":"<last received cursor>"}` for exclusive resume or `"follow":false` for a finite snapshot. Acknowledge delivered events cumulatively with `{"type":"ack","subscription":"history","through":"<event cursor>"}`. `caught_up` identifies the original captured watermark; subsequent events continue on the same subscription. Handle `discontinuity` as an uncertain crash boundary, with no exact lost count. See [Decision 003](docs/decisions/003-replay-continuation.md) for bounds and traversal costs.

## Run with your application's verification keys

Configure an issuer, audience, and a public JWKS containing 1–16 uniquely identified Ed25519 keys (`kty: OKP`, `crv: Ed25519`, `x`, `kid`). Private keys and symmetric keys are rejected. Have your backend sign compact JWTs with `alg: EdDSA` and the configured `kid`.

```sh
export HIGHWAY_STORAGE_MODE=development_memory
export HIGHWAY_JWKS_FILE=/absolute/path/to/public-jwks.json
export HIGHWAY_JWT_ISSUER=https://app.example.com
export HIGHWAY_JWT_AUDIENCE=highway
make run
```

Storage mode must be selected explicitly; missing/unknown modes fail startup. `HIGHWAY_BIND` defaults to `127.0.0.1`; explicit `0.0.0.0` is supported behind a private TLS proxy, and `HIGHWAY_PORT` defaults to `4000`. Browser Origin must equal `HIGHWAY_ORIGIN` (default `http://localhost:3000`); native clients without Origin still require JWT authentication. Query strings are rejected on `/v1/ws`; send tokens only in JWT-bearing protocol frames (v1 `auth`, or v2 scoped commands).

`GET /healthz` returns 200. Memory readiness stays 503. S3 readiness requires a fresh successful read/write canary probe and available storage/admission capacity, even before the first stream. It becomes 503 during drain, stale/failed probes, owner degradation or full admission capacity. `/metrics` exposes fixed node-level Prometheus counters/gauges on the private listener. `/v1/ws` advertises the selected mode and `durable: false`: live acceptance never promises durability.

An optional `HIGHWAY_LIMITS_FILE` points to a JSON object overriding validated positive limits: `max_connections`, `max_streams`, `stream_bytes`, `node_bytes`, `max_records`, `pending_age_ms`, `subscriptions`, `output_bytes`, `slow_ms`, `auth_ms`, `idle_ms`, `tick_ms`, `replay_workers`, `replay_pages_per_step`, `replay_max_pages`, and `replay_timeout_ms`. Defaults and fixed transport limits are in the [protocol](docs/protocol.md). In memory mode, all accepted data remains in memory until an owner terminates; publishes stop at the byte/record limit or 30 seconds after its first accepted event. Restart the demonstration to clear capacity. This is a bounded development adapter, not a continuously draining storage service.

## Enable S3 archival

Supply the same JWT settings above plus an existing private S3 general-purpose bucket and credentials:

```sh
export HIGHWAY_STORAGE_MODE=s3_async
export HIGHWAY_S3_BUCKET=your-private-bucket
export HIGHWAY_S3_PREFIX=highway/dev
export AWS_REGION=us-east-1
export AWS_ACCESS_KEY_ID=your-access-key
export AWS_SECRET_ACCESS_KEY=your-secret-key
# export AWS_SESSION_TOKEN=your-temporary-session-token
# export HIGHWAY_S3_ENDPOINT=https://s3.us-east-1.amazonaws.com
make run
```

No credentials are stored in the repository. Environment credentials are fixed for the process lifetime. Each AWS credential also supports an exclusive `_FILE` setting for mounted secrets. `HIGHWAY_S3_TIMEOUT_MS` defaults to 2000 (allowed 100–10000). Prefixes are nonempty, at most 64 bytes, and canonical lowercase path components; tenant/stream IDs are hex-encoded before becoming object keys. Use the private backend with the documented TLS proxy for public access. Run exactly one server per prefix, using stop-then-start deployments.

An owner's initial operation can return retryable `unavailable` while its worker allocates a durable epoch. Retry that command; no publish has been accepted during initialization. Thereafter, live append/fanout never waits on S3. Batches flush at 1 MiB or the next 250 ms timer, with one commit per stream and up to 16 active storage operations. Pending capacity is released only after the authoritative head commit. Storage outages retain accepted records and reject new admission at byte/record/age limits.

Before an AWS release, explicitly run:

```sh
HIGHWAY_S3_CONFORMANCE=1 make s3-conformance
```

This writes fresh test objects only under the configured prefix's `conformance/` subdirectory and leaves them for operator cleanup. It checks actual conditional writes and read visibility; normal tests never require AWS. See [Decision 002](docs/decisions/002-s3-archival.md) for archival and [Decision 004](docs/decisions/004-production-operations.md) for v2 open/closed heads, drain, deployment, and migration. Segments/indexes remain v1; v1 open heads remain readable.

## Contract and limitations

- `accepted` assigns a cursor; delivery and durability are separate. Retries can append duplicates, including repeated `message_id` values.
- Latest subscriptions follow ordered events and use cumulative `ack` credit. Slow subscriptions terminate explicitly and never silently skip events.
- Earliest/after reads require history permission and resume exclusively. `follow: false` ends at the captured watermark, including its pending suffix. S3 recovery emits ordered `discontinuity` frames for uncertain tails, even when resuming beyond the archived prefix. Prior memory incarnations remain invalid.
- Replay uses bounded workers, page steps, bytes, and time; `replay_limit` explicitly terminates excessive work. Latest subscriptions may fetch their own post-start backlog from S3 with only subscribe access. No missing cache range is silently skipped.
- Expiry and the auth deadline apply on idle connections. Refresh keeps subject/tenant fixed and immediately cancels removed subscriptions.
- SIGTERM closes admission, drains accepted records through authoritative head commits, and confirms closed heads before successful shutdown. Timeout exits nonzero; unconfirmed data stays uncertain. Recovery conservatively reports interruption boundaries even after clean shutdown.
- TLS terminates at the proxy. Real AWS conformance, automatic AWS credential refresh, tenant rate policies, long-running scale validation and retention remain future work. Cluster mode has no read replicas or region-aware readiness yet, and it relies on Erlang distribution between nodes.

Builds apply a reviewed, hash-checked [Mist input-cap patch](patches/mist-input-cap.patch) before compilation. Use `make setup/check/run/smoke` so the cap is present. The Makefile also suppresses only JOSE's deprecated `catch` compiler warning under OTP 29; signature verification uses the maintained library's strict algorithm API. See the [implementation decision](docs/decisions/001-development-live.md).

## Start here

- [Architecture](docs/architecture.md): implemented live, archival, and replay paths.
- [Protocol](docs/protocol.md): authentication, replay frames, and limits.
- [Implementation plan](docs/implementation-plan.md): milestone status and validation.
- [Contributing](CONTRIBUTING.md): development and verification.
- [Agent instructions](AGENTS.md): constraints for coding agents.
- [Fresh session prompt](docs/fresh-session-prompt.md): continuation handoff.

[S2](https://github.com/s2-streamstore/s2) inspired stream ownership and ordered appends. Highway has a separate protocol and deliberately allows live delivery before S3 archival; no S2 compatibility or exactly-once delivery is claimed.
