# Single-node deployment

Milestone 6 provides a precompiled OTP shipment, a non-root container, graceful SIGTERM drain, active storage/admission readiness, Prometheus metrics, and Node/browser clients. Run **exactly one server per S3 prefix**. Use stop-then-start upgrades. Head CAS fences stale storage requests; it is not a distributed live ownership lease. Do not run overlapping replicas, rolling replacements, or automatic failover.

## Package and configure

```sh
make setup
make check
make release
# Copy build/erlang-shipment to a compatible host with Erlang/OTP 29.1.2.
./build/erlang-shipment/entrypoint.sh run
```

The shipment contains compiled application/dependencies and entrypoints; Gleam/rebar3 and repository sources are not needed at runtime. Build on the deployment OS/architecture. The [Dockerfile](../Dockerfile) uses digest-pinned multi-architecture Gleam/OTP base images, applies the verified Mist cap patch, and copies only the shipment to the runtime stage. The runtime uses UID/GID 10001 and works with a read-only root, dropped capabilities, and a temporary `/tmp`:

```sh
make image
make smoke-container
```

The container image is built locally as `highway:0.1.0`; this task does not push it to a registry. [deploy/highway.service](../deploy/highway.service) is a systemd example for a host shipment. Install it only after creating the service account/configuration. [deploy/environment.example](../deploy/environment.example) names every required setting without containing credentials.

The default bind is `127.0.0.1`. Containers may explicitly select `HIGHWAY_BIND=0.0.0.0` on a private network; never expose the plaintext backend or administrative endpoints publicly. There is no built-in TLS. Publish the container port on host loopback and use the TLS proxy:

```sh
docker run --name highway --read-only \
  --tmpfs /tmp:noexec,nosuid,size=16m --cap-drop ALL \
  --security-opt no-new-privileges --stop-timeout 45 \
  -p 127.0.0.1:4000:4000 \
  --env-file /etc/highway/container.env -e HIGHWAY_BIND=0.0.0.0 \
  --mount type=bind,source=/etc/highway/public-jwks.json,target=/etc/highway/public-jwks.json,readonly \
  --mount type=bind,source=/run/highway-secrets,target=/run/secrets,readonly \
  highway:0.1.0
```

Supply `HIGHWAY_STORAGE_MODE=s3_async`, the trusted JWT issuer/audience/public JWKS, the private bucket/prefix/region, and AWS credentials. Missing configuration fails startup; there is no production fallback to memory. Memory remains explicitly ephemeral and always unready.

`AWS_ACCESS_KEY_ID`, `AWS_SECRET_ACCESS_KEY`, and optional `AWS_SESSION_TOKEN` each support a corresponding `_FILE` setting for mounted secrets. Setting both forms is an error. Files are UTF-8, at most 8192 bytes, and trimmed for a trailing newline; unreadable/empty/oversized files fail with the setting name only. Ensure UID/GID 10001 can traverse the mounted directory and read the files (for example root:10001, directory 0750/files 0640). Public JWKS need no private key. Never put credentials in the image, git, command arguments, or WebSocket URLs. Credentials are loaded once for the process lifetime; replace expiring AWS session credentials with a drained stop-then-start restart. Automatic AWS credential-chain refresh is not implemented. Client JWT refresh is implemented separately.

## TLS, Origin, and S3 IAM

[deploy/Caddyfile](../deploy/Caddyfile) exposes only `/v1/ws` through Caddy with automatic HTTPS; set `HIGHWAY_PUBLIC_HOST=streams.example.com` in Caddy's environment and configure DNS/certificate issuance normally. Set Highway's `HIGHWAY_ORIGIN` to the exact browser application origin, such as `https://app.example.com`. Caddy preserves the WebSocket Origin; do not strip it or replace it with a trusted value. Native clients without Origin still need a valid JWT. Keep `/healthz`, `/readyz`, and `/metrics` on the private backend; they have no separate admin authentication. Restrict network access and monitor them privately.

Use an existing private general-purpose S3 bucket with Block Public Access and default encryption enabled. [deploy/iam-policy.json](../deploy/iam-policy.json) is a template: replace bucket/prefix placeholders and attach to the service identity, not application JWT holders. Object `GetObject`/`PutObject` are prefix-scoped, with no delete permission. Bucket-scoped `ListBucket` permits reliable missing-object discovery (404 rather than an ambiguous authorization 403); Highway itself never lists to determine history. If stricter listing conditions or KMS encryption are required, validate the resulting permissions against AWS. Use an HTTPS bucket policy and the default HTTPS S3 endpoint. Loopback HTTP is only for local fixture testing; arbitrary plaintext remote S3 endpoints are rejected.

The service writes one small versioned canary under `<prefix>/health/<boot-id>.json`, then conditionally updates and reads that same object every probe. Permit that path along with `<prefix>/v1/…`. Conformance objects live under `<prefix>/conformance/…`. Old canaries/conformance objects may be cleaned separately; never apply lifecycle deletion to referenced stream heads/indexes/segments. Canary metadata is not an ownership lock.

Before an AWS release, run explicitly with test credentials/configuration:

```sh
HIGHWAY_S3_CONFORMANCE=1 make s3-conformance
```

This writes fresh test objects and leaves them for operator cleanup. Real AWS conformance remains outstanding here. The local fixture verifies conditional/transport behavior but does not cryptographically verify SigV4; local tests are not an AWS deployment certification.

## Readiness and metrics

- `/healthz`: process liveness, including during drain.
- `/readyz`: 200 only in S3 mode, outside drain, with a fresh successful read/conditional-write/readback probe and available connection/pending capacity. Starting, fenced, retrying, old-pending, or full owners make it 503. The first stream is no longer required. A blocked probe ages out, and permission/network failure makes the next completed probe unhealthy.
- `/metrics`: Prometheus text with fixed node-level names and no tenant/stream/message labels. Counters include accepted/persisted events/logical bytes, publish rejection, storage operations/errors/duration/read bytes, replay items/errors, connection starts, auth/slow-consumer failures, and drain outcomes. Gauges include active connections/replays/owners, pending bytes, unhealthy owners, drain phase, readiness, and BEAM allocated memory. BEAM memory is not OS/container RSS.

Storage operation counters include canary traffic and expected negative responses (such as missing objects/preconditions), not just outages. Duration is summed milliseconds; derive averages from counts or use the reproducible benchmark for latency percentiles. Counters reset per process start. Historical events recovered from S3 are not counted as new acceptance/persistence. There is no arbitrary metric-label input.

Service logs are JSON with safe operation context. The production logger redacts all arbitrary OTP/dependency reports to event/level, because crash reports can contain session state or message payloads. Specific storage failures retain safe typed causes in service logs and metrics. Startup/tool output can include a listener line; no tokens, private keys, or message bodies belong in logs. The formatter deliberately trades raw crash dumps for privacy. Keep `ERL_CRASH_DUMP=/dev/null` in deployed environments as in both packaging examples.

| Setting | Default | Allowed |
| --- | --- | --- |
| `HIGHWAY_DRAIN_MS` | 30000 | 100–300000 |
| `HIGHWAY_PROBE_MS` | 5000 | 100–60000 |
| `HIGHWAY_PROBE_STALE_MS` | 15000 | greater than probe interval, at most 120000 |

Each probe performs up to three requests and uses one dedicated worker, in addition to the shared 16 archive/replay operations. The next probe is scheduled after the prior attempt finishes; there is no probe queue. Choose a stale timeout above the interval plus normal probe duration; the defaults cover the 2-second S3 request timeout. A shorter timeout intentionally reports slow storage unready. Test/development examples use faster probes to exercise failure transitions.

## Shutdown and upgrades

On SIGTERM, the service latches drain before serializing the registry barrier. New upgrades/publishes/subscribes stop; already enqueued admissions are included. Every original owner rejects later appends, flushes pending batches, retries frozen commits within the deadline, and conditionally commits a **closed head** only after its last accepted cursor is confirmed archived. Saturated mailbox barriers retry rather than silently omitting an owner. A dead/fenced owner cannot count as successfully drained. Sockets remain alive briefly for the final confirmed `persisted` notices, then receive a retryable unavailable error and close code 1012.

A confirmed drain exits 0. An expired deadline exits 1 and leaves unconfirmed data uncertain; it never turns a PUT alone into `persisted` or claims the pending tail was saved. The deadline bounds archival work; an in-progress registry RPC can add up to 100 ms, followed by two socket grace intervals of `tick_ms + 100` and bounded OTP shutdown. Set orchestrator/systemd stop grace above drain plus socket grace and at least ten extra seconds (45 seconds for defaults). SIGKILL, host loss, and non-graceful interruption still lose unpersisted memory. Memory-mode shutdown is ephemeral even if its barrier completes.

Upgrade by sending SIGTERM, waiting for the old process/container to exit, replacing the package, then starting its successor. Do not start the successor while the old process is running, including when readiness turns 503. Investigate nonzero drain outcomes and let clients reconcile the uncertain tail after restart. Recovery still allocates a fresh epoch and conservatively emits the existing interruption frame, even after a clean close; no exact lost count is inferred.

**Migration:** heads are now written as storage format v2 with `status: open|closed`; existing v1 open heads remain readable. Segments/indexes and WebSocket protocol remain v1. Pre-milestone-6 binaries reject v2 heads, so rollback requires a compatible reader; do not rewrite metadata by hand. A close uses its own frozen commit ID/ETag and reconciles ambiguous responses. It cannot overwrite a newer recovered epoch.

## Verification and sizing

```sh
make check          # Gleam contracts + JavaScript unit tests
make sdk-check      # Locked development TypeScript compiler + SDK tests
make smoke          # Memory/JWT/Mist network behavior
make smoke-s3       # S3 crash, archive/replay and commit races
make smoke-release  # Native shipment, SDK, SIGTERM, restart, limits and benchmark
make smoke-container # Non-root read-only runtime and SIGTERM
make proxy-check    # Caddy route isolation (no public certificate issuance)
```

`make benchmark` runs the release scenario and writes `build/benchmark.json`. See [benchmarks.md](benchmarks.md) for a measured local sample, test configuration, and what it does not establish. Default connection/stream/memory settings remain conservative configurable bounds, not measured production throughput promises. Deep reverse-linked replay still has explicit page/time limits; checkpoint indexing, tenant rate policies, long-running scale/load validation, retention, and distributed ownership remain future work. The Node/browser SDK is locally packaged and tested, not published to npm.

References: [Gleam official images](https://github.com/gleam-lang/gleam/pkgs/container/gleam), [Caddy reverse proxy](https://caddyserver.com/docs/caddyfile/directives/reverse_proxy), [S3 GetObject missing-key permissions](https://docs.aws.amazon.com/AmazonS3/latest/API/API_GetObject.html), and [S3 conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html).
