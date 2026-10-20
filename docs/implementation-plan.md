# Highway implementation plan

Build a single-node Gleam service in stages. Preserve immediate live delivery, asynchronous S3 scrollback, and per-stream JWT authorization throughout. The architecture and wire protocol are proposed contracts; implementation should refine details with focused tests and record any material changes.

## Milestone 0 Repository bootstrap

- [x] Initialize the Gleam application targeting Erlang.
- [x] Resolve Mist, OTP, HTTP, JSON, logging, and gleeunit dependencies; commit the lockfile.
- [x] Add toolchain versions, formatting rules, and local commands.
- [x] Add a runnable loopback HTTP server with liveness and deliberately unavailable readiness/stream routes.
- [x] Add bootstrap contract tests and GitHub Actions checks.
- [x] Document architecture, protocol, contribution rules, and an agent handoff.

Exit criteria: `make check` passes; running `make run` serves `/healthz`; unavailable features are identified as planned. GitHub linkage and remote CI are verified during initial setup.

## Milestone 1 Domain and protocol

- [x] Add typed tenant/stream IDs, capability actions, events, cursors, and protocol error types.
- [x] Freeze canonical cursor encoding and validation with fixtures, including counters beyond JavaScript's safe integer range.
- [x] Implement explicit JSON command decoders and response encoders.
- [x] Implement a pure stream transition model for sequence assignment, admission capacity, persistence progress, and memory restart discontinuities (durable interrupted epochs implemented in milestone 4).
- [x] Introduce runtime configuration with validated limits, bind address, port, and explicit storage mode.

Exit criteria: malformed commands, cross-stream cursors, unsupported versions, ordering, capacity rejection before acceptance, and restart cursor uniqueness have deterministic tests. No network or AWS account is needed.

## Milestone 2 Authenticated WebSocket transport

- [x] Select and document a maintained JWT/JOSE implementation after verifying required algorithm and key support.
- [x] Implement trusted issuer/audience/key configuration and typed verification.
- [x] Add Mist WebSocket upgrade, hello/auth state machine, auth deadline, and frame limits.
- [x] Authorize exact tenant/stream/action grants for every operation.
- [x] Enforce expiration on idle connections and refresh capability removal.
- [x] Add Origin policy and redact sensitive logs.

Exit criteria: real signed fixtures and connection tests cover valid access, wrong stream/tenant, missing history permission, expired token, invalid signature, unknown key, algorithm confusion, refresh removal, and unauthenticated deadline. Keep network exposure loopback by default.

## Milestone 3 Live stream ownership

- [x] Add supervised on-demand stream registry and owners with duplicate-start protection.
- [x] Implement publish acceptance and ordered live fanout.
- [x] Multiplex subscriptions and publishers on a connection.
- [x] Implement credit, byte accounting, admission limits, cleanup, and bounded cross-process outstanding work.
- [x] Handle owner failure by closing affected subscriptions and rejecting stale owner/cursor references (storage completion validation added in milestone 4).
- [x] Add an explicit development-only memory storage adapter and minimal client smoke example.

Exit criteria: two authenticated clients can publish and subscribe in order; forbidden operations fail; a slow client does not grow memory without bound or stall other subscribers. Memory mode advertises itself as ephemeral, never emits `persisted`, and cannot claim S3 readiness. Its incarnation changes on restart; it must reject old cursors rather than pretend to resume.

Milestones 1 through 3 are the first useful local demonstration. No fake JWT verifier or production fallback to memory is permitted.

## Milestone 4 S3 archival and recovery

- [x] Define storage operations for head reads, immutable writes, conditional head commits, and object reads.
- [x] Select an S3 client or small typed binding with SigV4, session credentials, custom endpoint support, timeouts, and conditional headers.
- [x] Add a local S3 test service and separate opt-in AWS S3 conformance tests.
- [x] Durably allocate owner epochs before accepting in S3 mode.
- [x] Implement bounded batch flush by bytes and timer, immutable segments/index pages, and authoritative head commits.
- [x] Emit cumulative persisted progress only after publication through the head.
- [x] Reconcile ambiguous timeouts, detect ETag conflicts, and recover interrupted epochs.
- [x] Reject admission on archive backlog and keep accepted data pending.

Exit criteria: inject failures after data upload, index upload, head update, and before response. Only objects reachable from the committed head are exposed by storage reads; WebSocket replay remains milestone 5. Crash and restart preserves persisted history, never reuses cursors, and reports an uncertain lost tail. An S3 outage respects memory limits. Unit tests require no live AWS; AWS checks validate actual precondition behavior before release.

## Milestone 5 Scrollback and live continuation

- [x] Implement earliest/after/latest, finite replay, and exclusive resume semantics.
- [x] Read bounded index pages and segments with checksum/version validation.
- [x] Capture H and register follow atomically at the owner.
- [x] Join S3 archive, retained memory suffix, and post-H follow with cursor deduplication.
- [x] Handle persisted-boundary movement and interrupted epochs during replay.
- [x] Enforce history authorization and token expiry throughout long replays.

Exit criteria: publish continuously during replay and assert ordered output with no skips at the archive/live boundary. Exercise a resume cursor in a crash-lost tail, several interrupted epochs, owner failure mid-replay, slow readers, and frame/byte limits. Durable replay can repeat across reconnect; clients reconcile by cursor.

## Milestone 6 Deployment and operational limits

- [x] Add production release/container packaging, explicit secrets configuration, TLS/proxy instructions, and private S3 IAM setup.
- [x] Add readiness based on storage/admission state, graceful drain, metrics, and structured redacted logs.
- [x] Provide a minimal browser/client SDK example with auth refresh, cursor resume, and accepted/persisted distinction.
- [x] Benchmark live latency, accepted-to-persisted lag, replay speed, memory at limits, and S3 request volume.
- [x] Test singleton stop-then-start upgrades and document the unsupported overlap/failover case.

Exit criteria: a documented single-node deployment passes an end-to-end publish, live receive, archive, restart, and replay scenario. Publish measured limits rather than guessed throughput targets. No retention deletion or automatic multi-node ownership in this release.

## Decisions to validate during implementation

These do not block the bootstrap or first local milestones:

1. S3 library selection based on API and maintenance checks; JOSE 1.11.12 is selected for the live slice.
2. Expected active streams, clients, payload sizes, history length, and latency budget.
3. Batch interval and memory defaults, based on workload measurements.
4. Whether a later optional disk WAL or durable-before-live mode is worth its operational and latency costs.
5. Index checkpoint strategy if historical traversal becomes expensive.

## Milestones 1–3 verification and handoff

Milestones 1–3 are implemented in explicit `development_memory` mode. `make check` covers formatting, type checking, and 22 tests. `make smoke` starts a real loopback server with ephemeral Node-signed Ed25519 tokens and two publishing/subscribing clients; it checks ordered delivery, forbidden operations, tenant isolation, refresh removal, idle expiry/auth deadlines, slow consumers, and oversized input. Owner failure, concurrent startup/order, mailbox permits, admission bounds, canonical cursors, and JWT negative cases are covered locally. CI runs both commands and checks lockfile stability. An expected supervisor report is produced by the deliberate owner-kill test.

Decision 001 records the original cursor grammar, memory reset behavior, JOSE binding, bounded pull/permit design, and Mist parser patch. At that milestone memory had no replay or archive drain and never advanced persistence. Milestone 5 adds bounded memory scrollback; it still never advances persistence. Decision 002 extends the shared model and transport with S3 persistence and recovery. Interrupted memory owners keep their fresh-incarnation behavior. Production TLS, rate policies, packaging, benchmarks, and real AWS conformance remain unfinished.

## Milestone 4 verification and handoff

S3 archival/recovery is implemented in explicit `s3_async`. Decision 002 freezes storage format v1 and documents durable epoch allocation, immutable JSON segments/indexes, conditional head commits, ambiguous timeout reconciliation, conflict fencing, and bounded workers/cache. The memory adapter remains ephemeral and never emits persisted.

`make check` now covers 38 tests, including failed segment/index/head writes, lost head responses, failed reconciliation, repeated frozen-plan commits, interrupted/empty epochs, checksum/version/path validation, blocked archival with responsive live operations, capacity release only after head confirmation, stale completions, timed-out queued byte reservations, and persisted-watch authorization. Expected safe warning/supervisor reports come from intentional failure tests. `make smoke` verifies the memory path. `make smoke-s3` runs conformance and real HTTP/Mist clients through a local contract server, verifying live-before-head, delayed/lost commit responses, full process restart, durable cursor epochs/boundaries, and old-writer fencing. CI runs both smokes and unit checks.

The separate opt-in `s3-conformance` target is implemented for AWS conditional create/update, opaque ETags, read visibility, and concurrent immutable creation. It has not been run against real AWS here. The local fixture checks required SigV4/session metadata but does not verify signatures cryptographically. Docker was unavailable; no third-party emulator is claimed validated. Run AWS conformance before a production release.

At the milestone 4 boundary, WebSocket replay and archive-backed continuation were still pending. Milestone 5 implements both; its limits and validation are below. Production TLS/packaging, ongoing credential refresh, graceful archive drain/clean epoch closure, metrics, performance sizing, and refined readiness remain milestone 6. At that milestone the server bound only to loopback; milestone 6 adds explicit private wildcard binding. Exactly one server per storage prefix is required; CAS is not a distributed live ownership lease.

## Milestone 5 verification and handoff

Earliest/after/latest, finite snapshots, exclusive resume, and archive/live continuation are implemented. Decision 003 records atomic owner view capture, asynchronous supervised readers, authoritative reverse-index traversal, logical interruption ordering, grant checks throughout I/O, cancellation generations, and work/byte/time bounds. Memory scrollback is limited to the active incarnation. Live delivery and accepted/persisted semantics are unchanged.

Validation: `make check` covers 54 tests. New cases include pinned-root traversal with advancing heads/orphan uploads, bounded page steps and total work, storage errors/corrupt cycles, crash-lost resume positions, multiple empty epochs, finite pending snapshots, live publication/cache eviction during replay, cross-epoch cumulative ACKs, subscribe-only post-floor S3 continuation, history removal/same-grant refresh, stale replies/subscription ID reuse, blocked-I/O expiry, owner/connection death, worker capacity, oversized replay, and negative Erlang monotonic timestamps. Expected safe warning/supervisor reports come from deliberate failure tests. `make smoke` verifies the memory live path; extended `make smoke-s3` verifies real Mist/HTTP clients replaying after full process restart, credit pauses with publication/cache eviction, exact H ordering, finite lost-tail resume, and latest-only archive continuation. Both require no AWS credentials; the fixture limitation from milestone 4 remains.

At the milestone 5 boundary, remaining limitations included: reverse-linked history seeks can rescan pages for each batch and become expensive; explicit `replay_limit` bounds this work rather than silently truncating history. Initial replay has a configurable 30-second deadline including credit pauses. No retention, clean epoch closure, distributed ownership, or exactly-once claim. Production packaging/TLS, credential refresh, graceful drain, observability, performance measurements/checkpoint indexing, and refined readiness remain milestone 6 or later. Real AWS conformance is still outstanding. This was the milestone 5 boundary; milestone 6 is recorded below.


## Milestone 6 verification and handoff

Milestone 6 is implemented locally. Decision 004 records native/digest-pinned container packaging, private TLS/IAM/secrets examples, bounded SIGTERM barriers and authoritative final commits/closed heads, active write/read storage probing, fixed-cardinality metrics, structured redacted logs, and the shared Node.js/browser SDK. Heads write v2 open/closed status and read legacy v1 open heads; segment/index/wire formats remain v1. Graceful closure does not reuse epochs or replace conservative restart boundaries. The SDK is locally packaged with declarations and is not published to npm.

Validation: `make check` covers 64 Gleam tests and 13 SDK tests, including drain saturation/retries, pending and ambiguous-close commit boundaries, timeout without false persistence/closure, close fencing/legacy decode, stale blocked probes and empty-service readiness, admission capacity, secret-file conflicts/redaction, operational latch/activity cleanup, finite ACK/completion race receipts with count/position bounds, and SDK credit/reconnect/refresh/unknown publish outcomes. `make sdk-check` verifies TypeScript declarations with a typed consumer. `make smoke` and `make smoke-s3` retain live/JWT/crash/replay coverage. `make smoke-release` runs the native shipment with the actual SDK, mounted credentials, SIGTERM admission rejection, confirmed final notices, clean and failed drain exits, singleton restart/resume, finite replay, and a measured 64-record pending cap. `make smoke-container` builds and exercises the non-root read-only Linux runtime, public JWKS mount, JWT live streams, metrics, and SIGTERM. Local container validation uses Linux arm64 on OrbStack; CI is configured to run the container smoke on Linux amd64. Local samples and their bounds are documented in benchmarks.md; they do not establish production-scale throughput or AWS costs.

Release gates/remaining limits: real AWS conformance and actual deployment-specific IAM/TLS validation remain outstanding; the contract fixture is not a SigV4 verifier. AWS credentials are process-static; JWT refresh is supported. Exactly one server per prefix and stop-then-start upgrades remain required. Tenant rate policies, a maintained AWS credential provider chain, checkpoint/range indexing, long-running/high-cardinality load testing, retention deletion, clustering and failover are not implemented. Container images and the SDK are not published externally. No further milestone is implied without a new request. Keep committing/pushing coherent verified changes as requested.


## Local MinIO Compose follow-up

- [x] Add reproducible pinned MinIO/mc source builds, private bucket initialization, generated ignored local credentials/JWKS, and a persistent named volume.
- [x] Keep all published ports on host loopback and preserve Highway's HTTPS-or-loopback S3 policy through a shared network namespace.
- [x] Exercise real SigV4 and invalid-signature rejection, conditional-write conformance, ordered live delivery during a storage outage, readiness recovery, authoritative head progress, graceful drain, both-service restart and credit-bounded SDK replay.
- [x] Document local JWT/client setup, restart/reset behavior, unmaintained MinIO community source status and remaining AWS release gates; add the scenario to container CI.

Validation: `make check` retains 64 Gleam and 13 SDK tests; `make compose-smoke` builds and waits for the non-root/read-only packaged service against real MinIO, runs the Gleam conformance module inside the image, pauses MinIO while two SDK clients continue ordered live delivery without persistence, then confirms committed head progress and clean closure. It stops/restarts both processes using the named volume, checks the new epoch discontinuity, resumes the established SDK view without skipped events, and replays the exact archive with window 2. Local execution uses Linux arm64 on OrbStack; Linux amd64 is configured in CI. The stack is left running for development. Details are in [local-compose.md](local-compose.md).

The documented local JWT/Node publish example and MinIO console also pass; the example retries only explicit startup rejections for up to five seconds. Existing memory, fixture-S3, SDK declaration, native release, container and proxy checks pass. Whole-stack down/up recreates containers while preserving the volume and issuer files.

No delivery/storage format or milestone contract changes. The Python fixture remains for precise injected failures. MinIO now validates real signatures locally; AWS conformance, deployment-specific IAM/TLS, scale tests and previously listed production limitations remain outstanding. This local stack uses generated administrator credentials and archived upstream MinIO community source; it is not a production object-store recommendation. Credentials/signing keys and build output are ignored and never committed.
