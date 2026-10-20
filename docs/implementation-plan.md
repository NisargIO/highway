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

- [ ] Add typed tenant/stream IDs, capability actions, events, cursors, and protocol error types.
- [ ] Freeze canonical cursor encoding and validation with fixtures, including counters beyond JavaScript's safe integer range.
- [ ] Implement explicit JSON command decoders and response encoders.
- [ ] Implement a pure stream transition model for sequence assignment, admission capacity, persistence progress, and interrupted epochs.
- [ ] Introduce runtime configuration with validated limits, bind address, port, and explicit storage mode.

Exit criteria: malformed commands, cross-stream cursors, unsupported versions, ordering, capacity rejection before acceptance, and restart cursor uniqueness have deterministic tests. No network or AWS account is needed.

## Milestone 2 Authenticated WebSocket transport

- [ ] Select and document a maintained JWT/JOSE implementation after verifying required algorithm and key support.
- [ ] Implement trusted issuer/audience/key configuration and typed verification.
- [ ] Add Mist WebSocket upgrade, hello/auth state machine, auth deadline, and frame limits.
- [ ] Authorize exact tenant/stream/action grants for every operation.
- [ ] Enforce expiration on idle connections and refresh capability removal.
- [ ] Add Origin policy and redact sensitive logs.

Exit criteria: real signed fixtures and connection tests cover valid access, wrong stream/tenant, missing history permission, expired token, invalid signature, unknown key, algorithm confusion, refresh removal, and unauthenticated deadline. Keep network exposure loopback by default.

## Milestone 3 Live stream ownership

- [ ] Add supervised on-demand stream registry and owners with duplicate-start protection.
- [ ] Implement publish acceptance and ordered live fanout.
- [ ] Multiplex subscriptions and publishers on a connection.
- [ ] Implement credit, byte accounting, admission limits, cleanup, and bounded cross-process outstanding work.
- [ ] Handle owner failure by closing affected subscriptions and discarding stale completions.
- [ ] Add an explicit development-only memory storage adapter and minimal client smoke example.

Exit criteria: two authenticated clients can publish and subscribe in order; forbidden operations fail; a slow client does not grow memory without bound or stall other subscribers. Memory mode advertises itself as ephemeral, never emits `persisted`, and cannot claim S3 readiness. Its incarnation changes on restart; it must reject old cursors rather than pretend to resume.

Milestones 1 through 3 are the first useful local demonstration. No fake JWT verifier or production fallback to memory is permitted.

## Milestone 4 S3 archival and recovery

- [ ] Define storage operations for head reads, immutable writes, conditional head commits, and object reads.
- [ ] Select an S3 client or small typed binding with SigV4, session credentials, custom endpoint support, timeouts, and conditional headers.
- [ ] Add a local S3 test service and separate opt-in AWS S3 conformance tests.
- [ ] Durably allocate owner epochs before accepting in S3 mode.
- [ ] Implement bounded batch flush by bytes and timer, immutable segments/index pages, and authoritative head commits.
- [ ] Emit cumulative persisted progress only after publication through the head.
- [ ] Reconcile ambiguous timeouts, detect ETag conflicts, and recover interrupted epochs.
- [ ] Reject admission on archive backlog and keep accepted data pending.

Exit criteria: inject failures after data upload, index upload, head update, and before response. Only referenced objects replay. Crash and restart preserves persisted history, never reuses cursors, and reports an uncertain lost tail. An S3 outage respects memory limits. Unit tests require no live AWS; AWS checks validate actual precondition behavior before release.

## Milestone 5 Scrollback and live continuation

- [ ] Implement earliest/after/latest, finite replay, and exclusive resume semantics.
- [ ] Read bounded index pages and segments with checksum/version validation.
- [ ] Capture H and register follow atomically at the owner.
- [ ] Join S3 archive, retained memory suffix, and post-H follow with cursor deduplication.
- [ ] Handle persisted-boundary movement and interrupted epochs during replay.
- [ ] Enforce history authorization and token expiry throughout long replays.

Exit criteria: publish continuously during replay and assert ordered output with no skips at the archive/live boundary. Exercise a resume cursor in a crash-lost tail, several interrupted epochs, owner failure mid-replay, slow readers, and frame/byte limits. Durable replay can repeat across reconnect; clients reconcile by cursor.

## Milestone 6 Deployment and operational limits

- [ ] Add production release/container packaging, explicit secrets configuration, TLS/proxy instructions, and private S3 IAM setup.
- [ ] Add readiness based on storage/admission state, graceful drain, metrics, and structured redacted logs.
- [ ] Provide a minimal browser/client SDK example with auth refresh, cursor resume, and accepted/persisted distinction.
- [ ] Benchmark live latency, accepted-to-persisted lag, replay speed, memory at limits, and S3 request volume.
- [ ] Test singleton stop-then-start upgrades and document the unsupported overlap/failover case.

Exit criteria: a documented single-node deployment passes an end-to-end publish, live receive, archive, restart, and replay scenario. Publish measured limits rather than guessed throughput targets. No retention deletion or automatic multi-node ownership in this release.

## Decisions to validate during implementation

These do not block the bootstrap or first local milestones:

1. The exact JWT/JOSE and S3 libraries, based on API and maintenance checks.
2. Expected active streams, clients, payload sizes, history length, and latency budget.
3. Batch interval and memory defaults, based on workload measurements.
4. Whether a later optional disk WAL or durable-before-live mode is worth its operational and latency costs.
5. Index checkpoint strategy if historical traversal becomes expensive.

## Bootstrap verification

Local formatting, type checks, and three bootstrap contract tests pass. The loopback server is smoke-tested during setup. Dependencies currently emit deprecation warnings about the upstream HTTP Header type when compiling from scratch; the bootstrap compiles successfully. CI result and GitHub repository are reported in the setup handoff.
