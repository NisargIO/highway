# Decision 001: bounded development live slice

Historical milestone 1–3 decision; [Decision 002](002-s3-archival.md) extends this with S3 while preserving memory-mode semantics.

Milestones 1–3 implement the live path before S3. This decision versions the memory adapter's behavior under protocol v1; it does not change the future S3 commit contract.

## Storage and cursors

`HIGHWAY_STORAGE_MODE=development_memory` is mandatory. Bind only to `127.0.0.1`. Unknown modes fail closed. Hello advertises `development_memory` and `durable: false`; readiness remains 503. There is no `persisted` response constructor in this slice.

Canonical cursors are `v1:<incarnation>:<epoch>:<offset>`. Incarnation uses 1–128 ASCII letters, digits, underscores, or hyphens; generated memory incarnations are 128-bit OTP random values encoded as 32 lowercase hexadecimal digits. Epoch and offset use canonical decimal strings without leading zeroes; epoch is positive and offset nonnegative. BEAM integers preserve counters beyond JavaScript's safe range. Every memory owner receives a new incarnation and epoch 1, including after an owner crash. Cursors are validated against that owner's incarnation and epoch. Memory never synthesizes an interrupted-epoch history boundary or resumes a prior owner. S3 will use durable incarnations and allocated epochs in milestone 4.

Accepted events are retained with bounded logical encoded-byte accounting (payload, message ID, and 256 bytes of event overhead) and a separate record-count cap. No accepted event is evicted to admit another. Limits stop writes at 8 MiB/65,536 records per stream, 256 MiB per node, or 30 seconds after the stream's first event. These are admission accounts, not a claim that BEAM RSS equals encoded bytes. No archival worker drains the tail; restart to reset the demonstration. Idle owners are not evicted. Dead owners are reaped on subsequent registry operations and lose their tails.

Latest subscriptions capture a watermark and pull after it. Earliest/after authorization is implemented, but replay returns `unavailable`; old/wrong-incarnation cursors return `cursor_invalid`, and future offsets return `cursor_ahead`. Implement replay only with the milestone 5 handoff rules.

## Supervision and bounds

A static one-for-all supervisor owns the registry and Mist listener. The registry owns a temporary-child factory supervisor and is the only owner starter. Temporary owner children do not silently restart under old subscription references. The next publish creates a fresh owner. Connections detect dead owners on their next bounded polling turn, cancel affected subscriptions, and close with 1011. Registry failure restarts the listener and owner subtree, terminating connections.

Each connection makes at most one synchronous stream/registry call at a time. Additionally, each actor has 128 nonblocking ETS admission permits, acquired **before enqueueing** and released by the receiver **when dequeued**. A timed-out or dead caller does not release its queued request's permit. Failures can conservatively consume capacity until dequeue/restart. Permits carry unique references so late releases cannot release a reused slot. No event payloads are pushed into subscriber mailboxes. Factory start requests come only from the serialized registry. The connection slot table caps authenticated and unauthenticated active handlers; close releases slots, and a later acquisition reclaims slots from dead handlers.

Connections poll owners every 20 ms, with at most 16 events per turn and rotating subscription priority. This is live delivery without an archival dependency, not a latency SLA. One outstanding timer per connection enforces auth, expiry, idle, and slow-credit deadlines. Subscriptions store only cursors and bounded delivery metadata. Their undispatched backlog remains in the shared, capped owner tail. The aggregate unacknowledged encoded-event bytes per connection are capped at 1 MiB. Exceeding bytes or 5 seconds without cumulative ack progress terminates the subscription with `slow_consumer`. Per-event delivery timestamps ensure partial acknowledgments cannot extend an old event indefinitely. Controls do not require event credit and use bounded, synchronous response lists.

Socket send buffering uses a 64 KiB send buffer/high watermark and a 100 ms send timeout with close on timeout. Mist's active-once input limits ingress mailboxes; the small checked patch removes redundant ping rearming and rejects an accumulated raw WebSocket buffer above 256 KiB **before decoding**. Compression is disabled. This conservative raw-buffer bound includes transport framing/coalesced input; a sufficiently large coalesced burst may be rejected even if its individual messages are smaller. The JSON-frame cap is also 256 KiB; auth frames are 16 KiB, payloads 64 KiB. No generated dependency checkout is committed. `make setup` applies the patch and verifies exact original/patched source hashes; dependency upgrades require reviewing the patch. Direct clean `gleam run` bypassing preparation is unsupported.

## Authentication

Use [Erlang JOSE 1.11.12](https://hex.pm/packages/jose/1.11.12) with [`verify_strict`](https://github.com/potatosalad/erlang-jose/blob/1.11.12/src/jwt/jose_jwt.erl). The small Erlang boundary delegates all signature work to JOSE, accepts only public Ed25519 JWKs, and returns payload JSON for typed Gleam claim decoding. The local dependency source and real Node-signed integration fixtures validate EdDSA/Ed25519 support on OTP 29. No handwritten cryptography is used. OTP random bytes only generate opaque memory identifiers.

Issuer, audience, keys, and algorithm come from trusted configuration. Headers cannot select remote keys or extensions (`crit`, `b64`, embedded keys, and key URLs are rejected). Require exact grants, tenant, nonempty subject, integer `iat`/`exp`, and valid `nbf` when supplied. Audience may be a string or list. Maximum age and lifetime are 3600 seconds; future `iat`/`nbf` tolerate 5 seconds of skew. Expiry itself is strict with no grace. Refresh cannot change subject or tenant. A rejected refresh retains only the previous token's original lifetime; removing a subscribe grant drops that subscription immediately.

No tokens, private keys, message payloads, or untrusted exception values are logged. Actor call failures log safe context and exception class and return `unavailable`; socket errors retain safe OS causes. Node-based smoke signing keeps private keys in process memory. Production key rotation, rate policies, TLS, and packaging remain later work.

JOSE's locked release treats Erlang warnings as errors and uses old `catch` syntax deprecated by OTP 29. The Makefile exports `ERL_COMPILER_OPTIONS=[nowarn_deprecated_catch]` for reproducible dependency builds; this does not disable project warnings or checks.

## Alternatives and migration

Unbounded broadcast mailboxes and per-subscriber backlog copies were rejected because list-size limits alone cannot bound cross-process queues. A fake JWT verifier or automatic production fallback to memory would obscure guarantees. No WAL, database, or broker is introduced.

Milestone 4 replaces the development adapter with explicitly configured S3 archival and durable epoch allocation. It must publish data only through the authoritative head and emit `persisted` only after that commit. Memory cursors are not migrated into durable history. Protocol v1 clients must continue treating cursors as opaque and `accepted` as non-durable.
