# Decision 003: bounded authorized replay and live continuation

Status: implemented for milestone 5. Extends protocol v1's already specified subscription semantics and adds the explicit `replay_limit` error. Storage format v1 and existing cursors are unchanged. Decisions 001 and 002 describe earlier milestone behavior; this decision supersedes their replay-unavailable/cache-eviction limitations.

## Capture and join

The owner serializes a view registration and capture of live H in one mailbox operation. A view contains a unique ID, immutable exclusive floor, H, and whether it needs History. Latest starts at H and requires Subscribe; earliest starts at incarnation epoch 1 offset 0; after starts exclusively at its cursor and requires Subscribe plus History throughout its lifetime. The registered floor prevents a latest reader from accessing earlier data after it moves into S3. A finite latest request completes immediately. Views attach to the exact owner process; failure forces reconnect, never silent replacement.

Socket ticks pull up to 16 items per turn. Pending memory and the last committed cache serve the fast path, so live events never wait for archival. When a needed record has left the cache, it must already have been published through the head; an asynchronous reader seeks committed history. Pending records remain retained until head confirmation. After each returned archive batch, the socket rechecks memory and starts a fresh head seek if necessary. Thus movement between pending, cache, and S3 cannot turn a missing range into end-of-stream. A traversal pins immutable links while head commits continue. Orphan segments/indexes are never scanned or listed.

Before sending, the socket requires exactly the next offset in its current epoch or an explicit boundary to the next epoch at logical offset zero. It clips initial replay to H, emits `caught_up` once at H, and then follows later records on the same subscription. `follow: false` emits `complete` at H, including the captured pending suffix; neither control frame promises persistence. Unknown holes and storage failures terminate explicitly, without a caught-up notice. Reconnecting may repeat records; no exactly-once delivery is introduced.

## Authoritative reader and interrupted epochs

Each scan starts from a decoded authoritative head, then traverses its bounded reverse-linked index pages. It holds only a checkpoint and one earliest eligible candidate entry. Strictly decreasing record/epoch bounds, archive anchors, and predecessor continuity detect missing prefixes, invalid links, and cycles without an unbounded visited set. Segment reads verify version, checksum, count, and contiguous cursor bounds. Existing path validation binds every object to the tenant/stream scope.

An interruption is ordered at `(incarnation, before_epoch, 0)`, independently of its `after` field. A resume cursor in a lost tail can exceed `after`; it still receives the boundary and continues in the next epoch. Consecutive empty interrupted epochs can repeat the same archived cursor and still produce distinct ordered boundaries. `lost_count` remains null. Cumulative event ACKs compare full epoch/offset positions, so acknowledging a high old-epoch offset never clears lower-offset events from a later epoch. Boundary frames consume no event credit and cannot be ACKed themselves.

## Authorization, supervision, and cancellation

Every subscribe checks exact tenant/stream grants before existence lookup. Each worker checks the view, owner, connection, Subscribe, and (for historical views) History before and after every storage GET, including segment reads. The socket rechecks its current claims before retaining a result and before delivery. Expiry uses elapsed real time during blocked I/O as well as the connection timer. Refresh verifies the new identity, cancels all existing jobs, discards buffered/partial scans without advancing the sent cursor, and cancels views whose required grants disappeared.

Replay uses temporary supervised one-step actors. Each job has a random generation; late replies only apply to the still-active matching generation. A linked guardian monitors its job, connection, and owner; losing either dependency cancels even blocked I/O. Unsubscribe, completion, refresh, expiry, and socket cleanup cancel jobs and release owner views. View release is a bounded asynchronous owner request; if admission is saturated, safe context is logged and dead-connection leases are reaped on the owner timer. There are at most 1024 view leases per owner, so failed cleanup cannot create an unbounded dictionary. Owner RPCs retain the reply-alias/permit boundary from Decision 002.

## Bounds and costs

Default limits, configurable via `HIGHWAY_LIMITS_FILE`:

| Setting | Default | Maximum |
| --- | --- | --- |
| `replay_workers` per node | 8 | 16 |
| `replay_pages_per_step` | 8 | 64 |
| `replay_max_pages` per seek | 16,384 | 1,000,000 |
| `replay_timeout_ms` | 30,000 | 300,000 |

Only one job is in flight per connection. It reserves at most 128 KiB and returns at most 16 items, additionally limited by available event credit. Reservations, buffered encoded frames, and unacknowledged event bytes share the connection output budget (default 1 MiB). A result cannot exceed its reservation. Read steps share the node's 16-operation storage cap with archival; native response/header limits remain 2 MiB/16 KiB. Saturation defers a job for 100 ms; no cursor advances. Socket auth/cancellation continues while I/O runs. A 5-second unacknowledged-event timeout still applies. Initial replay includes credit pauses in its time limit; each archive scan also has an elapsed bound. Exhausted page/time bounds emit `replay_limit`; oversized/slow output emits `slow_consumer`; storage/corruption failure emits `unavailable`. None silently truncates history.

A reverse-linked scan is linear per seek. Returning a small bounded batch can require rescanning later pages, so full-history replay can have quadratic request cost across many small segments. This bounded first implementation favors correctness and a small retained checkpoint over a full manifest or per-subscriber history copy. Checkpoint/range indexes and measured production sizing remain future work; no throughput or cheap arbitrarily deep replay is promised. Raising limits is an operator choice, not evidence of measured capacity.

## Validation and migration

Unit tests exercise authoritative roots/orphans, moving commit boundaries, checksum/continuity rules, page limits, multiple/empty epochs, lost-tail resume, finite pending snapshots, cumulative ACK epoch resets, restricted latest floors, grant refresh/expiry during blocked I/O, worker capacity, stale generations, cancellation, owner/connection death, storage errors, output limits, and negative monotonic clocks. `make smoke-s3` uses real HTTP/SigV4 and Mist clients through the local fixture, crashes/restarts the service, then replays with one-event credit while publishers advance the head and evict caches. It checks exact H placement, no missing/duplicate events, finite exclusive lost-tail resume, and subscribe-only continuation.

No stored-object migration is needed. Clients must handle the protocol v1 discontinuity frame and `replay_limit`. Deployment remains one server per prefix with stop-then-start updates. AWS conformance is still opt-in and unexecuted against real AWS here; the fixture does not cryptographically verify SigV4. Memory mode remains ephemeral and never emits persisted. Production TLS, packaging, clean shutdown/drain, metrics, and benchmarks remain future work.
