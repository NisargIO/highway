# Decision 002: S3 archival and recovery (storage format v1)

Historical milestone 4 record. [Decision 004](004-production-operations.md) extends heads to v2 open/closed status, adds drain and active readiness; v1 segment/index formats and archival guarantees remain.

Milestone 4 adds explicitly configured `s3_async` alongside the development memory adapter. Protocol version remains 1. This extends Decision 001; its memory guarantees still apply. WebSocket scrollback and the archive/live replay join remain milestone 5.

## Publication and recovery

A stream owner starts a linked storage worker and returns retryable `unavailable` while initializing. The worker reads the authoritative head and conditionally allocates a new epoch before admission. A new stream uses `If-None-Match: *`. An existing stream keeps its incarnation, writes an immutable interrupted-epoch index entry, and publishes that root together with `epoch + 1` using the head's prior ETag. Recovered offsets start at 1. Startup subscribes can load existing streams; a missing stream is not created by a subscribe. Every unclosed epoch is conservatively interrupted, even if it had no actual loss. Graceful durable closure is future shutdown work.

The interruption stores `after` as the last archived cursor, `before_epoch` as the new epoch, `reason` via kind `interrupted_epoch`, and `lost_count: null`. It never invents the highest lost live offset. Multiple restarts retain linked boundaries, including empty epochs. A failed epoch head update leaves an unreachable boundary object and does not permit admission. Initialization PUT timeouts are reconciled; an unconfirmed attempt can leave an extra conservative empty epoch on retry, but cannot reuse an accepted cursor.

Ordinary append and live pull never do network I/O. One worker operation per owner freezes a contiguous batch, an immutable segment key/body, an immutable index key/body, and the expected base head/ETag. Flush starts at 1 MiB of pending admission bytes or on the next 250 ms timer, with at most 4096 records per batch. A lone event flushes too. There is one committing batch per stream, with up to 16 active storage worker operations per registry/node. A worker failure kills its linked owner; subscribers close on the old reference. Explicit owner shutdown kills its writer. No pending tail transfers between owners.

Commit order:

1. PUT the immutable segment with `If-None-Match: *`.
2. PUT the immutable linked index with `If-None-Match: *`.
3. PUT `head.json` with `If-Match: <prior opaque ETag>`, referencing that index and advancing the archived cursor.
4. Only after a successful authoritative head PUT, or a GET confirming the exact proposed head after a failed/ambiguous PUT, advance confirmed owner progress and release pending capacity.

A segment/index PUT alone is never persistence. A failed immutable PUT is reconciled by GET: only identical frozen bytes make retry safe. Head PUT errors also trigger GET. The exact proposed head confirms success; an unchanged base permits a bounded retry of the same plan; another head fences the owner. A failed reconciliation stays unconfirmed. No unconditional head overwrite, bucket listing, or S3 notification determines publication. A lost publisher/worker response may leave durable data without a notice; it does not undo the commit. One frozen batch is retried after a 1-second delay. Conflicts/precondition failures or corrupt storage fence admission and live reads until restart; accepted data is not discarded to make room.

Storage completions include epoch and batch identity. Owners ignore mismatches, and reply subjects also bind completions to their original process. A delayed old-epoch request cannot replace a newer head with its stale ETag. This is conditional storage coordination, **not** a distributed live lease: one server per prefix and stop-then-start deployment remain mandatory. No transparent takeover or exactly-once delivery is introduced.

## Layout and codecs

```text
<prefix>/v1/tenants/<UTF-8 tenant bytes as lowercase hex>/streams/<stream bytes as lowercase hex>/
  head.json
  segments/<decimal epoch>/<random batch ID>.json
  indexes/<random index ID>.json
```

The JSON segment envelope replaces the proposed JSONL sketch. Every head, segment, and index has `version: 1`. Cursor/epoch counters are canonical decimal strings, preserving values beyond JavaScript's safe integer range. Head epoch decimals are bounded to 64 digits; generated object paths fit S3's 1024-byte key limit with 200-byte tenant/stream IDs and a prefix of at most 64 bytes. Prefix components contain lowercase letters, digits, dots, or hyphens without traversal/empty components.

Head fields: `version`, `incarnation`, `epoch`, `status: open`, nullable `index`, `archived`, `commit_id`. A null index is valid only for an initial empty epoch. The index root is the sole publication point. Each bounded linked index page has `previous` and one typed `entry`: either segment key/SHA256/first/last/count, or an interrupted-epoch boundary. Segment envelopes contain ordered cursor/message ID/data records. SHA256 covers the full encoded segment bytes; ETags remain opaque CAS tokens. Explicit decoders reject unknown versions, invalid cursor relationships, noncontiguous record bounds, checksum failures, and cross-stream object references. Recovery validates the referenced tip and its segment, not an unbounded traversal of all history. The bounded published-tip reader follows only the committed root. Full chain traversal belongs to replay in milestone 5.

## Memory, progress, and readiness

Pending events retain the existing per-stream byte, record-count, node byte, and oldest-pending-age limits. Append bytes are reserved before enqueueing, and remain charged if the synchronous caller times out; only the receiving owner transfers/removes the reservation after updating its pending state. The shared account snapshot combines pending and queued bytes atomically, so a stalled mailbox cannot bypass the node cap. Synchronous owner/registry calls use OTP reply aliases and always remove monitors on completion or failure; timed-out late replies are dropped instead of reaching actor unexpected-message logs. Registry accounting reads this bounded shared owner metadata; successful head commits release node/stream pending capacity. Rejection occurs before accepting another event. Storage outages preserve accepted data within those caps, then return `storage_backlog`. New owners cannot allocate epochs during an outage. Memory mode never advances persisted progress.

An S3 owner retains the most recently committed batch as an additional bounded live cache (at most 1 MiB of logical event bytes, plus pending data). It is distinct from pending admission accounting; encoded bytes are not a BEAM RSS measurement. A latest subscriber that falls behind cache eviction receives `slow_consumer` instead of a silent skip. Reading older committed backlog from S3 for that subscription is milestone 5. Idle owners are not evicted in this milestone.

Connections poll confirmed progress for authorized active publishers and subscriptions. Notices are cumulative within the owner epoch and coalesced; `accepted` and `persisted` remain separate frames. Refresh rechecks publish/subscribe grants and removes unauthorized watches immediately. Progress watches are bounded by the connection subscription limit, including publisher-only watches. A socket can receive its first notice at an already committed cursor after subscribing; this is storage progress, not a statement that an event was delivered on that subscription.

Hello advertises `storage: s3_async` and `durable: false` because live acceptance is still non-durable. Memory readiness remains 503. S3 readiness stays 503 until at least one owner has confirmed storage initialization, then requires healthy owner/admission snapshots; retries, fenced owners, age/capacity exhaustion, or unconfirmed initialization degrade it. It is not an ongoing bucket health probe while streams are idle. Missing-stream lookups do not make otherwise healthy owners unready and are cleaned up on subsequent registry work. Production readiness policy and metrics remain milestone 6.

## S3 transport and conformance

Use maintained [aws_signature 0.4.3](https://hex.pm/packages/aws_signature/0.4.3), inspected locally, for SigV4. A small Erlang boundary delegates HTTP/TLS to OTP `httpc` and hashing to OTP crypto. It preserves conditional headers, signs session tokens, uses path-style bucket requests, validates TLS peers/hostnames, disables redirects, and enforces a configurable 100–10000 ms request timeout (default 2000). OTP 29 request options bound response bodies to 2 MiB and headers to 16 KiB before full accumulation. Request receives run in a linked short-lived process so late HTTP replies cannot enter actor logging. Errors retain safe operation causes without request credentials, tokens, message bodies, or response bodies.

Configuration requires an existing private general-purpose S3 bucket, explicit region, bucket, nonempty prefix, environment access key/secret, and optional `AWS_SESSION_TOKEN`. Credentials are supplied by the operator; no custom credential discovery/refresh chain is implemented. Temporary credentials must remain valid for the process lifetime or the service must be restarted with replacements. Default endpoint is `https://s3.<region>.amazonaws.com`; custom endpoints require HTTPS, except loopback HTTP for tests. A bucket policy must allow the required conditional `GetObject`/`PutObject` operations and missing-key discovery (normally prefix-scoped `ListBucket` permission). Do not grant application JWT holders direct bucket access. Overlapping servers, lifecycle deletion of referenced objects, and directory buckets are unsupported.

`make smoke-s3` runs a loopback Python contract fixture through the **real** SigV4/HTTP adapter and Mist WebSockets. It verifies conditional headers, session-token propagation, opaque ETags, races, live delivery during paused head commits, ambiguous responses, full server crash/restart, stored boundaries, and stale writer fencing. The fixture does not cryptographically validate SigV4 and is not evidence of AWS compatibility by itself. Unit tests use an atomic in-memory object-store model with deterministic failure injection; neither requires AWS.

`HIGHWAY_S3_CONFORMANCE=1 make s3-conformance` is a separate opt-in check against the configured endpoint/bucket. It writes only below a fresh `<prefix>/conformance/<random>` path, checks read visibility, conditional create/update, stale ETag rejection, and competing immutable creation. It leaves test objects in place and never deletes production objects. Run it against AWS before release. It has not been run against real AWS in this implementation session; production validation remains outstanding. Docker was unavailable, so no third-party S3 emulator was claimed tested.

Sources: [AWS conditional writes](https://docs.aws.amazon.com/AmazonS3/latest/userguide/conditional-writes.html), [AWS Signature v4](https://docs.aws.amazon.com/AmazonS3/latest/API/sig-v4-header-based-auth.html), and the installed OTP 29 `httpc` source/documentation for request limits and TLS options.
