# Highway WebSocket protocol

Protocol v1 implements authenticated publishing, earliest/after/latest replay and live continuation, and confirmed S3 persistence progress on `/v1/ws`. Select explicit `development_memory` or `s3_async`. S3 interrupted epochs produce ordered discontinuity frames during replay; memory scrollback lasts only for the current owner incarnation. Use UTF-8 JSON text frames with one command or server message per frame. Production connections use `wss://`; loopback development may use `ws://`.

## Connection and authentication

The server sends `hello` after upgrade. Within five seconds the client sends `auth` with a signed JWT. No publish, subscription, or history operation is permitted beforehand. Limit every authentication frame to 16 KiB, independently of application payload limits. Browser Origin must exactly match `HIGHWAY_ORIGIN` (default `http://localhost:3000`). Native clients without Origin are allowed on the enforced loopback bind. Reject query strings on the WebSocket route.

```json
{"type":"hello","version":1,"auth_timeout_ms":5000,"storage":"development_memory","durable":false}
```

```json
{"type":"auth","id":"a1","token":"<signed JWT>"}
```

```json
{"type":"authenticated","id":"a1","subject":"user_123","tenant":"team_123","expires_at":1792454400}
```

Token strings never go in query parameters or logs. Using a first frame works with the browser WebSocket constructor, which accepts a URL and subprotocols rather than arbitrary request headers. Send only safe authentication failure codes, then close with 1008. Expiration closes with 1008 as well; enforce expiry even when a connection receives no client frames.

An illustrative decoded JWT payload is:

```json
{
  "iss":"https://app.example.com",
  "aud":"highway",
  "sub":"user_123",
  "iat":1792450800,
  "exp":1792454400,
  "tenant":"team_123",
  "grants":[
    {"stream":"project_abc/events","actions":["publish","subscribe","history"]},
    {"stream":"project_xyz/events","actions":["subscribe"]}
  ]
}
```

These are example timestamps, not a reusable token. The configured issuer, audience, algorithm, and key set are trusted server configuration. Require exact stream matches and a finite action enum. Wildcards, regexes, path-prefix grants, and client-supplied key URLs are outside version one. One token belongs to one tenant.

Clients refresh using `auth` again before expiry. Verify the replacement fully before changing state. Keep subject and tenant fixed for a connection; reject a replacement that changes either. Replace grants and cancel subscriptions whose access disappeared. If verification fails, retain the previous valid token only until its original expiry; never extend it implicitly.

## Common fields

Commands use `type` and a client-generated `id`, unique among outstanding commands on a connection. Server replies echo `id`. Subscription IDs are client-selected and unique among active subscriptions; reject reuse. All stream identities are scoped to the authenticated tenant. Reject malformed frames, unknown command types, and unsupported versions with typed errors.

A publish `message_id` is an application-generated reconciliation key stored with the event. It is not a guaranteed idempotency key in version one: repeating it can append another event. Request IDs only correlate replies. A later deduplication feature must specify its scope, retention, and crash behavior explicitly.

Cursor strings are versioned opaque values. Canonical encoding is `v1:<incarnation>:<epoch>:<offset>`, with positive epoch and nonnegative offset in decimal without leading zeroes. Incarnations contain 1–128 ASCII letters, digits, underscores, or hyphens. Fixtures include counters beyond JavaScript’s safe integer range. The examples use `v1:s_abc:7:42`; memory mode generates a fresh 32-character random hexadecimal incarnation per owner and uses epoch 1. Clients must not parse cursors. Never use timestamps as ordering positions.

## Publish and durability

```json
{"type":"publish","id":"p1","stream":"project_abc/events","message_id":"msg_001","data":{"text":"hello"}}
```

```json
{"type":"accepted","id":"p1","stream":"project_abc/events","cursor":"v1:s_abc:7:42","message_id":"msg_001"}
```

The owner has accepted the event into bounded memory. It may already be visible to live subscribers. Acceptance does not promise persistence or delivery to every subscriber.

**Implemented S3 mode only:** after the segment, index, and authoritative head are committed (or the exact head is confirmed by reconciliation):

```json
{"type":"persisted","stream":"project_abc/events","through":"v1:s_abc:7:42"}
```

Persistence notices are cumulative per stream within an epoch and may cover several publishes. A publisher-only connection receives them too; watches are bounded by the configured subscription limit and require current publish/subscribe access. Memory mode never emits them. S3 hello advertises `storage: "s3_async"`, `durable: false`, since acceptance is still non-durable. Send them to active publishers and subscribers with access to that stream. Progress across epochs must be interpreted with the stored epoch boundaries, not by treating missing offsets as records. A lost response does not imply a lost commit. Clients can resolve durable state through authorized history reads.

If storage is unavailable after acceptance, keep the event pending and report degradation through status/metrics; do not turn the prior acceptance into a rejection. Reject subsequent publishes before admission once capacity is reached.

## Subscribe and scrollback

**Implemented in both modes:** earliest/after/latest, exclusive resume, finite snapshots, and live continuation. Earliest/after require history access throughout the subscription, including during storage reads and after token refresh. Memory reads use retained data from the current incarnation only. S3 reads use the authoritative index plus pending/cache; orphan uploads are never history. A foreign/old memory incarnation returns `cursor_invalid`; a future epoch/current offset above the live tail returns `cursor_ahead`. Old interrupted-epoch offsets above the archived tail are valid resume positions and produce a boundary. Owner initialization may return retryable `unavailable`; subscribe never creates a missing stream. Latest with `follow: false` returns `subscribed`, `caught_up`, and `complete` without events.


```json
{"type":"subscribe","id":"s1","subscription":"sub1","stream":"project_abc/events","from":{"after":"v1:s_abc:7:30"},"follow":true,"window":128}
```

```json
{"type":"subscribed","id":"s1","subscription":"sub1","high_watermark":"v1:s_abc:7:42"}
```

`from` is one of:

- `"latest"`: start after the owner's captured live high watermark; requires `subscribe`.
- `"earliest"`: replay from the beginning; requires `subscribe` and `history`.
- `{"after":"<cursor>"}`: replay strictly after that cursor; requires `subscribe` and `history`, even if it happens to be near the live tail.

Default `from` is `"latest"`; default `follow` is `true`. Version one uses explicit stream names, not wildcard subscriptions. A valid empty stream gets a boundary cursor at offset zero. Unknown streams are created on first authorized publish; a subscribe to an uncreated stream returns `stream_not_found` in version one.

For a latest-only subscription, the subscribe grant covers records accepted after its captured start watermark, even if the server later fetches those records from S3 for that same subscription. It does not grant access to earlier records or permit cursor-based resume after the subscription closes.

Events arrive in stream order:

```json
{"type":"event","subscription":"sub1","stream":"project_abc/events","cursor":"v1:s_abc:7:31","message_id":"msg_000","data":{"text":"earlier"}}
```

After reaching the captured watermark:

```json
{"type":"caught_up","subscription":"sub1","through":"v1:s_abc:7:42"}
```

For `follow: true`, continue with later live events. For `follow: false`, send `complete` and end the subscription after the captured snapshot. The snapshot includes its retained unpersisted live suffix; it is not a durability promise. No `caught_up` notice permits silent skipping of a missing tail.

A restart can create an uncertain boundary:

```json
{"type":"discontinuity","subscription":"sub1","after":"v1:s_abc:7:40","before_epoch":"8","reason":"interrupted_epoch","lost_count":null}
```

The boundary reports that records beyond the last archived cursor may have been lost. Its last archived cursor can precede a client's resume cursor. Send the relevant boundary even in that case, then continue into the next epoch. Do not invent an exact lost count. Multiple interrupted epochs produce ordered boundary notices.

Reject a cursor from another stream incarnation with `cursor_invalid`. Reject a future epoch or an offset above the current live tail with `cursor_ahead`. An offset beyond a closed interrupted epoch's archived tail can be valid as a former live position: return the discontinuity, not a permanent cursor error. No retention is enabled initially; future retention needs an explicit `cursor_expired` contract.

## Flow control and cancellation

The negotiated `window` is the maximum unacknowledged event count, capped by a byte limit. The client returns cumulative acknowledgments for delivered events:

```json
{"type":"ack","subscription":"sub1","through":"v1:s_abc:7:42"}
```

Validate that the acknowledged cursor was actually delivered on that subscription; ignore exact duplicate acknowledgments, reject future or backwards ones. Acknowledgment grants more send credit. It neither persists an application checkpoint nor makes an event durable. Control frames have separate bounded capacity so credit exhaustion cannot block auth, cancellation, or errors.

Slow consumers do not block a stream owner. Pause reads when credit ends, and terminate a subscription with `slow_consumer` if its byte/time limits are exceeded. Reconnect and replay may recover persisted backlog, but cannot guarantee recovery of a crash-lost live tail.

```json
{"type":"unsubscribe","id":"u1","subscription":"sub1"}
```

```json
{"type":"unsubscribed","id":"u1","subscription":"sub1"}
```

Mist handles transport ping/pong. The connection closes with an `unavailable` error and code 1008 after 60 seconds without an application command; transport pings do not reset this idle policy. There is no extra heartbeat task. A single lifecycle-bound timer checks auth deadline, strict token expiry, slow consumer timeout, and live availability every 20 ms. A missing owner closes the connection with 1011. Refresh and other commands do not extend an old token’s expiry.

## Errors and initial limits

```json
{"type":"error","id":"p2","code":"storage_backlog","retryable":true,"retry_after_ms":1000}
```

Error codes include `unauthenticated`, `token_expired`, `forbidden`, `invalid_frame`, `frame_too_large`, `unknown_command`, `stream_not_found`, `cursor_invalid`, `cursor_ahead`, `replay_limit`, `slow_consumer`, `rate_limited`, `storage_backlog`, `unavailable`, `unsupported_version`, and `internal`. Authentication/authorization errors must not reveal another tenant's stream existence or storage keys. Authorize before looking up existence. Malformed connections may close with 1008, oversized frames with 1009, and internal failures with 1011.

Implemented memory defaults (positive overrides via `HIGHWAY_LIMITS_FILE`; production sizing remains unmeasured):

| Limit | Initial value |
| --- | --- |
| Auth frame | 16 KiB |
| Complete frame, including envelope | 256 KiB |
| Application payload | 64 KiB encoded JSON |
| Subscriptions per connection | 64 |
| Outstanding publish commands per connection | 32 |
| Unacknowledged events per subscription | 128 default, 1024 maximum |
| Socket output memory | 1 MiB |
| Pending accepted bytes per stream | 8 MiB, plus 65,536 record cap |
| Retained accepted bytes per node | 256 MiB |
| Maximum age of unpersisted accepted data before blocking new publishes | 30 seconds |

Memory mode additionally defaults to 128 active connections, 128 owners, 128 queued requests per owner/registry, at most 16 events per polling turn, and a 5-second oldest-unacknowledged-event timeout. One synchronous publish is outstanding per connection (below the planned maximum of 32). Encoded event bytes are accounted across subscriptions; controls use separate synchronous capacity. Memory streams retain their accepted data and stop 30 seconds after the first event. S3 streams release pending capacity only after confirmed head commits; their 30-second bound measures the oldest still-pending event. They keep one additional last-committed-batch cache (up to 1 MiB). Readers behind cache eviction fetch committed backlog asynchronously; eviction never becomes end-of-stream. Logical event accounting includes 256 bytes of overhead and has a separate record-count bound; this is not an RSS measurement.

Compression is disabled. A hash-checked Mist patch caps accumulated raw input at 256 KiB before frame decoding and preserves active-once ingress under pings. The cap includes WebSocket framing and coalesced input, so large bursts may be rejected conservatively. Oversized raw input closes with 1009. See [Decision 001](decisions/001-development-live.md) for socket buffers and actor admission permits. Archival uses one batch per stream, a 250 ms/1 MiB flush trigger, and at most 16 active storage operations. Failed batches retry the same frozen plan after 1 second; a conflicting head fences the owner. Replay defaults to 8 temporary supervised workers (maximum 16), sharing the 16-operation storage budget. Each connection has at most one replay job and reserves at most 128 KiB/16 items within its output budget. A job reads at most 8 index pages per step; a scan stops at 16,384 pages and initial replay at 30 seconds (including credit pauses). The four positive replay settings are configurable through `HIGHWAY_LIMITS_FILE`: `replay_workers` (max 16), `replay_pages_per_step` (max 64), `replay_max_pages` (max 1,000,000), and `replay_timeout_ms` (max 300,000). Exceeding work/time bounds ends the subscription with `replay_limit`, never `caught_up`. Saturated worker capacity retries after 100 ms without advancing the cursor. Storage/corruption failures terminate explicitly with `unavailable`; a missing owner closes with 1011. Cancellation/refresh/expiry discards stale generations and cancels active I/O; current grants are checked before and after each GET and before delivering results. Each owner holds at most 1024 live view leases and reaps dead connections every 250 ms. Controls and boundary frames are bounded too; `discontinuity` advances the logical epoch position but consumes no event credit and cannot be acknowledged. Cumulative event ACKs preserve order across epoch offset resets. See [Decision 003](decisions/003-replay-continuation.md) for traversal cost and limitations. Per-tenant rate policies and production capacity validation remain milestone 6. Tests inject auth/admission/consumer clocks.

## Sources

- [Browser WebSocket constructor](https://developer.mozilla.org/en-US/docs/Web/API/WebSocket/WebSocket): available connection arguments.
- [JWT best current practices](https://www.rfc-editor.org/rfc/rfc8725): algorithm verification and claim validation.
