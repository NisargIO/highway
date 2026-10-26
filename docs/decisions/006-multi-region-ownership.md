# Decision 006: multi-region stream ownership behind one endpoint

Status: proposed design; nothing here is implemented. Version one's rule of exactly one server per storage prefix stays in force until this decision is accepted and its fencing is built and tested. The live/durable split, cursor rules and authorization model are unchanged.

## Goal

Clients use a single endpoint such as `wss://streams.example.com/v1/ws`. The first authorized publish creates a stream in the region nearest that client. While a stream is awake, every command for it, from any region, reaches the one owner in the region that created or last woke it. A sleeping stream wakes in the region nearest whoever touches it next. The SDK and wire protocol need no changes for this; routing is entirely server-side.

One owner still orders each stream. "Nearest region" chooses where that single owner runs. It never means writing in several regions at once.

## Non-goals

- Multi-writer or active-active streams, consensus on the publish path, or exactly-once delivery.
- A new database, broker or WAL (see `AGENTS.md`). Coordination uses the existing S3 conditional writes.
- Automatic failover of the S3 primary bucket. Regional storage disaster recovery is an operator runbook, described below.

## The head object is the directory

Stream startup already performs the needed compare-and-set operations (Decision 002): a new stream PUTs `head.json` with `If-None-Match: *`, and an existing stream publishes `epoch + 1` with `If-Match` on the prior ETag. **Head format v3** adds ownership fields:

- `owner_region`: the region whose node owns the open epoch.
- `owner_node`: an opaque routing hint within that region.
- `lease_until`: a UTC millisecond deadline. It is meaningful only while `status` is `open`.

Those conditional writes then double as "create here" and "wake here", and S3 resolves races between regions. A losing region receives 412, re-reads the head and routes to the winner. There is no separate global registry. As with v2, older binaries fail closed on v3 heads, and rollbacks need a compatible reader.

## Resolution

A client connects through latency-based DNS or anycast and reaches region R. Before starting or looking up a local owner, R's registry resolves the stream from a bounded local cache, falling back to a GET of the head:

| Head state | Action in region R |
|---|---|
| Missing, command is an authorized publish | Conditional create with `owner_region = R` and epoch 1. On 412, re-read and resolve again. |
| Missing, command is subscribe or history | Return the existing not-found behavior. Subscribe never creates a stream. |
| `closed` (sleeping), or `open` with an expired lease | Conditional `If-Match` write of `epoch + 1` with `owner_region = R`. On 412, re-read and route to the winner. |
| `open`, valid lease, `owner_region = R` | Hand off to the local owner node (in-region lookup via `pg` or `syn`). |
| `open`, valid lease, `owner_region = X` | Relay to region X. |

A finite snapshot (`follow: false`) of a sleeping stream is served from storage without waking it, so a history reader does not pull ownership away from the publishers.

Cached routes can go stale. A region that no longer owns a stream answers a relay with `moved {epoch, owner_region}`, and the edge resolves again. Relay links carry the epoch they expect, so a stale route cannot deliver data from an old incarnation.

## Lifecycle

- **Awake:** the owner keeps `lease_until` ahead of the current time. Active streams renew it as part of their normal head commits. A quiet stream that still has subscribers writes a renewal at a fraction of the lease period.
- **Live admission fence:** an owner stops admitting publishes once `lease_until - max_clock_skew` has passed without a confirmed renewal. A taking-over region does not admit publishes until the previous `lease_until + max_clock_skew` has passed and its own conditional head write is confirmed. This closes the gap `architecture.md` describes, where head preconditions fence storage but not live sends.
- **Sleep:** after N minutes with no publishes and no subscribers, the owner runs a per-stream version of the Decision 004 drain: it closes admission, confirms every pending commit and publishes a closed head.
- **Wake:** the next touch claims the stream in the toucher's region with `epoch + 1`. Today, recovery emits an interrupted-epoch boundary even after a clean close (Decision 004). Skipping that `discontinuity` when the stream wakes from a confirmed closed head is desirable, but it changes what clients see, so it is a separate versioned change. The first version keeps the conservative boundary.
- **Crash:** the lease expires, and the next toucher takes over under the fence above. The conservative `discontinuity` applies, as it does for recovery today.

## BEAM's role

Within a region, nodes cluster with Erlang distribution: DNS-based discovery, `pg` or `syn` to map streams to owner nodes, `pg` for local fan-out, and node monitors so takeover attempts start promptly. `:global`, Horde and `syn` are available during network splits rather than consistent, so they never authorize admission; they are routing hints only. Authority is the head lease. `ra` could later provide faster intra-region fencing if lease waits prove too slow.

Regions are not joined by distribution, because of the full mesh, head-of-line blocking on one connection per node pair, WAN tick tuning and cookie security. The cross-region relay is an ordinary highway protocol-v2 client. It forwards the end client's scoped JWT, honors credit and resume, and fans out locally through `pg`. Many local subscribers to a remote stream share one upstream subscription.

## Storage topology

Compare-and-set holds only within one bucket. S3 replication, including Cross-Region Replication and Multi-Region Access Points, is asynchronous and resolves conflicts by last writer wins. **Heads therefore live in one primary bucket in one region and are never written through a replica.** Segments and index pages are immutable, use unique keys and are checksummed by SHA256 in the index, so a replica either returns exact bytes or 404. They can be replicated or cached safely.

Selected starting point: **one primary bucket for heads and data.** Live delivery never waits for S3, so owners in distant regions only see slower `accepted`→`persisted` progress. Known costs:

- Persistence latency for distant owners: a 250 ms flush plus three sequential round trips (segment, index, head). From a region about 200 ms away, that is roughly 850 ms.
- Per-stream archival throughput: one commit in flight per stream caps a distant owner near 1 MiB/s or about 4k records/s before the 8 MiB stream pending cap returns `storage_backlog`.
- A larger crash window for accepted but unpersisted data on distant owners.
- Replay walks linked index pages one round trip at a time, so a distant reader reaches the 30 s `replay_timeout` after far fewer pages.
- Inter-region transfer charges on every byte written from outside the bucket's region.
- If the primary region fails, awake streams keep delivering live until their pending caps fill, while creates, wakes and persistence fail. The current readiness check requires a fresh S3 canary, so it would mark every node in every region unready. Readiness must distinguish "cannot persist" (degraded, keep serving live) from "cannot serve" (unready).

Improvements that need no format change:

1. PUT the segment and index in parallel, then the head. This cuts a commit from three round trips to two.
2. Confirm that `httpc` reuses TLS connections across regions (`highway_s3_ffi.erl` passes SSL options per request). Otherwise each request pays an extra handshake.
3. Allow longer flush intervals for distant owners.
4. Owners reuse the head they last wrote. Only routing lookups GET heads, and those results are cached.

S3 Transfer Acceleration offers little between AWS regions, which already use the AWS backbone, and it requires virtual-hosted requests where highway uses path-style.

Escalations, in order:

- **Read replicas (preferred next step):** Cross-Region Replication filtered to the segment and index prefixes, or CloudFront in front of the primary for those immutable objects. Replay readers try the local copy first and fall back to the primary on 404. Heads are always read from the primary. This fixes distant replay without write-path changes. Costs are replication lag (most objects arrive within seconds, Replication Time Control promises 99.99% within 15 minutes), per-object request charges multiplied by replica regions, and duplicated storage.
- **Heads central, data local:** owners write segments and indexes to their own region's bucket and only the head to the primary bucket. A commit's cross-region cost drops to one round trip and most bytes stay local. This needs an index format change (bucket or region per entry), history that spans several buckets after a stream wakes elsewhere, and IAM, readiness and conformance checks per bucket. Adopt it only if measured egress or persistence latency justifies it.
- **Disaster recovery runbook (not automated):** replicate everything, including heads, to a standby. A replicated head may lag the primary, and replication does not preserve order across objects, so it can reference an index that has not arrived yet. On operator failover, every stream starts a new `incarnation` or treats its tail as interrupted, and clients resume across a declared `discontinuity`. Two-way head replication, and Multi-Region Access Points in active-active mode for head writes, are prohibited.

## Configuration sketch

```sh
HIGHWAY_REGION=eu-west-1
HIGHWAY_PEERS_FILE=/etc/highway/regions.json   # region -> private relay endpoint
HIGHWAY_LEASE_MS=10000
HIGHWAY_MAX_CLOCK_SKEW_MS=1000
HIGHWAY_SLEEP_IDLE_MS=300000
```

Every region shares one JWT issuer, audience and JWKS. `HIGHWAY_S3_BUCKET` names the primary bucket in every region.

## Phasing

1. Head v3 with the lease fields, renewal, and the live admission fence. Deterministic tests cover lease expiry, clock-skew margins, concurrent claims, and a stale owner that must stop admitting.
2. A per-stream idle drain to a closed head, plus handoff between two nodes in one region with in-region clustering.
3. The edge resolve step and the relay client across regions, with `moved` re-resolution.
4. Region-aware readiness and the commit latency improvements above. Measure persistence and replay from a distant region against the primary bucket using `smoke-release` and `s3-conformance`.
5. Read replicas for immutable objects, then the optional versioned clean-wake change that drops the `discontinuity`.

## Open questions

- Should a quiet stream with subscribers but no publishers sleep, which requires a way to wake it when a publish arrives elsewhere, or keep renewing its lease?
- Default lease length and clock-skew margin, and how hosts will bound clock skew (for example, the Amazon Time Sync Service).
- Whether owners should later move proactively based on where publishers are, in addition to moving on wake.
- How relay links are authenticated between regions (mTLS or a private network), alongside the forwarded client JWT.
