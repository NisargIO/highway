# Decision 006: multi-region stream ownership behind one endpoint

Status: implemented as opt-in **cluster mode**. Without `HIGHWAY_CLUSTER_NODE`, a server stays standalone: exactly one server per prefix, v2 heads with no lease, and no admission fence, exactly as before. The live/durable split, cursor rules, wire protocol and authorization model are unchanged in both modes. Items marked *not implemented* below remain future work.

## Goal

Clients use a single endpoint such as `wss://streams.example.com/v1/ws`, placed behind latency-based DNS or anycast. The first authorized publish creates a stream on the node, and so in the region, that the client reached. While a stream is awake, every command for it reaches the one owner that created or last woke it, from any node. A sleeping stream wakes wherever it is touched next. Clients and the SDK need no changes.

One owner still orders each stream. "Nearest region" chooses where that owner runs. It never means writing in several places at once.

## The head object is the directory

Every clustered node shares one primary bucket and prefix. **Head format v3** adds `owner: {node, region, lease_until}` (`lease_until` is a canonical-decimal wall-clock millisecond deadline). The startup compare-and-set operations from Decision 002 double as the ownership protocol, implemented in `archive.claim`:

- **Missing head and an authorized publish:** a conditional create (`If-None-Match: *`) with this node's lease.
- **Closed (sleeping) head, or an open head whose lease plus skew has passed:** `If-Match` on the prior ETag, writing `epoch + 1` and this node's lease. The interrupted-epoch boundary is written as before.
- **Open head with an unexpired lease held by another node:** no write. The claim returns `Leased(node, region, until)`.
- **Open head with a lease held by this same node name:** reclaimed immediately. A running BEAM node owns its name, so a lease under that name belongs to a dead owner process.
- **Standalone recovery:** ignores leases and writes a lease-free v2 head.

S3 settles races between nodes. A loser receives a precondition failure, its owner exits, and the next request resolves to the winner. Lease node names are validated as `name@host` with a restricted character set before they become distribution atoms, and heads whose version disagrees with lease presence are rejected. Older binaries fail closed on v3 heads.

## Leases and the live admission fence

A clustered primary tracks the lease deadline of its last *confirmed* head write. Every data commit extends the lease. An idle primary rewrites only the lease (`archive.renewal`: same index and archived cursor) once half the term has passed. Failed or ambiguous renewals are retried as the same frozen plan, so reconciliation can recognize a write that actually landed.

The owner refuses publishes (`unavailable`) once `now >= lease_until - skew`. A claimant only writes after `lease_until + skew` by its own clock. With clock error below `skew`, the old owner stops admitting before any new owner can start. This closes the gap where head preconditions fenced storage but not live sends. A renewal or commit that later succeeds because nobody claimed the stream resumes admission. A precondition conflict fences the owner; a clustered owner then exits so the registry resolves the stream again. As always, events accepted but unpersisted at that point are the uncertain tail, and recovery reports them with a `discontinuity`.

Standalone owners are never lease-fenced, so storage outages behave exactly as in version one.

## Routing an awake stream: mirrors

A registry always starts a local owner for a requested stream. When that owner's claim returns `Leased(node, …)`, it becomes a **mirror** of the remote primary instead of failing:

1. A short-lived process calls `highway@relay:attach/3` on the lease holder through `erpc`. Only fixed module/function entry points are used, never closures. `attach` looks up that node's registry and returns its *primary* owner; it refuses to return a mirror, so mirrors never chain. It also spawns a relay **agent** on the primary's node.
2. The agent captures a view on the primary from its persisted cursor (everything after it is still in the primary's memory). It sends `Begin(floor)`, then ordered `Events` batches of up to 256, and `Persisted` progress that never runs ahead of events already sent. If it falls behind the primary's live cache, it sends a new `Begin` at the persisted cursor; the mirror treats everything at or before that as archived.
3. The mirror keeps an ordinary stream state built from that feed. Local sessions capture views, pull, replay and watch persistence against it unchanged; their reads before the mirror's floor go to S3 through the existing replay workers. A gap or foreign cursor in the feed stops the mirror rather than skipping events.
4. Publishes bypass the mirror actor. `owner.append` sees the stored primary handle and runs `owner:append_here/3` on the primary's node, using that node's monotonic clock. The primary assigns the cursor and applies its own admission and fence. The registry hands mirror publishes back to the caller (`Forward`), so a cross-node round trip never blocks other streams on the edge node.

The agent's first `Begin` can arrive before the `erpc` reply that names it, so a locating owner accepts it; the agent handle is attached when the reply lands. If the reply fails after a feed has started, the mirror exits rather than run without a monitor on its primary. If the holder cannot be reached, the owner retries the locate every second and claims the stream once the lease plus skew has passed. That is crash takeover.

When the primary or its node goes down, the agent exits and the mirror stops abnormally. Local subscribers see the same owner failure as before (`unavailable`, then socket close 1011), and clients reconnect and resume from their cursor. Re-resolution then finds the new owner. There is no separate `moved` frame.

Routing is per node. Region is a label recorded in the lease for operators and placement; any node can own any stream, and nodes in one region coordinate exactly as nodes in different regions do.

## Sleep and wake

A clustered primary with nothing pending, no view leases (no subscribers, local or relayed) and no publish or subscribe for `HIGHWAY_SLEEP_IDLE_MS` drains to a closed head and exits. A mirror with no local views for the same period simply exits, releasing its agent's view so the primary can sleep too. Both record the exit in a node-local *retired* table for 60 seconds. A connection holding only a publisher progress watch on a retired owner drops that watch instead of closing the socket; the next publish re-resolves the stream.

A sleeping stream is claimed with `epoch + 1` by whichever node is touched next, with no lease wait. SIGTERM drain also ends in closed heads, so a graceful shutdown hands streams to other nodes immediately. Waking still writes the conservative interrupted-epoch boundary (Decision 004), so resumers see a `discontinuity` across a wake. Skipping it after a confirmed clean close would change what clients see, so it remains a separate, versioned change (*not implemented*).

Streams with any subscriber never sleep; their owner renews its lease instead.

## Transport and trust

Nodes talk over Erlang distribution: `erpc` calls with fixed MFAs, a remote spawn for the agent, cross-node monitors and ordinary message sends for the feed. Connections are opened on demand to the node named in a lease, so no discovery service or full-mesh configuration is needed. If the VM was not booted with `-name`, the server starts distribution itself (running `epmd -daemon` if needed) and applies the shared cookie.

**Distribution is full trust.** Any holder of the cookie can run code on every node. Run nodes on a private network, keep the cookie secret (`HIGHWAY_CLUSTER_COOKIE` or `_FILE`), and use TLS distribution across regions: boot with `ERL_FLAGS="-name … -proto_dist inet_tls -ssl_dist_optfile …"`, which the server detects and keeps. The proposal's alternative of a cross-region relay written as an ordinary highway protocol client, which avoids distribution over WANs, is *not implemented*. Feed batching keeps per-event round trips off the read path, but one distribution connection per node pair still shares head-of-line blocking between streams.

## Storage topology

Compare-and-set holds only within one bucket, so heads live in one primary bucket in one region and are never written through a replica. Live delivery never waits for S3; distant owners only see slower `accepted`→`persisted` progress, plus the other costs in the proposal (per-stream archival throughput, a larger crash window, slow distant replay, inter-region transfer, and dependence on the primary region).

*Not implemented yet:* region-aware readiness that separates "cannot persist" from "cannot serve" (today a primary-region S3 outage still marks every node unready); parallel segment/index PUTs; read replicas or CloudFront for immutable segments and indexes; the heads-central/data-local layout; and the disaster-recovery runbook (new incarnation on failover; never two-way head replication or active-active Multi-Region Access Points for heads).

## Configuration

```sh
HIGHWAY_STORAGE_MODE=s3_async           # required; memory mode cannot cluster
HIGHWAY_CLUSTER_NODE=highway-a@10.0.1.5 # enables cluster mode
HIGHWAY_REGION=eu-west-1
HIGHWAY_CLUSTER_COOKIE_FILE=/run/secrets/highway-cookie
HIGHWAY_LEASE_MS=10000                  # 1000..120000
HIGHWAY_MAX_CLOCK_SKEW_MS=1000          # <= lease / 4
HIGHWAY_SLEEP_IDLE_MS=300000            # 0 disables sleep; otherwise >= lease
```

Every node uses the same bucket, prefix, JWT issuer, audience and JWKS. Idle awake streams cost one head PUT per half lease term; sleeping streams cost nothing.

## Verification

`test/highway/cluster_test.gleam` covers v3 encoding and validation, claims against unexpired, expired, same-node and closed heads, standalone v2 fallback, lease-only renewal, an owner that waits out an unreachable holder's lease plus skew before taking over, admission fencing while head writes fail and resumption after the frozen retry commits, idle renewal without history change, and sleep to a closed head.

`make smoke-cluster` runs two native shipments as `us-east-1` and `eu-west-1` nodes against the loopback S3 fixture. It verifies, in order: creation on the node a client reached; a subscriber on the other node receiving through a mirror with identical cursors; a publish on the non-owner forwarded and ordered by the owner; persisted notices delivered to that publisher; history from the non-owner in exact order; SIGKILL of the owner followed by takeover only after the lease, with a `discontinuity` and a new epoch; sleep without disconnecting an idle publisher; wake on the other node with no lease wait and full history; a remote mirror idling out so the primary can sleep again; and SIGTERM drain handing a stream to the other node at once. All existing checks and smokes pass unchanged in standalone mode.

## Open questions

- Default lease and skew for production, and how hosts bound clock skew (for example, the Amazon Time Sync Service).
- Whether owners should move proactively towards their publishers, in addition to moving on wake.
- Replacing distribution across regions with a protocol-client relay, if WAN head-of-line blocking or the trust model proves limiting.
