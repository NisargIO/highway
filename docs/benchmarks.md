# Reproducible local measurements

Run `make benchmark` (equivalent to `make smoke-release`). It builds the native OTP shipment, starts the Python S3 contract fixture and real Node SDK clients, exercises graceful/failed drain and singleton restart, fills a **64-record pending cap**, then writes `build/benchmark.json`. Generated output remains ignored; this document records one sample from 2026-10-19.

Environment: macOS arm64, Apple M5 Max, Node v25.9.0, Gleam 1.19.2, Erlang/OTP 29.1.2. Storage is the loopback Python contract fixture using the real signing/HTTP adapter; WebSockets use local plaintext HTTP with no proxy/TLS. Probe interval is 100 ms/stale bound 800 ms to exercise readiness, rather than the production default 5000/15000 ms. The test uses one stream, one sequential publisher, a follower with window 16, and a finite replayer with window 128. Application payload contains 256 text bytes plus JSON framing.

| Observation | Sample |
| --- | ---: |
| Accepted publish RTT, p50 / p95 | 0.218 / 0.489 ms |
| Publish start to live delivery, p50 / p95 | 40.777 / 60.245 ms |
| Publish start to authoritative persisted notice, p50 / p95 | 240.149 / 246.339 ms |
| Acceptance response to persisted notice, p50 / p95 | 239.950 / 246.043 ms |
| Paused head release to final persisted notice | 154.165 ms |
| Finite replay of 67 records across the restart boundary | 210.062 ms |
| Pending logical bytes at the 64-record limit | 33,974 |
| BEAM allocated bytes, baseline / at pending cap | 62,469,390 / 62,779,422 |
| Storage request delta, archival + probes | 18 |
| Storage request delta, replay + probes | 27 |

The fixture pauses authoritative head writes while 64 events are accepted and delivered live. The 65th request is explicitly rejected with `storage_backlog` before admission. After release, every accepted event is confirmed persisted and a finite history replay exactly matches all 67 records (three scenario records plus the 64-record sample), without a missing/duplicate event. Persistence timing is observed only from cumulative server notices; the measurement maps their exact opaque cursors to the publisher's known ordered list without parsing cursors. The deliberately paused interval is included in accepted-to-persisted timings, so these are failure/recovery observations rather than unconstrained archival latency.

BEAM allocated memory is the runtime's node-level allocation gauge, not OS RSS or a bound on every BEAM allocation. The test reaches the configured record limit; it does not fill the default 8 MiB stream/256 MiB node byte limits or establish large-node capacity. The live p95 reflects the 16-item/20 ms polling budget and local scheduling; it is not a tail-latency guarantee. Summed S3 request counts include probes and conditional/missing-object checks, so they are not exactly a count of billable AWS data writes. Batch placement and request counts can vary with scheduling.

This is one small local run, not an AWS, TLS, multi-tenant, sustained-throughput or deep-history benchmark. It provides a reproducible limit/recovery check and an honest baseline for future sizing. Do not infer fleet throughput, production instance size, or AWS pricing from it. Before deployment, run AWS conformance with the actual IAM/endpoint and measure the expected stream/client/payload/history mix, credential lifetime, proxy/TLS, long replay, and sustained outage behavior. Reverse-linked replay can rescan index pages; checkpoint indexing remains future work. The configured production defaults remain conservative explicit bounds rather than measured throughput promises.
