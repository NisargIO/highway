# Fresh session implementation prompt

Copy the prompt below into a new coding session opened in this repository.

---

Work in the Highway repository at `/Users/nisustudio/Documents/nisarg/highway` (GitHub: `NisargIO/highway`). Read `AGENTS.md`, `CONTRIBUTING.md`, `README.md`, `docs/architecture.md`, `docs/protocol.md`, and `docs/implementation-plan.md` first, then inspect the actual code and git state.

Milestones 1–5 implement typed codecs, real EdDSA JWT verification, authenticated Mist WebSockets, supervised owners, and asynchronous S3 archival/recovery and authorized scrollback/live continuation alongside explicit development memory mode. Read all three decisions in `docs/decisions/`, inspect the tests, and run `make setup`, `make check`, `make smoke`, and `make smoke-s3` to establish the baseline. The Make targets apply the reviewed Mist buffer-limit patch. Continue with milestone 6 only when requested.

The product is a Gleam service on Erlang/OTP, inspired by S2's stream model, with a custom WebSocket protocol. Clients publish and subscribe on the same socket. A signed JWT from the application backend grants exact tenant/stream permissions for publish, subscribe, and history. One socket can multiplex several streams. Live events must be accepted and delivered immediately; S3 archival happens asynchronously through the implemented milestone 4 worker. `accepted` must never mean `persisted`.

The bounded memory adapter remains ephemeral. S3 workers now allocate epochs before admission, write immutable segments/indexes, conditionally publish the authoritative head, and only then advance persisted progress. Preserve the frozen-plan reconciliation, stale-completion rejection, logical pending/cache limits, and conservative interruption boundaries. Real AWS conformance is an explicit opt-in pre-release check and has not yet been executed; the local contract fixture does not validate SigV4 signatures cryptographically.

Preserve the implemented authoritative replay, pending/cache/live join, authorization before and after storage reads, expiry, work/byte/concurrency limits, cancellation generations, and ordered interrupted epochs even when a resume cursor is ahead of the archived prefix. Never expose orphan uploads or silently skip cache-evicted events. Keep the existing storage failure, authorization, ordering, mailbox, expiry, and two network smokes. Finish with `make check`, both smokes, and focused archive/live race tests. Do not introduce a database, broker, WAL requirement, failover, or exactly-once claim.

Make routine implementation decisions autonomously. If a proposed detail needs changing, record the reason and update the protocol/architecture consistently. Keep milestone checkboxes and README status accurate. Keep the requested milestone coherent; implement production packaging/drain/metrics only when milestone 6 is requested. Finish with what works, the checks run, material limitations, and the next milestone.
