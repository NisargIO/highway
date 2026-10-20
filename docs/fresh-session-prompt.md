# Fresh session implementation prompt

Copy the prompt below into a new coding session opened in this repository.

---

Work in the Highway repository at `/Users/nisustudio/Documents/nisarg/highway` (GitHub: `NisargIO/highway`). Read `AGENTS.md`, `CONTRIBUTING.md`, `README.md`, `docs/architecture.md`, `docs/protocol.md`, and `docs/implementation-plan.md` first, then inspect the actual code and git state.

Implement the first authenticated live publish/subscribe slice, covering milestones 1 through 3 in order. The repo currently has a working Gleam/Mist HTTP bootstrap, locked dependencies, CI, and three bootstrap tests. There is no WebSocket, JWT, stream actor, or S3 implementation yet. Run `make check` to establish the baseline.

The product is a Gleam service on Erlang/OTP, inspired by S2's stream model, with a custom WebSocket protocol. Clients publish and subscribe on the same socket. A signed JWT from the application backend grants exact tenant/stream permissions for publish, subscribe, and history. One socket can multiplex several streams. Live events must be accepted and delivered immediately; S3 archival will happen asynchronously in milestone 4. `accepted` must never mean `persisted`.

Build typed protocol codecs and cursors first, then real JWT verification and Mist WebSocket auth, then supervised stream owners and bounded live fanout. Use a maintained crypto/JOSE library and verify uncertain Gleam APIs in current dependency source or HexDocs. Preserve errors, keep I/O out of owner loops, and bound outstanding messages across processes as well as application queues.

For this slice, introduce only an explicit development memory mode. It must advertise ephemeral storage, never emit `persisted`, use a fresh incarnation after restart, and reject old cursors. Production/S3 mode should fail closed until S3 exists. Do not implement fake auth or pretend in-memory history is durable. Keep the S3 interfaces compatible with the documented future epoch and commit design, without building the full archival engine in this slice.

Use focused tests for real JWT verification, unauthorized stream/tenant access, auth expiry and refresh, ordering under concurrent publish, subscription cleanup, and slow-client limits. Add a reproducible two-client smoke example. Update obsolete bootstrap tests as routes become functional. Run `gleam check` frequently and finish with `make check` and the end-to-end local smoke test.

Make routine implementation decisions autonomously. If a proposed detail needs changing, record the reason and update the protocol/architecture consistently. Keep milestone checkboxes and README status accurate. Stop after the authenticated live slice works; leave S3 archival and scrollback explicitly pending. Finish with what works, the checks run, material limitations, and the next milestone.
