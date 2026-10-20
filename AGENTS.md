# Agent instructions

Read `CONTRIBUTING.md`, `docs/architecture.md`, `docs/protocol.md`, and `docs/implementation-plan.md` before changing behavior. User instructions take priority. Keep progress and completion checkboxes in the implementation plan accurate.

## Project constraints

- Implement the service in Gleam targeting Erlang/OTP. Use Mist for HTTP and WebSockets.
- Live messages precede S3 archival. Never equate `accepted` with `persisted`, or claim exactly-once delivery.
- Authorize every publish, subscribe, and history read against tenant and stream capabilities. Verify signatures with a maintained library; do not write cryptography.
- One owner serializes a stream. Never reuse cursors across owner restarts. An actor restart is a data-loss boundary for its pending tail.
- Bound pending bytes, connection queues, replay concurrency, and actor mailboxes. Reject overload before accepting a publish.
- Never expose an S3 segment until the authoritative head references its index. Do not acknowledge persistence after a PUT alone.
- Version one supports one server per storage prefix. Do not introduce transparent failover or distributed ownership without a new design decision.
- Do not add a database, message broker, local WAL requirement, or Rust storage engine to bypass the chosen scope.

## Gleam workflow

- Look up uncertain Gleam, OTP, and dependency APIs in local dependency source or current HexDocs. Do not invent functions from memory.
- Run `gleam check` in small increments and `make check` before finishing.
- Use typed domain values and explicit decoders at JSON and storage boundaries.
- Preserve error causes. Propagate failures or log safe context; never silently discard operational errors. Tokens, private keys, and message bodies must not appear in logs.
- Prefer `use value <- result.try(...)` for Result chains; keep branching readable. Use `Option` for genuine absence, and `Result` for operations that can fail.
- Use Gleam for service logic. If Erlang FFI is necessary, keep it small, document its type contract, and test the boundary. Give `.erl` modules distinct names such as `highway_crypto_ffi`, and place them beside their Gleam bindings.
- Make protocol and storage format changes explicitly versioned. Add focused tests for authorization, ordering, crash boundaries, or replay races affected by a change.
- Commit `manifest.toml`. Do not commit credentials, build output, or generated reference checkouts.

## Work scope

Complete one coherent milestone at a time. Keep the server runnable. Distinguish implemented behavior from planned behavior in the README and milestone notes. Record validation and remaining limitations in the handoff. Do not copy external repositories' agent policies wholesale.
