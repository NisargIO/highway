# Contributing to Highway

Highway is early in development. The product choices in the architecture and protocol are the starting contract; proposed implementation details can evolve when measurements or correctness tests justify it. Update the docs when behavior changes.

## Development

Use `.tool-versions` for Gleam, Erlang, and rebar3. Dependencies are declared in `gleam.toml` and locked in `manifest.toml`.

```sh
make setup    # Download locked dependencies
make run      # Start on 127.0.0.1:4000 with explicit memory/JWT config
make smoke    # Self-contained Node 22+ memory-mode network demonstration
make smoke-s3 # S3 archival, restart, replay/live races through real adapter and local fixture
make format   # Format source and tests
make check    # Format check, type check, and tests
```

There is no production deployment target yet. The memory demonstration needs no AWS credentials. `make smoke` generates only ephemeral signing material; manual runs require the public JWKS and issuer/audience settings in the README. `make setup` applies a hash-checked Mist input-buffer patch; always use the Make targets to prepare clean dependencies. Python 3 and `patch` are needed for preparation. The Makefile suppresses the locked JOSE release’s deprecated-catch warning on OTP 29. The S3 contract fixture is `scripts/s3-test-server.py`; unit tests use deterministic object-store failpoints. `make smoke-s3` requires only Python 3 and Node 22+. Neither needs production credentials. `HIGHWAY_S3_CONFORMANCE=1 make s3-conformance` explicitly opts into test-object writes against the configured endpoint/bucket; run against AWS before release. See Decision 002 for current credentials, IAM, storage-format, and fixture limitations, and Decision 003 for replay limits and traversal costs.

## Code organization

Keep the entry point in `src/highway.gleam`. Put service modules under `src/highway/`, with transport code separate from pure protocol and stream logic. Mirror modules under `test/highway/`. Add internal modules as needed, instead of scaffolding empty abstractions for every planned feature.

Use small functions, typed errors, and explicit state transitions. Preserve operational error causes while redacting credentials and payloads. Keep the stream owner responsive by doing network I/O in supervised workers. Every queue needs a limit and a defined overload response.

## Verification

Run `make check` for each change. Tests should exercise meaningful contracts: unauthorized requests, duplicate retries, concurrent appends, replay/live overlap, storage failures, and restart discontinuities. Storage tests must distinguish an uploaded object from an authoritative committed segment. Use injected clocks and storage failures for deterministic tests.

When changing dependencies, review the resolved versions and commit the lockfile. CI runs formatting, type checks, tests, and verifies that dependency resolution leaves the lockfile unchanged.

## Design changes

Record a short decision in `docs/` when changing delivery guarantees, token capabilities, cursor encoding, storage format, or deployment topology. Include the problem, selected behavior, alternatives, and migration implications. PR descriptions should explain behavior and relevant validation; AI assistance is welcome when the contributor understands the result.
