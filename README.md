# Highway

Highway is a planned Gleam service for publishing and subscribing to named streams over WebSockets. Signed JWT capabilities grant access to individual streams. Messages travel live immediately; S3 stores their history asynchronously for scrollback and reconnects.

**Status: repository bootstrap.** The HTTP server and CI are implemented. JWT verification, WebSockets, stream actors, and S3 persistence are planned. A live acceptance response will not mean a message is durable: a crash can lose the unpersisted tail.

## Run locally

Use the toolchain in `.tool-versions`: Gleam 1.19.2, Erlang/OTP 29.1.2, and rebar3 3.27.3. On macOS, `brew install gleam erlang rebar3` installs the tools; use a version manager if Homebrew has newer releases.

```sh
make setup
make check
make run
```

The development server binds to `127.0.0.1:4000`.

```sh
curl -i http://127.0.0.1:4000/healthz
```

`GET /healthz` returns 200. `GET /readyz` returns 503 until the stream service exists. `GET /v1/ws` currently returns 501 and does not upgrade. Runtime configuration and production packaging belong to later milestones.

## Product decisions

- Clients publish and subscribe on the same WebSocket; one socket can carry several subscriptions.
- The application backend signs access tokens. Highway verifies them and authorizes each stream operation.
- Live fanout happens before S3 persistence. Publishers receive separate `accepted` and `persisted` messages.
- Stream order comes from one owner process per stream. Cursor epochs prevent reuse after a restart.
- S3 is the durable store. The first deployment is one BEAM node per storage prefix, with bounded memory and explicit discontinuities after failures.

## Start here

- [Architecture](docs/architecture.md): guarantees, process model, S3 commits, recovery, and tradeoffs.
- [Protocol](docs/protocol.md): authentication, claims, frames, reconnects, and limits.
- [Implementation plan](docs/implementation-plan.md): ordered milestones and acceptance criteria.
- [Contributing](CONTRIBUTING.md): commands, code conventions, and verification.
- [Agent instructions](AGENTS.md): constraints for coding agents.
- [Fresh session prompt](docs/fresh-session-prompt.md): a ready-to-use implementation handoff.

## References

[S2](https://github.com/s2-streamstore/s2) supplies the durable stream model. Highway borrows stream ownership, ordered appends, and cursor-based reads, while choosing asynchronous history. This is a separate protocol, with no S2 API compatibility planned.

[Arc's Gleam guidance](https://github.com/alii/arc/blob/master/CLAUDE.md) informs the development workflow: check unfamiliar APIs against actual docs, compile frequently, preserve errors, and keep Erlang FFI small. Highway has its own contribution policy.
