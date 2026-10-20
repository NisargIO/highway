# Local Highway and MinIO

Use a recent Docker installation with BuildKit, Docker Compose v2+, and Node.js 22+. No host Gleam/Go installation or AWS account is required.

```sh
make compose-up
make compose-smoke
```

Setup generates random storage credentials and an Ed25519 development issuer in ignored `build/compose/`, builds the images, initializes the private `highway` bucket, and waits for storage readiness. The first source build downloads Go dependencies and takes longer than later cached starts. The repeatable two-client smoke leaves the stack running and checks:

- Real SigV4, wrong-signature rejection, opaque ETags, read visibility, conditional create/update and concurrent-create conformance through Highway's Gleam adapter.
- Protocol v2 per-operation JWT SDK publish/subscribe and denied anonymous object reads.
- Paused MinIO with immediate ordered live delivery, retained pending capacity, no false `persisted`, and liveness/readiness transitions.
- Confirmed authoritative heads, graceful drain/closed v2 heads, and restart of **both** processes using the named volume.
- Exclusive SDK resume through a fresh epoch/discontinuity and exact finite replay with a credit window of two.

Each run uses a fresh stream. Committed streams, canaries and conformance objects remain in the local bucket. The Python fixture still provides precise head-commit failpoints. MinIO adds real signature verification and an object-store process restart test; AWS-specific IAM, encryption, TLS and AWS conformance still require separate validation.

## Connect clients

| Endpoint | Local address |
| --- | --- |
| WebSocket | `ws://127.0.0.1:4000/v1/ws` |
| Private service checks | `http://127.0.0.1:4000/readyz`, `/metrics` |
| MinIO S3 API | `http://127.0.0.1:9000` |
| MinIO object browser | `http://127.0.0.1:9001` |

All ports bind **host loopback**. Export `HIGHWAY_LOCAL_PORT`, `MINIO_LOCAL_PORT`, or `MINIO_CONSOLE_PORT` before running the Make targets to override them. The smoke honors the same overrides; internal ports stay fixed.

Generate a local ten-minute JWT granting tenant `demo`, stream `events`, and publish/subscribe/history:

```sh
make compose-token
HIGHWAY_TOKEN_FILE=build/compose/token node examples/node-client.mjs publish
# Another terminal: receive history and continue live
HIGHWAY_TOKEN_FILE=build/compose/token node examples/node-client.mjs
```

Generation writes `build/compose/token` without printing it. Regenerate before expiry; the Node example reads the file on refresh. For the browser example, serve the checkout with `python3 -m http.server 3000 --bind 127.0.0.1`, open `http://localhost:3000/examples/browser/`, and copy that token into its password field. Publish first to create the stream. Never use these development keys or credentials outside the local stack. Highway mounts only the public JWKS; the private signing key stays on the host.

The Node publish example retries only explicit `unavailable` rejections during epoch startup, for at most five seconds. Unknown/sent publish outcomes are never retried automatically.

The MinIO console uses values from `build/compose/minio-user` and `build/compose/minio-password`. The host directory is 0700 and private key/token files are 0600. Individually mounted credential/public files are 0444 for container UID 10001; their host directory prevents access by other host users. This local stack deliberately uses MinIO administrator credentials for bootstrap and Highway. Production must use a separate scoped identity from [deployment.md](deployment.md).

```sh
docker compose run --rm --no-deps mc ls local/highway/local/
docker compose ps
docker compose logs highway
```

The helper reads mounted credentials into its environment, keeps temporary config in `/tmp`, and requires no host `mc` installation. Do not dump segment contents, keys or tokens into shared logs.

## Stop and restart

```sh
make compose-down       # Drains Highway first; preserves the named data volume
make compose-up         # Reuses credentials, bucket and committed history
```

Keep `build/compose/` together with the volume. Setup never silently regenerates existing configuration. Do not delete/rotate these files under an existing stack. `docker compose down --volumes` **deletes all local history**; use it only for an intentional reset, then remove `build/compose/` before generating fresh credentials.

Highway and MinIO share a network namespace, so S3 uses `http://127.0.0.1:9000` without weakening Highway's HTTPS-or-loopback restriction. Filesystems and processes stay separate. MinIO owns the published ports: stop Highway before stopping/recreating MinIO, then start MinIO first. Whole-stack Compose up/down enforces dependency order. Exactly one Highway uses prefix `local`; no replicas, overlapping replacements or automatic failover/restart are configured. Highway runs non-root/read-only with bounded temporary storage, dropped capabilities, and 25-second stop grace above its 10-second drain deadline. `accepted` remains admission to memory; only an authoritative head commit produces `persisted`.

## Source builds and limits

Community MinIO is archived/unmaintained and distributes source only; the former image tag is unavailable. This is a **local test environment**, not a production MinIO recommendation. [deploy/minio.Dockerfile](../deploy/minio.Dockerfile) builds unmodified MinIO commit `7aac2a2c5b7c882e68c1ce017d8256be2feea27f` and `mc` commit `77f82e18b5401a65958f1619df6ebb994634bd88`, checks source archive SHA256 hashes, pins Go/Alpine image digests, and retains AGPLv3 license files. See the [upstream distribution notice](https://github.com/minio/minio#source-only-distribution), [MinIO source](https://github.com/minio/minio/tree/7aac2a2c5b7c882e68c1ce017d8256be2feea27f), and [mc source](https://github.com/minio/mc/tree/77f82e18b5401a65958f1619df6ebb994634bd88).

Local validation uses Linux arm64 containers on OrbStack; CI is also configured for Linux amd64. Images and SDK remain unpublished. A single-disk Docker volume is not replication, backup, AWS certification or a production durability guarantee. HTTPS and production IAM are separate in [deployment.md](deployment.md).

The smoke also runs locally with Node 22.23.3. A failed upgrade in that runtime can emit `error` without `close`; the SDK detaches on transport error or handshake timeout, schedules one reconnect, ignores late callbacks, and retains uncertain publish semantics. Regression tests cover opening failures, silent timeouts, stale callbacks, processed-cursor resume and no publish retry.

The Node/browser examples now place `getToken` on each subscription/publish (SDK 0.2.0, protocol v2). `make smoke-scoped` separately reproduces two stream JWTs sharing one socket, independent expiry/refresh/revocation and restart/resume against the deterministic S3 fixture. The real MinIO smoke uses v2 as well.
