# Highway JavaScript SDK

Dependency-free ESM for **Node.js 22+** and browsers, with TypeScript declarations. The package is implemented locally as `@nisargio/highway` version 0.2.0; it has not been published to npm. Install from this checkout:

```sh
npm install /absolute/path/to/highway/sdk/javascript
```

The SDK uses the runtime's native WebSocket. An alternative compatible constructor can be supplied with `WebSocket`. TLS belongs in `wss://` and the deployment proxy. URLs containing credentials, query parameters, or fragments are rejected. JWTs go only in protocol frames, never URLs; the SDK never logs tokens/payloads or saves them to browser storage.

## Node.js

```js
import { readFile } from 'node:fs/promises';
import { HighwayClient, PublishUncertainError } from '@nisargio/highway';

const client = new HighwayClient({ url: 'wss://streams.example.com/v1/ws' });
const getToken = async () => (await readFile('/run/secrets/events-jwt', 'utf8')).trim();
client.on('persisted', frame => {
  // Cumulative storage progress for this stream, separate from acceptance.
  console.log(`Persisted through ${frame.through}`);
});
await client.connect();
try {
  const accepted = await client.publish({ stream: 'events', messageId: 'application-key', data: { text: 'hello' }, getToken });
  console.log(`Accepted into memory at ${accepted.cursor}`);
} catch (error) {
  if (error instanceof PublishUncertainError) {
    // The request may have been accepted. Retrying can append a duplicate.
    // Reconcile using application message IDs and authorized history.
  } else throw error;
}
```

First use of a new S3 stream can return a typed `unavailable` while its owner allocates an epoch. That explicit rejection is safe for the caller to retry. The SDK never automatically retries or queues a publish, even during reconnect. A disconnection/send failure/timeout after sending produces `PublishUncertainError` with the application's `messageId`. A local request-cap rejection was never sent and uses `client_overloaded`. `accepted` does not promise durability, exactly-once processing, or receipt by every subscriber. `persisted` is a separate cumulative notification; it does not have to equal every individual accepted cursor. Cursors remain opaque.

[../../examples/node-client.mjs](../../examples/node-client.mjs) is a runnable Node example with token-file refresh and received/persisted cursor notices. Set `HIGHWAY_TOKEN_FILE`, optionally `HIGHWAY_URL`, `HIGHWAY_STREAM`, and `HIGHWAY_AFTER`. Use `node examples/node-client.mjs publish` to publish once and stay connected for persistence notices; otherwise it subscribes to history. An application's token issuer remains outside Highway.

## Independent stream credentials

Omit constructor `getToken` to select protocol v2. Each subscription has its own callback, verified claims, automatic refresh, `expiresAt` and `sub.refresh()`. Different streams share a socket without inheriting one another's grants or expiry:

```js
const events = await client.subscribe({
  stream: 'events',
  getToken: () => getStreamJwt('events'), // your authenticated backend
  from: 'earliest',
  window: 16,
});
const notifications = await client.subscribe({
  stream: 'notifications',
  getToken: () => getStreamJwt('notifications'),
  from: 'latest',
});
await events.refresh(); // only events; notifications continues independently
```

All tokens must have the same subject/tenant, pinned for the client lifetime, including reconnect. A scoped client refuses servers without advertised v2 support. `connect()` opens the transport; successfully authorize an operation within the server's five-second auth deadline. Calling `subscribe()` directly opens it automatically. Initial bad signatures/grants never create a view. Expiry, revocation and failed refresh end only the affected iterator and clear its buffered frames. An expired lease must be recreated from a processed cursor, rather than revived by a late refresh. One socket still shares byte/credit limits and one active server replay job.

Publish also requires its own `getToken`; it never borrows a view's credentials. The callback runs before each send, without an offline queue. Publisher-only persistence watches expire with the latest successful publish's token and are not automatically renewed. An outage can outlast that token and its notices: use authorized history to reconcile. Active valid subscriptions can independently permit persistence progress for their stream.

## Ordered subscriptions and backpressure

```js
const sub = await client.subscribe({
  stream: 'events',
  getToken, // this view's own JWT provider, granting Subscribe + History
  from: 'earliest', // or { after: savedCursor }, or 'latest'
  follow: true,
  window: 16,
});
for await (const frame of sub) {
  if (frame.type === 'event') {
    await applyApplicationWork(frame);
    sub.ack(frame.cursor); // optional with autoAck: true
    // Save the processed cursor in your application's checkpoint store.
  } else if (frame.type === 'discontinuity') {
    // The previous epoch's unarchived tail is uncertain. lost_count is null.
    // It can precede the requested resume cursor. Do not wait for lost offsets.
  } else if (frame.type === 'caught_up') {
    // Reached the original captured live watermark; follow continues here.
  }
}
```

A subscription is a single-consumer async iterator. By default an event is ACKed when the caller requests its next item, after finishing the preceding iteration's work. Set `autoAck: false` for explicit cumulative ACKs of events already yielded to that iterator. ACKs use actual delivered cursors without parsing epoch/offset strings. `return()`/`close()` cancels the server view. Finite `follow: false` completes at its captured watermark; queued records stay readable and the SDK sends no ACK after completion. `highWatermark` is the captured boundary; `checkpoint` is the latest processed/ACKed event (or latest's initial start boundary).

Reconnect discards unconsumed buffered frames, fetches a fresh JWT per scope, and resumes exclusively from the processed checkpoint. Scoped providers restart independently so one stalled issuer cannot block healthy subscriptions behind the opening auth deadline. Recent processed cursors suppress overlap when an ACK races a reconnect; duplicates can still repeat across process restarts or application checkpoint failure. No server-side or durable SDK checkpoint store is provided. Boundary frames can repeat across reconnect until a later event is processed. The SDK never fabricates a cursor or silently switches a failed historical resume to latest.

**History permission is required for reconnect resume**, including a subscription originally started at latest. A subscribe-only token can follow its existing view but cannot create an `after` view after reconnect. If resume is forbidden, the SDK ends the subscription explicitly and emits `subscriptionError`. Choose `reconnect: false` for live-only sessions that cannot resume. Initial missing streams also fail explicitly. Only typed `unavailable`/`rate_limited` subscription rejections are retried (within the request deadline); they mean no view was created. This supports owner initialization during stop-then-start upgrades.

## Refresh, cancellation, and limits

`getToken({ signal, stream, subscription })` runs independently for each scoped subscription start, refresh and reconnect; publish callbacks run per publish. Put the callback on the operation: Use its AbortSignal for HTTP requests:

```js
getToken: async ({ signal, stream }) => {
  const response = await fetch('/api/highway-token', {
    method: 'POST', credentials: 'same-origin', signal,
    headers: { 'content-type': 'application/json' }, body: JSON.stringify({ stream }),
  });
  if (!response.ok) throw new Error('Token request failed');
  return (await response.json()).token;
}
```

Subject and tenant stay fixed for one client instance across refresh and reconnect; create a new instance for another identity. Use your own authenticated backend endpoint; do not embed a signing key in a client. Replace token files atomically or issue fresh backend tokens before expiry. Refresh happens before token expiry and at most 25 seconds apart by default, also supplying an application command to meet Highway's idle policy. `sub.refresh()` requests an immediate replacement for that view. The callback wait is bounded by `requestTimeoutMs` and its signal is aborted on completion, timeout, cancellation or disconnect. A rejected replacement never extends the old token; server capability removal ends the affected subscription. Scoped request timeouts cancel that view while siblings continue; transport failure can still reconnect the shared socket. `close()` cancels refresh/reconnect timers, aborts the token-provider signal, fails pending operations, and closes the socket. A provider should honor cancellation and impose its own HTTP timeout.

| Option | Default | Bound |
| --- | --- | --- |
| `maxPending` correlated commands | 32 | 128 |
| `maxSubscriptions` | 64 | 64 |
| `maxBufferedBytes` receive/shared socket-send budget | 1 MiB | 16 MiB |
| `requestTimeoutMs` | 10,000 | 300,000 |
| `refreshMs` | 25,000 | 25,000 |
| `retryMs` initial reconnect delay | 250 | 5,000 |
| subscription `window` | 128 | 1,024 |

Reconnect delay backs off to five seconds. There is no offline publish queue. Incoming frames are capped at 256 KiB. Each subscription queues at most its window plus 16 control frames, within the shared byte cap. Exhaustion cancels that subscription with `slow_consumer`, rather than discarding events. Pending commands and transport buffering also reject overload explicitly. Concurrent calls to an iterator's `next()` are rejected. The application's own retained objects/checkpoint storage are outside these limits.

Transport errors and handshake deadlines detach the failed socket and start one reconnect even if the WebSocket implementation never emits `close` (including failed upgrades in Node 22). Later callbacks from that socket are ignored. A sent publish still fails as `publish_uncertain` and is never replayed.

In v2 the `connected` event contains `{ type: "connected", version: 2 }` and indicates transport readiness, not stream authority. In v1 it contains the authenticated identity.

Events: `connected`, `disconnected`, `reconnecting`, `accepted`, `persisted`, `caughtUp`, `subscriptionError`, `error`. `on(name, callback)` returns an unsubscribe function. Listener exceptions are isolated; callbacks should handle application errors themselves. Use the async iterator for processing/credit rather than an unbounded event-handler backlog. The [browser example](../../examples/browser/index.html) shares this SDK and keeps at most 40 preview rows.

## Protocol v1 compatibility

Providing a constructor-level callback keeps the original connection-wide API:

```js
const legacy = new HighwayClient({ url, getToken: getCombinedJwt });
const sub = await legacy.subscribe({ stream: 'events', from: 'earliest' });
await legacy.refresh();
```

V1 refresh replaces the whole connection token and its expiry closes the connection. Do not pass scoped callbacks in this mode: the SDK rejects them instead of ignoring credentials. Conversely a v2 client requires per-operation callbacks, and its `client.refresh()` rejects with `subscription_auth_required`; use `sub.refresh()`. Modes cannot be mixed on one socket. See [Decision 005](../../docs/decisions/005-scoped-stream-auth.md).

## Verification

`make check` runs the SDK's Node unit tests alongside Gleam tests. `make sdk-check` installs the locked development-only TypeScript compiler, checks declarations with a typed consumer, and runs the unit tests. `make smoke-release` uses the actual SDK through the native shipment, S3 fixture, SIGTERM drain, restart/resume, and finite replay. `make smoke-scoped` uses two independently signed streams on one socket through refresh, expiry/revocation, paused head commits and restart/resume. `make compose-smoke` exercises v2 through real MinIO signatures, outages and both-service restart. `npm pack --dry-run` checks distributable files. No publishing credentials or runtime npm dependencies are required.
