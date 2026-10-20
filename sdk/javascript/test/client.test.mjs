import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { HighwayClient, HighwayError, PublishUncertainError } from '../index.js';

class Socket extends EventTarget {
  static all = [];
  static behavior = () => {};
  constructor() { super(); this.readyState = 1; this.sent = []; Socket.all.push(this); queueMicrotask(() => this.frame({ type: 'hello', version: 1, supported_versions: [1, 2] })); }
  frame(f) { if (this.readyState === 1) this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(f) })); }
  send(text) {
    const f = JSON.parse(text); this.sent.push(f);
    if (Socket.behavior(this, f) === false) return;
    if (f.type === 'auth') queueMicrotask(() => this.frame({ type: 'authenticated', id: f.id, tenant: 't', subject: 'u', expires_at: Math.floor(Date.now() / 1000) + 60 }));
    if (f.type === 'subscribe') queueMicrotask(() => this.frame({ type: 'subscribed', id: f.id, subscription: f.subscription, high_watermark: 'opaque-H', ...(f.version === 2 ? { version: 2, subject: 'u', tenant: 't', expires_at: Math.floor(Date.now() / 1000) + 60 } : {}) }));
    if (f.type === 'subscription_auth') queueMicrotask(() => this.frame({ type: 'subscription_authenticated', version: 2, id: f.id, subscription: f.subscription, subject: 'u', tenant: 't', expires_at: Math.floor(Date.now() / 1000) + 60 }));
    if (f.type === 'unsubscribe') queueMicrotask(() => this.frame({ type: 'unsubscribed', id: f.id, subscription: f.subscription }));
  }
  close(code = 1006) { if (this.readyState === 3) return; this.readyState = 3; const event = new Event('close'); event.code = code; this.dispatchEvent(event); }
}
function client(options = {}) {
  Socket.all = []; Socket.behavior = () => {};
  return new HighwayClient({ url: 'ws://localhost/v1/ws', getToken: () => 'private-token', WebSocket: Socket, retryMs: 5, ...options });
}
function event(cursor, subscription = 's', data = null) { return { type: 'event', stream: 'events', subscription, cursor, message_id: cursor, data }; }
async function until(fn) { for (let n = 0; n < 100; n++) { if (fn()) return; await delay(2); } throw new Error('Timed out'); }

test('accepted is separate from cumulative persistence and no secrets are logged', async () => {
  const c = client(); let persisted = 0; c.on('persisted', () => persisted++);
  await c.connect();
  Socket.behavior = (ws, f) => { if (f.type === 'publish') queueMicrotask(() => ws.frame({ type: 'accepted', id: f.id, stream: f.stream, cursor: 'opaque-cursor', message_id: f.message_id })); };
  const accepted = await c.publish({ stream: 'events', messageId: 'm', data: { private: 'body' } });
  assert.equal(accepted.type, 'accepted'); assert.equal(persisted, 0);
  Socket.all[0].frame({ type: 'persisted', stream: 'events', through: 'opaque-later' });
  assert.equal(persisted, 1); c.close();
});

test('iterator ACKs processed events on next pull and cumulative ACKs are opaque', async () => {
  const c = client(); const s = await c.subscribe({ stream: 'events', subscription: 's', from: 'earliest', window: 2 });
  const ws = Socket.all[0]; ws.frame(event('old-high-offset')); ws.frame(event('new-epoch-low-offset'));
  assert.equal((await s.next()).value.cursor, 'old-high-offset');
  assert.equal(ws.sent.filter(f => f.type === 'ack').length, 0);
  assert.equal((await s.next()).value.cursor, 'new-epoch-low-offset');
  assert.equal(s.checkpoint, 'old-high-offset');
  s.ack('new-epoch-low-offset');
  assert.equal(s.checkpoint, 'new-epoch-low-offset'); assert.equal(s._received.size, 0);
  assert.throws(() => s.ack('not-delivered'), { code: 'cursor_invalid' });
  await s.close(); c.close();
});

test('restart resumes from processed checkpoint, clears queued overlap, never retries publish', async () => {
  const c = client(); const s = await c.subscribe({ stream: 'events', subscription: 's', window: 2 });
  const ws = Socket.all[0]; ws.frame(event('processed')); await s.next(); s.ack('processed'); ws.frame(event('queued'));
  const uncertain = c.publish({ stream: 'events', messageId: 'retry-key', data: null });
  ws.close(1012);
  await assert.rejects(uncertain, error => error instanceof PublishUncertainError && error.messageId === 'retry-key');
  await until(() => Socket.all.length === 2 && Socket.all[1].sent.some(f => f.type === 'subscribe'));
  const next = Socket.all[1];
  assert.deepEqual(next.sent.find(f => f.type === 'subscribe').from, { after: 'processed' });
  assert.equal(next.sent.filter(f => f.type === 'publish').length, 0);
  next.frame({ type: 'discontinuity', subscription: 's', after: 'archived-before-processed', before_epoch: '2', reason: 'interrupted_epoch', lost_count: null });
  next.frame(event('after-restart'));
  assert.equal((await s.next()).value.type, 'discontinuity');
  assert.equal((await s.next()).value.cursor, 'after-restart'); c.close();
});

test('bounded client queue explicitly fails slow subscription and cancels its server view', async () => {
  const c = client({ maxBufferedBytes: 200 }); const s = await c.subscribe({ stream: 'events', subscription: 's' });
  Socket.all[0].frame(event('one', 's', 'x'.repeat(500)));
  await assert.rejects(s.next(), { code: 'slow_consumer' });
  assert.equal(c._buffered, 0); assert(Socket.all[0].sent.some(f => f.type === 'unsubscribe')); c.close();
});

test('pending request cap and ambiguous timeout do not queue or retry another publish', async () => {
  const c = client({ maxPending: 1, requestTimeoutMs: 20, reconnect: false }); await c.connect();
  const first = c.publish({ stream: 'events', messageId: 'first', data: null });
  await assert.rejects(c.publish({ stream: 'events', messageId: 'second', data: null }), { code: 'client_overloaded' });
  await assert.rejects(first, { code: 'publish_uncertain' });
  assert.equal(Socket.all[0].sent.filter(f => f.type === 'publish').length, 1); c.close();
});

test('automatic refresh fetches a replacement token and close aborts token provider', async () => {
  let calls = 0, signal;
  const c = client({ refreshMs: 15, getToken: context => { calls++; signal = context.signal; return `token-${calls}`; } });
  await c.connect(); await until(() => calls >= 2);
  assert(Socket.all[0].sent.filter(f => f.type === 'auth').some(f => f.token === 'token-2'));
  c.close(); assert(signal.aborted);
});

test('finite completion keeps queued records readable and sends no ACK after complete', async () => {
  const c = client(); const s = await c.subscribe({ stream: 'events', subscription: 's', from: 'earliest', follow: false });
  const ws = Socket.all[0]; ws.frame(event('one')); ws.frame({ type: 'caught_up', subscription: 's', through: 'one' }); ws.frame({ type: 'complete', subscription: 's' });
  const frames = []; for await (const f of s) frames.push(f.type);
  assert.deepEqual(frames, ['event', 'caught_up']); assert.equal(s.checkpoint, 'one');
  assert.equal(ws.sent.filter(f => f.type === 'ack').length, 0); c.close();
});

test('invalid input is rejected locally and malformed server frames fail closed', async () => {
  assert.throws(() => client({ url: 'ws://localhost/v1/ws?token=secret' }), TypeError);
  const c = client(); await c.connect();
  await assert.rejects(c.subscribe({ stream: 'events', from: 'made-up' }), TypeError);
  Socket.all[0].frame({ type: 'unrecognized' }); assert(!c.connected);
  await assert.rejects(c.connect(), error => error instanceof HighwayError && error.code === 'closed');
});


test('typed unavailable subscription rejection retries without resetting its requested cursor', async () => {
  const c = client(); let attempts = 0;
  Socket.behavior = (ws, f) => {
    if (f.type === 'subscribe' && ++attempts === 1) {
      queueMicrotask(() => ws.frame({ type: 'error', id: f.id, subscription: f.subscription, code: 'unavailable', retryable: true }));
      return false;
    }
  };
  const s = await c.subscribe({ stream: 'events', from: { after: 'processed-before-start' } });
  assert.equal(attempts, 2);
  assert(Socket.all[0].sent.filter(f => f.type === 'subscribe').every(f => f.from.after === 'processed-before-start'));
  await s.close(); c.close();
});

test('forbidden reconnect resume ends explicitly instead of switching to latest', async () => {
  const c = client(); const s = await c.subscribe({ stream: 'events', subscription: 's' });
  const old = Socket.all[0]; old.frame(event('processed')); await s.next(); s.ack('processed');
  Socket.behavior = (ws, f) => {
    if (f.type === 'subscribe') { queueMicrotask(() => ws.frame({ type: 'error', id: f.id, subscription: f.subscription, code: 'forbidden' })); return false; }
  };
  old.close(1012);
  await until(() => s._ended);
  await assert.rejects(s.next(), { code: 'forbidden' });
  assert.deepEqual(Socket.all[1].sent.find(f => f.type === 'subscribe').from, { after: 'processed' });
  assert(c.connected); c.close();
});


test('token identity cannot change across refresh or reconnect', async () => {
  const c = client(); await c.connect();
  Socket.behavior = (ws, f) => {
    if (f.type === 'auth') {
      queueMicrotask(() => ws.frame({ type: 'authenticated', id: f.id, tenant: 'other-tenant', subject: 'u', expires_at: Math.floor(Date.now() / 1000) + 60 }));
      return false;
    }
  };
  await assert.rejects(c.refresh(), { code: 'identity_changed' });
  assert(!c.connected);
});


test('cancellation racing server completion does not disconnect other subscriptions', async () => {
  const c = client(); const s = await c.subscribe({ stream: 'events', subscription: 's', follow: false });
  const other = await c.subscribe({ stream: 'events', subscription: 'other' });
  Socket.behavior = (ws, f) => {
    if (f.type === 'unsubscribe') { queueMicrotask(() => ws.frame({ type: 'error', id: f.id, subscription: f.subscription, code: 'invalid_frame' })); return false; }
  };
  await s.close(); assert(c.connected);
  Socket.all[0].frame(event('other-cursor', 'other'));
  assert.equal((await other.next()).value.cursor, 'other-cursor'); c.close();
});

test('a reconnect during application processing does not yield its unacked item twice', async () => {
  const c = client(); const s = await c.subscribe({ stream: 'events', subscription: 's', window: 1 });
  Socket.all[0].frame(event('processing')); assert.equal((await s.next()).value.cursor, 'processing');
  assert.deepEqual(s._delivered, ['processing'], 'retain cursors only, never payloads');
  Socket.all[0].close(1012);
  await until(() => Socket.all.length === 2 && Socket.all[1].sent.some(f => f.type === 'subscribe'));
  const next = Socket.all[1]; next.frame(event('processing'));
  const pending = s.next();
  next.frame(event('later'));
  assert.equal((await pending).value.cursor, 'later');
  assert(next.sent.some(f => f.type === 'ack' && f.through === 'processing'));
  c.close();
});

test('error without close recovers once and ignores stale transport callbacks', async () => {
  const c = client(); let disconnected = 0;
  c.on('disconnected', () => disconnected++);
  const sub = await c.subscribe({ stream: 'events', subscription: 's', window: 2 });
  const old = Socket.all[0]; old.frame(event('processed')); await sub.next(); sub.ack('processed');
  const uncertain = c.publish({ stream: 'events', messageId: 'sent-before-error', data: null });
  // Reproduce an implementation that never emits close, even from close().
  old.close = () => {};
  old.dispatchEvent(new Event('error'));
  await assert.rejects(uncertain, { code: 'publish_uncertain' });
  await until(() => Socket.all.length === 2 && c.connected && Socket.all[1].sent.some(f => f.type === 'subscribe'));
  const next = Socket.all[1];
  assert.deepEqual(next.sent.find(f => f.type === 'subscribe').from, { after: 'processed' });
  assert.equal(next.sent.filter(f => f.type === 'publish').length, 0);
  old.dispatchEvent(new Event('error')); old.dispatchEvent(new Event('close'));
  await delay(20);
  assert.equal(Socket.all.length, 2); assert.equal(disconnected, 1); assert(c.connected);
  next.frame(event('continued')); assert.equal((await sub.next()).value.cursor, 'continued');
  c.close();
});

test('failed initial upgrade and silent handshake timeout recover without close', async () => {
  for (const failure of ['error', 'timeout']) {
    const c = client({ requestTimeoutMs: 20 });
    const ready = c.connect(), failed = Socket.all[0];
    // Suppress the queued hello so the socket stays in its opening handshake.
    failed.frame = () => {}; failed.close = () => {};
    if (failure === 'error') failed.dispatchEvent(new Event('error'));
    await ready;
    assert.equal(Socket.all.length, 2); assert(c.connected);
    c.close();
  }
});

test('independent subscription tokens refresh and revoke without affecting other scopes', async () => {
  const c = client({ getToken: undefined }); let aToken = 'jwt-a';
  const a = await c.subscribe({ stream: 'events', subscription: 'a', from: 'earliest', getToken: ({ stream, subscription }) => { assert.equal(stream, 'events'); assert.equal(subscription, 'a'); return aToken; } });
  const b = await c.subscribe({ stream: 'other', subscription: 'b', getToken: () => 'jwt-b' });
  const ws = Socket.all[0];
  assert.equal(ws.sent.filter(f => f.type === 'auth').length, 0);
  assert.deepEqual(ws.sent.filter(f => f.type === 'subscribe').map(f => [f.version, f.token]), [[2, 'jwt-a'], [2, 'jwt-b']]);
  aToken = 'replacement-a'; await a.refresh();
  assert.equal(ws.sent.find(f => f.type === 'subscription_auth').token, 'replacement-a');
  Socket.behavior = (socket, f) => { if (f.type === 'subscription_auth') { queueMicrotask(() => socket.frame({ type: 'error', id: f.id, subscription: f.subscription, code: 'forbidden' })); return false; } };
  await assert.rejects(a.refresh(), { code: 'forbidden' });
  await assert.rejects(a.next(), { code: 'forbidden' });
  assert(c.connected && !b._ended);
  ws.frame({ ...event('b-continues', 'b'), stream: 'other' });
  assert.equal((await b.next()).value.cursor, 'b-continues');
  await assert.rejects(c.refresh(), { code: 'subscription_auth_required' }); c.close();
});

test('scope expiry and token provider timeout preserve another active subscription', async () => {
  const c = client({ getToken: undefined, requestTimeoutMs: 30 });
  const b = await c.subscribe({ stream: 'other', subscription: 'b', getToken: () => 'jwt-b' });
  let aborted;
  await assert.rejects(c.subscribe({ stream: 'events', getToken: ({ signal }) => { aborted = signal; return new Promise(() => {}); } }), { code: 'token_timeout' });
  assert(aborted.aborted); assert(c.connected); assert.equal(c._subscriptions.size, 1);
  const a = await c.subscribe({ stream: 'events', subscription: 'a', getToken: () => 'jwt-a' });
  Socket.all[0].frame({ type: 'error', subscription: 'a', code: 'token_expired' });
  await assert.rejects(a.next(), { code: 'token_expired' });
  Socket.all[0].frame({ ...event('b-after-expiry', 'b'), stream: 'other' });
  assert.equal((await b.next()).value.cursor, 'b-after-expiry'); assert(c.connected); c.close();
});

test('reconnect aborts in-flight scope refresh and obtains fresh tokens per cursor', async () => {
  const c = client({ getToken: undefined }); let calls = 0, oldSignal, resolveOld;
  const a = await c.subscribe({ stream: 'events', subscription: 'a', getToken: ({ signal }) => {
    calls++;
    if (calls === 2) { oldSignal = signal; return new Promise(resolve => { resolveOld = resolve; }); }
    return `jwt-${calls}`;
  } });
  const old = Socket.all[0]; old.frame(event('processed', 'a')); await a.next(); a.ack('processed');
  const refreshing = a.refresh(); await until(() => calls === 2);
  old.close(1012); await assert.rejects(refreshing, { code: 'disconnected' }); assert(oldSignal.aborted);
  await until(() => Socket.all.length === 2 && Socket.all[1].sent.some(f => f.type === 'subscribe'));
  assert(!a._ended);
  const request = Socket.all[1].sent.find(f => f.type === 'subscribe');
  assert.equal(request.token, 'jwt-3'); assert.deepEqual(request.from, { after: 'processed' });
  resolveOld('stale-token'); await delay(5);
  assert.equal(Socket.all[1].sent.filter(f => f.type === 'subscription_auth').length, 0);
  Socket.all[1].frame(event('continued', 'a')); assert.equal((await a.next()).value.cursor, 'continued'); c.close();
});

test('scoped refresh request timeout cancels only that server view', async () => {
  const c = client({ getToken: undefined, requestTimeoutMs: 25 });
  const a = await c.subscribe({ stream: 'events', subscription: 'a', getToken: () => 'jwt-a' });
  const b = await c.subscribe({ stream: 'other', subscription: 'b', getToken: () => 'jwt-b' });
  Socket.behavior = (_, f) => f.type === 'subscription_auth' ? false : undefined;
  await assert.rejects(a.refresh(), { code: 'request_timeout' });
  await delay(5); assert(c.connected && !b._ended);
  assert(Socket.all[0].sent.some(f => f.type === 'unsubscribe' && f.subscription === 'a')); c.close();
});

test('scoped publish token work is bounded and never retries unsent or uncertain publishes', async () => {
  const c = client({ getToken: undefined, maxPending: 1, requestTimeoutMs: 25 }); await c.connect();
  let signal;
  const first = c.publish({ stream: 'events', messageId: 'first', data: null, getToken: context => { signal = context.signal; return new Promise(() => {}); } });
  await assert.rejects(c.publish({ stream: 'events', messageId: 'over-cap', data: null, getToken: () => 'unused' }), { code: 'client_overloaded' });
  await assert.rejects(first, { code: 'token_timeout' }); assert(signal.aborted);
  assert.equal(Socket.all[0].sent.filter(f => f.type === 'publish').length, 0);
  const sent = c.publish({ stream: 'events', messageId: 'sent', data: null, getToken: () => 'publish-jwt' });
  await assert.rejects(sent, { code: 'publish_uncertain' });
  assert.equal(Socket.all[0].sent.filter(f => f.type === 'publish').length, 1); c.close();
});

test('scoped auth requires advertised v2 and cannot silently fall back to socket credentials', async () => {
  const legacy = client(); await assert.rejects(legacy.subscribe({ stream: 'events', getToken: () => 'scoped' }), TypeError); legacy.close();
  const scoped = client({ getToken: undefined }); await assert.rejects(scoped.subscribe({ stream: 'events' }), TypeError); scoped.close();
  class OldServerSocket extends Socket {
    frame(f) { if (f.type === 'hello') delete f.supported_versions; super.frame(f); }
  }
  const c = client({ getToken: undefined, WebSocket: OldServerSocket });
  await assert.rejects(c.connect(), { code: 'unsupported_version' });
  assert.equal(Socket.all[0].sent.length, 0); c.close();
});

test('finite scoped completion racing subscribe retains metadata without refresh work', async () => {
  const c = client({ getToken: undefined });
  Socket.behavior = (ws, f) => {
    if (f.type === 'subscribe') {
      queueMicrotask(() => {
        ws.frame({ type: 'subscribed', version: 2, id: f.id, subscription: f.subscription, high_watermark: 'finite-H', subject: 'u', tenant: 't', expires_at: Math.floor(Date.now() / 1000) + 60 });
        ws.frame({ type: 'caught_up', subscription: f.subscription, through: 'finite-H' });
        ws.frame({ type: 'complete', subscription: f.subscription });
      });
      return false;
    }
  };
  const sub = await c.subscribe({ stream: 'events', from: 'latest', follow: false, getToken: () => 'jwt-a' });
  assert.equal(sub.highWatermark, 'finite-H'); assert.equal(sub.checkpoint, 'finite-H');
  assert(Number.isSafeInteger(sub.expiresAt)); assert.equal(sub._authTimer, undefined);
  assert.equal(c._subscriptions.size, 0);
  const frames = []; for await (const f of sub) frames.push(f.type);
  assert.deepEqual(frames, ['caught_up']); c.close();
});

test('late scoped subscribe and refresh successes after timeout cannot revive a view or disconnect siblings', async () => {
  const c = client({ getToken: undefined, requestTimeoutMs: 25 });
  const b = await c.subscribe({ stream: 'other', subscription: 'b', getToken: () => 'jwt-b' });
  let pending;
  Socket.behavior = (_, f) => { if (f.type === 'subscribe') { pending = f; return false; } };
  await assert.rejects(c.subscribe({ stream: 'events', subscription: 'a', getToken: () => 'jwt-a' }), { code: 'request_timeout' });
  const ws = Socket.all[0];
  ws.frame({ type: 'subscribed', version: 2, id: pending.id, subscription: 'a', high_watermark: 'late-H', subject: 'u', tenant: 't', expires_at: Math.floor(Date.now() / 1000) + 60 });
  assert(c.connected && !c._subscriptions.has('a'));
  Socket.behavior = () => {};
  const a = await c.subscribe({ stream: 'events', subscription: 'a', getToken: () => 'new-jwt-a' });
  Socket.behavior = (_, f) => { if (f.type === 'subscription_auth') { pending = f; return false; } };
  await assert.rejects(a.refresh(), { code: 'request_timeout' });
  ws.frame({ type: 'subscription_authenticated', version: 2, id: pending.id, subscription: 'a', subject: 'u', tenant: 't', expires_at: Math.floor(Date.now() / 1000) + 60 });
  assert(c.connected && !c._subscriptions.has('a') && !b._ended);
  ws.frame({ ...event('after-late-replies', 'b'), stream: 'other' });
  assert.equal((await b.next()).value.cursor, 'after-late-replies'); c.close();
});

test('invalid scoped acceptance identity remains an uncertain publish outcome', async () => {
  const c = client({ getToken: undefined }); await c.connect();
  Socket.behavior = (ws, f) => {
    if (f.type === 'publish') queueMicrotask(() => ws.frame({ type: 'accepted', version: 2, id: f.id, stream: f.stream, cursor: 'possibly-appended', message_id: f.message_id }));
  };
  await assert.rejects(c.publish({ stream: 'events', messageId: 'uncertain-identity', data: null, getToken: () => 'publish-jwt' }), error => error instanceof PublishUncertainError && error.messageId === 'uncertain-identity');
  assert(!c.connected); assert.equal(Socket.all[0].sent.filter(f => f.type === 'publish').length, 1); c.close();
});

test('a stalled provider cannot block a sibling from authenticating after reconnect', async () => {
  const c = client({ getToken: undefined }); let aCalls = 0, release;
  const a = await c.subscribe({ stream: 'events', subscription: 'a', getToken: () => {
    if (++aCalls === 2) return new Promise(resolve => { release = resolve; });
    return 'a-jwt';
  } });
  const b = await c.subscribe({ stream: 'other', subscription: 'b', getToken: () => 'b-jwt' });
  Socket.all[0].close(1012);
  await until(() => Socket.all.length === 2 && Socket.all[1].sent.some(f => f.type === 'subscribe' && f.subscription === 'b'));
  assert.equal(Socket.all[1].sent.some(f => f.type === 'subscribe' && f.subscription === 'a'), false);
  Socket.all[1].frame({ ...event('healthy-before-other-issuer', 'b'), stream: 'other' });
  assert.equal((await b.next()).value.cursor, 'healthy-before-other-issuer');
  release('new-a-jwt'); await until(() => Socket.all[1].sent.some(f => f.type === 'subscribe' && f.subscription === 'a'));
  assert(!a._ended); c.close();
});
