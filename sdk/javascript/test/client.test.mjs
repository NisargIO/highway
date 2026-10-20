import { test } from 'node:test';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';
import { HighwayClient, HighwayError, PublishUncertainError } from '../index.js';

class Socket extends EventTarget {
  static all = [];
  static behavior = () => {};
  constructor() { super(); this.readyState = 1; this.sent = []; Socket.all.push(this); queueMicrotask(() => this.frame({ type: 'hello', version: 1 })); }
  frame(f) { if (this.readyState === 1) this.dispatchEvent(new MessageEvent('message', { data: JSON.stringify(f) })); }
  send(text) {
    const f = JSON.parse(text); this.sent.push(f);
    if (Socket.behavior(this, f) === false) return;
    if (f.type === 'auth') queueMicrotask(() => this.frame({ type: 'authenticated', id: f.id, tenant: 't', subject: 'u', expires_at: Math.floor(Date.now() / 1000) + 60 }));
    if (f.type === 'subscribe') queueMicrotask(() => this.frame({ type: 'subscribed', id: f.id, subscription: f.subscription, high_watermark: 'opaque-H' }));
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
