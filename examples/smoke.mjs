// Node 22+; uses built-in crypto, fetch and WebSocket. No private key is saved.
import { generateKeyPairSync, sign } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import assert from 'node:assert/strict';
import { setTimeout as delay } from 'node:timers/promises';

const port = Number(process.env.SMOKE_PORT || 4401);
const base = `http://127.0.0.1:${port}`;
const temporary = await mkdtemp(join(tmpdir(), 'highway-smoke-'));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const jwk = { ...publicKey.export({ format: 'jwk' }), kid: 'smoke', alg: 'EdDSA' };
await writeFile(join(temporary, 'jwks.json'), JSON.stringify({ keys: [jwk] }));
const encode = value => Buffer.from(JSON.stringify(value)).toString('base64url');
function token({ stream = 'demo/events', actions = ['publish', 'subscribe'], tenant = 'demo', lifetime = 60 } = {}) {
  const now = Math.floor(Date.now() / 1000);
  const input = `${encode({ alg: 'EdDSA', kid: 'smoke' })}.${encode({ iss: 'smoke-issuer', aud: 'highway-smoke', sub: 'smoke-user', tenant, iat: now, exp: now + lifetime, grants: [{ stream, actions }, { stream: 'demo/second', actions }] })}`;
  return `${input}.${sign(null, Buffer.from(input), privateKey).toString('base64url')}`;
}
class Client {
  constructor(ws) {
    this.ws = ws; this.queue = []; this.all = []; this.waiters = [];
    this.closed = new Promise(resolve => ws.addEventListener('close', e => resolve(e.code)));
    ws.addEventListener('message', e => {
      const frame = JSON.parse(e.data); this.all.push(frame); this.queue.push(frame); this.flush();
    });
  }
  flush() {
    for (const w of [...this.waiters]) {
      const at = this.queue.findIndex(w.match);
      if (at >= 0) { clearTimeout(w.timer); this.waiters.splice(this.waiters.indexOf(w), 1); w.resolve(this.queue.splice(at, 1)[0]); }
    }
  }
  wait(type, id, timeout = 7000) {
    return new Promise((resolve, reject) => {
      const w = { match: f => f.type === type && (id === undefined || f.id === id), resolve };
      w.timer = setTimeout(() => { this.waiters.splice(this.waiters.indexOf(w), 1); reject(new Error(`Timed out waiting for ${type} ${id || ''}; received types: ${this.all.map(x => x.type).join(',')}`)); }, timeout);
      this.waiters.push(w); this.flush();
    });
  }
  send(frame) { this.ws.send(JSON.stringify(frame)); }
  async auth(jwt = token(), id = 'auth') { this.send({ type: 'auth', id, token: jwt }); return this.wait('authenticated', id); }
  publish(id, stream = 'demo/events') { this.send({ type: 'publish', id, stream, message_id: id, data: { text: id } }); return this.wait('accepted', id); }
}
const clients = [];
async function connect(auth = true, jwt) {
  const ws = new WebSocket(`ws://127.0.0.1:${port}/v1/ws`);
  const c = new Client(ws); clients.push(c);
  ws.addEventListener('error', () => {});
  const hello = await c.wait('hello');
  assert.equal(hello.storage, 'development_memory'); assert.equal(hello.durable, false);
  if (auth) await c.auth(jwt);
  return c;
}
let output = '';
const server = spawn('gleam', ['run'], { cwd: new URL('..', import.meta.url), env: { ...process.env,
  ERL_COMPILER_OPTIONS: '[nowarn_deprecated_catch]', HIGHWAY_STORAGE_MODE: 'development_memory', HIGHWAY_PORT: String(port),
  HIGHWAY_JWKS_FILE: join(temporary, 'jwks.json'), HIGHWAY_JWT_ISSUER: 'smoke-issuer', HIGHWAY_JWT_AUDIENCE: 'highway-smoke' }, detached: true, stdio: ['ignore', 'pipe', 'pipe'] });
for (const stream of [server.stdout, server.stderr]) stream.on('data', b => { output = (output + b).slice(-12000); });
try {
  let ready = false;
  for (let i = 0; i < 200; i++) {
    if (server.exitCode !== null) throw new Error(`Server exited (${server.exitCode}): ${output}`);
    try { ready = (await fetch(`${base}/healthz`)).status === 200; } catch {}
    if (ready) break;
    await delay(50);
  }
  assert(ready, 'Server did not start');
  assert.equal((await fetch(`${base}/readyz`)).status, 503);
  assert.equal((await fetch(`${base}/v1/ws`, { headers: { origin: 'https://evil.example' } })).status, 403);
  assert.equal((await fetch(`${base}/v1/ws?token=forbidden`)).status, 403);
  const publisher = await connect();
  const subscriber = await connect();
  await publisher.publish('seed');
  subscriber.send({ type: 'subscribe', id: 'subscribe', subscription: 'live', stream: 'demo/events', window: 128 });
  const subscribed = await subscriber.wait('subscribed', 'subscribe');
  await subscriber.wait('caught_up');
  const accepts = [];
  // Both clients publish on the same stream; a single owner orders all events.
  for (let i = 0; i < 20; i++) accepts.push((i % 2 ? subscriber : publisher).publish(`p${i}`));
  const accepted = await Promise.all(accepts);
  const received = [];
  for (let i = 0; i < accepted.length; i++) {
    const event = await subscriber.wait('event'); received.push(event);
    subscriber.send({ type: 'ack', subscription: 'live', through: event.cursor });
  }
  const offset = c => BigInt(c.split(':')[3]);
  assert.deepEqual(received.map(x => offset(x.cursor)), Array.from({ length: 20 }, (_, i) => offset(subscribed.high_watermark) + BigInt(i + 1)));
  assert.deepEqual(new Set(received.map(x => x.cursor)), new Set(accepted.map(x => x.cursor)));
  assert(received.every(x => x.data.text === x.message_id));
  publisher.send({ type: 'publish', id: 'forbidden', stream: 'other', message_id: 'x', data: null });
  assert.equal((await publisher.wait('error', 'forbidden')).code, 'forbidden');
  subscriber.send({ type: 'subscribe', id: 'history', subscription: 'old', stream: 'demo/events', from: 'earliest' });
  assert.equal((await subscriber.wait('error', 'history')).code, 'forbidden');
  const otherTenant = await connect(true, token({ tenant: 'other' }));
  otherTenant.send({ type: 'subscribe', id: 'tenant', subscription: 'x', stream: 'demo/events' });
  assert.equal((await otherTenant.wait('error', 'tenant')).code, 'stream_not_found');
  otherTenant.ws.close();
  // The same socket can follow a second stream without changing event routing.
  await publisher.publish('second-seed', 'demo/second');
  subscriber.send({ type: 'subscribe', id: 'second', subscription: 'second', stream: 'demo/second' });
  await subscriber.wait('subscribed', 'second'); await subscriber.wait('caught_up');
  await publisher.publish('second-live', 'demo/second');
  const multiplexed = await subscriber.wait('event');
  assert.equal(multiplexed.subscription, 'second'); assert.equal(multiplexed.stream, 'demo/second');
  subscriber.send({ type: 'ack', subscription: 'second', through: multiplexed.cursor });
  // Refresh removes both existing live subscriptions immediately.
  await subscriber.auth(token({ actions: ['publish'] }), 'refresh');
  assert.deepEqual(new Set([(await subscriber.wait('error')).subscription, (await subscriber.wait('error')).subscription]), new Set(['live', 'second']));
  const slow = await connect();
  slow.send({ type: 'subscribe', id: 'slow-sub', subscription: 'slow', stream: 'demo/events', window: 1 });
  await slow.wait('subscribed', 'slow-sub'); await slow.wait('caught_up');
  const fast = await connect();
  fast.send({ type: 'subscribe', id: 'fast-sub', subscription: 'fast', stream: 'demo/events' });
  await fast.wait('subscribed', 'fast-sub'); await fast.wait('caught_up');
  await publisher.publish('unacked'); await slow.wait('event');
  await publisher.publish('still-responsive');
  const fastEvents = [await fast.wait('event'), await fast.wait('event')];
  assert.deepEqual(fastEvents.map(x => x.message_id), ['unacked', 'still-responsive']);
  fast.send({ type: 'ack', subscription: 'fast', through: fastEvents[1].cursor });
  // Expiry and auth deadlines are enforced on idle sockets, alongside slow credit.
  const expired = await connect(true, token({ lifetime: 2 }));
  expired.send({ type: 'subscribe', id: 'expiring-sub', subscription: 'expiring', stream: 'demo/events' });
  await expired.wait('subscribed', 'expiring-sub'); await expired.wait('caught_up');
  const invalid = await connect(false);
  const parts = token().split('.'); parts[2] = (parts[2][0] === 'A' ? 'B' : 'A') + parts[2].slice(1);
  invalid.send({ type: 'auth', id: 'invalid', token: parts.join('.') });
  assert.equal((await invalid.wait('error', 'invalid')).code, 'unauthenticated');
  assert.equal(await invalid.closed, 1008);
  const unauthenticated = await connect(false);
  assert.equal((await expired.wait('error')).code, 'token_expired');
  assert.equal(await expired.closed, 1008);
  assert.equal((await slow.wait('error')).code, 'slow_consumer');
  assert.equal((await unauthenticated.wait('error')).code, 'unauthenticated');
  assert.equal(await unauthenticated.closed, 1008);
  const oversized = await connect();
  oversized.ws.send('x'.repeat(300000));
  assert.equal(await oversized.closed, 1009);
  assert(clients.every(c => c.all.every(f => f.type !== 'persisted')));
  console.log('PASS: two-client ordered live delivery and multiplexing, real JWT rejection/tenant isolation, refresh, active expiry/auth deadline, isolated slow consumer, frame cap; no persisted responses.');
} catch (error) {
  console.error(output); throw error;
} finally {
  for (const c of clients) c.ws.close();
  try { process.kill(-server.pid, 'SIGTERM'); } catch {}
  await Promise.race([new Promise(resolve => server.once('exit', resolve)), delay(3000)]);
  try { process.kill(-server.pid, 'SIGKILL'); } catch {}
  await rm(temporary, { recursive: true, force: true });
}
