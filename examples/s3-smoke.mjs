// Node 22+ and Python 3. Real Highway HTTP/SigV4 adapter, contract fixture only.
import { generateKeyPairSync, sign } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import assert from 'node:assert/strict';
const port = Number(process.env.S3_SMOKE_PORT || 4403), storePort = Number(process.env.S3_TEST_PORT || 4402);
const base = `http://127.0.0.1:${port}`, endpoint = `http://127.0.0.1:${storePort}`;
const temp = await mkdtemp(join(tmpdir(), 'highway-s3-smoke-'));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
await writeFile(join(temp, 'jwks.json'), JSON.stringify({ keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'smoke' }] }));
const b64 = x => Buffer.from(JSON.stringify(x)).toString('base64url');
function token(actions = ['publish', 'subscribe', 'history']) {
  const now = Math.floor(Date.now() / 1000);
  const input = `${b64({ alg: 'EdDSA', kid: 'smoke' })}.${b64({ iss: 'issuer', aud: 'highway', sub: 'user', tenant: 'demo', iat: now, exp: now + 120, grants: [{ stream: 'events', actions }] })}`;
  return `${input}.${sign(null, Buffer.from(input), privateKey).toString('base64url')}`;
}
let output = '', server;
const children = [], clients = [];
function launch(cmd, args, env = {}) {
  const child = spawn(cmd, args, { cwd: new URL('..', import.meta.url), detached: true, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  children.push(child);
  for (const s of [child.stdout, child.stderr]) s.on('data', b => { output = (output + b).slice(-16000); });
  return child;
}
async function stop(child, signal = 'SIGTERM') {
  try { process.kill(-child.pid, signal); } catch {}
  await Promise.race([new Promise(r => child.once('exit', r)), delay(1000)]);
  try { process.kill(-child.pid, 'SIGKILL'); } catch {}
}
async function until(fn, label, attempts = 150) {
  for (let i = 0; i < attempts; i++) { try { const value = await fn(); if (value) return value; } catch {} await delay(30); }
  throw new Error(`Timed out: ${label}`);
}
function startServer() {
  server = launch('gleam', ['run'], {
    ERL_COMPILER_OPTIONS: '[nowarn_deprecated_catch]', HIGHWAY_STORAGE_MODE: 's3_async', HIGHWAY_PORT: String(port),
    HIGHWAY_JWKS_FILE: join(temp, 'jwks.json'), HIGHWAY_JWT_ISSUER: 'issuer', HIGHWAY_JWT_AUDIENCE: 'highway',
    HIGHWAY_S3_BUCKET: 'fixture', HIGHWAY_S3_PREFIX: 'smoke', HIGHWAY_S3_ENDPOINT: endpoint,
    AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: 'fixture-access', AWS_SECRET_ACCESS_KEY: 'fixture-secret', AWS_SESSION_TOKEN: 'fixture-session',
  });
  return until(async () => (await fetch(`${base}/healthz`)).status === 200, 'server startup');
}
class Client {
  constructor(ws) {
    this.ws = ws; this.queue = []; this.all = []; this.waiters = [];
    ws.addEventListener('error', () => {});
    ws.addEventListener('message', e => { const f = JSON.parse(e.data); this.queue.push(f); this.all.push(f); this.flush(); });
  }
  flush() { for (const w of [...this.waiters]) { const n = this.queue.findIndex(w.match); if (n >= 0) { clearTimeout(w.timer); this.waiters.splice(this.waiters.indexOf(w), 1); w.resolve(this.queue.splice(n, 1)[0]); } } }
  wait(match) {
    const failure = new Error(`Timed out waiting: ${match}`);
    return new Promise((resolve, reject) => {
      const w = { match, resolve };
      w.timer = setTimeout(() => {
        this.waiters.splice(this.waiters.indexOf(w), 1);
        // Diagnostics exclude tokens and application payloads.
        failure.message += `; frames ${JSON.stringify(this.all.slice(-12).map(({ type, id, subscription, code, cursor, through }) => ({ type, id, subscription, code, cursor, through })))}`;
        reject(failure);
      }, 7000);
      this.waiters.push(w); this.flush();
    });
  }
  send(f) { this.ws.send(JSON.stringify(f)); }
  async command(f) { this.send(f); return this.wait(x => x.id === f.id); }
  async publish(id) {
    return until(async () => {
      const f = await this.command({ type: 'publish', id, stream: 'events', message_id: id, data: { text: id } });
      if (f.type === 'accepted') return f;
      assert.equal(f.code, 'unavailable'); return false;
    }, 'owner initialization');
  }
}
async function connect(actions) {
  const c = new Client(new WebSocket(`ws://127.0.0.1:${port}/v1/ws`)); clients.push(c);
  const hello = await c.wait(x => x.type === 'hello'); assert.equal(hello.storage, 's3_async'); assert.equal(hello.durable, false);
  assert.equal((await c.command({ type: 'auth', id: 'auth', token: token(actions) })).type, 'authenticated'); return c;
}
const control = data => fetch(`${endpoint}/__control`, { method: 'POST', body: JSON.stringify(data) });
const state = () => fetch(`${endpoint}/__state`).then(r => r.json());
const head = async () => Object.values((await state()).heads)[0];
const progress = (client, cursor) => client.wait(f => f.type === 'persisted' && f.through === cursor);
try {
  launch('python3', ['scripts/s3-test-server.py', '--port', String(storePort)]);
  await until(() => fetch(`${endpoint}/__state`).then(r => r.ok), 'S3 fixture');
  const conformance = launch('gleam', ['run', '-m', 'highway/storage/conformance'], {
    ERL_COMPILER_OPTIONS: '[nowarn_deprecated_catch]', HIGHWAY_S3_CONFORMANCE: '1',
    HIGHWAY_S3_BUCKET: 'fixture', HIGHWAY_S3_PREFIX: 'smoke', HIGHWAY_S3_ENDPOINT: endpoint,
    AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: 'fixture-access', AWS_SECRET_ACCESS_KEY: 'fixture-secret', AWS_SESSION_TOKEN: 'fixture-session',
  });
  const exit = await new Promise(resolve => conformance.once('exit', resolve)); assert.equal(exit, 0);
  await startServer();
  await until(async () => (await fetch(`${base}/readyz`)).status === 200, 'initial storage readiness');
  const publisher = await connect();
  const seed = await publisher.publish('seed'); await progress(publisher, seed.cursor);
  const subscriber = await connect();
  assert.equal((await subscriber.command({ type: 'subscribe', id: 'sub', subscription: 'live', stream: 'events' })).type, 'subscribed');
  await subscriber.wait(f => f.type === 'caught_up');
  await control({ pause_head: true });
  const live = await publisher.publish('live-before-archive');
  const event = await subscriber.wait(f => f.type === 'event'); assert.equal(event.cursor, live.cursor);
  subscriber.send({ type: 'ack', subscription: 'live', through: event.cursor });
  await delay(400);
  assert.equal((await head()).archived, seed.cursor);
  assert(!publisher.all.some(f => f.type === 'persisted' && f.through === live.cursor));
  assert(!subscriber.all.some(f => f.type === 'persisted' && f.through === live.cursor));
  const uploaded = await state(); assert(uploaded.keys.filter(k => k.includes('/segments/')).length >= 2);
  await control({ pause_head: false, lose_next_head_response: true });
  await Promise.all([progress(publisher, live.cursor), progress(subscriber, live.cursor)]);
  assert.equal((await head()).archived, live.cursor);
  assert.equal((await fetch(`${base}/readyz`)).status, 200);
  // Leave accepted live data uncommitted, crash the server, and fence its request.
  await control({ pause_head: true });
  const lost = await publisher.publish('uncertain-tail');
  await subscriber.wait(f => f.type === 'event' && f.cursor === lost.cursor);
  await delay(350);
  await stop(server, 'SIGKILL');
  await startServer();
  const recoveredClient = await connect();
  const next = await recoveredClient.publish('after-restart');
  const oldParts = live.cursor.split(':'), parts = next.cursor.split(':');
  assert.equal(parts[1], oldParts[1]); assert.equal(BigInt(parts[2]), BigInt(oldParts[2]) + 1n); assert.equal(parts[3], '1');
  const recoveredHead = await head();
  assert.equal(recoveredHead.archived, live.cursor);
  const snapshot = await state();
  const boundary = snapshot.indexes[`/fixture/${recoveredHead.index}`].entry;
  assert.equal(boundary.kind, 'interrupted_epoch'); assert.equal(boundary.after, live.cursor); assert.equal(boundary.lost_count, null);
  assert.equal(boundary.before_epoch, parts[2]);
  await control({ pause_head: false });
  await progress(recoveredClient, next.cursor);
  // Replay the recovered archive with one-event credit. Publishing continues
  // while the reader pauses, and multiple commits move the retained cache.
  const replay = await connect();
  const subscribed = await replay.command({ type: 'subscribe', id: 'replay', subscription: 'history', stream: 'events', from: 'earliest', follow: true, window: 1 });
  assert.equal(subscribed.type, 'subscribed'); assert.equal(subscribed.high_watermark, next.cursor);
  const firstReplay = await replay.wait(f => f.type === 'event' && f.subscription === 'history');
  assert.equal(firstReplay.cursor, seed.cursor);
  const later = [];
  for (const id of ['during-replay-1', 'during-replay-2', 'during-replay-3']) {
    const accepted = await recoveredClient.publish(id); later.push(accepted);
    await progress(recoveredClient, accepted.cursor);
  }
  assert(!replay.all.some(f => f.type === 'caught_up' && f.subscription === 'history'));
  const expected = [seed, live, next, ...later];
  replay.send({ type: 'ack', subscription: 'history', through: firstReplay.cursor });
  for (const accepted of expected.slice(1)) {
    const event = await replay.wait(f => f.type === 'event' && f.subscription === 'history');
    assert.equal(event.cursor, accepted.cursor); assert.equal(event.message_id, accepted.message_id);
    replay.send({ type: 'ack', subscription: 'history', through: event.cursor });
  }
  const caughtUp = await replay.wait(f => f.type === 'caught_up' && f.subscription === 'history');
  assert.equal(caughtUp.through, next.cursor);
  const sequence = replay.all.filter(f => f.subscription === 'history' && ['event', 'discontinuity', 'caught_up'].includes(f.type));
  assert.deepEqual(sequence.map(f => f.type), ['event', 'event', 'discontinuity', 'event', 'caught_up', 'event', 'event', 'event']);
  assert.equal(sequence[2].after, live.cursor); assert.equal(sequence[2].lost_count, null);
  assert(!sequence.some(f => f.cursor === lost.cursor));
  // A former live cursor in the uncertain tail resumes through the boundary,
  // rather than waiting forever for a nonexistent next offset in the old epoch.
  const resume = await connect();
  const res = await resume.command({ type: 'subscribe', id: 'resume', subscription: 'resume', stream: 'events', from: { after: lost.cursor }, follow: false, window: 16 });
  assert.equal(res.type, 'subscribed');
  await resume.wait(f => f.type === 'complete' && f.subscription === 'resume');
  const resumed = resume.all.filter(f => f.subscription === 'resume' && ['event', 'discontinuity', 'caught_up'].includes(f.type));
  assert.equal(resumed[0].type, 'discontinuity'); assert.equal(resumed[0].after, live.cursor);
  assert.deepEqual(resumed.filter(f => f.type === 'event').map(f => f.cursor), [next, ...later].map(f => f.cursor));
  assert.equal(resumed.at(-1).through, later.at(-1).cursor);
  // Latest needs only Subscribe, including fetching its post-start backlog from
  // S3 after cache eviction. History from the same token remains forbidden.
  const latest = await connect(['subscribe']);
  assert.equal((await latest.command({ type: 'subscribe', id: 'denied', subscription: 'denied', stream: 'events', from: 'earliest' })).code, 'forbidden');
  assert.equal((await latest.command({ type: 'subscribe', id: 'latest', subscription: 'latest', stream: 'events', window: 1 })).type, 'subscribed');
  const backlog = [];
  for (const id of ['latest-1', 'latest-2', 'latest-3']) {
    const accepted = await recoveredClient.publish(id); backlog.push(accepted);
    await progress(recoveredClient, accepted.cursor);
  }
  for (const accepted of backlog) {
    const event = await latest.wait(f => f.type === 'event'); assert.equal(event.cursor, accepted.cursor);
    latest.send({ type: 'ack', subscription: 'latest', through: event.cursor });
  }
  assert.equal(latest.all.filter(f => f.type === 'event').length, 3);
  const after = await state();
  assert(after.writes.every(x => x.condition && x.session_token_present));
  console.log('PASS: real S3 adapter, live before head commit, no early persisted, ambiguous commit reconciliation, process restart, interrupted-tail resume, ordered replay/live continuation during publication/cache eviction, finite replay, subscribe-only continuation, stale writer fencing.');
} catch (error) { console.error(output); throw error; }
finally {
  for (const c of clients) c.ws.close();
  await control({ pause_head: false }).catch(() => {});
  for (const child of children.reverse()) await stop(child);
  await rm(temp, { recursive: true, force: true });
}
