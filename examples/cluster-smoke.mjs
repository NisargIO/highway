// Two clustered native shipments ("regions") sharing one primary bucket on the
// loopback S3 contract fixture. Verifies nearest-node create, routing of an
// awake stream to its owner (mirror reads, forwarded publishes), crash
// takeover after lease + skew, and sleep/wake in another region.
// Requires Node 22+, Python 3, epmd from the OTP that built the shipment.
import { generateKeyPairSync, sign, randomBytes } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import assert from 'node:assert/strict';

const storePort = Number(process.env.S3_TEST_PORT || 4420);
const ports = { a: Number(process.env.CLUSTER_PORT_A || 4421), b: Number(process.env.CLUSTER_PORT_B || 4422) };
const nodes = { a: 'highway-a@127.0.0.1', b: 'highway-b@127.0.0.1' };
const regions = { a: 'us-east-1', b: 'eu-west-1' };
const endpoint = `http://127.0.0.1:${storePort}`;
const lease = 2000, skew = 200, sleepIdle = 3000;
const temp = await mkdtemp(join(tmpdir(), 'highway-cluster-smoke-'));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
await writeFile(join(temp, 'jwks.json'), JSON.stringify({ keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'smoke' }] }));
await writeFile(join(temp, 'cookie'), randomBytes(24).toString('hex'));
const b64 = x => Buffer.from(JSON.stringify(x)).toString('base64url');
function token() {
  const now = Math.floor(Date.now() / 1000);
  const actions = ['publish', 'subscribe', 'history'];
  const input = `${b64({ alg: 'EdDSA', kid: 'smoke' })}.${b64({ iss: 'issuer', aud: 'highway', sub: 'user', tenant: 'demo', iat: now, exp: now + 300, grants: [{ stream: 'events', actions }, { stream: 'quiet', actions }] })}`;
  return `${input}.${sign(null, Buffer.from(input), privateKey).toString('base64url')}`;
}

let output = '';
const children = [], clients = [], servers = {};
function launch(cmd, args, env = {}) {
  const childEnv = { ...process.env, ...env };
  const child = spawn(cmd, args, { cwd: new URL('..', import.meta.url), detached: true, env: childEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  child.exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  children.push(child);
  for (const s of [child.stdout, child.stderr]) s.on('data', b => { output = (output + b).slice(-24000); });
  return child;
}
async function stop(child, signal = 'SIGTERM') {
  try { process.kill(-child.pid, signal); } catch {}
  await Promise.race([child.exited, delay(3000)]);
  try { process.kill(-child.pid, 'SIGKILL'); } catch {}
}
async function until(fn, label, ms = 10000) {
  const end = Date.now() + ms;
  while (Date.now() < end) { try { const value = await fn(); if (value) return value; } catch {} await delay(25); }
  throw new Error(`Timed out: ${label}`);
}
const state = () => fetch(`${endpoint}/__state`).then(r => r.json());
async function head(stream) {
  const hex = Buffer.from(stream).toString('hex');
  return Object.entries((await state()).heads).find(([k]) => k.includes(`/streams/${hex}/`))?.[1];
}
const epoch = cursor => Number(cursor.split(':')[2]);
const offset = cursor => Number(cursor.split(':')[3]);

async function startNode(name) {
  servers[name] = launch('build/erlang-shipment/entrypoint.sh', ['run'], {
    HIGHWAY_STORAGE_MODE: 's3_async', HIGHWAY_PORT: String(ports[name]), HIGHWAY_BIND: '127.0.0.1',
    HIGHWAY_JWKS_FILE: join(temp, 'jwks.json'), HIGHWAY_JWT_ISSUER: 'issuer', HIGHWAY_JWT_AUDIENCE: 'highway',
    HIGHWAY_S3_BUCKET: 'fixture', HIGHWAY_S3_PREFIX: 'cluster', HIGHWAY_S3_ENDPOINT: endpoint,
    AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: 'fixture-access', AWS_SECRET_ACCESS_KEY: 'fixture-secret', AWS_SESSION_TOKEN: 'fixture-session',
    HIGHWAY_S3_TIMEOUT_MS: '500', HIGHWAY_DRAIN_MS: '3000', HIGHWAY_PROBE_MS: '200', HIGHWAY_PROBE_STALE_MS: '1500',
    HIGHWAY_CLUSTER_NODE: nodes[name], HIGHWAY_REGION: regions[name], HIGHWAY_CLUSTER_COOKIE_FILE: join(temp, 'cookie'),
    HIGHWAY_LEASE_MS: String(lease), HIGHWAY_MAX_CLOCK_SKEW_MS: String(skew), HIGHWAY_SLEEP_IDLE_MS: String(sleepIdle),
  });
  await until(async () => (await fetch(`http://127.0.0.1:${ports[name]}/readyz`)).status === 200, `node ${name} readiness`, 20000);
}

class Client {
  constructor(ws, label) {
    this.ws = ws; this.label = label; this.queue = []; this.all = []; this.waiters = []; this.n = 0;
    ws.addEventListener('error', () => {});
    ws.addEventListener('message', e => { const f = JSON.parse(e.data); this.queue.push(f); this.all.push(f); this.flush(); });
  }
  flush() { for (const w of [...this.waiters]) { const i = this.queue.findIndex(w.match); if (i >= 0) { clearTimeout(w.timer); this.waiters.splice(this.waiters.indexOf(w), 1); w.resolve(this.queue.splice(i, 1)[0]); } } }
  wait(match, ms = 8000) {
    const failure = new Error(`${this.label}: timed out waiting`);
    return new Promise((resolve, reject) => {
      const w = { match, resolve };
      w.timer = setTimeout(() => {
        this.waiters.splice(this.waiters.indexOf(w), 1);
        failure.message += `; frames ${JSON.stringify(this.all.slice(-10).map(({ type, id, subscription, code, cursor, through }) => ({ type, id, subscription, code, cursor, through })))}`;
        reject(failure);
      }, ms);
      this.waiters.push(w); this.flush();
    });
  }
  send(f) { this.ws.send(JSON.stringify(f)); }
  command(f) { this.send(f); return this.wait(x => x.id === f.id); }
  // Owners initialize asynchronously and ownership moves return retryable
  // `unavailable`; production clients retry exactly the same way.
  async retry(build, ok, label, ms = 10000) {
    return until(async () => {
      const f = await this.command(build(`${label}-${this.n++}`));
      if (ok(f)) return f;
      assert.equal(f.code, 'unavailable', `${label}: ${JSON.stringify(f)}`); return false;
    }, `${this.label} ${label}`, ms);
  }
  publish(stream, message, ms) {
    return this.retry(id => ({ type: 'publish', id, stream, message_id: message, data: { text: message } }), f => f.type === 'accepted', `publish ${message}`, ms);
  }
  subscribe(stream, subscription, from = 'latest', follow = true) {
    return this.retry(id => ({ type: 'subscribe', id, subscription, stream, from, follow, window: 64 }), f => f.type === 'subscribed', `subscribe ${subscription}`);
  }
  event(subscription, message) { return this.wait(x => x.type === 'event' && x.subscription === subscription && x.message_id === message); }
}
async function connect(name) {
  const c = new Client(new WebSocket(`ws://127.0.0.1:${ports[name]}/v1/ws`), `client@${name}`); clients.push(c);
  await c.wait(x => x.type === 'hello');
  assert.equal((await c.command({ type: 'auth', id: 'auth', token: token() })).type, 'authenticated');
  return c;
}

try {
  launch('python3', ['scripts/s3-test-server.py', '--port', String(storePort)]);
  await until(async () => (await fetch(`${endpoint}/__state`)).ok, 's3 fixture');
  await startNode('a');
  await startNode('b');

  // 1. Create in the region the client reached: node A owns `events`.
  const a1 = await connect('a');
  const first = await a1.publish('events', 'm1');
  assert.equal((await head('events')).owner.node, nodes.a);
  assert.equal((await head('events')).owner.region, regions.a);

  // 2. A subscriber in region B is routed to A's stream through a mirror.
  const b1 = await connect('b');
  await b1.subscribe('events', 'remote');
  await a1.subscribe('events', 'local');
  const second = await a1.publish('events', 'm2');
  assert.equal((await b1.event('remote', 'm2')).cursor, second.cursor);
  await a1.event('local', 'm2');

  // 3. Publishing in region B is forwarded to A, which assigns the order.
  const third = await b1.publish('events', 'm3');
  assert.equal(epoch(third.cursor), epoch(first.cursor));
  assert.equal(offset(third.cursor), offset(second.cursor) + 1);
  assert.equal((await a1.event('local', 'm3')).cursor, third.cursor);
  assert.equal((await b1.event('remote', 'm3')).cursor, third.cursor);
  // B's publisher still receives A's persistence progress.
  await b1.wait(x => x.type === 'persisted' && x.stream === 'events' && offset(x.through) >= offset(third.cursor));
  assert.equal((await head('events')).owner.node, nodes.a, 'forwarding must not move ownership');

  // 4. History from region B: archive + pending suffix, exact order.
  const b2 = await connect('b');
  await b2.subscribe('events', 'history', 'earliest', false);
  for (const [m, f] of [['m1', first], ['m2', second], ['m3', third]]) assert.equal((await b2.event('history', m)).cursor, f.cursor);

  // 5. A crashes. B waits out lease + skew, then claims with a new epoch.
  const crashed = Date.now();
  await stop(servers.a, 'SIGKILL');
  // Owner loss ends B's mirrored subscription and closes that socket, exactly
  // like a local owner failure; clients reconnect and resume from a cursor.
  await until(() => b1.ws.readyState === WebSocket.CLOSED, 'mirror owner loss closes the socket');
  const b4 = await connect('b');
  const fourth = await b4.publish('events', 'm4', lease * 4);
  assert.ok(Date.now() - crashed >= lease - 600, 'takeover must wait for the lease');
  assert.ok(epoch(fourth.cursor) > epoch(third.cursor));
  assert.equal((await head('events')).owner.node, nodes.b);
  const b3 = await connect('b');
  await b3.subscribe('events', 'after-crash', { after: third.cursor }, false);
  await b3.wait(x => x.type === 'discontinuity' && x.subscription === 'after-crash');
  assert.equal((await b3.event('after-crash', 'm4')).cursor, fourth.cursor);

  // 6. Sleep/wake: `quiet` is created in B, sleeps when idle, wakes in A.
  const quietFirst = await b4.publish('quiet', 'q1');
  assert.equal((await head('quiet')).owner.node, nodes.b);
  await until(async () => (await head('quiet')).status === 'closed', 'quiet sleeps', sleepIdle * 4);
  await delay(300);
  assert.equal(b4.ws.readyState, WebSocket.OPEN, 'sleeping must not disconnect an idle publisher');
  await startNode('a');
  const a2 = await connect('a');
  const woke = Date.now();
  const quietSecond = await a2.publish('quiet', 'q2');
  assert.ok(Date.now() - woke < lease, 'waking a closed head needs no lease wait');
  assert.ok(epoch(quietSecond.cursor) > epoch(quietFirst.cursor));
  assert.equal((await head('quiet')).owner.node, nodes.a);
  assert.equal((await head('quiet')).owner.region, regions.a);
  const a3 = await connect('a');
  await a3.subscribe('quiet', 'quiet-history', 'earliest', false);
  assert.equal((await a3.event('quiet-history', 'q1')).cursor, quietFirst.cursor);
  assert.equal((await a3.event('quiet-history', 'q2')).cursor, quietSecond.cursor);

  // 7. A remote reader's mirror idles out after it leaves, so the primary
  // (no longer pinned by the relay agent) can sleep again.
  const b5 = await connect('b');
  await b5.subscribe('quiet', 'remote-quiet');
  const quietThird = await a2.publish('quiet', 'q3');
  assert.equal((await b5.event('remote-quiet', 'q3')).cursor, quietThird.cursor);
  b5.ws.close();
  await until(async () => (await head('quiet')).status === 'closed', 'primary sleeps after mirror idles', sleepIdle * 5);

  // 8. Graceful handoff: SIGTERM drains B to closed heads, so A claims B's
  // stream immediately instead of waiting out the lease.
  const awake = await b4.publish('events', 'm4b');
  const before = await head('events');
  assert.equal(before.owner.node, nodes.b);
  assert.equal(before.status, 'open');
  await stop(servers.b);
  assert.equal((await head('events')).status, 'closed');
  const handoff = Date.now();
  const fifth = await a2.publish('events', 'm5');
  assert.ok(Date.now() - handoff < lease, 'a drained head is claimable at once');
  assert.ok(epoch(fifth.cursor) > epoch(awake.cursor));
  assert.equal((await head('events')).owner.node, nodes.a);

  console.log('Cluster smoke passed: nearest-node create, mirrored reads, forwarded publish, crash takeover, sleep/wake, graceful handoff.');
} catch (error) {
  console.error(output.split("\n").filter(l => !l.includes("\"level\":\"info\"")).join("\n"));
  for (const c of clients) console.error(c.label, c.ws.readyState, JSON.stringify(c.all.slice(-6).map(({ type, id, subscription, code }) => ({ type, id, subscription, code }))));
  throw error;
} finally {
  for (const c of clients) try { c.ws.close(); } catch {}
  for (const child of children.reverse()) await stop(child);
  await rm(temp, { recursive: true, force: true });
}
