// Two actual SDK clients, independent stream JWTs on one socket, S3 fixture.
// Ephemeral signing key stays in memory. Run `make smoke-scoped` (Node 22+).
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { HighwayClient } from '../sdk/javascript/index.js';
const port = Number(process.env.SCOPED_SMOKE_PORT || 4410), storePort = Number(process.env.SCOPED_STORE_PORT || 4411);
const base = `http://127.0.0.1:${port}`, store = `http://127.0.0.1:${storePort}`;
const dir = await mkdtemp(join(tmpdir(), 'highway-scoped-'));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
await writeFile(join(dir, 'jwks.json'), JSON.stringify({ keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'scoped' }] }));
const jwt = (stream, actions = ['publish', 'subscribe', 'history'], lifetime = 120, tenant = 'demo', subject = 'user') => {
  const b64 = x => Buffer.from(JSON.stringify(x)).toString('base64url'), now = Math.floor(Date.now() / 1000);
  const input = `${b64({ alg: 'EdDSA', kid: 'scoped' })}.${b64({ iss: 'issuer', aud: 'highway', sub: subject, tenant, iat: now, exp: now + lifetime, grants: [{ stream, actions }] })}`;
  return `${input}.${sign(null, Buffer.from(input), privateKey).toString('base64url')}`;
};
const children = [], clients = [], tasks = [], received = { a: [], b: [] }, persisted = { a: [], b: [] };
let output = '', server, paused = false, bCalls = 0;
const launch = (cmd, args, env = {}) => {
  const child = spawn(cmd, args, { cwd: new URL('..', import.meta.url), detached: true, env: { ...process.env, ...env }, stdio: ['ignore', 'pipe', 'pipe'] });
  child.exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  children.push(child); for (const s of [child.stdout, child.stderr]) s.on('data', b => { output = (output + b).slice(-12000); });
  return child;
};
const until = async (fn, label, ms = 15000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) { if (await fn()) return; await delay(20); }
  throw new Error(`Timed out: ${label}`);
};
const healthy = async () => { try { return (await fetch(`${base}/readyz`)).status === 200; } catch { return false; } };
const control = async pause => { await fetch(`${store}/__control`, { method: 'POST', body: JSON.stringify({ pause_head: pause }) }); paused = pause; };
async function start() {
  server = launch('build/erlang-shipment/entrypoint.sh', ['run'], {
    HIGHWAY_STORAGE_MODE: 's3_async', HIGHWAY_BIND: '127.0.0.1', HIGHWAY_PORT: String(port),
    HIGHWAY_JWKS_FILE: join(dir, 'jwks.json'), HIGHWAY_JWT_ISSUER: 'issuer', HIGHWAY_JWT_AUDIENCE: 'highway',
    HIGHWAY_S3_ENDPOINT: store, HIGHWAY_S3_BUCKET: 'fixture', HIGHWAY_S3_PREFIX: 'scoped',
    AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: 'fixture-access', AWS_SECRET_ACCESS_KEY: 'fixture-secret', AWS_SESSION_TOKEN: 'fixture-session',
    HIGHWAY_PROBE_MS: '100', HIGHWAY_PROBE_STALE_MS: '1000', HIGHWAY_S3_TIMEOUT_MS: '500', HIGHWAY_DRAIN_MS: '3000',
  });
  await until(healthy, 'readiness');
}
function sdk() { const c = new HighwayClient({ url: base.replace('http:', 'ws:') + '/v1/ws', retryMs: 50, refreshMs: 300 }); clients.push(c); return c; }
async function publish(c, stream, messageId, data = null) {
  for (let i = 0; i < 200; i++) {
    try { return await c.publish({ stream, messageId, data, getToken: () => jwt(stream, ['publish']) }); }
    catch (error) { if (error.code !== 'unavailable') throw error; }
    await delay(20);
  }
  throw new Error('Epoch allocation deadline');
}
function consume(sub, target) {
  const state = { error: undefined };
  tasks.push((async () => { try { for await (const f of sub) target.push(f); } catch (e) { state.error = e.code; } })());
  return state;
}
try {
  launch('python3', ['scripts/s3-test-server.py', '--port', String(storePort)]);
  await until(async () => { try { return (await fetch(`${store}/__state`)).ok; } catch { return false; } }, 'fixture');
  await start();
  const pub = sdk(), client = sdk(); await pub.connect(); await client.connect();
  pub.on('persisted', f => persisted[f.stream]?.push(f.through));
  const expected = { a: [await publish(pub, 'a', 'seed-a', { text: 'x'.repeat(24000) })], b: [await publish(pub, 'b', 'seed-b')] };
  await until(() => ['a', 'b'].every(s => persisted[s].includes(expected[s][0].cursor)), 'seed head commits');
  const a = await client.subscribe({ stream: 'a', from: 'earliest', window: 2, getToken: () => jwt('a', ['subscribe', 'history']) });
  const b = await client.subscribe({ stream: 'b', from: 'earliest', window: 2, getToken: () => { bCalls++; return jwt('b', ['subscribe', 'history'], 3); } });
  const aState = consume(a, received.a), bState = consume(b, received.b);
  await until(() => received.a.some(f => f.type === 'event') && received.b.some(f => f.type === 'event'), 'both streams replay on one socket');
  await assert.rejects(client.subscribe({ stream: 'b', from: 'earliest', getToken: () => jwt('a') }), { code: 'forbidden' });
  await assert.rejects(client.subscribe({ stream: 'a', from: 'earliest', getToken: () => jwt('a', ['subscribe']) }), { code: 'forbidden' });
  for (const identity of [['another-tenant', 'user'], ['demo', 'another-user']]) {
    await assert.rejects(client.subscribe({ stream: 'a', getToken: () => jwt('a', ['subscribe'], 120, ...identity) }), { code: 'forbidden' });
  }
  await until(() => bCalls >= 2, 'independent automatic refresh');
  const fixed = jwt('a', ['subscribe', 'history'], 2);
  const expiring = await client.subscribe({ stream: 'a', from: 'latest', getToken: () => fixed });
  const expired = consume(expiring, []);
  await until(() => expired.error === 'token_expired', 'strict independent expiry');
  assert(client.connected && !aState.error && !bState.error);
  let actions = ['subscribe', 'history'];
  const revoked = await client.subscribe({ stream: 'a', from: 'earliest', getToken: () => jwt('a', actions) });
  actions = ['subscribe']; await assert.rejects(revoked.refresh(), { code: 'forbidden' });
  assert(client.connected && !aState.error && !bState.error);
  await control(true);
  for (const stream of ['a', 'b']) expected[stream].push(await publish(pub, stream, `live-${stream}`));
  await until(() => ['a', 'b'].every(s => received[s].some(f => f.cursor === expected[s][1].cursor)), 'live before authoritative commit');
  assert(['a', 'b'].every(s => !persisted[s].includes(expected[s][1].cursor)));
  await control(false);
  await until(() => ['a', 'b'].every(s => persisted[s].includes(expected[s][1].cursor)), 'authoritative persistence');
  const old = server; old.kill('SIGTERM');
  const exit = await Promise.race([old.exited, delay(8000).then(() => { throw new Error('Drain deadline'); })]);
  assert.deepEqual(exit, { code: 0, signal: null });
  await start(); await until(() => pub.connected && client.connected, 'reconnect');
  for (const stream of ['a', 'b']) expected[stream].push(await publish(pub, stream, `restarted-${stream}`));
  await until(() => ['a', 'b'].every(s => received[s].some(f => f.cursor === expected[s][2].cursor)), 'both scoped resumes');
  for (const stream of ['a', 'b']) {
    assert.deepEqual(received[stream].filter(f => f.type === 'event').map(f => f.cursor), expected[stream].map(f => f.cursor));
    assert(received[stream].some(f => f.type === 'discontinuity'));
    const finite = await client.subscribe({ stream, from: 'earliest', follow: false, window: 2, getToken: () => jwt(stream, ['subscribe', 'history']) });
    const frames = []; for await (const f of finite) if (f.type === 'event') frames.push(f.cursor);
    assert.deepEqual(frames, expected[stream].map(f => f.cursor));
  }
  assert(!aState.error && !bState.error);
  assert(!output.includes('fixture-secret') && !output.includes('x'.repeat(100)));
  console.log('PASS: protocol v2, two independently signed streams on one socket, scoped publish, large first frame, grant/identity isolation, refresh, independent expiry/revocation, live before head, drain/restart and exact ordered resume/replay.');
} catch (error) { console.error(output); throw error; }
finally {
  clients.forEach(c => c.close());
  if (paused) await control(false);
  for (const child of children.reverse()) {
    if (child.exitCode === null && child.signalCode === null) { try { process.kill(-child.pid, 'SIGKILL'); } catch {} await child.exited; }
  }
  await Promise.allSettled(tasks); await rm(dir, { recursive: true, force: true });
}
