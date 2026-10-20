// Native production shipment, actual Node SDK, loopback S3 contract fixture.
// Requires Node 22+, Python 3, and the OTP used to build the shipment.
import assert from 'node:assert/strict';
import { generateKeyPairSync, sign } from 'node:crypto';
import { spawn } from 'node:child_process';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir, cpus, platform, arch } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import { HighwayClient } from '../sdk/javascript/index.js';
const port = Number(process.env.RELEASE_SMOKE_PORT || 4405), storePort = Number(process.env.RELEASE_STORE_PORT || 4404);
const base = `http://127.0.0.1:${port}`, endpoint = `http://127.0.0.1:${storePort}`;
const temp = await mkdtemp(join(tmpdir(), 'highway-release-'));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
await writeFile(join(temp, 'jwks.json'), JSON.stringify({ keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'smoke' }] }));
await writeFile(join(temp, 'access'), 'fixture-access\n');
await writeFile(join(temp, 'secret'), 'fixture-secret\n');
await writeFile(join(temp, 'session'), 'fixture-session\n');
await writeFile(join(temp, 'limits.json'), JSON.stringify({ max_records: 64 }));
const b64 = value => Buffer.from(JSON.stringify(value)).toString('base64url');
const token = () => {
  const now = Math.floor(Date.now() / 1000);
  const input = `${b64({ alg: 'EdDSA', kid: 'smoke' })}.${b64({ iss: 'issuer', aud: 'highway', sub: 'user', tenant: 'demo', iat: now, exp: now + 120, grants: [{ stream: 'events', actions: ['publish', 'subscribe', 'history'] }] })}`;
  return `${input}.${sign(null, Buffer.from(input), privateKey).toString('base64url')}`;
};
let output = '', server;
const children = [], clients = [], background = [];
const launch = (cmd, args, env = {}) => {
  const childEnv = { ...process.env, ...env };
  for (const key of ['AWS_ACCESS_KEY_ID', 'AWS_SECRET_ACCESS_KEY', 'AWS_SESSION_TOKEN']) if (childEnv[key] === '') delete childEnv[key];
  const child = spawn(cmd, args, { cwd: new URL('..', import.meta.url), detached: true, env: childEnv, stdio: ['ignore', 'pipe', 'pipe'] });
  child.exited = new Promise(resolve => child.once('exit', (code, signal) => resolve({ code, signal })));
  children.push(child); for (const s of [child.stdout, child.stderr]) s.on('data', b => { output = (output + b).slice(-16000); });
  return child;
};
const until = async (fn, label, max = 200) => {
  for (let i = 0; i < max; i++) { try { const value = await fn(); if (value) return value; } catch {} await delay(20); }
  throw new Error(`Timed out: ${label}`);
};
const deadline = (promise, label, ms = 7000) => {
  let timer;
  return Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Timed out: ${label}`)), ms); })]).finally(() => clearTimeout(timer));
};
const control = data => fetch(`${endpoint}/__control`, { method: 'POST', body: JSON.stringify(data) });
const state = () => fetch(`${endpoint}/__state`).then(r => r.json());
const head = async () => Object.values((await state()).heads)[0];
const metrics = () => fetch(`${base}/metrics`).then(r => r.text());
const gauge = (text, name) => Number(text.match(new RegExp(`^highway_${name} (\\d+)$`, 'm'))?.[1] ?? NaN);
async function startServer(drain = 3000) {
  server = launch('build/erlang-shipment/entrypoint.sh', ['run'], {
    HIGHWAY_STORAGE_MODE: 's3_async', HIGHWAY_PORT: String(port), HIGHWAY_BIND: '127.0.0.1',
    HIGHWAY_JWKS_FILE: join(temp, 'jwks.json'), HIGHWAY_JWT_ISSUER: 'issuer', HIGHWAY_JWT_AUDIENCE: 'highway',
    HIGHWAY_S3_BUCKET: 'fixture', HIGHWAY_S3_PREFIX: 'release', HIGHWAY_S3_ENDPOINT: endpoint,
    AWS_REGION: 'us-east-1', AWS_ACCESS_KEY_ID: '', AWS_SECRET_ACCESS_KEY: '', AWS_SESSION_TOKEN: '',
    AWS_ACCESS_KEY_ID_FILE: join(temp, 'access'), AWS_SECRET_ACCESS_KEY_FILE: join(temp, 'secret'), AWS_SESSION_TOKEN_FILE: join(temp, 'session'),
    HIGHWAY_S3_TIMEOUT_MS: '500', HIGHWAY_DRAIN_MS: String(drain), HIGHWAY_PROBE_MS: '100', HIGHWAY_PROBE_STALE_MS: '800',
    HIGHWAY_LIMITS_FILE: join(temp, 'limits.json'),
  });
  await until(async () => (await fetch(`${base}/readyz`)).status === 200, 'shipment readiness');
}
function sdk() {
  const client = new HighwayClient({ url: `ws://127.0.0.1:${port}/v1/ws`, getToken: () => token(), retryMs: 50 });
  clients.push(client); return client;
}
async function publish(client, messageId, data = null) {
  for (let i = 0; i < 200; i++) {
    try { return await client.publish({ stream: 'events', messageId, data }); }
    catch (error) { if (error.code !== 'unavailable') throw error; }
    await delay(20);
  }
  throw new Error('Owner did not initialize');
}

async function cleanup(child) {
  if (child.exitCode !== null || child.signalCode !== null) return;
  try { process.kill(-child.pid, 'SIGKILL'); } catch {}
  await deadline(child.exited, 'cleanup');
}
const received = [], persisted = [], persistNotices = [], faults = [], sentAt = new Map(), liveMs = [];
try {
  launch('python3', ['scripts/s3-test-server.py', '--port', String(storePort)]);
  await until(() => fetch(`${endpoint}/__state`).then(r => r.ok), 'fixture');
  await startServer();
  const publisher = sdk(); await publisher.connect(); publisher.on('persisted', f => { persisted.push(f.through); persistNotices.push({ cursor: f.through, time: performance.now() }); });
  const seed = await publish(publisher, 'seed');
  await until(() => persisted.includes(seed.cursor), 'seed persistence');
  const subscriber = sdk(); await subscriber.connect();
  const sub = await subscriber.subscribe({ stream: 'events', from: 'earliest', window: 16 });
  background.push((async () => {
    try { for await (const frame of sub) {
      received.push(frame);
      if (frame.type === 'event' && sentAt.has(frame.message_id)) liveMs.push(performance.now() - sentAt.get(frame.message_id));
    } } catch (error) { if (error.code !== 'closed') faults.push(error.code); }
  })());
  await until(() => received.some(f => f.cursor === seed.cursor), 'SDK initial replay');
  await control({ pause_head: true });
  const tail = await publish(publisher, 'accepted-before-drain');
  await until(() => received.some(f => f.cursor === tail.cursor), 'live before persistence');
  assert(!persisted.includes(tail.cursor));
  const first = server; first.kill('SIGTERM');
  await until(async () => (await fetch(`${base}/readyz`)).status === 503, 'drain readiness');
  assert.equal((await fetch(`${base}/v1/ws`)).status, 503);
  await assert.rejects(publisher.publish({ stream: 'events', messageId: 'rejected-during-drain', data: null }), { code: 'unavailable' });
  await control({ pause_head: false });
  assert.deepEqual(await deadline(first.exited, 'graceful exit'), { code: 0, signal: null });
  assert.equal((await head()).status, 'closed'); assert.equal((await head()).version, 2); assert.equal((await head()).archived, tail.cursor);
  assert(persisted.includes(tail.cursor), 'last persisted notice must reach the publisher before close');
  // Stop-then-start, same prefix. Established SDK subscription resumes through
  // the conservative boundary, with no silent reset to latest or publish retry.
  await startServer();
  await until(() => publisher.connected && subscriber.connected, 'SDK reconnect');
  const next = await publish(publisher, 'after-upgrade');
  await until(() => received.some(f => f.cursor === next.cursor), 'SDK live continuation after restart');
  assert(received.some(f => f.type === 'discontinuity' && f.after === tail.cursor));
  assert.deepEqual(received.filter(f => f.type === 'event').map(f => f.cursor), [seed.cursor, tail.cursor, next.cursor]);
  await publisher.refresh();
  // Reproducible local measurements, deliberately modest and never production
  // throughput claims. Fill the 64-record pending cap during an S3 outage.
  const baseline = await metrics(), rtt = [], accepted = [], acceptedAt = new Map(), data = { text: 'x'.repeat(256) };
  await until(() => persisted.includes(next.cursor), 'post-upgrade persistence');
  await control({ pause_head: true });
  for (let i = 0; i < 64; i++) {
    const id = `bench-${i}`, start = performance.now(); sentAt.set(id, start);
    const event = await publish(publisher, id, data); rtt.push(performance.now() - start); acceptedAt.set(event.cursor, performance.now()); accepted.push(event);
  }
  await assert.rejects(publisher.publish({ stream: 'events', messageId: 'over-cap', data }), { code: 'storage_backlog' });
  const atCap = await metrics(); assert(gauge(atCap, 'pending_bytes') > 0);
  assert(Number.isFinite(gauge(atCap, 'beam_memory_bytes')));
  await until(() => received.filter(f => f.type === 'event' && f.message_id.startsWith('bench-')).length === 64, 'live at pending cap');
  const lagStart = performance.now(); await control({ pause_head: false });
  await until(() => persisted.includes(accepted.at(-1).cursor), 'benchmark persistence');
  const lag = performance.now() - lagStart;
  const persistedMetrics = await metrics();
  const persistMs = [], publishPersistMs = [];
  let assigned = 0;
  for (const notice of persistNotices) {
    const through = accepted.findIndex(f => f.cursor === notice.cursor);
    while (assigned <= through) { const event = accepted[assigned++]; persistMs.push(notice.time - acceptedAt.get(event.cursor)); publishPersistMs.push(notice.time - sentAt.get(event.message_id)); }
  }
  assert.equal(persistMs.length, 64);
  const replayClient = sdk(); await replayClient.connect(); const replayStart = performance.now();
  const finite = await replayClient.subscribe({ stream: 'events', from: 'earliest', follow: false, window: 128 });
  const replayed = []; for await (const f of finite) if (f.type === 'event') replayed.push(f.cursor);
  const replayMs = performance.now() - replayStart;
  assert.deepEqual(replayed, [seed, tail, next, ...accepted].map(e => e.cursor));
  const last = await metrics();
  const percentile = (samples, p) => [...samples].sort((a, b) => a - b)[Math.floor((samples.length - 1) * p)];
  const result = {
    environment: { platform: platform(), arch: arch(), cpu: cpus()[0].model, node: process.version, storage: 'loopback Python contract fixture' },
    records: 64, payload_text_bytes: 256, pending_record_limit: 64,
    publish_rtt_ms: { p50: percentile(rtt, .5), p95: percentile(rtt, .95) },
    publish_to_live_ms: { p50: percentile(liveMs, .5), p95: percentile(liveMs, .95) },
    publish_to_persisted_ms: { p50: percentile(publishPersistMs, .5), p95: percentile(publishPersistMs, .95) },
    accepted_to_persisted_ms: { p50: percentile(persistMs, .5), p95: percentile(persistMs, .95) },
    blocked_head_release_to_persisted_ms: lag, replay_records: replayed.length, replay_ms: replayMs,
    pending_bytes_at_cap: gauge(atCap, 'pending_bytes'), beam_memory_bytes_at_cap: gauge(atCap, 'beam_memory_bytes'),
    beam_memory_bytes_baseline: gauge(baseline, 'beam_memory_bytes'),
    storage_requests_delta: gauge(last, 'storage_requests_total') - gauge(baseline, 'storage_requests_total'),
    archive_and_probe_requests: gauge(persistedMetrics, 'storage_requests_total') - gauge(baseline, 'storage_requests_total'),
    replay_and_probe_requests: gauge(last, 'storage_requests_total') - gauge(persistedMetrics, 'storage_requests_total'),
  };
  await writeFile('build/benchmark.json', JSON.stringify(result, null, 2) + '\n');
  console.log(`MEASUREMENTS ${JSON.stringify(result)}`);
  assert.equal(faults.length, 0);
  // Failed drain must exit nonzero and leave the head open. No successful close
  // is inferred from a segment/index upload or pending memory becoming full.
  await control({ pause_head: true });
  const uncertain = await publish(publisher, 'timeout-tail');
  const second = server; second.kill('SIGTERM');
  assert.deepEqual(await deadline(second.exited, 'failed drain exit'), { code: 1, signal: null });
  assert.equal((await head()).status, 'open'); assert.notEqual((await head()).archived, uncertain.cursor);
  await control({ pause_head: false });
  assert(output.includes('Drain deadline expired'));
  assert(!output.includes('fixture-secret') && !output.includes('fixture-session') && !output.includes('accepted-before-drain\"'));
  console.log('PASS: native shipment, secret files, empty/idle readiness, metrics, SIGTERM admission barrier, committed drain/closed head, nonzero drain timeout, singleton restart, real Node SDK refresh/resume/flow, finite browser-compatible replay, 64-record capacity measurements.');
} catch (error) { console.error(output); throw error; }
finally {
  for (const client of clients) client.close();
  await control({ pause_head: false }).catch(() => {});
  for (const child of children.reverse()) await cleanup(child);
  await Promise.allSettled(background);
  await rm(temp, { recursive: true, force: true });
}
