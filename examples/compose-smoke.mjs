// Real MinIO + packaged Highway, using the actual Node/browser SDK.
// Run `make compose-smoke`; leaves the local stack running with its volume.
import assert from 'node:assert/strict';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { randomBytes } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { HighwayClient } from '../sdk/javascript/index.js';
import { token } from '../scripts/compose-config.mjs';

const run = promisify(execFile);
const compose = async (...args) => (await run('docker', ['compose', ...args], { maxBuffer: 1024 * 1024, timeout: 90000 })).stdout;
const base = `http://127.0.0.1:${process.env.HIGHWAY_LOCAL_PORT || 4000}`;
const store = `http://127.0.0.1:${process.env.MINIO_LOCAL_PORT || 9000}`;
const stream = `smoke-${randomBytes(6).toString('hex')}`;
const headKey = `local/v1/tenants/${Buffer.from('demo').toString('hex')}/streams/${Buffer.from(stream).toString('hex')}/head.json`;
const head = async () => JSON.parse(await compose('run', '--rm', '--no-deps', '-T', 'mc', 'cat', `local/highway/${headKey}`));
const clients = [], tasks = [], received = [], progress = [], faults = [];
let paused = false, stopped = false;
const until = async (read, label, ms = 15000) => {
  const end = Date.now() + ms;
  while (Date.now() < end) {
    if (await read()) return;
    await delay(30);
  }
  throw new Error(`Timed out: ${label}`);
};
const status = async path => {
  try { return (await fetch(base + path, { signal: AbortSignal.timeout(1000) })).status; }
  catch { return 0; }
};
const deadline = async (promise, label) => {
  let timer;
  try { return await Promise.race([promise, new Promise((_, reject) => { timer = setTimeout(() => reject(new Error(`Timed out: ${label}`)), 15000); })]); }
  finally { clearTimeout(timer); }
};
function sdk() {
  const client = new HighwayClient({ url: base.replace('http:', 'ws:') + '/v1/ws', retryMs: 50 });
  clients.push(client); return client;
}
async function publish(client, messageId) {
  for (let i = 0; i < 200; i++) {
    try { return await client.publish({ stream, messageId, data: { sample: true }, getToken: () => token(stream) }); }
    catch (error) { if (error.code !== 'unavailable') throw error; }
    await delay(30);
  }
  throw new Error('Owner did not initialize');
}
const gauge = (text, name) => Number(text.match(new RegExp(`^highway_${name} (\\d+)$`, 'm'))?.[1] ?? NaN);

try {
  await until(async () => await status('/readyz') === 200, 'initial storage readiness');
  // Run the same Gleam transport contract used for the AWS release gate, inside
  // the packaged runtime. This verifies SigV4 and actual MinIO preconditions.
  const contract = `exec erl -pa /app/*/ebin -noshell -eval "'highway@@main':run('highway@storage@conformance')"`;
  const conformance = await compose('exec', '-T', '-e', 'HIGHWAY_S3_CONFORMANCE=1', 'highway', '/bin/sh', '-c', contract);
  assert(conformance.includes('S3 conditional writes, read visibility, opaque ETags, and concurrent immutable creation passed'));
  // A public file supplies an intentionally wrong signing secret. Unlike the
  // Python fixture, MinIO must cryptographically reject the signature.
  await assert.rejects(compose('exec', '-T', '-e', 'HIGHWAY_S3_CONFORMANCE=1', '-e',
    'AWS_SECRET_ACCESS_KEY_FILE=/run/secrets/public-jwks', 'highway', '/bin/sh', '-c', contract),
  error => `${error.stdout}${error.stderr}`.includes('Http(403)'));
  console.log('PASS: real MinIO SigV4 transport, invalid-signature rejection and conditional-write conformance.');

  const publisher = sdk(), subscriber = sdk();
  publisher.on('persisted', f => { if (f.stream === stream) progress.push(f.through); });
  await publisher.connect(); await subscriber.connect();
  const seed = await publish(publisher, 'seed');
  await until(() => progress.includes(seed.cursor), 'seed persistence');
  const sub = await subscriber.subscribe({ stream, from: 'earliest', window: 8, getToken: () => token(stream) });
  tasks.push((async () => {
    try { for await (const frame of sub) received.push(frame); }
    catch (error) { if (error.code !== 'closed') faults.push(error.code || 'subscription failed'); }
  })());
  await until(() => received.some(f => f.cursor === seed.cursor), 'initial archive replay');
  assert.equal((await fetch(`${store}/highway/${headKey}`)).status, 403, 'bucket must deny anonymous reads');

  // Freeze only the storage process. Highway stays responsive in the shared
  // network namespace, and live data must arrive before storage can commit.
  await compose('pause', 'minio'); paused = true;
  const a = await publish(publisher, 'outage-a'), b = await publish(publisher, 'outage-b');
  await until(() => received.some(f => f.cursor === b.cursor), 'live delivery while MinIO paused', 3000);
  assert(!progress.includes(a.cursor) && !progress.includes(b.cursor), 'no false persistence during outage');
  assert.equal(await status('/healthz'), 200);
  await until(async () => await status('/readyz') === 503, 'unready during storage outage');
  const atOutage = await fetch(`${base}/metrics`).then(r => r.text());
  assert(gauge(atOutage, 'pending_bytes') > 0, 'accepted data remains pending');
  await compose('unpause', 'minio'); paused = false;
  await until(() => progress.includes(b.cursor), 'persistence after MinIO recovery');
  await until(async () => await status('/readyz') === 200, 'readiness recovery');
  assert.equal((await head()).archived, b.cursor, 'persisted progress must match authoritative head');
  console.log('PASS: immediate ordered live delivery during storage outage, no early persisted, readiness failure/recovery, authoritative head.');

  await compose('stop', 'highway'); stopped = true;
  const container = (await compose('ps', '-a', '-q', 'highway')).trim();
  const exit = await run('docker', ['inspect', '--format', '{{.State.ExitCode}}', container]);
  assert.equal(exit.stdout.trim(), '0', 'graceful drain exit');
  const closed = await head();
  assert.equal(closed.version, 2); assert.equal(closed.status, 'closed'); assert.equal(closed.archived, b.cursor);
  // Restart the object store too: recovery must use the named volume, rather
  // than either process retaining its prior memory.
  await compose('stop', 'minio');
  await compose('start', 'minio');
  await until(async () => {
    try { return (await fetch(`${store}/minio/health/live`, { signal: AbortSignal.timeout(1000) })).ok; }
    catch { return false; }
  }, 'MinIO restart readiness');
  await compose('start', 'highway');
  await until(async () => await status('/readyz') === 200, 'Highway restart readiness'); stopped = false;
  await until(() => publisher.connected && subscriber.connected, 'SDK reconnect');
  const c = await publish(publisher, 'after-restart');
  await until(() => received.some(f => f.cursor === c.cursor), 'replay/live continuation after full restart');
  await until(() => progress.includes(c.cursor), 'new epoch persistence');
  assert(received.some(f => f.type === 'discontinuity' && f.after === b.cursor), 'restart boundary required');
  const expected = [seed, a, b, c].map(f => f.cursor);
  assert.deepEqual(received.filter(f => f.type === 'event').map(f => f.cursor), expected, 'no skips or reordering across restart');
  const replay = sdk(); await replay.connect();
  const finite = await replay.subscribe({ stream, from: 'earliest', follow: false, window: 2, getToken: () => token(stream) });
  const archived = [];
  await deadline((async () => { for await (const frame of finite) if (frame.type === 'event') archived.push(frame.cursor); })(), 'bounded-credit finite replay');
  assert.deepEqual(archived, expected);
  assert.equal(faults.length, 0);
  assert.equal((await head()).archived, c.cursor);
  console.log('PASS: clean drain/closed head, MinIO volume + Highway restart, fresh epoch boundary, SDK resume and exact finite replay with window 2.');
} finally {
  for (const client of clients) client.close();
  if (paused) await compose('unpause', 'minio');
  // Restore the development stack even if a restart assertion failed.
  if (stopped) await compose('up', '-d', '--wait');
  await Promise.allSettled(tasks);
}
