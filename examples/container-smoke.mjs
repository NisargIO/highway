// Proves the runtime image boots as a non-root, read-only service, supports JWT
// WebSockets/metrics and catches SIGTERM. Native release smoke covers S3/restart.
import { generateKeyPairSync, sign } from 'node:crypto';
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { mkdtemp, writeFile, rm } from 'node:fs/promises';
import { tmpdir } from 'node:os';
import { join } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import assert from 'node:assert/strict';
import { HighwayClient } from '../sdk/javascript/index.js';
const run = promisify(execFile), name = `highway-smoke-${process.pid}`;
const port = Number(process.env.CONTAINER_SMOKE_PORT || 4407), dir = await mkdtemp(join(tmpdir(), 'highway-container-'));
const { publicKey, privateKey } = generateKeyPairSync('ed25519');
const jwks = join(dir, 'public.json');
await writeFile(jwks, JSON.stringify({ keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'smoke' }] }), { mode: 0o444 });
const b64 = x => Buffer.from(JSON.stringify(x)).toString('base64url');
const now = Math.floor(Date.now() / 1000), input = `${b64({ alg: 'EdDSA', kid: 'smoke' })}.${b64({ iss: 'issuer', aud: 'highway', sub: 'user', tenant: 'demo', iat: now, exp: now + 120, grants: [{ stream: 'events', actions: ['publish', 'subscribe', 'history'] }] })}`;
const token = `${input}.${sign(null, Buffer.from(input), privateKey).toString('base64url')}`;
const client = new HighwayClient({ url: `ws://127.0.0.1:${port}/v1/ws`, getToken: () => token, reconnect: false });
try {
  await run('docker', ['run', '-d', '--name', name, '--read-only', '--tmpfs', '/tmp:noexec,nosuid,size=16m', '--cap-drop', 'ALL', '--security-opt', 'no-new-privileges', '-p', `127.0.0.1:${port}:4000`, '--mount', `type=bind,source=${jwks},target=/run/secrets/public.json,readonly`, '-e', 'HIGHWAY_STORAGE_MODE=development_memory', '-e', 'HIGHWAY_JWKS_FILE=/run/secrets/public.json', '-e', 'HIGHWAY_JWT_ISSUER=issuer', '-e', 'HIGHWAY_JWT_AUDIENCE=highway', 'highway:0.1.0']);
  let healthy = false;
  for (let i = 0; i < 150; i++) { try { if ((await fetch(`http://127.0.0.1:${port}/healthz`)).ok) { healthy = true; break; } } catch {} await delay(50); }
  assert(healthy, 'container did not boot');
  assert.equal((await run('docker', ['exec', name, 'id', '-u'])).stdout.trim(), '10001');
  assert.equal((await fetch(`http://127.0.0.1:${port}/readyz`)).status, 503);
  await client.connect();
  await client.publish({ stream: 'events', messageId: 'seed', data: null });
  const sub = await client.subscribe({ stream: 'events', from: 'latest' });
  let persisted = 0; client.on('persisted', () => persisted++);
  const accepted = await client.publish({ stream: 'events', messageId: 'live', data: { text: 'container' } });
  let event;
  for (let i = 0; i < 3; i++) { const next = await sub.next(); if (next.value.type === 'event') { event = next.value; break; } }
  assert.equal(event.cursor, accepted.cursor); sub.ack(event.cursor);
  assert.equal(persisted, 0);
  const metrics = await fetch(`http://127.0.0.1:${port}/metrics`).then(r => r.text());
  assert(metrics.includes('highway_accepted_events_total 2\n'));
  await run('docker', ['stop', '--time', '10', name]);
  assert.equal((await run('docker', ['inspect', '--format', '{{.State.ExitCode}}', name])).stdout.trim(), '0');
  const logs = await run('docker', ['logs', name]);
  assert(logs.stdout.includes('Drain completed'));
  assert(!logs.stdout.includes(token) && !logs.stderr.includes(token));
  console.log('PASS: non-root read-only container, public JWKS mount, JWT live WebSockets, metrics, explicit ephemeral semantics and SIGTERM drain.');
} catch (error) { const logs = await run('docker', ['logs', name]).catch(() => ({ stdout: '', stderr: '' })); console.error(logs.stdout, logs.stderr); throw error; }
finally { client.close(); await run('docker', ['rm', '-f', name]).catch(() => {}); await rm(dir, { recursive: true, force: true }); }
