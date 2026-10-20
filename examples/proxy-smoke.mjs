// Validate public route isolation through actual Caddy routing, without DNS or
// public certificate issuance. The deployed Caddyfile uses automatic HTTPS.
import { execFile } from 'node:child_process';
import { promisify } from 'node:util';
import { resolve } from 'node:path';
import { setTimeout as delay } from 'node:timers/promises';
import assert from 'node:assert/strict';
const run = promisify(execFile), name = `highway-proxy-smoke-${process.pid}`;
const port = Number(process.env.PROXY_SMOKE_PORT || 4408), base = `http://127.0.0.1:${port}`;
try {
  await run('docker', ['run', '-d', '--name', name, '-p', `127.0.0.1:${port}:8080`, '-e', 'HIGHWAY_PUBLIC_HOST=http://:8080', '--mount', `type=bind,source=${resolve('deploy/Caddyfile')},target=/etc/caddy/Caddyfile,readonly`, 'caddy:2-alpine']);
  let ready = false;
  for (let i = 0; i < 100; i++) { try { if ((await fetch(`${base}/`)).status === 404) { ready = true; break; } } catch {} await delay(50); }
  assert(ready, 'Caddy did not boot');
  for (const route of ['healthz', 'readyz', 'metrics', 'unknown']) assert.equal((await fetch(`${base}/${route}`)).status, 404);
  // Backend is deliberately absent: 502 proves /v1/ws reached reverse_proxy
  // rather than the fallback 404 (Caddy directive ordering matters).
  assert.equal((await fetch(`${base}/v1/ws`)).status, 502);
  console.log('PASS: Caddy public WebSocket route reaches proxy; readiness, liveness and metrics stay private.');
} finally { await run('docker', ['rm', '-f', name]).catch(() => {}); }
