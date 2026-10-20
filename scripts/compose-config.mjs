// Local-only issuer and storage credentials, never copied into build contexts.
import { generateKeyPairSync, createPrivateKey, randomBytes, sign } from 'node:crypto';
import { mkdir, readFile, writeFile, access } from 'node:fs/promises';
import { fileURLToPath } from 'node:url';
import { resolve, join } from 'node:path';

export const directory = fileURLToPath(new URL('../build/compose/', import.meta.url));
const names = ['minio-user', 'minio-password', 'public-jwks.json', 'signing-key.json'];

export async function configure() {
  await mkdir(resolve(directory, '..'), { recursive: true });
  try { await mkdir(directory, { mode: 0o700 }); }
  catch (error) {
    if (error.code !== 'EEXIST') throw error;
    // Never silently rotate credentials under an existing MinIO volume.
    for (const name of names) await access(join(directory, name));
    return;
  }
  const { publicKey, privateKey } = generateKeyPairSync('ed25519');
  const values = [randomBytes(12).toString('hex'), randomBytes(32).toString('hex'),
    JSON.stringify({ keys: [{ ...publicKey.export({ format: 'jwk' }), kid: 'compose' }] }),
    JSON.stringify(privateKey.export({ format: 'jwk' }))];
  for (let i = 0; i < names.length; i++) {
    // Files mounted individually must be readable by container UID 10001.
    // Host directory 0700 protects credentials from other host users.
    await writeFile(join(directory, names[i]), values[i] + '\n', { flag: 'wx', mode: i === 3 ? 0o600 : 0o444 });
  }
}

export async function token(stream = 'events') {
  const key = createPrivateKey({ key: JSON.parse(await readFile(join(directory, 'signing-key.json'), 'utf8')), format: 'jwk' });
  const now = Math.floor(Date.now() / 1000), b64 = x => Buffer.from(JSON.stringify(x)).toString('base64url');
  const input = `${b64({ alg: 'EdDSA', kid: 'compose' })}.${b64({ iss: 'highway-compose', aud: 'highway', sub: 'local-user', tenant: 'demo', iat: now, exp: now + 600, grants: [{ stream, actions: ['publish', 'subscribe', 'history'] }] })}`;
  return `${input}.${sign(null, Buffer.from(input), key).toString('base64url')}`;
}

if (process.argv[1] && resolve(process.argv[1]) === fileURLToPath(import.meta.url)) {
  await configure();
  if (process.argv[2] === 'token') {
    await writeFile(join(directory, 'token'), await token() + '\n', { mode: 0o600 });
    console.log('Local 10-minute JWT written to build/compose/token.');
  } else console.log('Local configuration ready in build/compose (ignored by git).');
}
