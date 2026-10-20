// Node 22+. Keep the token file private and untracked; replace before expiry.
// The same getToken callback is used on connect and automatic refresh.
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { setTimeout as delay } from 'node:timers/promises';
import { HighwayClient } from '../sdk/javascript/index.js';
if (!process.env.HIGHWAY_TOKEN_FILE) throw new Error('Set HIGHWAY_TOKEN_FILE to a short-lived JWT file');
const client = new HighwayClient({
  url: process.env.HIGHWAY_URL || 'ws://127.0.0.1:4000/v1/ws',
});
const stream = process.env.HIGHWAY_STREAM || 'events';
const getToken = async () => (await readFile(process.env.HIGHWAY_TOKEN_FILE, 'utf8')).trim();
client.on('persisted', frame => console.log(`Persisted ${frame.stream} through ${frame.through}`));
client.on('reconnecting', () => console.log('Reconnecting from processed cursor'));
client.on('subscriptionError', ({ error }) => console.error(`Subscription ended: ${error.code}`));
client.on('error', error => console.error(`Connection: ${error.code}`));
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => client.close());
try {
  await client.connect();
  if (process.argv[2] === 'publish') {
    const messageId = randomUUID(), deadline = Date.now() + 5000;
    let accepted;
    for (;;) {
      try { accepted = await client.publish({ stream, messageId, data: { text: 'Hello from Node.js' }, getToken }); break; }
      catch (error) {
        // A new/recovered S3 owner allocates its epoch before admission. Retry
        // only an explicit rejection; never retry an unknown publish outcome.
        if (error.code !== 'unavailable' || Date.now() >= deadline) throw error;
        await delay(100);
      }
    }
    console.log(`Accepted ${accepted.cursor} into memory. Await separate persistence notices.`);
    // Remain connected for notices; Ctrl-C closes without retrying the publish.
  } else {
    const sub = await client.subscribe({ stream, getToken, from: process.env.HIGHWAY_AFTER ? { after: process.env.HIGHWAY_AFTER } : 'earliest' });
    for await (const frame of sub) {
      if (frame.type === 'event') {
        // Apply application work here. ACK follows processing, and callers may
        // save sub.checkpoint in their own storage. No payloads enter logs.
        console.log(`Received ${frame.cursor}`);
        sub.ack(frame.cursor);
      } else if (frame.type === 'discontinuity') console.log(`Uncertain tail after ${frame.after}, before epoch ${frame.before_epoch}`);
      else console.log(`Caught up through ${frame.through}`);
    }
  }
} catch (error) { if (error.code !== 'closed') { console.error(error.code ?? 'client_error'); client.close(); process.exitCode = 1; } }
