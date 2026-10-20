// Node 22+. Token file lives outside the repository and may be replaced before
// expiry. The same getToken callback is used on connect and automatic refresh.
import { readFile } from 'node:fs/promises';
import { randomUUID } from 'node:crypto';
import { HighwayClient } from '../sdk/javascript/index.js';
if (!process.env.HIGHWAY_TOKEN_FILE) throw new Error('Set HIGHWAY_TOKEN_FILE to a short-lived JWT file');
const client = new HighwayClient({
  url: process.env.HIGHWAY_URL || 'ws://127.0.0.1:4000/v1/ws',
  getToken: async () => (await readFile(process.env.HIGHWAY_TOKEN_FILE, 'utf8')).trim(),
});
const stream = process.env.HIGHWAY_STREAM || 'events';
client.on('persisted', frame => console.log(`Persisted ${frame.stream} through ${frame.through}`));
client.on('reconnecting', () => console.log('Reconnecting from processed cursor'));
client.on('subscriptionError', ({ error }) => console.error(`Subscription ended: ${error.code}`));
client.on('error', error => console.error(`Connection: ${error.code}`));
for (const signal of ['SIGINT', 'SIGTERM']) process.on(signal, () => client.close());
try {
  await client.connect();
  if (process.argv[2] === 'publish') {
    const accepted = await client.publish({ stream, messageId: randomUUID(), data: { text: 'Hello from Node.js' } });
    console.log(`Accepted ${accepted.cursor} into memory. Await separate persistence notices.`);
    // Remain connected for notices; Ctrl-C closes without retrying the publish.
  } else {
    const sub = await client.subscribe({ stream, from: process.env.HIGHWAY_AFTER ? { after: process.env.HIGHWAY_AFTER } : 'earliest' });
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
