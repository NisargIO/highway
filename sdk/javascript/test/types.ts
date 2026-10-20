import { HighwayClient, type Event, type SubscriptionFrame, PublishUncertainError } from '../index.js';
const c = new HighwayClient({ url: 'wss://stream.example.com/v1/ws', getToken: async ({ signal }) => {
  const response = await fetch('/api/highway-token', { method: 'POST', signal });
  return (await response.json() as { token: string }).token;
} });
c.on('persisted', frame => { const cursor: string = frame.through; void cursor; });
const s = await c.subscribe<{ text: string }>({ stream: 'events', from: 'earliest', window: 16 });
for await (const f of s) {
  const frame: SubscriptionFrame<{ text: string }> = f;
  if (frame.type === 'event') { const event: Event<{ text: string }> = frame; event.data.text.toUpperCase(); s.ack(event.cursor); }
}
try { await c.publish({ stream: 'events', messageId: 'm', data: null }); }
catch (error) { if (error instanceof PublishUncertainError) error.messageId.toUpperCase(); }
c.close();
