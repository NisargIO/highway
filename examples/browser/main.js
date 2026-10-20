import { HighwayClient } from '../../sdk/javascript/index.js';
const el = id => document.getElementById(id);
let client, subscription, token, accepted = 0, persisted = 0;
const status = text => { el('status').textContent = text; };
function row(text) {
  const item = document.createElement('li'); item.textContent = text.slice(0, 1200);
  el('events').prepend(item); while (el('events').children.length > 40) el('events').lastChild.remove();
}
function progress() { el('progress').textContent = `Accepted: ${accepted} · Persistence notices: ${persisted}`; }
function takeToken() { const value = el('token').value.trim(); el('token').value = ''; if (value) token = value; }
el('connect').onclick = async () => {
  el('connect').disabled = true;
  try {
    client?.close(); subscription = undefined; el('refresh').disabled = true; takeToken();
    if (!token) throw new Error('Paste a JWT issued by your backend');
    client = new HighwayClient({ url: el('url').value });
    client.on('connected', () => status('Connected — replay and live delivery share this connection'));
    client.on('reconnecting', () => status('Reconnecting from the processed cursor…'));
    client.on('error', error => status(`Connection: ${error.code ?? 'error'}`));
    client.on('subscriptionError', ({ error }) => status(`Subscription ended: ${error.code}`));
    client.on('accepted', frame => { accepted++; progress(); status(`Accepted ${frame.cursor} into memory; persistence is separate`); });
    client.on('persisted', frame => { persisted++; progress(); row(`Persisted through ${frame.through}`); });
    await client.connect();
    for (const id of ['disconnect', 'publish']) el(id).disabled = false;
    // A missing stream must first be created by an authorized publisher.
    const sub = subscription = await client.subscribe({ stream: el('stream').value, getToken: () => token, from: 'latest', window: 16 });
    el('refresh').disabled = false;
    for await (const frame of sub) {
      if (frame.type === 'event') row(`${frame.cursor}  ${JSON.stringify(frame.data)}`);
      else if (frame.type === 'discontinuity') row(`Interrupted epoch before ${frame.before_epoch}; tail after ${frame.after} is uncertain`);
      else row(`Caught up through ${frame.through}`);
    }
  } catch (error) { status(error.code ?? error.message); }
  finally { el('connect').disabled = false; }
};
el('refresh').onclick = async () => { try { takeToken(); await subscription.refresh(); status('Replacement token verified'); } catch (error) { status(`Refresh: ${error.code}`); } };
el('publish').onclick = async () => {
  try { await client.publish({ stream: el('stream').value, messageId: crypto.randomUUID(), data: JSON.parse(el('data').value), getToken: () => token }); }
  catch (error) { status(error.code === 'publish_uncertain' ? 'Publish outcome unknown. A retry may create a duplicate.' : error.code ?? 'Invalid JSON'); }
};
el('disconnect').onclick = () => { client?.close(); token = undefined; status('Disconnected'); for (const id of ['refresh', 'disconnect', 'publish']) el(id).disabled = true; };
addEventListener('pagehide', () => client?.close());
