// Dependency-free ESM. Tokens and payloads are never written to logs or storage.
const bytes = value => new TextEncoder().encode(value).byteLength;
const identifier = value => typeof value === 'string' && bytes(value) > 0 && bytes(value) <= 200 && !/[\u0000-\u001f\u007f]/u.test(value);
const deferred = () => { let resolve, reject; const promise = new Promise((a, b) => { resolve = a; reject = b; }); return { promise, resolve, reject }; };
const bounded = (n, fallback, max) => {
  n ??= fallback;
  if (!Number.isInteger(n) || n < 1 || n > max) throw new RangeError('Invalid client capacity or timeout');
  return n;
};
export class HighwayError extends Error {
  constructor(code, message = code, details = {}) { super(message); this.name = 'HighwayError'; this.code = code; Object.assign(this, details); }
}
export class PublishUncertainError extends HighwayError {
  constructor(messageId) { super('publish_uncertain', 'Publish outcome is unknown; retry can append a duplicate', { messageId }); this.name = 'PublishUncertainError'; }
}

export class HighwayClient {
  constructor(options) {
    const url = new URL(options.url);
    if (!['ws:', 'wss:'].includes(url.protocol) || url.username || url.password || url.search || url.hash)
      throw new TypeError('Use a WebSocket URL without credentials, query, or fragment');
    if (options.getToken !== undefined && typeof options.getToken !== 'function') throw new TypeError('getToken must be a function');
    this._scoped = options.getToken === undefined;
    this.url = url.href; this.getToken = options.getToken;
    this.WebSocket = options.WebSocket ?? globalThis.WebSocket;
    if (!this.WebSocket) throw new TypeError('A WebSocket implementation is required');
    this.reconnect = options.reconnect ?? true;
    this.requestTimeoutMs = bounded(options.requestTimeoutMs, 10_000, 300_000);
    this.refreshMs = bounded(options.refreshMs, 25_000, 25_000);
    this.maxPending = bounded(options.maxPending, 32, 128);
    this.maxBufferedBytes = bounded(options.maxBufferedBytes, 1_048_576, 16_777_216);
    this.maxSubscriptions = bounded(options.maxSubscriptions, 64, 64);
    this.retryMs = bounded(options.retryMs, 250, 5000);
    this._pending = new Map(); this._subscriptions = new Map(); this._listeners = new Map();
    this._tokenRequests = 0; this._buffered = 0; this._serial = 0; this._epoch = 0; this._attempt = 0;
    this._closed = false; this._connected = false;
  }
  get connected() { return this._connected; }
  on(type, listener) {
    let group = this._listeners.get(type);
    if (!group) this._listeners.set(type, group = new Set());
    group.add(listener); return () => group.delete(listener);
  }
  _emit(type, value) { for (const fn of this._listeners.get(type) ?? []) { try { fn(value); } catch { /* Listener errors must not break connection cleanup. */ } } }
  async connect() {
    if (this._closed) throw new HighwayError('closed');
    if (this._connected) return;
    if (!this._ready) this._ready = deferred();
    if (!this._ws && !this._retryTimer) this._open();
    return this._ready.promise;
  }
  _open() {
    const epoch = ++this._epoch;
    let ws;
    try { ws = new this.WebSocket(this.url); } catch { this._retry(); return; }
    this._ws = ws; this._authAbort = new AbortController();
    const valid = () => this._ws === ws && epoch === this._epoch;
    const disconnect = code => {
      if (!valid()) return;
      clearTimeout(this._handshake); clearTimeout(this._refreshTimer);
      this._authAbort.abort();
      this._ws = undefined; this._connected = false; this._refreshing = undefined;
      for (const [id, p] of this._pending) {
        clearTimeout(p.timer); p.reject(p.publish ? new PublishUncertainError(p.messageId) : new HighwayError('disconnected'));
        this._pending.delete(id);
      }
      for (const sub of this._subscriptions.values()) sub._pause();
      this._emit('disconnected', { code }); this._retry();
    };
    const fail = () => {
      if (!valid()) return;
      // Node 22's native WebSocket can emit only error on failed upgrades,
      // leaving readyState CONNECTING and never emitting close. Detach first
      // so an eventual close/error cannot schedule a second reconnect.
      disconnect(1006);
      try { ws.close(); } catch { /* Already detached; transport cleanup only. */ }
    };
    this._handshake = setTimeout(fail, this.requestTimeoutMs);
    ws.addEventListener('message', e => {
      if (!valid()) return;
      try {
        if (typeof e.data !== 'string' || bytes(e.data) > 262_144) throw new HighwayError('invalid_frame');
        const frame = JSON.parse(e.data);
        this._frame(frame, epoch);
      } catch (error) { const safe = error instanceof HighwayError ? error : new HighwayError('invalid_frame'); this._emit('error', safe); this._terminate(safe); }
    });
    ws.addEventListener('error', fail);
    ws.addEventListener('close', e => disconnect(e.code));
  }
  _retry() {
    if (this._closed) return;
    if (!this.reconnect) { this._terminate(new HighwayError('disconnected')); return; }
    const delay = Math.min(5000, this.retryMs * 2 ** Math.min(this._attempt++, 5));
    this._emit('reconnecting', { delay });
    this._retryTimer = setTimeout(() => { this._retryTimer = undefined; this._open(); }, delay);
  }
  _frame(f, epoch) {
    if (!f || typeof f !== 'object' || typeof f.type !== 'string') throw new HighwayError('invalid_frame');
    if (f.type === 'hello') {
      if (this._helloEpoch === epoch) throw new HighwayError('invalid_frame');
      this._helloEpoch = epoch;
      if (f.version !== 1) throw new HighwayError('unsupported_version');
      if (this._scoped) {
        if (!Array.isArray(f.supported_versions) || !f.supported_versions.includes(2)) throw new HighwayError('unsupported_version');
        this._authenticated({ type: 'connected', version: 2 });
        return;
      }
      this._authenticate(epoch).then(frame => this._authenticated(frame)).catch(error => { if (epoch === this._epoch && !this._closed) { this._emit('error', error); this._ws?.close(); } });
      return;
    }
    if (f.id && this._pending.has(f.id)) {
      const p = this._pending.get(f.id);
      this._pending.delete(f.id); clearTimeout(p.timer);
      if (f.type === 'error') p.reject(new HighwayError(f.code, f.code, { retryable: f.retryable ?? false }));
      else p.resolve(f);
      return;
    }
    // Timed-out scoped requests are followed by ordered unsubscribe. Their
    // late success replies must not resurrect a view or break sibling scopes.
    // Monotonic generated IDs bound this check without a growing tombstone set.
    if (this._scoped && ['subscribed', 'subscription_authenticated'].includes(f.type)
      && typeof f.id === 'string' && /^c[1-9][0-9]*$/.test(f.id)
      && Number.isSafeInteger(Number(f.id.slice(1))) && Number(f.id.slice(1)) <= this._serial) return;
    const sub = this._subscriptions.get(f.subscription);
    switch (f.type) {
      case 'event':
        if (typeof f.cursor !== 'string' || typeof f.message_id !== 'string' || typeof f.stream !== 'string' || !('data' in f)) throw new HighwayError('invalid_frame');
        if (sub) sub._receive(f); break;
      case 'discontinuity':
        if (typeof f.after !== 'string' || typeof f.before_epoch !== 'string' || f.reason !== 'interrupted_epoch' || f.lost_count !== null) throw new HighwayError('invalid_frame');
        if (sub) sub._receive(f); break;
      case 'caught_up':
        if (typeof f.through !== 'string') throw new HighwayError('invalid_frame');
        this._emit('caughtUp', f); if (sub) sub._receive(f); break;
      case 'complete': if (sub) sub._finish(); break;
      case 'persisted':
        if (typeof f.stream !== 'string' || typeof f.through !== 'string') throw new HighwayError('invalid_frame');
        this._emit('persisted', f); break;
      case 'error': {
        const error = new HighwayError(f.code ?? 'protocol_error', f.code ?? 'protocol_error', { retryable: f.retryable ?? false });
        if (sub) sub._fail(error, false); else this._emit('error', error); break;
      }
      default: throw new HighwayError('invalid_frame');
    }
  }
  async _authenticate(epoch) {
    const token = await this.getToken({ signal: this._authAbort.signal });
    if (epoch !== this._epoch || !this._ws || this._closed) throw new HighwayError('disconnected');
    if (typeof token !== 'string' || !token || bytes(token) > 16_000) throw new HighwayError('invalid_token');
    const frame = await this._request({ type: 'auth', token });
    if (frame.type !== 'authenticated' || !Number.isSafeInteger(frame.expires_at) || typeof frame.subject !== 'string' || typeof frame.tenant !== 'string') throw new HighwayError('invalid_frame');
    this._recordIdentity(frame);
    if (epoch !== this._epoch || this._closed) throw new HighwayError('disconnected');
    this._expires = frame.expires_at; this._scheduleRefresh();
    return frame;
  }
  _recordIdentity(frame) {
    if (!Number.isSafeInteger(frame.expires_at) || typeof frame.subject !== 'string' || typeof frame.tenant !== 'string') throw new HighwayError('invalid_frame');
    if (this._identity && (this._identity.subject !== frame.subject || this._identity.tenant !== frame.tenant)) {
      const error = new HighwayError('identity_changed'); this._terminate(error); throw error;
    }
    this._identity = { subject: frame.subject, tenant: frame.tenant };
  }
  _scheduleRefresh() {
    clearTimeout(this._refreshTimer);
    const remaining = this._expires * 1000 - Date.now();
    const delay = Math.max(50, Math.min(this.refreshMs, remaining > 2000 ? remaining - 1000 : remaining / 2));
    this._refreshTimer = setTimeout(() => this.refresh().catch(error => {
      this._emit('error', error);
      // A failed refresh never extends the old JWT. Closing lets reconnect get
      // fresh credentials; replay still starts at the processed checkpoint.
      this._ws?.close();
    }), delay);
  }
  async refresh() {
    if (this._scoped) throw new HighwayError('subscription_auth_required', 'Refresh each subscription with sub.refresh()');
    if (this._refreshing) return this._refreshing;
    if (!this._ws) throw new HighwayError('disconnected');
    const pending = this._authenticate(this._epoch);
    this._refreshing = pending;
    try { return await pending; } finally { if (this._refreshing === pending) this._refreshing = undefined; }
  }
  _request(command, publish = false) {
    if (!this._ws || this._ws.readyState !== 1) return Promise.reject(new HighwayError('disconnected'));
    if (this._pending.size + this._tokenRequests >= this.maxPending) return Promise.reject(new HighwayError('client_overloaded'));
    const id = `c${++this._serial}`;
    let text;
    try { text = JSON.stringify({ ...command, id }); } catch { return Promise.reject(new HighwayError('invalid_payload')); }
    if ((this._ws.bufferedAmount ?? 0) + bytes(text) > this.maxBufferedBytes) return Promise.reject(new HighwayError('client_overloaded'));
    if (bytes(text) > (['auth', 'subscription_auth'].includes(command.type) || command.version === 2 && command.type === 'subscribe' ? 16_384 : 262_144)) return Promise.reject(new HighwayError('frame_too_large'));
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        this._pending.delete(id);
        reject(publish ? new PublishUncertainError(command.message_id) : new HighwayError('request_timeout'));
        // A late command may have changed server state (especially subscribe).
        // Reset the connection instead of continuing with untracked state.
        if (command.version === 2 && ['subscribe', 'subscription_auth'].includes(command.type)) {
          this._cancelSubscription(command.subscription).catch(() => this._ws?.close());
        } else this._ws?.close();
      }, this.requestTimeoutMs);
      this._pending.set(id, { resolve, reject, timer, publish, messageId: command.message_id });
      try { this._ws.send(text); } catch {
        clearTimeout(timer); this._pending.delete(id);
        reject(publish ? new PublishUncertainError(command.message_id) : new HighwayError('disconnected'));
        this._ws?.close();
      }
    });
  }
  async publish({ stream, messageId, data, getToken }) {
    // No offline publish queue and no automatic retries, including reconnect.
    if (!this._connected) throw new HighwayError('disconnected');
    if (!identifier(stream) || !identifier(messageId) || data === undefined) throw new TypeError('stream, messageId and data are required');
    let encoded;
    try { encoded = JSON.stringify(data); } catch { throw new HighwayError('invalid_payload'); }
    if (encoded === undefined || bytes(encoded) > 65_536) throw new HighwayError('frame_too_large');
    const epoch = this._epoch;
    let authorization = {};
    if (this._scoped) {
      if (typeof getToken !== 'function') throw new TypeError('Scoped publish requires getToken');
      if (this._tokenRequests + this._pending.size >= this.maxPending) throw new HighwayError('client_overloaded');
      this._tokenRequests++;
      let token;
      try { token = await scopedToken(getToken, this._authAbort.signal, { stream }, this.requestTimeoutMs); }
      finally { this._tokenRequests--; }
      if (epoch !== this._epoch || !this._connected) throw new HighwayError('disconnected');
      authorization = { version: 2, token };
    } else if (getToken !== undefined) throw new TypeError('Use a client without a default getToken for scoped authorization');
    const f = await this._request({ type: 'publish', stream, message_id: messageId, data, ...authorization }, true);
    if (f.type !== 'accepted' || typeof f.cursor !== 'string' || f.stream !== stream || f.message_id !== messageId) throw new PublishUncertainError(messageId);
    if (this._scoped) {
      try { this._recordIdentity(f); } catch (error) { this._terminate(error); throw new PublishUncertainError(messageId); }
    }
    this._emit('accepted', f); return f;
  }
  async subscribe(options) {
    if (this._subscriptions.size >= this.maxSubscriptions) throw new HighwayError('client_overloaded');
    const sub = new Subscription(this, options);
    if (this._subscriptions.has(sub.id)) throw new HighwayError('duplicate_subscription');
    this._subscriptions.set(sub.id, sub);
    try { await this.connect(); await sub._start(); return sub; } catch (error) { sub._fail(error); throw error; }
  }
  _authenticated(frame) {
    clearTimeout(this._handshake); this._connected = true; this._attempt = 0;
    const epoch = this._epoch;
    const restoreOne = async sub => {
      try { await sub._start(); } catch (error) {
        if (this._connected && epoch === this._epoch) sub._fail(error);
      }
    };
    if (this._scoped) {
      // Providers are independent and bounded by maxSubscriptions. A stalled
      // issuer must not keep healthy scopes behind the opening auth deadline.
      for (const sub of this._subscriptions.values()) void restoreOne(sub);
    } else {
      void (async () => {
        for (const sub of [...this._subscriptions.values()]) {
          if (!this._connected || epoch !== this._epoch) break;
          await restoreOne(sub);
        }
      })();
    }
    const ready = this._ready; this._ready = undefined; ready?.resolve();
    this._emit('connected', frame);
  }
  async _cancelSubscription(id) {
    try { await this._request({ type: 'unsubscribe', subscription: id, ...(this._scoped ? { version: 2 } : {}) }); }
    catch (error) { if (error.code !== 'invalid_frame') throw error; }
  }
  _ack(id, cursor) {
    if (this._connected && this._ws?.readyState === 1) {
      if ((this._ws.bufferedAmount ?? 0) > this.maxBufferedBytes - 1024) { this._emit('error', new HighwayError('client_overloaded')); this._ws.close(); return; }
      this._ws.send(JSON.stringify({ type: 'ack', subscription: id, through: cursor, ...(this._scoped ? { version: 2 } : {}) }));
    }
  }
  close() { this._terminate(new HighwayError('closed')); }
  _terminate(error) {
    if (this._closed) return;
    this._closed = true; this._connected = false;
    clearTimeout(this._handshake); clearTimeout(this._retryTimer); clearTimeout(this._refreshTimer);
    this._ready?.reject(error); this._ready = undefined;
    for (const p of this._pending.values()) { clearTimeout(p.timer); p.reject(p.publish ? new PublishUncertainError(p.messageId) : error); }
    this._pending.clear();
    for (const sub of [...this._subscriptions.values()]) sub._fail(error);
    this._authAbort?.abort(); this._ws?.close(); this._ws = undefined;
  }
}

export class Subscription {
  constructor(client, options) {
    this.client = client; this.stream = options.stream;
    this.getToken = options.getToken;
    if (client._scoped && typeof this.getToken !== 'function') throw new TypeError('Scoped subscribe requires getToken');
    if (!client._scoped && this.getToken !== undefined) throw new TypeError('Use a client without a default getToken for scoped authorization');
    if (!identifier(this.stream)) throw new TypeError('stream is required');
    this.id = options.subscription ?? `s${++client._serial}`;
    if (!identifier(this.id)) throw new TypeError('Invalid subscription ID');
    this.from = options.from ?? 'latest';
    if (this.from !== 'latest' && this.from !== 'earliest' && !(this.from && typeof this.from === 'object' && typeof this.from.after === 'string' && this.from.after.length > 0 && bytes(this.from.after) <= 512)) throw new TypeError('Invalid from'); this.follow = options.follow ?? true;
    this.window = bounded(options.window, 128, 1024); this.autoAck = options.autoAck ?? true;
    this.checkpoint = typeof this.from === 'object' ? this.from.after : undefined;
    this._queue = []; this._delivered = []; this._recent = new Set(); this._received = new Set();
    this._startedEpoch = 0; this._ended = false;
  }
  async _start() {
    if (this._ended || !this.client._connected) return;
    if (this._startedEpoch === this.client._epoch) return this._starting;
    const epoch = this._startedEpoch = this.client._epoch;
    this._starting = (async () => {
      const deadline = Date.now() + this.client.requestTimeoutMs;
      this._tokenAbort = new AbortController();
      const authorization = this.client._scoped ? { version: 2, token: await scopedToken(this.getToken, this._tokenAbort.signal, { stream: this.stream, subscription: this.id }, this.client.requestTimeoutMs) } : {};
      let f;
      while (!this._ended && epoch === this.client._epoch && this.client._connected) {
        try {
          f = await this.client._request({ type: 'subscribe', subscription: this.id, stream: this.stream,
            from: this.checkpoint ? { after: this.checkpoint } : this.from, follow: this.follow, window: this.window, ...authorization });
          break;
        } catch (error) {
          // These typed rejections mean no subscription was created. This is
          // safe to retry during owner epoch initialization; publishes differ.
          if (!['unavailable', 'rate_limited', 'client_overloaded'].includes(error.code) || Date.now() >= deadline) throw error;
          await new Promise(resolve => setTimeout(resolve, 50));
        }
      }
      if (!f) return;
      if (epoch !== this.client._epoch) return;
      if (f.type !== 'subscribed' || typeof f.high_watermark !== 'string') throw new HighwayError('invalid_frame');
      if (!this._ended) this._serverActive = true;
      if (this.client._scoped) { this.client._recordIdentity(f); this.expiresAt = f.expires_at; }
      this.highWatermark = f.high_watermark;
      if (this.from === 'latest' && !this.checkpoint) this.checkpoint = f.high_watermark;
      if (this._ended) return;
      this._serverActive = true;
      this._scheduleAuth();
    })();
    return this._starting;
  }
  _scheduleAuth() {
    clearTimeout(this._authTimer);
    if (!this.client._scoped || this._ended) return;
    const remaining = this.expiresAt * 1000 - Date.now();
    const delay = Math.max(50, Math.min(this.client.refreshMs, remaining > 2000 ? remaining - 1000 : remaining / 2));
    this._authTimer = setTimeout(() => this.refresh().catch(() => {}), delay);
  }
  async refresh() {
    if (!this.client._scoped) return this.client.refresh();
    if (this._ended) throw new HighwayError('closed');
    if (!this.client._connected || !this._serverActive) throw new HighwayError('disconnected');
    if (this._refreshing) return this._refreshing;
    const epoch = this.client._epoch;
    const pending = (async () => {
      try {
        const token = await scopedToken(this.getToken, this._tokenAbort.signal, { stream: this.stream, subscription: this.id }, this.client.requestTimeoutMs);
        if (this._ended || epoch !== this.client._epoch) throw new HighwayError('disconnected');
        const frame = await this.client._request({ version: 2, type: 'subscription_auth', subscription: this.id, token });
        if (this._ended || epoch !== this.client._epoch) throw new HighwayError('disconnected');
        if (frame.type !== 'subscription_authenticated' || frame.subscription !== this.id) throw new HighwayError('invalid_frame');
        this.client._recordIdentity(frame); this.expiresAt = frame.expires_at; this._scheduleAuth();
        return frame;
      } catch (error) {
        if (!this._ended && epoch === this.client._epoch && this.client._connected && this._serverActive) this._fail(error);
        throw error;
      }
    })();
    this._refreshing = pending;
    try { return await pending; } finally { if (this._refreshing === pending) this._refreshing = undefined; }
  }
  _receive(frame) {
    if (this._ended) return;
    if (frame.stream !== undefined && frame.stream !== this.stream) { this._fail(new HighwayError('invalid_frame')); return; }
    if (frame.type === 'event') {
      this._received.add(frame.cursor);
      // Already yielded before reconnect: the application still owns this
      // unacknowledged item. Its eventual ACK must credit the new connection.
      if (this._delivered.includes(frame.cursor)) return;
      if (this._recent.has(frame.cursor)) { this.client._ack(this.id, frame.cursor); this._received.delete(frame.cursor); return; }
    }
    const size = bytes(JSON.stringify(frame));
    if (this._queue.length >= this.window + 16 || this.client._buffered + size > this.client.maxBufferedBytes) {
      this._fail(new HighwayError('slow_consumer')); return;
    }
    this._queue.push({ frame, size }); this.client._buffered += size; this._wake();
  }
  ack(cursor) {
    if (cursor === this.checkpoint) return;
    const index = this._delivered.indexOf(cursor);
    if (index < 0) throw new HighwayError('cursor_invalid');
    const canSend = this._received.has(cursor);
    for (const delivered of this._delivered.splice(0, index + 1)) {
      this._received.delete(delivered);
      this._recent.add(delivered);
      while (this._recent.size > this.window) this._recent.delete(this._recent.values().next().value);
    }
    this.checkpoint = cursor;
    if (canSend && !this._ended) this.client._ack(this.id, cursor);
  }
  [Symbol.asyncIterator]() { return this; }
  async next() {
    if (this._nextBusy) throw new HighwayError('concurrent_iteration');
    this._nextBusy = true;
    try {
      if (this.autoAck && this._lastEvent) { this.ack(this._lastEvent); this._lastEvent = undefined; }
      while (!this._queue.length && !this._ended) { this._wait = deferred(); await this._wait.promise; this._wait = undefined; }
      if (this._error) throw this._error;
      let item = this._queue.shift();
      while (item?.frame.type === 'event' && this._recent.has(item.frame.cursor)) {
        this.client._buffered -= item.size;
        if (this._received.has(item.frame.cursor) && !this._ended) this.client._ack(this.id, item.frame.cursor);
        this._received.delete(item.frame.cursor);
        item = this._queue.shift();
        if (!item && !this._ended) { this._wait = deferred(); await this._wait.promise; this._wait = undefined; if (this._error) throw this._error; item = this._queue.shift(); }
      }
      if (!item) return { done: true, value: undefined };
      this.client._buffered -= item.size;
      if (item.frame.type === 'event') {
        this._delivered.push(item.frame.cursor); this._lastEvent = item.frame.cursor;
        if (this._delivered.length > this.window) { this._fail(new HighwayError('slow_consumer')); throw this._error; }
      }
      return { done: false, value: item.frame };
    } finally { this._nextBusy = false; }
  }
  async return() { await this.close(); return { done: true, value: undefined }; }
  async close() {
    if (this._ended) return;
    if (this.autoAck && this._lastEvent) { this.ack(this._lastEvent); this._lastEvent = undefined; }
    this._finish(); this._clear();
    if (this.client._connected) await this.client._cancelSubscription(this.id);
  }
  _clear() { for (const item of this._queue) this.client._buffered -= item.size; this._queue = []; }
  _pause() { clearTimeout(this._authTimer); this._tokenAbort?.abort(); this._refreshing = undefined; this._clear(); this._received.clear(); this._startedEpoch = 0; this._serverActive = false; }
  _wake() { this._wait?.resolve(); }
  _finish() { clearTimeout(this._authTimer); this._tokenAbort?.abort(); this._ended = true; this.client._subscriptions.delete(this.id); this._wake(); }
  _fail(error, cancel = true) {
    if (this._ended) return;
    const wasActive = this._serverActive;
    this._error = error; this._clear(); this._finish();
    this.client._emit('subscriptionError', { subscription: this.id, error });
    // Free server-side view/credit as well. Closing the socket is the fallback
    // when a bounded request slot cannot be allocated for cancellation.
    if (cancel && wasActive && this.client._connected) this.client._cancelSubscription(this.id).catch(() => this.client._ws?.close());
  }
}

// Bound provider waiting independently per scope, and discard late callbacks.
async function scopedToken(provider, signal, context, timeoutMs) {
  let timer, abort;
  const controller = new AbortController();
  try {
    if (signal.aborted) throw new HighwayError('disconnected');
    const token = await Promise.race([
      Promise.resolve().then(() => provider({ ...context, signal: controller.signal })),
      new Promise((_, reject) => {
        abort = () => { controller.abort(); reject(new HighwayError('disconnected')); };
        signal.addEventListener('abort', abort, { once: true });
        timer = setTimeout(() => { controller.abort(); reject(new HighwayError('token_timeout')); }, timeoutMs);
      }),
    ]);
    if (signal.aborted) throw new HighwayError('disconnected');
    if (typeof token !== 'string' || !token || bytes(token) > 16_000) throw new HighwayError('invalid_token');
    return token;
  } finally { clearTimeout(timer); controller.abort(); if (abort) signal.removeEventListener('abort', abort); }
}
