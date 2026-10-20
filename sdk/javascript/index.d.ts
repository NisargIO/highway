export interface TokenContext { signal: AbortSignal }
export interface StreamTokenContext extends TokenContext { stream: string; subscription?: string }
export type StreamTokenProvider = (context: StreamTokenContext) => string | Promise<string>;
export type TokenProvider = (context: TokenContext) => string | Promise<string>;
export interface ClientOptions {
  url: string;
  /** Omit for protocol v2 per-subscription/per-publish credentials. */
  getToken?: TokenProvider;
  WebSocket?: typeof globalThis.WebSocket;
  reconnect?: boolean;
  requestTimeoutMs?: number;
  refreshMs?: number;
  retryMs?: number;
  maxPending?: number;
  maxBufferedBytes?: number;
  maxSubscriptions?: number;
}
export type From = 'earliest' | 'latest' | { after: string };
export interface SubscribeOptions { stream: string; subscription?: string; from?: From; follow?: boolean; window?: number; autoAck?: boolean; getToken?: StreamTokenProvider }
export interface Accepted { type: 'accepted'; id: string; stream: string; cursor: string; message_id: string }
export interface Persisted { type: 'persisted'; stream: string; through: string }
export interface Event<T = unknown> { type: 'event'; subscription: string; stream: string; cursor: string; message_id: string; data: T }
export interface Discontinuity { type: 'discontinuity'; subscription: string; after: string; before_epoch: string; reason: 'interrupted_epoch'; lost_count: null }
export interface CaughtUp { type: 'caught_up'; subscription: string; through: string }
export interface Authenticated { type: 'authenticated'; id: string; subject: string; tenant: string; expires_at: number }
export interface SubscriptionAuthenticated { type: 'subscription_authenticated'; version: 2; id: string; subscription: string; subject: string; tenant: string; expires_at: number }
export interface TransportConnected { type: 'connected'; version: 2 }
export interface PublishOptions { stream: string; messageId: string; data: unknown; getToken?: StreamTokenProvider }
export type SubscriptionFrame<T = unknown> = Event<T> | Discontinuity | CaughtUp;
export class HighwayError extends Error { code: string; retryable?: boolean; constructor(code: string, message?: string, details?: Record<string, unknown>) }
export class PublishUncertainError extends HighwayError { messageId: string; constructor(messageId: string) }
export interface ClientEvents {
  accepted: Accepted; persisted: Persisted; caughtUp: CaughtUp; connected: Authenticated | TransportConnected;
  disconnected: { code: number }; reconnecting: { delay: number };
  error: Error; subscriptionError: { subscription: string; error: Error };
}
export class HighwayClient {
  constructor(options: ClientOptions);
  readonly connected: boolean;
  connect(): Promise<void>;
  refresh(): Promise<Authenticated>;
  publish(input: PublishOptions): Promise<Accepted>;
  subscribe<T = unknown>(options: SubscribeOptions): Promise<Subscription<T>>;
  on<K extends keyof ClientEvents>(type: K, listener: (event: ClientEvents[K]) => void): () => void;
  close(): void;
}
/** One consumer per subscription. Automatic ACK happens when next() is called
 * after processing an event. Explicit ack is available for batch/manual control.
 * Reconnect requires History and resumes from the processed checkpoint.
 */
export class Subscription<T = unknown> implements AsyncIterableIterator<SubscriptionFrame<T>> {
  private constructor();
  readonly id: string; readonly stream: string; readonly window: number;
  readonly expiresAt: number | undefined;
  readonly checkpoint: string | undefined; readonly highWatermark: string | undefined;
  [Symbol.asyncIterator](): this;
  next(): Promise<IteratorResult<SubscriptionFrame<T>>>;
  return(): Promise<IteratorResult<SubscriptionFrame<T>>>;
  ack(cursor: string): void;
  refresh(): Promise<SubscriptionAuthenticated | Authenticated>;
  close(): Promise<void>;
}
