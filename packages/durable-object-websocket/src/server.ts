import { Effect } from "effect";
import { DurableObject } from "cloudflare:workers";
import { newWebSocketRpcSession, RpcStub, RpcTarget } from "capnweb";
import {
  Topic as CoreTopic,
  type MutationMap,
  type QueryMap,
  type SyncEngineInterface,
  type OperationParams,
  type StringKey,
  type Listener,
} from "@do-sync-engine/core";
import { Subscription, type QueryEvent, type Topic, type WebSocketRpcApi } from "./protocol";

type SubscriptionEntry = {
  readonly topic: CoreTopic;
  readonly queue: QueryEvent[];
  readonly waiters: Array<(event: QueryEvent | null) => void>;
  readonly listener: (event: { readonly topic: CoreTopic; readonly value: unknown }) => void;
  readonly release: () => Promise<void>;
  closed: boolean;
};

class SubscriptionTarget extends Subscription {
  constructor(private readonly entry: SubscriptionEntry) {
    super();
  }
  next(): Promise<QueryEvent | null> {
    const event = this.entry.queue.shift();
    if (event) return Promise.resolve(event);
    if (this.entry.closed) return Promise.resolve(null);
    const pending = Promise.withResolvers<QueryEvent | null>();
    this.entry.waiters.push(pending.resolve);
    return pending.promise;
  }
  async close(): Promise<void> {
    if (this.entry.closed) return;
    this.entry.closed = true;
    for (const resolve of this.entry.waiters.splice(0)) resolve(null);
    await this.entry.release();
    this.entry.queue.length = 0;
  }
  [Symbol.dispose](): void {
    void this.close();
  }
}

class ApiTarget<Q extends QueryMap<Q>, M extends MutationMap<M>>
  extends RpcTarget
  implements WebSocketRpcApi
{
  constructor(private readonly engine: SyncEngineInterface<Q, M>) {
    super();
  }
  async subscribe(wireTopic: Topic): Promise<Subscription> {
    const queryName = wireTopic.name as StringKey<Q>;
    const queryParams = wireTopic.params as OperationParams<Q[typeof queryName]>;
    const topic = new CoreTopic({ name: queryName, params: queryParams });
    const queue: QueryEvent[] = [];
    const waiters: Array<(event: QueryEvent | null) => void> = [];
    let closed = false;
    const listener = (event: { readonly topic: CoreTopic; readonly value: unknown }): void => {
      const value = {
        topic: { name: event.topic.name, params: event.topic.params },
        value: event.value,
      };
      const waiter = waiters.shift();
      if (waiter) waiter(value);
      else queue.push(value);
    };
    await Effect.runPromise(
      this.engine.subscribe(
        topic,
        listener as Listener<{
          readonly topic: CoreTopic;
          readonly value: unknown;
        }>,
      ),
    );
    const entry: SubscriptionEntry = {
      topic,
      queue,
      waiters,
      listener,
      closed,
      release: async () => {
        if (closed) return;
        closed = true;
        await Effect.runPromise(
          this.engine.unsubscribe(
            topic,
            listener as Listener<{
              readonly topic: CoreTopic;
              readonly value: unknown;
            }>,
          ),
        );
      },
    };
    return new SubscriptionTarget(entry);
  }
  async unsubscribe(subscription: Subscription): Promise<void> {
    await subscription.close();
  }
  async sync(request: {
    readonly mutation: string;
    readonly params: readonly unknown[];
  }): Promise<void> {
    const mutation = request.mutation as StringKey<M>;
    const params = request.params as OperationParams<M[typeof mutation]>;
    await Effect.runPromise(this.engine.sync(mutation, params));
  }
}

export type CapnWebSession = RpcStub<WebSocketRpcApi>;

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> extends DurableObject<Env> {
  private readonly initialization: Promise<SyncEngineInterface<Q, M>>;
  private readonly sessions = new WeakMap<WebSocket, CapnWebSession>();
  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<Q, M> | Promise<SyncEngineInterface<Q, M>>,
  ) {
    super(ctx, env);
    this.initialization = ctx.blockConcurrencyWhile(async () => initialize());
  }
  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket")
      return new Response("Expected WebSocket", { status: 426 });
    const pair = new WebSocketPair();
    pair[1].accept();
    this.sessions.set(
      pair[1],
      newWebSocketRpcSession<WebSocketRpcApi>(pair[1], new ApiTarget(await this.initialization)),
    );
    return new Response(null, { status: 101, webSocket: pair[0] });
  }
  webSocketMessage(): void {}
  webSocketClose(socket: WebSocket): void {
    this.sessions.get(socket)?.[Symbol.dispose]();
  }
  webSocketError(socket: WebSocket): void {
    this.sessions.get(socket)?.[Symbol.dispose]();
  }
}
