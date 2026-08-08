import { DurableObject } from "cloudflare:workers";
import { type RpcStub, RpcTarget, newWorkersWebSocketRpcResponse } from "capnweb";
import type {
  ListenerId,
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";

export type DurableObjectWebSocketBinding<Q extends QueryMap<Q>, M extends MutationMap<M>> = {
  readonly engine: SyncEngineInterface<Q, M>;
};
export type DurableObjectWebSocketInitializer<
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> = () => DurableObjectWebSocketBinding<Q, M> | Promise<DurableObjectWebSocketBinding<Q, M>>;
export type DurableObjectWebSocketEvent = { readonly topic: Topic; readonly value: unknown };
export type DurableObjectWebSocketListener = (event: DurableObjectWebSocketEvent) => void;
export type DurableObjectWebSocketSubscription = {
  unsubscribe(): boolean;
};
export type DurableObjectWebSocketApi<Q extends QueryMap<Q>, M extends MutationMap<M>> = {
  subscribe<N extends StringKey<Q>>(
    query: N,
    params: OperationParams<Q[N]>,
    listener: DurableObjectWebSocketListener,
  ): Promise<DurableObjectWebSocketSubscription | Error>;
  sync<N extends StringKey<M>>(mutation: N, params: OperationParams<M[N]>): Promise<void | Error>;
};

class Subscription<Q extends QueryMap<Q>, M extends MutationMap<M>> extends RpcTarget {
  #listenerId: ListenerId | null = null;
  #active = true;
  private constructor(
    private readonly engine: SyncEngineInterface<Q, M>,
    private readonly listener: RpcStub<DurableObjectWebSocketListener>,
  ) {
    super();
  }

  static create<Q extends QueryMap<Q>, M extends MutationMap<M>>({
    engine,
    listener,
    topic,
  }: {
    engine: SyncEngineInterface<Q, M>;
    listener: RpcStub<DurableObjectWebSocketListener>;
    topic: Topic;
  }): Error | Subscription<Q, M> {
    const subscription = new Subscription(engine, listener.dup());
    const listenerId = engine.subscribe(topic as never, (event) => void subscription.notify(event));
    if (listenerId instanceof Error) {
      subscription[Symbol.dispose]();
      return listenerId;
    }
    subscription.#listenerId = listenerId;
    return subscription;
  }

  async notify(event: DurableObjectWebSocketEvent): Promise<Error | void> {
    const notified = await this.listener(event).catch(
      (cause) => new Error("Failed to deliver subscription update", { cause }),
    );
    if (!(notified instanceof Error)) return;
    console.warn(notified.message, notified);
    this.unsubscribe();
    return notified;
  }

  unsubscribe(): boolean {
    if (!this.#active) return false;
    this.#active = false;
    this.listener[Symbol.dispose]();
    if (this.#listenerId === null) return false;
    return this.engine.unsubscribe(this.#listenerId);
  }

  [Symbol.dispose](): void {
    this.unsubscribe();
  }
}

class SyncSession<Q extends QueryMap<Q>, M extends MutationMap<M>> extends RpcTarget {
  constructor(private readonly engine: SyncEngineInterface<Q, M>) {
    super();
  }

  async subscribe(
    query: StringKey<Q>,
    params: unknown[],
    listener: RpcStub<DurableObjectWebSocketListener>,
  ): Promise<DurableObjectWebSocketSubscription | Error> {
    const topic = await this.engine.createTopic<StringKey<Q>>(
      query,
      params as OperationParams<Q[StringKey<Q>]>,
    );
    if (topic instanceof Error) return topic;
    const value = this.engine.query<StringKey<Q>>(topic);
    if (value instanceof Error) return value;

    const subscription = Subscription.create({ engine: this.engine, listener, topic });
    if (subscription instanceof Error) return subscription;
    const notified = await subscription.notify({ topic, value });
    if (notified instanceof Error) return notified;
    return subscription;
  }

  async sync(mutation: StringKey<M>, params: unknown[]): Promise<void | Error> {
    return this.engine.sync(mutation, params as OperationParams<M[StringKey<M>]>);
  }
}

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> extends DurableObject<Env> {
  private readonly initialization: Promise<void>;
  private engine!: SyncEngineInterface<Q, M>;

  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: DurableObjectWebSocketInitializer<Q, M>,
  ) {
    super(ctx, env);
    this.initialization = ctx.blockConcurrencyWhile(async () => {
      const { engine } = await initialize();
      this.engine = engine;
    });
  }

  async fetch(request: Request): Promise<Response> {
    await this.initialization;
    return newWorkersWebSocketRpcResponse(request, new SyncSession(this.engine));
  }
}
