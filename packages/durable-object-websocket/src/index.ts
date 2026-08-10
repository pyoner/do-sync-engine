import { DurableObject } from "cloudflare:workers";
import { type RpcStub, RpcTarget, newWorkersWebSocketRpcResponse } from "capnweb";
import type {
  ListenerEvent,
  MutationMap,
  OperationParams,
  OperationResult,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";

export type DurableObjectWebSocketSubscription = {
  unsubscribe(): boolean;
};
export type DurableObjectWebSocketApi<Q extends QueryMap<Q>, M extends MutationMap<M>> = {
  subscribe<N extends StringKey<Q>>(
    query: N,
    params: OperationParams<Q[N]>,
    listener: (event: ListenerEvent<N, OperationParams<Q[N]>, OperationResult<Q[N]>>) => void,
  ): DurableObjectWebSocketSubscription | Error;
  sync<N extends StringKey<M>>(mutation: N, params: OperationParams<M[N]>): void | Error;
};

type SubscriptionTopic<Q extends QueryMap<Q>> = Topic<
  StringKey<Q>,
  OperationParams<Q[StringKey<Q>]>
>;
type SubscriptionListener<Q extends QueryMap<Q>> = (
  event: ListenerEvent<
    StringKey<Q>,
    OperationParams<Q[StringKey<Q>]>,
    OperationResult<Q[StringKey<Q>]>
  >,
) => void;

class Subscription<Q extends QueryMap<Q>, M extends MutationMap<M>> extends RpcTarget {
  #active = true;
  readonly #listener: SubscriptionListener<Q>;
  readonly #topic: SubscriptionTopic<Q>;
  private constructor(
    private readonly engine: SyncEngineInterface<Q, M>,
    private readonly listener: RpcStub<SubscriptionListener<Q>>,
    topic: SubscriptionTopic<Q>,
  ) {
    super();
    this.#topic = topic;
    this.#listener = (event) => void this.#notify(event);
  }

  static create<Q extends QueryMap<Q>, M extends MutationMap<M>>({
    engine,
    listener,
    topic,
  }: {
    engine: SyncEngineInterface<Q, M>;
    listener: RpcStub<SubscriptionListener<Q>>;
    topic: SubscriptionTopic<Q>;
  }): Error | Subscription<Q, M> {
    const subscription = new Subscription(engine, listener.dup(), topic);
    const subscribed = engine.subscribe(topic, subscription.#listener);
    if (subscribed instanceof Error) {
      subscription[Symbol.dispose]();
      return subscribed;
    }
    return subscription;
  }

  async #notify(event: Parameters<SubscriptionListener<Q>>[0]): Promise<void> {
    const notified = await this.listener(event).catch(
      (cause) => new Error("Failed to deliver subscription update", { cause }),
    );
    if (!(notified instanceof Error)) return;
    console.warn(notified.message, notified);
    this.unsubscribe();
  }

  unsubscribe(): boolean {
    if (!this.#active) return false;
    this.#active = false;
    this.listener[Symbol.dispose]();
    this.engine.unsubscribe(this.#topic, this.#listener);
    return true;
  }

  [Symbol.dispose](): void {
    this.unsubscribe();
  }
}

class SyncSession<Q extends QueryMap<Q>, M extends MutationMap<M>> extends RpcTarget {
  constructor(private readonly engine: SyncEngineInterface<Q, M>) {
    super();
  }

  subscribe(
    query: StringKey<Q>,
    params: unknown[],
    listener: RpcStub<SubscriptionListener<Q>>,
  ): DurableObjectWebSocketSubscription | Error {
    const topic = this.engine.createTopic<StringKey<Q>>(
      query,
      params as OperationParams<Q[StringKey<Q>]>,
    );
    if (topic instanceof Error) return topic;

    const subscription = Subscription.create({ engine: this.engine, listener, topic });
    if (subscription instanceof Error) return subscription;
    return subscription;
  }

  sync(mutation: StringKey<M>, params: unknown[]): void | Error {
    return this.engine.sync(mutation, params as OperationParams<M[StringKey<M>]>);
  }
}

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> extends DurableObject<Env> {
  private engine!: SyncEngineInterface<Q, M>;

  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<Q, M> | Promise<SyncEngineInterface<Q, M>>,
  ) {
    super(ctx, env);
    void ctx.blockConcurrencyWhile(async () => {
      this.engine = await initialize();
    });
  }

  fetch(request: Request): Response {
    return newWorkersWebSocketRpcResponse(request, new SyncSession(this.engine));
  }
}
