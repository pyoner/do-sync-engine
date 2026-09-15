import type {
  Listener,
  ListenerEvent,
  MutationRecord,
  OpParams,
  OpResult,
  QueryRecord,
  StringKey,
  Subscription,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";
import { RpcStub, RpcTarget, serialize } from "capnweb";
import * as errore from "errore";

export type QueryTopic<Queries extends QueryRecord> = {
  [Name in StringKey<Queries>]: Topic<Name, OpParams<Queries[Name]>>;
}[StringKey<Queries>];

export type RpcListener<E extends ListenerEvent = ListenerEvent> = RpcStub<
  Listener<E> & { readonly listenerId: string }
>;

export type ServiceSubscriptions<Id extends string, Q extends QueryRecord> = {
  [Name in StringKey<Q>]: Subscription<
    Id,
    Topic<Name, OpParams<Q[Name]>>,
    OpResult<Q[Name]>,
    RpcListener<ListenerEvent<Topic<Name, OpParams<Q[Name]>>, OpResult<Q[Name]>>>
  >;
}[StringKey<Q>];

export interface ServiceIterator<Id extends string, Q extends QueryRecord>
  extends RpcTarget, IterableIterator<Readonly<ServiceSubscriptions<Id, Q>>, undefined> {
  next(): IteratorResult<Readonly<ServiceSubscriptions<Id, Q>>, undefined>;
  return(): IteratorReturnResult<undefined>;
  [Symbol.dispose](): void;
}

export interface Service<
  Id extends string,
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
> extends Pick<SyncEngineInterface<Id, Queries, Mutations>, "createTopic" | "sync"> {
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
  ): Promise<Id | Error>;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
    id: Id,
  ): Promise<Id | Error>;
  unsubscribe(id: Id): Promise<void | Error>;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    id: Id,
  ): Promise<void | Error>;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
  ): Promise<void | Error>;
  subscriptions(): ServiceIterator<Id, Queries>;
}

type RetainedSubscription = {
  readonly id: string;
  readonly listenerId: string;
  readonly ownedListener: RpcListener;
  readonly wrapper: Listener;
};

type TopicEntry<Q extends QueryRecord> = {
  readonly topic: QueryTopic<Q>;
  readonly subscriptions: Map<string, RetainedSubscription>;
};

export class SocketService<Q extends QueryRecord, M extends MutationRecord>
  extends RpcTarget
  implements Service<string, Q, M>
{
  readonly #engine: SyncEngineInterface<string, Q, M>;
  readonly #sessionId = crypto.randomUUID();
  readonly #topics = new Map<string, TopicEntry<Q>>();
  #disposed = false;

  constructor(engine: SyncEngineInterface<string, Q, M>) {
    super();
    this.#engine = engine;
  }

  createTopic<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    name: Name,
    params: Params,
  ): Topic<Name, Params> | Error {
    return this.#engine.createTopic(name, params);
  }

  async subscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>,
    id?: string,
  ): Promise<string | Error> {
    const topicKey = this.#encodeTopic(topic);
    if (topicKey instanceof Error) return topicKey;

    const listenerId = await this.#listenerId(listener as unknown as RpcListener);
    if (listenerId instanceof Error) return listenerId;
    if (listenerId.length === 0) return new Error("RPC listenerId must be a non-empty string");
    if (this.#disposed) return new Error("WebSocket RPC session is closed");

    const suppliedId = arguments.length >= 3;
    if (suppliedId && typeof id !== "string") return new Error("Subscription ID must be a string");

    const entry =
      this.#topics.get(topicKey) ??
      (() => {
        const created = { topic, subscriptions: new Map() };
        this.#topics.set(topicKey, created);
        return created;
      })();
    const publicId = suppliedId
      ? id!
      : ([...entry.subscriptions.values()].find((record) => record.listenerId === listenerId)?.id ??
        crypto.randomUUID());
    const internalId = this.#internalId(publicId);
    const existing = entry.subscriptions.get(publicId);
    if (existing?.listenerId === listenerId) {
      const result = this.#engine.subscribe(entry.topic, existing.wrapper, internalId);
      if (result instanceof Error) return result;
      return publicId;
    }

    const ownedListener = listener.dup() as unknown as RpcListener;
    const wrapper: Listener = (event) => {
      void Promise.resolve((ownedListener as unknown as (e: unknown) => void)(event)).catch(
        (cause) => {
          console.error(
            new Error("WebSocket subscription listener failed", {
              cause,
            }),
          );
        },
      );
    };
    entry.subscriptions.set(publicId, {
      id: publicId,
      listenerId,
      ownedListener,
      wrapper,
    });
    const result = this.#engine.subscribe(entry.topic, wrapper, internalId);
    if (existing !== undefined) existing.ownedListener[Symbol.dispose]();
    if (result instanceof Error) return result;
    return publicId;
  }

  unsubscribe(id: string): Promise<void | Error>;

  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    id: string,
  ): Promise<void | Error>;

  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>,
  ): Promise<void | Error>;

  async unsubscribe(
    topicOrId: QueryTopic<Q> | string,
    idOrListener?: unknown,
  ): Promise<void | Error> {
    if (arguments.length === 1 || idOrListener === undefined) {
      if (typeof topicOrId !== "string") return new Error("Subscription ID must be a string");
      for (const [topicKey, entry] of [...this.#topics.entries()]) {
        this.#remove(entry, topicOrId);
        if (entry.subscriptions.size === 0) this.#topics.delete(topicKey);
      }
      return;
    }

    const topicKey = this.#encodeTopic(topicOrId as QueryTopic<Q>);
    if (topicKey instanceof Error) return topicKey;
    const entry = this.#topics.get(topicKey);
    if (entry === undefined) return;
    if (typeof idOrListener === "string") {
      this.#remove(entry, idOrListener);
    } else if (typeof idOrListener === "function") {
      const listenerId = await this.#listenerId(idOrListener as RpcListener);
      if (listenerId instanceof Error) return listenerId;
      if (listenerId.length === 0) return new Error("RPC listenerId must be a non-empty string");
      for (const record of [...entry.subscriptions.values()]) {
        if (record.listenerId === listenerId) this.#remove(entry, record.id);
      }
    } else {
      return new Error("Subscription ID must be a string");
    }
    if (entry.subscriptions.size === 0) this.#topics.delete(topicKey);
  }

  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): void | Error {
    return this.#engine.sync(mutation, params);
  }

  subscriptions(): ServiceIterator<string, Q> {
    return new SubscriptionIterator(this.#iterateSubscriptions());
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    for (const entry of this.#topics.values()) {
      for (const record of entry.subscriptions.values()) {
        this.#engine.unsubscribe(entry.topic, this.#internalId(record.id));
        record.ownedListener[Symbol.dispose]();
      }
      entry.subscriptions.clear();
    }
    this.#topics.clear();
  }

  #encodeTopic(topic: QueryTopic<Q>): string | Error {
    return errore.try({
      try: () => serialize([topic.name, topic.params]),
      catch: (cause) => new Error("Failed to encode subscription topic", { cause }),
    });
  }

  async #listenerId(listener: RpcListener): Promise<string | Error> {
    const value = await (listener as unknown as { listenerId: Promise<string> }).listenerId.catch(
      (cause: unknown) => new Error("Failed to read RPC listener identity", { cause }),
    );
    if (value instanceof Error) return value;
    if (typeof value !== "string") return new Error("RPC listenerId must be a non-empty string");
    return value;
  }

  #internalId(publicId: string): string {
    return `${this.#sessionId}:${publicId}`;
  }

  #remove(entry: TopicEntry<Q>, publicId: string): void {
    const record = entry.subscriptions.get(publicId);
    if (record === undefined) return;
    this.#engine.unsubscribe(entry.topic, this.#internalId(publicId));
    record.ownedListener[Symbol.dispose]();
    entry.subscriptions.delete(publicId);
  }

  *#iterateSubscriptions(): Generator<Readonly<ServiceSubscriptions<string, Q>>, undefined> {
    for (const entry of this.#topics.values()) {
      for (const record of entry.subscriptions.values()) {
        yield {
          id: record.id,
          topic: entry.topic,
          listener: record.ownedListener.dup(),
        } as unknown as Readonly<ServiceSubscriptions<string, Q>>;
      }
    }
  }
}

class SubscriptionIterator<Q extends QueryRecord>
  extends RpcTarget
  implements ServiceIterator<string, Q>
{
  readonly #generator: Generator<Readonly<ServiceSubscriptions<string, Q>>, undefined>;
  #closed = false;

  constructor(generator: Generator<Readonly<ServiceSubscriptions<string, Q>>, undefined>) {
    super();
    this.#generator = generator;
  }

  next(): IteratorResult<Readonly<ServiceSubscriptions<string, Q>>, undefined> {
    if (this.#closed) return { done: true, value: undefined };
    const result = this.#generator.next();
    if (result.done) this.#closed = true;
    return result;
  }

  return(): IteratorReturnResult<undefined> {
    this.#close();
    return { done: true, value: undefined };
  }

  [Symbol.iterator](): IterableIterator<Readonly<ServiceSubscriptions<string, Q>>, undefined> {
    return this;
  }

  [Symbol.dispose](): void {
    this.#close();
  }

  #close(): void {
    if (this.#closed) return;
    this.#closed = true;
    this.#generator.return(undefined);
  }
}
