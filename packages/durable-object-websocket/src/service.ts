import type {
  Listener,
  ListenerEvent,
  MutationRecord,
  OpParams,
  OpResult,
  QueryRecord,
  StringKey,
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

export interface Service<Id extends string, Queries extends QueryRecord = QueryRecord> {
  createTopic<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    name: Name,
    params: Params,
  ): Topic<Name, Params> | Error;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
  ): Promise<Id | Error>;
  unsubscribe(id: Id): Promise<void | Error>;
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
  implements Service<string, Q>
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
  ): Promise<string | Error> {
    const topicKey = this.#encodeTopic(topic);
    if (topicKey instanceof Error) return topicKey;

    const listenerId = await this.#listenerId(listener as unknown as RpcListener);
    if (listenerId instanceof Error) return listenerId;
    if (listenerId.length === 0) return new Error("RPC listenerId must be a non-empty string");
    if (this.#disposed) return new Error("WebSocket RPC session is closed");

    const entry =
      this.#topics.get(topicKey) ??
      (() => {
        const created = { topic, subscriptions: new Map<string, RetainedSubscription>() };
        this.#topics.set(topicKey, created);
        return created;
      })();
    const existing = [...entry.subscriptions.values()].find(
      (record) => record.listenerId === listenerId,
    );
    if (existing !== undefined) {
      const result = this.#engine.subscribe(
        entry.topic,
        existing.wrapper,
        this.#internalId(existing.id),
      );
      if (result instanceof Error) return result;
      return existing.id;
    }

    const id = crypto.randomUUID();
    const ownedListener = listener.dup() as unknown as RpcListener;
    const wrapper: Listener = (event) => {
      void Promise.resolve((ownedListener as unknown as (value: unknown) => void)(event)).catch(
        (cause) => {
          console.error(new Error("WebSocket subscription listener failed", { cause }));
        },
      );
    };
    entry.subscriptions.set(id, { id, listenerId, ownedListener, wrapper });
    const result = this.#engine.subscribe(entry.topic, wrapper, this.#internalId(id));
    if (result instanceof Error) return result;
    return id;
  }

  async unsubscribe(id: string): Promise<void | Error> {
    if (typeof id !== "string") return new Error("Subscription ID must be a string");
    for (const [topicKey, entry] of [...this.#topics.entries()]) {
      this.#remove(entry, id);
      if (entry.subscriptions.size === 0) this.#topics.delete(topicKey);
    }
  }

  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): void | Error {
    return this.#engine.sync(mutation, params);
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

  #internalId(id: string): string {
    return `${this.#sessionId}:${id}`;
  }

  #remove(entry: TopicEntry<Q>, id: string): void {
    const record = entry.subscriptions.get(id);
    if (record === undefined) return;
    this.#engine.unsubscribe(entry.topic, this.#internalId(id));
    record.ownedListener[Symbol.dispose]();
    entry.subscriptions.delete(id);
  }
}
