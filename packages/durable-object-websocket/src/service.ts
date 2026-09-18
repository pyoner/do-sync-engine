import { RpcStub, RpcTarget } from "capnweb";

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

export type RpcListener<E extends ListenerEvent = ListenerEvent> = RpcStub<Listener<E>>;

type SyncArgs<Mutations extends MutationRecord> = {
  [Name in StringKey<Mutations>]: [mutation: Name, params: OpParams<Mutations[Name]>];
}[StringKey<Mutations>];

export interface Service<
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
> {
  createTopic<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    name: Name,
    params: Params,
  ): Topic<Name, Params> | Error;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
  ): void | Error;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): void | Error;
  sync(...args: SyncArgs<Mutations>): void | Error;
}

export type RpcClient<Queries extends QueryRecord, Mutations extends MutationRecord> = Omit<
  RpcStub<Service<Queries, Mutations>>,
  "sync"
> & {
  sync(...args: SyncArgs<Mutations>): Promise<void | Error>;
};

export class SocketService<Q extends QueryRecord, M extends MutationRecord>
  extends RpcTarget
  implements Service<Q, M>
{
  readonly #engine: SyncEngineInterface<WebSocket, Q, M, Disposable>;
  readonly #socket: WebSocket;
  #disposed = false;

  constructor(engine: SyncEngineInterface<WebSocket, Q, M, Disposable>, socket: WebSocket) {
    super();
    this.#engine = engine;
    this.#socket = socket;
  }

  createTopic<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    name: Name,
    params: Params,
  ): Topic<Name, Params> | Error {
    return this.#engine.createTopic(name, params);
  }

  subscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>,
  ): void | Error {
    if (this.#disposed) return new Error("WebSocket RPC session is closed");

    const previous = [...this.#engine.subscriptions(topic)].find(({ id }) => id === this.#socket);
    const ownedListener = listener.dup();

    const result = this.#engine.subscribe(topic, ownedListener, this.#socket);
    if (result instanceof Error) {
      ownedListener[Symbol.dispose]();
      return result;
    }
    previous?.listener[Symbol.dispose]();
  }

  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
  ): void | Error {
    const previous = [...this.#engine.subscriptions(topic)].find(({ id }) => id === this.#socket);
    if (previous === undefined) return;
    this.#engine.unsubscribe(topic, this.#socket);
    previous.listener[Symbol.dispose]();
  }

  sync(...[mutation, params]: SyncArgs<M>): void | Error {
    return this.#engine.sync(mutation, params);
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    const subscriptions = [...this.#engine.subscriptions()].filter(({ id }) => id === this.#socket);
    for (const { topic, listener } of subscriptions) {
      this.#engine.unsubscribe(topic, this.#socket);
      listener[Symbol.dispose]();
    }
  }
}
