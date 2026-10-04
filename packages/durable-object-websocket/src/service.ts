import { RpcStub, RpcTarget } from "capnweb";
import { Cause, Effect, Exit, Predicate } from "effect";

import type {
  Listener,
  ListenerEvent,
  MutationRecord,
  OpParams,
  OpResult,
  QueryRecord,
  StringKey,
  SyncEngine,
  Topic,
} from "@do-sync-engine/core";

export type RpcListener<E extends ListenerEvent = ListenerEvent> = RpcStub<Listener<E>>;

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
  sync<Name extends StringKey<Mutations>, Params extends OpParams<Mutations[Name]>>(
    mutation: Name,
    params: Params,
  ): void | Error;
}

function runToValue<A, E>(effect: Effect.Effect<A, E>): A | Error {
  const exit = Effect.runSyncExit(effect);
  if (Exit.isSuccess(exit)) return exit.value;
  const error = Cause.squash(exit.cause);
  return Predicate.isError(error) ? error : new Error(String(error), { cause: error });
}

export class SocketService<Q extends QueryRecord, M extends MutationRecord>
  extends RpcTarget
  implements Service<Q, M>
{
  readonly #engine: SyncEngine<WebSocket, Q, M, Disposable>;
  readonly #socket: WebSocket;
  #disposed = false;

  constructor(engine: SyncEngine<WebSocket, Q, M, Disposable>, socket: WebSocket) {
    super();
    this.#engine = engine;
    this.#socket = socket;
  }

  createTopic<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    name: Name,
    params: Params,
  ): Topic<Name, Params> | Error {
    return runToValue(this.#engine.createTopic(name, params));
  }

  subscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    listener: RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>,
  ): void | Error {
    if (this.#disposed) return new Error("WebSocket RPC session is closed");

    const previous = Effect.runSync(this.#engine.subscriptions(topic)).find(
      ({ id }) => id === this.#socket,
    );
    const ownedListener = listener.dup();

    const result = runToValue(this.#engine.subscribe(topic, ownedListener, this.#socket));
    if (result instanceof Error) {
      ownedListener[Symbol.dispose]();
      return result;
    }
    previous?.listener[Symbol.dispose]();
  }

  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
  ): void | Error {
    const previous = Effect.runSync(this.#engine.subscriptions(topic)).find(
      ({ id }) => id === this.#socket,
    );
    if (previous === undefined) return;
    Effect.runSync(this.#engine.unsubscribe(topic, this.#socket));
    previous.listener[Symbol.dispose]();
  }

  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): void | Error {
    const result = runToValue(this.#engine.sync(mutation, params));
    if (result instanceof Error) return result;
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    const subscriptions = Effect.runSync(this.#engine.subscriptions()).filter(
      ({ id }) => id === this.#socket,
    );
    for (const { topic, listener } of subscriptions) {
      Effect.runSync(this.#engine.unsubscribe(topic, this.#socket));
      listener[Symbol.dispose]();
    }
  }
}
