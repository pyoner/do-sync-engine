import { RpcStub, RpcTarget } from "capnweb";
import { Cause, Effect, Exit, Predicate, Schema } from "effect";

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

class SessionClosedError extends Schema.TaggedError<SessionClosedError>()(
  "SessionClosedError",
  {},
) {
  override get message(): string {
    return "WebSocket RPC session is closed";
  }
}

/** Edge between Effect and Cap'n Web: failures and defects become `Error` values. */
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
    return runToValue(
      Effect.gen({ self: this }, function* () {
        if (this.#disposed) return yield* new SessionClosedError();

        const previous = (yield* this.#engine.subscriptions(topic)).find(
          ({ id }) => id === this.#socket,
        );
        const ownedListener = listener.dup();

        yield* this.#engine
          .subscribe(topic, ownedListener, this.#socket)
          .pipe(Effect.onError(() => Effect.sync(() => ownedListener[Symbol.dispose]())));
        previous?.listener[Symbol.dispose]();
      }),
    );
  }

  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
  ): void | Error {
    return runToValue(
      Effect.gen({ self: this }, function* () {
        const previous = (yield* this.#engine.subscriptions(topic)).find(
          ({ id }) => id === this.#socket,
        );
        if (previous === undefined) return;
        yield* this.#engine.unsubscribe(topic, this.#socket);
        previous.listener[Symbol.dispose]();
      }),
    );
  }

  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): void | Error {
    return runToValue(this.#engine.sync(mutation, params));
  }

  [Symbol.dispose](): void {
    if (this.#disposed) return;
    this.#disposed = true;
    Effect.runSync(
      Effect.gen({ self: this }, function* () {
        const subscriptions = (yield* this.#engine.subscriptions()).filter(
          ({ id }) => id === this.#socket,
        );
        for (const { topic, listener } of subscriptions) {
          yield* this.#engine.unsubscribe(topic, this.#socket);
          listener[Symbol.dispose]();
        }
      }),
    );
  }
}
