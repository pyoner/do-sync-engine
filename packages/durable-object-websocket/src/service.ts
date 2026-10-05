import { RpcStub, RpcTarget } from "capnweb";
import { Array as Arr, Cause, Effect, Exit, Option, Predicate, Schema } from "effect";

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
  return Exit.match(Effect.runSyncExit(effect), {
    onSuccess: (value) => value,
    onFailure: (cause) => {
      const error = Cause.squash(cause);
      return Predicate.isError(error) ? error : new Error(String(error), { cause: error });
    },
  });
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

  #owned<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(topic: Topic<Name, Params>) {
    return Effect.map(this.#engine.subscriptions(topic), (subscriptions) =>
      Arr.findFirst(subscriptions, ({ id }) => id === this.#socket),
    );
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

        const previous = yield* this.#owned(topic);
        const ownedListener = listener.dup();

        yield* this.#engine
          .subscribe(topic, ownedListener, this.#socket)
          .pipe(Effect.onError(() => Effect.sync(() => ownedListener[Symbol.dispose]())));
        if (Option.isSome(previous)) previous.value.listener[Symbol.dispose]();
      }),
    );
  }

  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
  ): void | Error {
    return runToValue(
      Effect.gen({ self: this }, function* () {
        const previous = yield* this.#owned(topic);
        if (Option.isNone(previous)) return;
        yield* this.#engine.unsubscribe(topic, this.#socket);
        previous.value.listener[Symbol.dispose]();
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
