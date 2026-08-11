import { dequal } from "dequal";
import { RpcStub, RpcTarget, newWorkersWebSocketRpcResponse } from "capnweb";
import type {
  Listener,
  ListenerEvent,
  MutationMap,
  OperationParams,
  OperationResult,
  QueryMap,
  StringKey,
  SyncEngine,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";

type QueryTopic<Queries extends QueryMap<Queries>> = Topic<
  StringKey<Queries>,
  OperationParams<Queries[StringKey<Queries>]>
>;
type QueryListener<Queries extends QueryMap<Queries>> = Listener<
  ListenerEvent<
    StringKey<Queries>,
    OperationParams<Queries[StringKey<Queries>]>,
    OperationResult<Queries[StringKey<Queries>]>
  >
>;
// Capnweb disposes callback parameter stubs when subscribe returns.
// Retain the duplicate and stable engine listener for unsubscribe and connection cleanup.
type Registration<Queries extends QueryMap<Queries>> = {
  topic: QueryTopic<Queries>;
  callback: { [Symbol.dispose](): void };
  engineListener: (...args: never[]) => unknown;
  active: boolean;
};

export class ServerAPI<Queries extends QueryMap<Queries>, Mutations extends MutationMap<Mutations>>
  extends RpcTarget
  implements SyncEngineInterface<Queries, Mutations>
{
  readonly #registrations: Registration<Queries>[] = [];
  readonly #engine: SyncEngine<Queries, Mutations>;

  constructor(engine: SyncEngine<Queries, Mutations>) {
    super();
    this.#engine = engine;
  }

  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Topic<Name, OperationParams<Queries[Name]>> | Error {
    return this.#engine.createTopic(name, params);
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void | Error {
    return this.#engine.sync(mutation, params);
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): void | Error {
    let registration!: Registration<Queries>;
    const callback = listener instanceof RpcStub ? listener.dup() : new RpcStub(listener);
    const engineListener = (event: Parameters<typeof listener>[0]) => {
      void Promise.resolve()
        .then(() => callback(event))
        .catch((cause: unknown) => {
          const deliveryError = new Error("Failed to deliver subscription update", { cause });
          console.warn(deliveryError.message, deliveryError);
          this.#cleanup(registration);
        });
    };

    registration = {
      topic,
      callback,
      engineListener,
      active: true,
    };
    const existing = this.#registrations.find((registration) => dequal(registration.topic, topic));
    callback.onRpcBroken(() => this.#cleanup(registration));

    const subscribed = this.#engine.subscribe(topic, engineListener);
    if (subscribed instanceof Error) {
      this.#cleanup(registration);
      return subscribed;
    }

    if (existing === undefined) this.#registrations.push(registration);
    else {
      const index = this.#registrations.indexOf(existing);
      if (index === -1) this.#registrations.push(registration);
      else this.#registrations[index] = registration;
      this.#cleanup(existing);
    }
  }

  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    _listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): void {
    const registration = this.#registrations.find((registration) =>
      dequal(registration.topic, topic),
    );
    if (registration !== undefined) this.#cleanup(registration);
  }

  [Symbol.dispose](): void {
    while (this.#registrations.length > 0) this.#cleanup(this.#registrations[0]!);
  }

  #cleanup(registration: Registration<Queries>): void {
    if (!registration.active) return;
    registration.active = false;
    try {
      this.#engine.unsubscribe(
        registration.topic,
        registration.engineListener as QueryListener<Queries>,
      );
    } finally {
      try {
        registration.callback[Symbol.dispose]();
      } finally {
        const index = this.#registrations.indexOf(registration);
        if (index !== -1) this.#registrations.splice(index, 1);
      }
    }
  }
}

export function newWebSocketSyncResponse<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
>(request: Request, engine: SyncEngine<Queries, Mutations>): Response {
  return newWorkersWebSocketRpcResponse(request, new ServerAPI(engine));
}
