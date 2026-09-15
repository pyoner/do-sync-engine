import type {
  Listener,
  ListenerEvent,
  MutationRecord,
  OpParams,
  OpResult,
  QueryRecord,
  StringKey,
  Topic,
} from "@do-sync-engine/core";
import { RpcStub, newWebSocketRpcSession as createRpcSession } from "capnweb";
import type { RpcListener, Service, ServiceSubscriptions } from "./service";

type ClientListener<
  Q extends QueryRecord,
  Name extends StringKey<Q>,
  Params extends OpParams<Q[Name]>,
> =
  | Listener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>
  | RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>;

type RemoteService<Q extends QueryRecord, M extends MutationRecord> = Pick<
  Service<string, Q, M>,
  "subscribe" | "unsubscribe"
> & {
  createTopic<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    name: Name,
    params: Params,
  ): Promise<Topic<Name, Params> | Error>;
  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): Promise<void | Error>;
  subscriptions(): Promise<RemoteIterator<Q>>;
  onRpcBroken(callback: (error: unknown) => void): void;
  [Symbol.dispose](): void;
};
type RemoteIterator<Q extends QueryRecord> = {
  next(): Promise<IteratorResult<Readonly<ServiceSubscriptions<string, Q>>, undefined>>;
  return(): Promise<IteratorReturnResult<undefined>>;
  [Symbol.dispose](): void;
};

export interface WebSocketRpcClient<Q extends QueryRecord, M extends MutationRecord> {
  createTopic<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    name: Name,
    params: Params,
  ): Promise<Topic<Name, Params> | Error>;
  subscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    listener: ClientListener<Q, Name, Params>,
  ): Promise<string | Error>;
  subscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    listener: ClientListener<Q, Name, Params>,
    id: string,
  ): Promise<string | Error>;
  unsubscribe(id: string): Promise<void | Error>;
  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    id: string,
  ): Promise<void | Error>;
  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
    listener: ClientListener<Q, Name, Params>,
  ): Promise<void | Error>;
  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): Promise<void | Error>;
  subscriptions(): AsyncIterableIterator<Readonly<ServiceSubscriptions<string, Q>> & Disposable>;
  onRpcBroken(listener: (error: Error) => void): void;
  [Symbol.dispose](): void;
}

type IdentifiedListener = Listener & { readonly listenerId: string };
type DisposableSubscription<Q extends QueryRecord> = Readonly<ServiceSubscriptions<string, Q>> &
  Disposable;

export function newWebSocketRpcSession<Q extends QueryRecord, M extends MutationRecord>(
  socket: string | WebSocket,
): WebSocketRpcClient<Q, M> {
  // Capnweb 0.12 erases generic methods in RpcStub's mapped type; this assertion confines the
  // correction to the implemented wire contract instead of weakening the public client facade.
  const remote = createRpcSession(socket) as unknown as RemoteService<Q, M>;
  const identifiedListeners = new WeakMap<Listener, IdentifiedListener>();

  const identified = (listener: Listener): IdentifiedListener => {
    const existing = identifiedListeners.get(listener);
    if (existing !== undefined) return existing;
    const forwarding = ((event: ListenerEvent) => listener(event as never)) as IdentifiedListener;
    Object.defineProperty(forwarding, "listenerId", {
      value: crypto.randomUUID(),
      enumerable: true,
    });
    identifiedListeners.set(listener, forwarding);
    return forwarding;
  };

  const scopedListener = (listener: Listener | RpcListener): RpcListener => {
    if (listener instanceof RpcStub) {
      return new RpcStub(
        Object.assign(((e: ListenerEvent) => (listener as unknown as Listener)(e)) as Listener, {
          get listenerId() {
            return (listener as unknown as { listenerId: Promise<string> }).listenerId;
          },
        }),
      ) as unknown as RpcListener;
    }
    return new RpcStub(identified(listener as Listener));
  };

  const requestError = (cause: unknown): Error =>
    new Error("WebSocket RPC request failed", { cause });

  const client: WebSocketRpcClient<Q, M> = {
    createTopic<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
      name: Name,
      params: Params,
    ) {
      return Promise.resolve()
        .then(() => remote.createTopic(name, params))
        .catch(requestError);
    },

    async subscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
      topic: Topic<Name, Params>,
      listener: ClientListener<Q, Name, Params>,
      id?: string,
    ): Promise<string | Error> {
      const scoped = scopedListener(listener as Listener | RpcListener);
      try {
        const result =
          arguments.length >= 3
            ? await Promise.resolve()
                .then(() => remote.subscribe(topic, scoped, id!))
                .catch(requestError)
            : await Promise.resolve()
                .then(() => remote.subscribe(topic, scoped))
                .catch(requestError);
        return result;
      } finally {
        scoped[Symbol.dispose]();
      }
    },

    async unsubscribe(topicOrId: unknown, idOrListener?: unknown): Promise<void | Error> {
      if (arguments.length === 1 || idOrListener === undefined) {
        return Promise.resolve()
          .then(() => remote.unsubscribe(topicOrId as string))
          .catch(requestError);
      }
      if (typeof idOrListener === "string") {
        return Promise.resolve()
          .then(() => remote.unsubscribe(topicOrId as never, idOrListener))
          .catch(requestError);
      }
      const scoped = scopedListener(idOrListener as Listener | RpcListener);
      try {
        return await Promise.resolve()
          .then(() => remote.unsubscribe(topicOrId as never, scoped))
          .catch(requestError);
      } finally {
        scoped[Symbol.dispose]();
      }
    },

    sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
      mutation: Name,
      params: Params,
    ) {
      return Promise.resolve()
        .then(() => remote.sync(mutation, params))
        .catch(requestError);
    },

    async *subscriptions(): AsyncIterableIterator<DisposableSubscription<Q>> {
      let iterator: RemoteIterator<Q> | undefined;
      try {
        const iteratorResult = await Promise.resolve()
          .then(() => remote.subscriptions())
          .catch(requestError);
        if (iteratorResult instanceof Error) throw iteratorResult;
        iterator = iteratorResult as RemoteIterator<Q>;
        while (true) {
          const nextMethod = iterator;
          const nextCall = nextMethod.next();
          const result = await Promise.resolve()
            .then(() => nextCall)
            .catch(requestError);
          if (result instanceof Error) {
            (nextCall as unknown as Disposable)[Symbol.dispose]?.();
            throw result;
          }
          if (result.done) {
            (nextCall as unknown as Disposable)[Symbol.dispose]?.();
            return;
          }
          const entry = result.value as unknown as DisposableSubscription<Q>;
          let disposed = false;
          Object.defineProperty(entry, Symbol.dispose, {
            value: () => {
              if (disposed) return;
              disposed = true;
              (nextCall as unknown as Disposable)[Symbol.dispose]?.();
            },
          });
          yield entry;
        }
      } finally {
        if (iterator !== undefined) {
          const returnMethod = iterator;
          const closeResult = await Promise.resolve()
            .then(() => returnMethod.return())
            .catch((cause) => new Error("Failed to close subscription iterator", { cause }));
          if (closeResult instanceof Error) {
            console.error(closeResult);
          }
          iterator[Symbol.dispose]();
        }
      }
    },

    onRpcBroken(listener: (error: Error) => void): void {
      let notified = false;
      remote.onRpcBroken((cause) => {
        if (notified) return;
        notified = true;
        listener(
          cause instanceof Error ? cause : new Error("WebSocket RPC session failed", { cause }),
        );
      });
    },

    [Symbol.dispose](): void {
      remote[Symbol.dispose]();
    },
  };

  return client;
}
