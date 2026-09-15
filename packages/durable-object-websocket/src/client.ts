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
import type { RpcListener, Service } from "./service";

type ClientListener<
  Q extends QueryRecord,
  Name extends StringKey<Q>,
  Params extends OpParams<Q[Name]>,
> =
  | Listener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>
  | RpcListener<ListenerEvent<Topic<Name, Params>, OpResult<Q[Name]>>>;

type RemoteService<Q extends QueryRecord, M extends MutationRecord> = Service<string, Q> & {
  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): Promise<void | Error>;
  onRpcBroken(callback: (error: unknown) => void): void;
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
  unsubscribe(id: string): Promise<void | Error>;
  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): Promise<void | Error>;
  onRpcBroken(listener: (error: Error) => void): void;
  [Symbol.dispose](): void;
}

type IdentifiedListener = Listener & { readonly listenerId: string };

export function newWebSocketRpcSession<Q extends QueryRecord, M extends MutationRecord>(
  socket: string | WebSocket,
): WebSocketRpcClient<Q, M> {
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
    ): Promise<string | Error> {
      const scoped = scopedListener(listener as Listener | RpcListener);
      try {
        return await Promise.resolve()
          .then(() => remote.subscribe(topic, scoped))
          .catch(requestError);
      } finally {
        scoped[Symbol.dispose]();
      }
    },

    async unsubscribe(id: string): Promise<void | Error> {
      return Promise.resolve()
        .then(() => remote.unsubscribe(id))
        .catch(requestError);
    },

    sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
      mutation: Name,
      params: Params,
    ) {
      return Promise.resolve()
        .then(() => remote.sync(mutation, params))
        .catch(requestError);
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
