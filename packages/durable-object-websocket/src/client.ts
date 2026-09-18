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
type RemoteService<Q extends QueryRecord, M extends MutationRecord> = Service<Q, M> & {
  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): void | Error;
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
  ): Promise<void | Error>;
  unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
    topic: Topic<Name, Params>,
  ): Promise<void | Error>;
  sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): Promise<void | Error>;
  onRpcBroken(listener: (error: Error) => void): void;
  [Symbol.dispose](): void;
}

export function newWebSocketRpcSession<Q extends QueryRecord, M extends MutationRecord>(
  socket: string | WebSocket,
): WebSocketRpcClient<Q, M> {
  const remote = createRpcSession(socket) as unknown as RemoteService<Q, M>;
  const scopedListener = (listener: Listener | RpcListener): RpcListener => {
    if (listener instanceof RpcStub) return listener.dup();
    return new RpcStub(listener as Listener);
  };

  const requestError = (cause: unknown): Error =>
    new Error("WebSocket RPC request failed", { cause });

  const client: WebSocketRpcClient<Q, M> = {
    async createTopic<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
      name: Name,
      params: Params,
    ) {
      try {
        await Promise.resolve();
        return remote.createTopic(name, params);
      } catch (cause) {
        return requestError(cause);
      }
    },

    async subscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
      topic: Topic<Name, Params>,
      listener: ClientListener<Q, Name, Params>,
    ): Promise<void | Error> {
      const scoped = scopedListener(listener as Listener | RpcListener);
      try {
        return await Promise.resolve()
          .then(() => remote.subscribe(topic, scoped))
          .catch(requestError);
      } finally {
        scoped[Symbol.dispose]();
      }
    },

    async unsubscribe<Name extends StringKey<Q>, Params extends OpParams<Q[Name]>>(
      topic: Topic<Name, Params>,
    ): Promise<void | Error> {
      return Promise.resolve()
        .then(() => remote.unsubscribe(topic))
        .catch(requestError);
    },

    async sync<Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
      mutation: Name,
      params: Params,
    ) {
      try {
        await Promise.resolve();
        return remote.sync(mutation, params);
      } catch (cause) {
        return requestError(cause);
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
