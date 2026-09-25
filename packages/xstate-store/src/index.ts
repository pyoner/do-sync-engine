import { createStoreLogic } from "@xstate/store";
import { RpcStub, newWebSocketRpcSession } from "capnweb";
import type {
  MutationRecord,
  OpParams,
  OpResult,
  QueryRecord,
  StringKey,
  Topics,
} from "@do-sync-engine/core";
import type { Service } from "@do-sync-engine/durable-object-websocket";
import {
  SubscriptionOwnedConnectionStrategy,
  type ConnectionStrategy,
} from "./connection-strategy.ts";
export {
  AppLifetimeConnectionStrategy,
  ConnectionStrategy,
  IdleTimeoutConnectionStrategy,
  ManualConnectionStrategy,
  SubscriptionOwnedConnectionStrategy,
  type ConnectionController,
} from "./connection-strategy.ts";

type Status = "connecting" | "ready" | "disconnected";
type Context<Q extends QueryRecord> = {
  status: Status;
  topics: Record<string, OpResult<Q[StringKey<Q>]> | undefined>;
  error: Error | null;
};
type Session<Q extends QueryRecord, M extends MutationRecord> = {
  socket?: WebSocket;
  root?: RpcStub<Service<Q, M>>;
  chain: Promise<void>;
  ready: Promise<void>;
  open: () => void;
};
type Events<Q extends QueryRecord, M extends MutationRecord> = {
  connecting: {};
  disconnected: {};
  opened: { session: Session<Q, M> };
  closed: { session: Session<Q, M>; error: Error };
  synced: { session: Session<Q, M>; key: string; value: unknown };
  failed: { session: Session<Q, M> | undefined; error: Error | null };
};

export function createSyncStore<Q extends QueryRecord, M extends MutationRecord>({
  url,
  strategy = new SubscriptionOwnedConnectionStrategy(),
}: {
  url: string;
  strategy?: ConnectionStrategy;
}) {
  type ClientSession = Session<Q, M>;
  let current: ClientSession | undefined;

  const logic = createStoreLogic<Context<Q>, Events<Q, M>>({
    context: (): Context<Q> => ({ status: "disconnected", topics: {}, error: null }),
    on: {
      connecting: (context) => ({ ...context, status: "connecting", error: null }),
      disconnected: (context) => ({
        ...context,
        status: "disconnected",
        topics: {},
        error: null,
      }),
      opened: (context, { session }) =>
        current !== session ? context : { ...context, status: "ready" },
      closed: (context, { error }) => ({ ...context, status: "disconnected", topics: {}, error }),
      synced: (context, { session, key, value }) =>
        current !== session
          ? context
          : {
              ...context,
              topics: { ...context.topics, [key]: value as OpResult<Q[StringKey<Q>]> },
            },
      failed: (context, { session, error }) =>
        current !== session ? context : { ...context, error },
    },
  });
  const store = logic.createStore();

  const connectSocket = () => {
    if (store.getSnapshot().context.status !== "disconnected") return;
    let open = () => {};
    const ready = new Promise<void>((resolve) => {
      open = resolve;
    });
    const session: ClientSession = { chain: Promise.resolve(), ready, open };
    current = session;
    store.trigger.connecting();
    try {
      const socket = new WebSocket(url);
      session.socket = socket;
      socket.addEventListener("open", () => {
        if (current !== session) return;
        try {
          session.root = newWebSocketRpcSession<Service<Q, M>>(socket);
          session.root.onRpcBroken((error) => fail(session, error));
          session.open();
          store.trigger.opened({ session });
        } catch (cause) {
          fail(session, cause instanceof Error ? cause : new Error(String(cause), { cause }));
        }
      });
      socket.addEventListener("error", () =>
        fail(session, new Error("WebSocket connection failed")),
      );
      socket.addEventListener("close", () =>
        fail(session, new Error("WebSocket connection closed")),
      );
    } catch (cause) {
      fail(session, cause instanceof Error ? cause : new Error(String(cause), { cause }));
    }
  };
  const disconnectSocket = () => {
    if (current !== undefined) dispose(current, true);
    store.trigger.disconnected();
  };
  const dispose = (session: ClientSession, close: boolean) => {
    if (current !== session) return;
    current = undefined;
    session.open();
    session.root?.[Symbol.dispose]();
    if (close && session.socket !== undefined && session.socket.readyState < WebSocket.CLOSING) {
      session.socket.close();
    }
  };
  const fail = (session: ClientSession, error: Error) => {
    if (current !== session) return;
    strategy.failed();
    store.trigger.closed({ session, error });
    dispose(session, true);
  };
  const enqueueRpc = (session: ClientSession, run: () => Promise<void>) => {
    session.chain = session.chain.then(run).catch((cause: unknown) => {
      const error = new Error(cause instanceof Error ? cause.message : String(cause), { cause });
      store.trigger.failed({ session, error });
    });
  };
  const subscribeRpc = (session: ClientSession, topic: Topics<Q>, key: string) => {
    enqueueRpc(session, async () => {
      await session.ready;
      if (current !== session || session.root === undefined) return;
      const listener = new RpcStub((event: { value: unknown }) => {
        if (current === session) store.trigger.synced({ session, key, value: event.value });
      });
      const result = await session.root
        .subscribe(topic, listener)
        .finally(() => listener[Symbol.dispose]());
      if (result instanceof Error) store.trigger.failed({ session, error: result });
    });
  };
  const subscribe = (topic: Topics<Q>, key: string): void | Error => {
    strategy.subscribed();
    const session = current;
    if (session === undefined) return new Error("WebSocket RPC session is not ready");
    store.trigger.failed({ session, error: null });
    subscribeRpc(session, topic, key);
  };
  const unsubscribe = (topic: Topics<Q>) => {
    strategy.unsubscribed();
    const session = current;
    if (session === undefined) return;
    enqueueRpc(session, async () => {
      await session.ready;
      if (current !== session || session.root === undefined) return;
      const result = await session.root.unsubscribe(topic);
      if (result instanceof Error) store.trigger.failed({ session, error: result });
    });
  };
  const sync = async <Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ): Promise<void | Error> => {
    const session = current;
    if (store.getSnapshot().context.status !== "ready" || session?.root === undefined) {
      return new Error("WebSocket RPC session is not ready");
    }
    const result = await session.root
      .sync(mutation as never, params as never)
      .catch(
        (cause: unknown) =>
          new Error(cause instanceof Error ? cause.message : String(cause), { cause }),
      );
    if (current === session && result instanceof Error)
      store.trigger.failed({ session, error: result });
    return result;
  };

  strategy.attach({ connect: connectSocket, disconnect: disconnectSocket });
  return {
    subscribe,
    unsubscribe,
    sync,
    connect: () => strategy.connect(),
    disconnect: () => strategy.disconnect(),
    dispose: () => strategy.dispose(),
    store,
  };
}
