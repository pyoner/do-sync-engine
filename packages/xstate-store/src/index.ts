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
};
type Events<Q extends QueryRecord, M extends MutationRecord> = {
  connect: {};
  disconnect: {};
  opened: { session: Session<Q, M> };
  closed: { session: Session<Q, M>; error: Error };
  synced: { session: Session<Q, M>; key: string; value: unknown };
  failed: { session: Session<Q, M> | undefined; error: Error | null };
};

export function createSyncStore<Q extends QueryRecord, M extends MutationRecord>({
  url,
  autoconnect = true,
}: {
  url: string;
  autoconnect?: boolean;
}) {
  type ClientSession = Session<Q, M>;

  let current: ClientSession | undefined;

  const logic = createStoreLogic<Context<Q>, Events<Q, M>>({
    context: (): Context<Q> => ({ status: "disconnected", topics: {}, error: null }),
    on: {
      connect: (context, _event, enqueue) => {
        if (context.status !== "disconnected") return context;
        const session: ClientSession = { chain: Promise.resolve() };
        current = session;
        enqueue.effect(() => {
          try {
            const socket = new WebSocket(url);
            session.socket = socket;
            socket.addEventListener("open", () => {
              if (current !== session) return;
              try {
                session.root = newWebSocketRpcSession<Service<Q, M>>(socket);
                session.root.onRpcBroken((error) => fail(session, error));
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
        });
        return { ...context, status: "connecting", error: null };
      },
      disconnect: (context) => {
        if (current !== undefined) dispose(current, true);
        return { ...context, status: "disconnected", topics: {}, error: null };
      },
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

  const dispose = (session: ClientSession, close: boolean) => {
    if (current !== session) return;
    current = undefined;
    session.root?.[Symbol.dispose]();
    if (close && session.socket !== undefined && session.socket.readyState < WebSocket.CLOSING) {
      session.socket.close();
    }
  };
  const fail = (session: ClientSession, error: Error) => {
    if (current !== session) return;
    store.trigger.closed({ session, error });
    dispose(session, true);
  };
  const enqueueRpc = (session: ClientSession, run: () => Promise<void>) => {
    session.chain = session.chain.then(run).catch((cause: unknown) => {
      const error = new Error(cause instanceof Error ? cause.message : String(cause), { cause });
      store.trigger.failed({ session, error });
    });
  };
  const subscribe = (topic: Topics<Q>, key: string) => {
    const session = current;
    if (store.getSnapshot().context.status !== "ready" || session?.root === undefined) {
      store.trigger.failed({
        session,
        error: new Error("WebSocket RPC session is not ready"),
      });
      return;
    }
    store.trigger.failed({ session, error: null });
    enqueueRpc(session, async () => {
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
  const unsubscribe = (topic: Topics<Q>) => {
    const session = current;
    if (store.getSnapshot().context.status !== "ready" || session?.root === undefined) return;
    enqueueRpc(session, async () => {
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
  if (autoconnect) store.trigger.connect();
  return { subscribe, unsubscribe, sync, store };
}
