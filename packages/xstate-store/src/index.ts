import { createStoreLogic, type ExtractEvents, type Store } from "@xstate/store";
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

type Status = "idle" | "connecting" | "ready" | "disconnected";
type Context<Q extends QueryRecord> = {
  status: Status;
  error: Error | null;
  transport: WebSocket | null;
  topics: Record<string, OpResult<Q[StringKey<Q>]> | undefined>;
};
type SyncRequest<M extends MutationRecord> = {
  mutation: StringKey<M>;
  params: OpParams<M[StringKey<M>]>;
};
type Events<Q extends QueryRecord, M extends MutationRecord> = {
  connect: {};
  disconnect: {};
  opened: { transport: WebSocket };
  closed: { error: Error };
  failed: { error: Error };
  synced: { key: string; value: unknown };
  subscribe: { topic: Topics<Q>; key: string };
  unsubscribe: { topic: Topics<Q>; key: string };
  sync: SyncRequest<M>;
};
type Emitted<Q extends QueryRecord> = {
  connecting: {};
  disconnected: {};
  opened: {};
  closed: { error: Error };
  failed: { error: Error };
  synced: { key: string; value: OpResult<Q[StringKey<Q>]> };
};

export interface SyncStore<Q extends QueryRecord, M extends MutationRecord> {
  connect: () => void;
  disconnect: () => void;
  /** `key` defaults to `topic.name` and points at `store.context.topics[key]`. */
  subscribe: (topic: Topics<Q>, key?: string) => Error | undefined;
  unsubscribe: (topic: Topics<Q>, key?: string) => Error | undefined;
  sync: <Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
    mutation: Name,
    params: Params,
  ) => Error | undefined;
  store: Store<Context<Q>, Events<Q, M>, ExtractEvents<Emitted<Q>>>;
  [Symbol.dispose]: () => void;
}

const toError = (cause: unknown) =>
  new Error(cause instanceof Error ? cause.message : String(cause), { cause });
const notReady = () => new Error("WebSocket RPC session is not ready");

export function createSyncStore<Q extends QueryRecord, M extends MutationRecord>({
  url,
}: {
  url: string;
}): SyncStore<Q, M> {
  // Live socket + its RPC stub. Commands only run in `ready`, where these are current.
  let socket: WebSocket | undefined;
  let engine: RpcStub<Service<Q, M>> | undefined;

  const logic = createStoreLogic<Context<Q>, Events<Q, M>, Emitted<Q>>({
    context: (): Context<Q> => ({ status: "idle", error: null, transport: null, topics: {} }),
    on: {
      connect: (context, _event, enqueue) => {
        if (context.status !== "idle" && context.status !== "disconnected") return;
        enqueue.emit.connecting();
        enqueue.effect(open);
        return { ...context, status: "connecting", error: null };
      },
      disconnect: (context, _event, enqueue) => {
        if (context.status !== "connecting" && context.status !== "ready") return;
        enqueue.effect(release);
        enqueue.emit.disconnected();
        return { ...context, status: "disconnected", transport: null, topics: {}, error: null };
      },
      opened: (context, { transport }, enqueue) => {
        if (context.status !== "connecting") return;
        enqueue.emit.opened();
        return { ...context, status: "ready", transport };
      },
      closed: (context, { error }, enqueue) => {
        if (context.status !== "connecting" && context.status !== "ready") return;
        enqueue.effect(release);
        enqueue.emit.closed({ error });
        return { ...context, status: "disconnected", transport: null, topics: {}, error };
      },
      failed: (context, { error }, enqueue) => {
        if (context.status !== "ready") return;
        enqueue.emit.failed({ error });
        return { ...context, error };
      },
      synced: (context, { key, value }, enqueue) => {
        if (context.status !== "ready") return;
        const result = value as OpResult<Q[StringKey<Q>]>;
        enqueue.emit.synced({ key, value: result });
        return { ...context, topics: { ...context.topics, [key]: result } };
      },
      subscribe: (context, { topic, key }, enqueue) => {
        if (context.status !== "ready") return;
        enqueue.effect(() => {
          const current = engine;
          if (current === undefined) return;
          const listener = new RpcStub((event: { value: unknown }) => {
            if (engine === current) store.trigger.synced({ key, value: event.value });
          });
          void current
            .subscribe(topic, listener)
            .catch(toError)
            .finally(() => listener[Symbol.dispose]())
            .then((result) => {
              if (engine === current && result instanceof Error) {
                store.trigger.failed({ error: result });
              }
            });
        });
        return { ...context, error: null };
      },
      unsubscribe: (context, { topic, key }, enqueue) => {
        if (context.status !== "ready") return;
        enqueue.effect(() => {
          const current = engine;
          if (current === undefined) return;
          void current
            .unsubscribe(topic)
            .catch(toError)
            .then((result) => {
              if (engine === current && result instanceof Error) {
                store.trigger.failed({ error: result });
              }
            });
        });
        const { [key]: _removed, ...topics } = context.topics;
        return { ...context, topics };
      },
      sync: (context, { mutation, params }, enqueue) => {
        if (context.status !== "ready") return;
        enqueue.effect(() => {
          const current = engine;
          if (current === undefined) return;
          void current
            .sync(mutation as never, params as never)
            .catch(toError)
            .then((result) => {
              if (engine === current && result instanceof Error) {
                store.trigger.failed({ error: result });
              }
            });
        });
      },
    },
  });
  const store = logic.createStore();

  const fail = (ws: WebSocket, cause: unknown) => {
    if (socket === ws) store.trigger.closed({ error: toError(cause) });
  };
  function open() {
    try {
      const ws = new WebSocket(url);
      socket = ws;
      ws.addEventListener("open", () => {
        if (socket !== ws) return;
        try {
          const current = newWebSocketRpcSession<Service<Q, M>>(ws);
          engine = current;
          current.onRpcBroken((error) => fail(ws, error));
          store.trigger.opened({ transport: ws });
        } catch (cause) {
          fail(ws, cause);
        }
      });
      ws.addEventListener("error", () => fail(ws, new Error("WebSocket connection failed")));
      ws.addEventListener("close", () => fail(ws, new Error("WebSocket connection closed")));
    } catch (cause) {
      store.trigger.closed({ error: toError(cause) });
    }
  }
  function release() {
    const ws = socket;
    socket = undefined;
    engine?.[Symbol.dispose]();
    engine = undefined;
    if (ws !== undefined && ws.readyState < WebSocket.CLOSING) ws.close();
  }

  return {
    connect: (): void => store.trigger.connect(),
    disconnect: (): void => store.trigger.disconnect(),
    subscribe: (topic: Topics<Q>, key: string = topic.name) => {
      const event = { topic, key };
      if (!store.can.subscribe(event)) return notReady();
      store.trigger.subscribe(event);
      return undefined;
    },
    unsubscribe: (topic: Topics<Q>, key: string = topic.name) => {
      const event = { topic, key };
      if (!store.can.unsubscribe(event)) return notReady();
      store.trigger.unsubscribe(event);
      return undefined;
    },
    sync: <Name extends StringKey<M>, Params extends OpParams<M[Name]>>(
      mutation: Name,
      params: Params,
    ) => {
      const event = { mutation, params } as SyncRequest<M>;
      if (!store.can.sync(event)) return notReady();
      store.trigger.sync(event);
      return undefined;
    },
    store,
    [Symbol.dispose]: (): void => store.trigger.disconnect(),
  };
}
