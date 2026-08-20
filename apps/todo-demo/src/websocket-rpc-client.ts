import type { MutationMap, OperationParams, QueryMap, StringKey } from "@do-sync-engine/core";
import type {
  QueryTopic,
  ServerAPI,
  SubscriptionEvent,
} from "@do-sync-engine/durable-object-websocket";
import { rpcClient } from "typed-rpc";
import type { JsonRpcRequest, JsonRpcResponse, RpcTransport } from "typed-rpc";
type PendingResponse = {
  readonly reject: (reason: Error) => void;
  readonly resolve: (response: JsonRpcResponse) => void;
};

type SocketMessage = {
  readonly id?: string | number | null;
  readonly jsonrpc?: string;
  readonly method?: string;
  readonly params?: readonly [ReadonlyArray<SubscriptionEvent<QueryMap>>];
  readonly topic?: { readonly name: string; readonly params: readonly unknown[] };
  readonly value?: unknown;
};

export interface WebSocketRpcClient<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> {
  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): QueryTopic<Queries>;
  subscribe(
    topic: QueryTopic<Queries>,
    listener: (event: SubscriptionEvent<Queries>) => void,
  ): Promise<void | Error>;
  unsubscribe(
    topic: QueryTopic<Queries>,
    listener: (event: SubscriptionEvent<Queries>) => void,
  ): Promise<void | Error>;
  sync<Name extends StringKey<Mutations>>(
    name: Name,
    params: OperationParams<Mutations[Name]>,
  ): Promise<void | Error>;
  onRpcBroken(listener: (error: Error) => void): void;
  [Symbol.dispose](): void;
}

function topicKey(topic: { readonly name: string; readonly params: readonly unknown[] }): string {
  return JSON.stringify(topic);
}

export function newWebSocketRpcSession<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
>(url: string): WebSocketRpcClient<Queries, Mutations> {
  const socket = new globalThis.WebSocket(url);
  const pending = new Map<string | number, PendingResponse>();
  const listeners = new Map<string, Set<(event: SubscriptionEvent<Queries>) => void>>();
  const opened = (() => {
    let resolve!: () => void;
    const promise = new Promise<void>((res) => {
      resolve = res;
    });
    return { promise, resolve };
  })();
  const brokenListeners = new Set<(error: Error) => void>();
  const breakConnection = (error: Error) => {
    for (const pendingResponse of pending.values()) pendingResponse.reject(error);
    pending.clear();
    for (const listener of brokenListeners) listener(error);
  };

  socket.addEventListener("open", () => opened.resolve());
  socket.addEventListener("close", () => breakConnection(new Error("WebSocket connection closed")));
  socket.addEventListener("error", () => breakConnection(new Error("WebSocket connection failed")));
  socket.addEventListener("message", (message: MessageEvent) => {
    const parsed = (() => {
      try {
        return JSON.parse(String(message.data)) as SocketMessage;
      } catch (cause) {
        return new Error("Invalid WebSocket message", { cause });
      }
    })();
    if (parsed instanceof Error) {
      breakConnection(parsed);
      return;
    }
    if (parsed.jsonrpc === "2.0" && parsed.method === "synced") {
      for (const event of parsed.params?.[0] ?? []) {
        for (const listener of listeners.get(topicKey(event.topic)) ?? []) {
          listener(event as unknown as SubscriptionEvent<Queries>);
        }
      }
      return;
    }
    if (parsed.topic !== undefined && "value" in parsed) {
      const event = parsed as SubscriptionEvent<Queries>;
      for (const listener of listeners.get(topicKey(event.topic)) ?? []) listener(event);
      return;
    }
    if (parsed.id === undefined || parsed.id === null) return;
    const pendingResponse = pending.get(parsed.id);
    if (pendingResponse === undefined) return;
    pending.delete(parsed.id);
    pendingResponse.resolve(parsed as JsonRpcResponse);
  });

  const transport: RpcTransport = async (request: JsonRpcRequest, abortSignal: AbortSignal) => {
    await opened.promise;
    const id = request.id;
    if (id === undefined || id === null) return new Error("JSON-RPC request ID required") as never;
    let resolve!: (response: JsonRpcResponse) => void;
    let reject!: (reason: Error) => void;
    const promise = new Promise<JsonRpcResponse>((res, rej) => {
      resolve = res;
      reject = rej;
    });
    pending.set(id, { reject, resolve });
    abortSignal.addEventListener(
      "abort",
      () => {
        pending.delete(id);
        reject(new Error("JSON-RPC request aborted"));
      },
      { once: true },
    );
    socket.send(JSON.stringify(request));
    return promise;
  };
  const client = rpcClient<ServerAPI<Queries, Mutations>>({
    transport,
  });
  const request = <Result>(promise: Promise<Result>) =>
    promise.catch((cause) => new Error("WebSocket RPC request failed", { cause }));

  return {
    createTopic: (name, params) => ({ name, params }),
    subscribe: async (topic, listener) => {
      const key = topicKey(topic);
      const topicListeners = listeners.get(key) ?? new Set();
      topicListeners.add(listener);
      listeners.set(key, topicListeners);
      const result = await request(client.subscribe(topic));
      if (!(result instanceof Error)) return result;
      topicListeners.delete(listener);
      if (topicListeners.size === 0) listeners.delete(key);
      return result;
    },
    unsubscribe: async (topic, listener) => {
      const result = await request(client.unsubscribe(topic));
      if (result instanceof Error) return result;
      const key = topicKey(topic);
      const topicListeners = listeners.get(key);
      topicListeners?.delete(listener);
      if (topicListeners?.size === 0) listeners.delete(key);
    },
    sync: (name, params) => request(client.sync(name, params)),
    onRpcBroken: (listener) => brokenListeners.add(listener),
    [Symbol.dispose]: () => socket.close(),
  };
}
