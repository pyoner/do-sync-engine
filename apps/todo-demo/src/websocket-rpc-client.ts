import type { MutationMap, OperationParams, QueryMap, StringKey } from "@do-sync-engine/core";
import type {
  QueryTopic,
  ServiceAPI,
  SubscriptionEvent,
} from "@do-sync-engine/durable-object-websocket";
import { LISTENER_EVENT_METHOD } from "@do-sync-engine/durable-object-websocket";
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
  readonly params?: readonly [SubscriptionEvent<QueryMap>];
};
export interface WebSocketRpcClient<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  createTopic<Name extends StringKey<Q>>(
    name: Name,
    params: OperationParams<Q[Name]>,
  ): QueryTopic<Q>;
  subscribe(
    topic: QueryTopic<Q>,
    listener: (event: SubscriptionEvent<Q>) => void,
  ): Promise<void | Error>;
  unsubscribe(
    topic: QueryTopic<Q>,
    listener: (event: SubscriptionEvent<Q>) => void,
  ): Promise<void | Error>;
  sync<Name extends StringKey<M>>(
    name: Name,
    params: OperationParams<M[Name]>,
  ): Promise<void | Error>;
  onRpcBroken(listener: (error: Error) => void): void;
  [Symbol.dispose](): void;
}
const topicKey = (topic: { readonly name: string; readonly params: readonly unknown[] }) =>
  JSON.stringify(topic);
export function newWebSocketRpcSession<Q extends QueryMap<Q>, M extends MutationMap<M>>(
  url: string,
): WebSocketRpcClient<Q, M> {
  const socket = new globalThis.WebSocket(url);
  const pending = new Map<string | number, PendingResponse>();
  const listeners = new Map<string, Set<(event: SubscriptionEvent<Q>) => void>>();
  const brokenListeners = new Set<(error: Error) => void>();
  let opened!: () => void;
  const open = new Promise<void>((resolve) => (opened = resolve));
  const broken = (error: Error) => {
    for (const item of pending.values()) item.reject(error);
    pending.clear();
    for (const listener of brokenListeners) listener(error);
  };
  socket.addEventListener("open", opened);
  socket.addEventListener("close", () => broken(new Error("WebSocket connection closed")));
  socket.addEventListener("error", () => broken(new Error("WebSocket connection failed")));
  socket.addEventListener("message", (message: MessageEvent) => {
    let parsed: SocketMessage;
    try {
      parsed = JSON.parse(String(message.data)) as SocketMessage;
    } catch (cause) {
      broken(new Error("Invalid WebSocket message", { cause }));
      return;
    }
    if (
      parsed.jsonrpc === "2.0" &&
      parsed.method === LISTENER_EVENT_METHOD &&
      parsed.params?.length === 1
    ) {
      const event = parsed.params[0] as unknown as SubscriptionEvent<Q>;
      for (const listener of listeners.get(topicKey(event.topic)) ?? []) listener(event);
      return;
    }
    if (parsed.id === undefined || parsed.id === null) return;
    const response = pending.get(parsed.id);
    if (!response) return;
    pending.delete(parsed.id);
    response.resolve(parsed as JsonRpcResponse);
  });
  const transport: RpcTransport = async (request: JsonRpcRequest, signal: AbortSignal) => {
    await open;
    if (request.id === undefined || request.id === null)
      return new Error("JSON-RPC request ID required") as never;
    const promise = new Promise<JsonRpcResponse>((resolve, reject) =>
      pending.set(request.id!, { resolve, reject }),
    );
    signal.addEventListener(
      "abort",
      () => {
        pending.delete(request.id!);
      },
      { once: true },
    );
    socket.send(JSON.stringify(request));
    return promise;
  };
  const client = rpcClient<ServiceAPI<Q, M>>({ transport });
  const request = <T>(promise: Promise<T>) =>
    promise
      .then((result) => (result === null ? undefined : result))
      .catch((cause) => new Error("WebSocket RPC request failed", { cause }));
  return {
    createTopic: (name, params) => ({ name, params }),
    subscribe: async (topic, listener) => {
      const key = topicKey(topic);
      const set = listeners.get(key) ?? new Set();
      set.add(listener);
      listeners.set(key, set);
      const result = await request(client.subscribe(topic));
      if (!(result instanceof Error)) return;
      set.delete(listener);
      if (!set.size) listeners.delete(key);
      return result;
    },
    unsubscribe: async (topic, listener) => {
      const result = await request(client.unsubscribe(topic));
      if (result instanceof Error) return result;
      const set = listeners.get(topicKey(topic));
      set?.delete(listener);
      if (set && !set.size) listeners.delete(topicKey(topic));
    },
    sync: (name, params) => request(client.sync(name, params)),
    onRpcBroken: (listener) => brokenListeners.add(listener),
    [Symbol.dispose]: () => socket.close(),
  };
}
