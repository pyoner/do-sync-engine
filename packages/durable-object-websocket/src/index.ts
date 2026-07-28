import { DurableObject } from "cloudflare:workers";
import type {
  MutationMap,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  TopicHash,
} from "@do-sync-engine/core";
import { SubscriptionRegistry } from "./subscriptions.ts";
import type { QueryDefinitions } from "./subscriptions.ts";
import type { ErrorMessage } from "./protocol.ts";
export type * from "./protocol.ts";
export type DurableObjectWebSocketBinding<Q extends object, M extends object> = {
  readonly engine: SyncEngineInterface<QueryMap<Q>, MutationMap<M>>;
  readonly queries: QueryDefinitions<Q>;
};
export type DurableObjectWebSocketInitializer<Q extends object, M extends object> = () =>
  | DurableObjectWebSocketBinding<Q, M>
  | Promise<DurableObjectWebSocketBinding<Q, M>>;
type IncomingMessage = Record<string, unknown> & { requestId: string };
type ParseResult = { message: IncomingMessage } | { error: ErrorMessage };
export abstract class DurableObjectWebSocket<
  Env,
  Q extends object,
  M extends object,
> extends DurableObject<Env> {
  private readonly initialization: Promise<void>;
  private engine!: SyncEngineInterface<QueryMap<Q>, MutationMap<M>>;
  private registry!: SubscriptionRegistry<Q, M>;
  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: DurableObjectWebSocketInitializer<Q, M>,
  ) {
    super(ctx, env);
    this.initialization = ctx.blockConcurrencyWhile(async () => {
      const { engine, queries } = await initialize();
      this.engine = engine;
      this.registry = new SubscriptionRegistry(engine, queries, (ws, message) =>
        this.send(ws, message),
      );
      for (const ws of ctx.getWebSockets()) await this.registry.restore(ws);
    });
  }
  async fetch(request: Request): Promise<Response> {
    await this.initialization;
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket")
      return new Response("Expected WebSocket", { status: 426 });
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    await this.registry.restore(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.initialization;
    const parsed = parse(message);
    if ("error" in parsed) return this.send(ws, parsed.error);
    const { message: value } = parsed;
    const requestId = value.requestId;
    try {
      if (value.type === "subscribe") {
        if (typeof value.query !== "string" || !Array.isArray(value.params))
          return this.send(ws, { type: "error", requestId, message: "query and params required" });
        await this.registry.subscribe(ws, value.query as StringKey<Q>, value.params, requestId);
      } else if (value.type === "unsubscribe") {
        if (typeof value.topicHash !== "string" || !/^[0-9a-f]{64}$/.test(value.topicHash))
          return this.send(ws, { type: "error", requestId, message: "topicHash required" });
        this.send(ws, {
          type: "unsubscribed",
          requestId,
          topicHash: value.topicHash,
          removed: this.registry.unsubscribe(ws, value.topicHash as TopicHash),
        });
      } else if (value.type === "sync") {
        if (typeof value.mutation !== "string" || !Array.isArray(value.params))
          return this.send(ws, {
            type: "error",
            requestId,
            message: "mutation and params required",
          });
        this.engine.sync(value.mutation as StringKey<M>, value.params as never);
        this.send(ws, { type: "synced", requestId });
      } else this.send(ws, { type: "error", requestId, message: "Unknown message type" });
    } catch (error) {
      this.send(ws, { type: "error", requestId, message: String(error) });
    }
  }
  async webSocketClose(
    ws: WebSocket,
    _code: number,
    _reason: string,
    _wasClean: boolean,
  ): Promise<void> {
    await this.initialization;
    this.registry.clear(ws);
  }
  async webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    await this.initialization;
    this.registry.clear(ws);
  }
  private send(ws: WebSocket, message: unknown): void {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
  }
}
function parse(message: string | ArrayBuffer): ParseResult {
  if (typeof message !== "string")
    return { error: { type: "error", message: "Expected text WebSocket message" } };
  let value: unknown;
  try {
    value = JSON.parse(message);
  } catch {
    return { error: { type: "error", message: "Invalid JSON message" } };
  }
  if (!value || typeof value !== "object" || Array.isArray(value))
    return { error: { type: "error", message: "Invalid JSON message" } };
  const record = value as Record<string, unknown>;
  if (typeof record.requestId !== "string" || !record.requestId.trim())
    return { error: { type: "error", message: "requestId required" } };
  return { message: record as IncomingMessage };
}
