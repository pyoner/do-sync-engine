import { DurableObject } from "cloudflare:workers";
import type { MutationMap, QueryMap, StringKey, SyncEngineInterface } from "@do-sync-engine/core";
import { SubscriptionRegistry } from "./subscriptions.ts";
import type { QueryDefinitions } from "./subscriptions.ts";
export type { DurableObjectWebSocketAttachment } from "./subscriptions.ts";
export type * from "./protocol.ts";
type Binding<Q extends object, M extends object> = {
  readonly engine: SyncEngineInterface<QueryMap<Q>, MutationMap<M>>;
  readonly queries: QueryDefinitions<Q>;
};
export type DurableObjectWebSocketBinding<Q extends object, M extends object> = Binding<Q, M>;
export type DurableObjectWebSocketInitializer<Q extends object, M extends object> = () =>
  | Binding<Q, M>
  | Promise<Binding<Q, M>>;
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
    if (parsed.error) return this.send(ws, parsed.error);
    const requestId = parsed.value.requestId;
    try {
      if (parsed.value.type === "subscribe") {
        if (typeof parsed.value.query !== "string" || !Array.isArray(parsed.value.params))
          return this.send(ws, { type: "error", requestId, message: "query and params required" });
        await this.registry.subscribe(
          ws,
          parsed.value.query as StringKey<Q>,
          parsed.value.params,
          requestId,
        );
      } else if (parsed.value.type === "unsubscribe") {
        if (
          typeof parsed.value.topicHash !== "string" ||
          !/^[0-9a-f]{64}$/.test(parsed.value.topicHash)
        )
          return this.send(ws, { type: "error", requestId, message: "topicHash required" });
        this.send(ws, {
          type: "unsubscribed",
          requestId,
          topicHash: parsed.value.topicHash,
          removed: this.registry.unsubscribe(ws, parsed.value.topicHash),
        });
      } else if (parsed.value.type === "sync") {
        if (typeof parsed.value.mutation !== "string" || !Array.isArray(parsed.value.params))
          return this.send(ws, {
            type: "error",
            requestId,
            message: "mutation and params required",
          });
        this.engine.sync(parsed.value.mutation as StringKey<M>, parsed.value.params as never);
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
function parse(message: string | ArrayBuffer): { value?: any; error?: any } {
  if (typeof message !== "string")
    return { error: { type: "error", message: "Expected text WebSocket message" } };
  let value: any;
  try {
    value = JSON.parse(message);
  } catch {
    return { error: { type: "error", message: "Invalid JSON message" } };
  }
  if (!value || typeof value !== "object" || Array.isArray(value))
    return { error: { type: "error", message: "Invalid JSON message" } };
  if (typeof value.requestId !== "string" || !value.requestId.trim())
    return { error: { type: "error", message: "requestId required" } };
  return { value };
}
