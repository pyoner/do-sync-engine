import { Effect } from "effect";
import { TopicHasherLive } from "@do-sync-engine/core";
import { DurableObject } from "cloudflare:workers";
import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { SubscriptionRegistry } from "./subscriptions.ts";
import { decodeClientCommand } from "./protocol.ts";
export type * from "./protocol.ts";
export type DurableObjectWebSocketBinding<Q extends QueryMap<Q>, M extends MutationMap<M>> = {
  readonly engine: SyncEngineInterface<Q, M>;
};
export type DurableObjectWebSocketInitializer<
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> = () => DurableObjectWebSocketBinding<Q, M> | Promise<DurableObjectWebSocketBinding<Q, M>>;
export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> extends DurableObject<Env> {
  private readonly initialization: Promise<void>;
  private engine!: SyncEngineInterface<Q, M>;
  private registry!: SubscriptionRegistry<Q, M>;
  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: DurableObjectWebSocketInitializer<Q, M>,
  ) {
    super(ctx, env);
    this.initialization = ctx.blockConcurrencyWhile(async () => {
      const { engine } = await initialize();
      this.engine = engine;
      this.registry = new SubscriptionRegistry(engine, (ws, message) => this.send(ws, message));
      for (const ws of ctx.getWebSockets()) {
        await Effect.runPromise(
          this.registry.restore(ws, false).pipe(Effect.provide(TopicHasherLive)),
        );
      }
    });
  }
  async fetch(request: Request): Promise<Response> {
    await this.initialization;
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket")
      return new Response("Expected WebSocket", { status: 426 });
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    await Effect.runPromise(this.registry.restore(pair[1]).pipe(Effect.provide(TopicHasherLive)));
    return new Response(null, { status: 101, webSocket: pair[0] });
  }
  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.initialization;
    const decoded = decodeClientCommand(message);
    if ("error" in decoded) return this.send(ws, decoded.error);
    const { command } = decoded;
    const requestId = command.requestId;
    try {
      if (command.type === "subscribe") {
        await Effect.runPromise(
          this.registry
            .subscribe(ws, command.query as StringKey<Q>, command.params, requestId)
            .pipe(Effect.provide(TopicHasherLive)),
        );
      } else if (command.type === "unsubscribe") {
        this.send(ws, {
          type: "unsubscribed",
          requestId,
          topicHash: command.topicHash,
          removed: this.registry.unsubscribe(ws, command.topicHash),
        });
      } else {
        this.engine.sync(
          command.mutation as StringKey<M>,
          command.params as OperationParams<M[StringKey<M>]>,
        );
        this.send(ws, { type: "synced", requestId });
      }
    } catch (error) {
      const message =
        typeof error === "object" && error !== null && "_tag" in error
          ? error._tag === "UnknownQueryError" &&
            "query" in error &&
            typeof error.query === "string"
            ? `Unknown query: ${error.query}`
            : error._tag === "TopicBuildError" && "operation" in error
              ? error.operation === "clone"
                ? "Topic params must support structuredClone"
                : "cause" in error && error.cause instanceof Error
                  ? error.cause.message
                  : "Topic construction failed"
              : "Topic construction failed"
          : error instanceof Error
            ? error.message
            : "Unknown websocket error";
      this.send(ws, { type: "error", requestId, message });
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
