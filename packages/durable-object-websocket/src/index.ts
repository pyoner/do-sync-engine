import { Effect } from "effect";
import { DurableObject } from "cloudflare:workers";
import {
  TopicBuildError,
  TopicValidationError,
  UnknownMutationError,
  UnknownQueryError,
} from "@do-sync-engine/core";
import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { SubscriptionRegistry } from "./subscriptions.ts";
import { decodeClientCommand } from "./protocol.ts";
import type { ErrorMessage } from "./protocol.ts";
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
    this.initialization = ctx.blockConcurrencyWhile(() =>
      Effect.runPromise(
        Effect.gen(
          function* (this: DurableObjectWebSocket<Env, Q, M>) {
            const { engine } = yield* Effect.tryPromise({
              try: () => Promise.resolve(initialize()),
              catch: (cause) => cause,
            });
            this.engine = engine;
            this.registry = new SubscriptionRegistry(engine, (ws, message) =>
              this.send(ws, message),
            );
            for (const ws of ctx.getWebSockets()) yield* this.registry.restore(ws, false);
          }.bind(this),
        ),
      ),
    );
  }
  fetch(request: Request): Promise<Response> {
    return Effect.runPromise(this.fetchEffect(request));
  }
  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    return Effect.runPromise(this.webSocketMessageEffect(ws, message));
  }
  webSocketClose(ws: WebSocket, _code: number, _reason: string, _wasClean: boolean): Promise<void> {
    return Effect.runPromise(this.socketCleanupEffect(ws));
  }
  webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    return Effect.runPromise(this.socketCleanupEffect(ws));
  }
  private fetchEffect(request: Request): Effect.Effect<Response, unknown> {
    return Effect.gen(
      function* (this: DurableObjectWebSocket<Env, Q, M>) {
        yield* Effect.tryPromise({
          try: () => this.initialization,
          catch: (cause) => cause,
        });
        if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket")
          return new Response("Expected WebSocket", { status: 426 });
        const pair = new WebSocketPair();
        this.ctx.acceptWebSocket(pair[1]);
        yield* this.registry.restore(pair[1]);
        return new Response(null, { status: 101, webSocket: pair[0] });
      }.bind(this),
    );
  }
  private webSocketMessageEffect(
    ws: WebSocket,
    message: string | ArrayBuffer,
  ): Effect.Effect<void, unknown> {
    return Effect.gen(
      function* (this: DurableObjectWebSocket<Env, Q, M>) {
        yield* Effect.tryPromise({
          try: () => this.initialization,
          catch: (cause) => cause,
        });
        let requestId: string | undefined;
        const execute = Effect.gen(
          function* (this: DurableObjectWebSocket<Env, Q, M>) {
            const command = yield* decodeClientCommand(message);
            requestId = command.requestId;
            if (command.type === "subscribe") {
              yield* this.registry.subscribe(
                ws,
                command.query as StringKey<Q>,
                command.params,
                requestId,
              );
            } else if (command.type === "unsubscribe") {
              const removed = yield* this.registry.unsubscribe(ws, command.topic);
              yield* Effect.sync(() =>
                this.send(ws, {
                  type: "unsubscribed",
                  requestId,
                  topic: command.topic,
                  removed,
                }),
              );
            } else {
              yield* this.engine.sync(
                command.mutation as StringKey<M>,
                command.params as OperationParams<M[StringKey<M>]>,
              );
              yield* Effect.sync(() => this.send(ws, { type: "synced", requestId }));
            }
          }.bind(this),
        );
        yield* execute.pipe(
          Effect.matchEffect({
            onFailure: (error) =>
              Effect.sync(() => {
                const protocolError = isErrorMessage(error)
                  ? error
                  : { type: "error", requestId, message: formatError(error) };
                this.send(ws, protocolError);
              }),
            onSuccess: () => Effect.void,
          }),
        );
      }.bind(this),
    );
  }
  private socketCleanupEffect(ws: WebSocket): Effect.Effect<void, unknown> {
    return Effect.gen(
      function* (this: DurableObjectWebSocket<Env, Q, M>) {
        yield* Effect.tryPromise({
          try: () => this.initialization,
          catch: (cause) => cause,
        });
        yield* this.registry.clear(ws);
      }.bind(this),
    );
  }
  private send(ws: WebSocket, message: unknown): void {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
  }
}

const isErrorMessage = (value: unknown): value is ErrorMessage =>
  typeof value === "object" &&
  value !== null &&
  "type" in value &&
  value.type === "error" &&
  "message" in value &&
  typeof value.message === "string";

const formatError = (error: unknown): string => {
  if (error instanceof UnknownQueryError) return `Unknown query: ${error.query}`;
  if (error instanceof UnknownMutationError) return `Unknown mutation: ${error.mutation}`;
  if (error instanceof TopicBuildError) {
    if (error.operation === "clone") return "Topic params must support structuredClone";
    return error.cause instanceof Error ? error.cause.message : "Topic construction failed";
  }
  if (error instanceof TopicValidationError)
    return error.cause instanceof Error ? error.cause.message : "Topic construction failed";
  return error instanceof Error ? error.message : "Unknown websocket error";
};
