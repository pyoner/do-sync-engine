import { Cause, Effect } from "effect";
import { DurableObject } from "cloudflare:workers";
import { UnknownMutationError, UnknownQueryError } from "@do-sync-engine/core";
import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { SubscriptionRegistry, WebSocketOperationError } from "./subscriptions.ts";
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
  private readonly socketCallbacks = new WeakMap<WebSocket, Promise<void>>();
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
    return this.enqueueSocket(ws, this.webSocketMessageEffect(ws, message));
  }
  webSocketClose(ws: WebSocket, _code: number, _reason: string, _wasClean: boolean): Promise<void> {
    return this.enqueueSocket(ws, this.socketCleanupEffect(ws));
  }
  webSocketError(ws: WebSocket, _error: unknown): Promise<void> {
    return this.enqueueSocket(ws, this.socketCleanupEffect(ws));
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
        yield* decodeClientCommand(message).pipe(
          Effect.matchEffect({
            onFailure: (error) => this.sendEffect(ws, error).pipe(Effect.ignoreCause),
            onSuccess: (command) => {
              const execute = Effect.gen(
                function* (this: DurableObjectWebSocket<Env, Q, M>) {
                  if (command.type === "subscribe") {
                    yield* this.registry.subscribe(
                      ws,
                      command.query as StringKey<Q>,
                      command.params,
                      command.requestId,
                    );
                  } else if (command.type === "unsubscribe") {
                    const removed = yield* this.registry.unsubscribe(ws, command.topic);
                    yield* this.sendEffect(ws, {
                      type: "unsubscribed",
                      requestId: command.requestId,
                      topic: command.topic,
                      removed,
                    });
                  } else {
                    yield* this.engine.sync(
                      command.mutation as StringKey<M>,
                      command.params as OperationParams<M[StringKey<M>]>,
                    );
                    yield* this.sendEffect(ws, {
                      type: "synced",
                      requestId: command.requestId,
                    });
                  }
                }.bind(this),
              );
              return execute.pipe(
                Effect.matchCauseEffect({
                  onFailure: (cause) =>
                    this.sendEffect(ws, {
                      type: "error",
                      requestId: command.requestId,
                      message: formatError(Cause.squash(cause)),
                    }).pipe(Effect.ignoreCause),
                  onSuccess: () => Effect.void,
                }),
              );
            },
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
  private enqueueSocket(ws: WebSocket, workflow: Effect.Effect<void, unknown>): Promise<void> {
    const previous = this.socketCallbacks.get(ws) ?? Promise.resolve();
    const current = previous.then(() => Effect.runPromise(workflow));
    const recovered = current.catch(() => undefined);
    this.socketCallbacks.set(ws, recovered);
    return recovered;
  }

  private sendEffect(
    ws: WebSocket,
    message: unknown,
  ): Effect.Effect<void, WebSocketOperationError> {
    return Effect.try({
      try: () => this.send(ws, message),
      catch: (cause) => new WebSocketOperationError({ cause }),
    });
  }

  private send(ws: WebSocket, message: unknown): void {
    if (ws.readyState === WebSocket.OPEN) ws.send(JSON.stringify(message));
  }
}

const formatError = (error: unknown, seen = new Set<unknown>()): string => {
  if (error instanceof UnknownQueryError) return `Unknown query: ${error.query}`;
  if (error instanceof UnknownMutationError) return `Unknown mutation: ${error.mutation}`;
  if (error instanceof WebSocketOperationError) return formatError(error.cause, seen);
  if (error instanceof Error && error.message.trim() !== "") return error.message;
  if (typeof error === "object" && error !== null && !seen.has(error)) {
    seen.add(error);
    if ("cause" in error) return formatError(error.cause, seen);
  }
  return "Unknown websocket error";
};
