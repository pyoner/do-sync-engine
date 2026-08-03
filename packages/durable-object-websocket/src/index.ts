import { Effect, Exit, Scope, Stream } from "effect";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { UnknownMutationError, UnknownQueryError } from "@do-sync-engine/core";
import { DurableObject } from "cloudflare:workers";
import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { SubscriptionRegistry } from "./subscriptions.ts";
import type { PersistedSubscription } from "./subscriptions.ts";
import { RpcOperationError, WebSocketRpc } from "./protocol.ts";
import { makeCloudflareRpcServerTransport } from "./server-transport.ts";
import type { CloudflareRpcServerTransport } from "./server-transport.ts";

export { RpcOperationError, Subscribe, Sync, Unsubscribe, WebSocketRpc } from "./protocol.ts";
export type {
  OperationParamsFor,
  QueryEventPayload,
  SubscribePayload,
  SubscribeResult,
  SyncPayload,
  Topic,
  UnsubscribePayload,
} from "./protocol.ts";
export { makeWebSocketRpcClient } from "./client.ts";
export type { WebSocketRpcClient } from "./client.ts";

const toRpcOperationError = (error: unknown): RpcOperationError => {
  let current: unknown = error;
  const seen = new Set<unknown>();
  while (!seen.has(current)) {
    seen.add(current);
    if (current instanceof Error && current.message !== "") {
      return RpcOperationError.make({ message: current.message });
    }
    if (typeof current !== "object" || current === null || !("cause" in current)) break;
    current = current.cause;
  }
  return RpcOperationError.make({ message: "WebSocket operation failed" });
};

export type DurableObjectWebSocketBinding<Q extends QueryMap<Q>, M extends MutationMap<M>> = {
  readonly engine: SyncEngineInterface<Q, M>;
};

export type DurableObjectWebSocketInitializer<
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> = () => DurableObjectWebSocketBinding<Q, M> | Promise<DurableObjectWebSocketBinding<Q, M>>;

type SocketTransport = {
  readonly scope: Scope.Closeable;
  readonly transport: CloudflareRpcServerTransport;
};

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> extends DurableObject<Env> {
  private readonly initialization: Promise<void>;
  private readonly socketCallbacks = new WeakMap<WebSocket, Promise<void>>();
  private readonly socketTransports = new WeakMap<WebSocket, SocketTransport>();
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
            this.registry = new SubscriptionRegistry(engine);
            for (const ws of ctx.getWebSockets()) {
              const restored = yield* this.registry.restore(ws);
              yield* this.initializeSocketRpc(ws, restored);
            }
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
    return this.enqueueSocket(ws, this.socketCleanupEffect(ws, Exit.void));
  }

  webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    return this.enqueueSocket(ws, this.socketCleanupEffect(ws, Exit.die(error)));
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
        const restored = yield* this.registry.restore(pair[1]);
        yield* this.initializeSocketRpc(pair[1], restored);
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
        let entry = this.socketTransports.get(ws);
        if (!entry) {
          yield* this.initializeSocketRpc(ws);
          entry = this.socketTransports.get(ws);
        }
        if (entry) yield* entry.transport.receive(message);
      }.bind(this),
    );
  }

  private initializeSocketRpc(
    ws: WebSocket,
    restored: readonly PersistedSubscription[] = [],
  ): Effect.Effect<void, unknown> {
    if (this.socketTransports.has(ws)) return Effect.void;
    return Effect.gen(
      function* (this: DurableObjectWebSocket<Env, Q, M>) {
        const scope = yield* Scope.make();
        const transport = yield* makeCloudflareRpcServerTransport(ws);
        const handlers = yield* WebSocketRpc.toHandlers({
          subscribe: (payload, { requestId, headers }) =>
            this.registry
              .subscribeStream(ws, {
                requestId,
                query: payload.query,
                params: payload.params,
                headers: Object.entries(headers),
              })
              .pipe(
                Stream.mapError((error) =>
                  error instanceof UnknownQueryError ? error : toRpcOperationError(error),
                ),
              ),
          unsubscribe: (payload) =>
            this.registry
              .unsubscribe(ws, payload.topic)
              .pipe(Effect.mapError((error) => toRpcOperationError(error))),
          sync: (payload) =>
            this.engine
              .sync(
                payload.mutation as StringKey<M>,
                payload.params as OperationParams<M[StringKey<M>]>,
              )
              .pipe(
                Effect.mapError((error) =>
                  error instanceof UnknownMutationError ? error : toRpcOperationError(error),
                ),
              ),
        });
        const server = RpcServer.make(WebSocketRpc).pipe(
          Effect.provide(handlers),
          Effect.provideService(RpcServer.Protocol, transport.protocol),
          Effect.provideService(RpcSerialization.RpcSerialization, RpcSerialization.json),
          Scope.provide(scope),
        );
        yield* Effect.forkIn(server, scope);
        this.socketTransports.set(ws, { scope, transport });

        const parser = RpcSerialization.json.makeUnsafe();
        for (const session of restored) {
          const encoded = parser.encode({
            _tag: "Request",
            id: session.requestId,
            tag: "subscribe",
            payload: { query: session.query, params: session.params },
            headers: session.headers,
          });
          if (encoded !== undefined) {
            yield* transport.receive(
              typeof encoded === "string" ? encoded : new TextDecoder().decode(encoded),
            );
          }
        }
      }.bind(this),
    );
  }

  private socketCleanupEffect(
    ws: WebSocket,
    exit: Exit.Exit<unknown, unknown>,
  ): Effect.Effect<void, unknown> {
    return Effect.gen(
      function* (this: DurableObjectWebSocket<Env, Q, M>) {
        yield* Effect.tryPromise({
          try: () => this.initialization,
          catch: (cause) => cause,
        });
        const entry = this.socketTransports.get(ws);
        if (entry) {
          yield* entry.transport.disconnect;
          yield* Scope.close(entry.scope, exit);
          this.socketTransports.delete(ws);
        }
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
}
