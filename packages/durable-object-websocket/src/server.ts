import { Effect, Exit } from "effect";
import { DurableObject } from "cloudflare:workers";
import type { MutationMap, QueryMap, SyncEngineInterface } from "@do-sync-engine/core";
import { SocketRuntime } from "./socket-runtime.ts";

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> extends DurableObject<Env> {
  private readonly initialization: Promise<void>;
  private readonly sockets = new WeakMap<WebSocket, SocketRuntime<Q, M>>();
  private engine!: SyncEngineInterface<Q, M>;

  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<Q, M> | Promise<SyncEngineInterface<Q, M>>,
  ) {
    super(ctx, env);
    this.initialization = ctx.blockConcurrencyWhile(() =>
      Effect.runPromise(
        Effect.gen({ self: this }, function* (this: DurableObjectWebSocket<Env, Q, M>) {
          this.engine = yield* Effect.tryPromise({
            try: () => Promise.resolve(initialize()),
            catch: (cause) => cause,
          });
          for (const ws of ctx.getWebSockets())
            yield* Effect.tryPromise({
              try: () => this.runtime(ws).start(),
              catch: (cause) => cause,
            });
        }),
      ),
    );
  }

  fetch(request: Request): Promise<Response> {
    return Effect.runPromise(this.fetchEffect(request));
  }

  webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    return this.afterInitialization(() => this.runtime(ws).receive(message));
  }

  webSocketClose(ws: WebSocket, _code: number, _reason: string, _wasClean: boolean): Promise<void> {
    return this.closeSocket(ws, Exit.void);
  }

  webSocketError(ws: WebSocket, error: unknown): Promise<void> {
    return this.closeSocket(ws, Exit.die(error));
  }

  private closeSocket(ws: WebSocket, exit: Exit.Exit<unknown, unknown>): Promise<void> {
    return this.afterInitialization(async () => {
      await this.runtime(ws).close(exit);
      this.sockets.delete(ws);
    });
  }

  private fetchEffect(request: Request): Effect.Effect<Response, unknown> {
    return Effect.gen({ self: this }, function* (this: DurableObjectWebSocket<Env, Q, M>) {
      yield* Effect.tryPromise({
        try: () => this.initialization,
        catch: (cause) => cause,
      });
      if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket")
        return new Response("Expected WebSocket", { status: 426 });
      const pair = new WebSocketPair();
      this.ctx.acceptWebSocket(pair[1]);
      yield* Effect.tryPromise({
        try: () => this.runtime(pair[1]).start(),
        catch: (cause) => cause,
      });
      return new Response(null, { status: 101, webSocket: pair[0] });
    });
  }

  private runtime(ws: WebSocket): SocketRuntime<Q, M> {
    let runtime = this.sockets.get(ws);
    if (!runtime) {
      runtime = new SocketRuntime(ws, this.engine);
      this.sockets.set(ws, runtime);
    }
    return runtime;
  }

  private afterInitialization(work: () => Promise<void>): Promise<void> {
    return this.initialization.then(work);
  }
}
