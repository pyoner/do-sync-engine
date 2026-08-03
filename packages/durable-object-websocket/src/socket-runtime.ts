import { Effect, Exit, Scope, Stream } from "effect";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { ListenerIdSchema, UnknownMutationError, UnknownQueryError } from "@do-sync-engine/core";
import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { RpcOperationError, WebSocketRpc } from "./protocol.ts";
import { makeCloudflareRpcServerTransport } from "./server-transport.ts";
import type { CloudflareRpcServerTransport } from "./server-transport.ts";
import { SubscriptionRegistry } from "./subscriptions.ts";
import type { PersistedSubscription } from "./subscriptions.ts";

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

type SocketTransport = {
  readonly scope: Scope.Closeable;
  readonly transport: CloudflareRpcServerTransport;
};

export class SocketRuntime<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  private readonly registry: SubscriptionRegistry<Q, M>;
  private pending: Promise<void> = Promise.resolve();
  private socketTransport: SocketTransport | undefined;

  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<Q, M>,
  ) {
    this.registry = new SubscriptionRegistry(engine);
  }

  start(): Promise<void> {
    return this.enqueue(
      Effect.gen({ self: this }, function* (this: SocketRuntime<Q, M>) {
        if (this.socketTransport) return;
        const restored = yield* this.registry.restore(this.socket);
        yield* this.initializeRpc(restored);
      }),
    );
  }

  receive(message: string | ArrayBuffer): Promise<void> {
    return this.enqueue(
      Effect.gen({ self: this }, function* (this: SocketRuntime<Q, M>) {
        yield* this.initializeRpc();
        if (this.socketTransport) yield* this.socketTransport.transport.receive(message);
      }),
    );
  }

  close(exit: Exit.Exit<unknown, unknown>): Promise<void> {
    return this.enqueue(
      Effect.gen({ self: this }, function* (this: SocketRuntime<Q, M>) {
        if (this.socketTransport) {
          yield* this.socketTransport.transport.disconnect;
          yield* Scope.close(this.socketTransport.scope, exit);
          this.socketTransport = undefined;
        }
        yield* this.registry.clear(this.socket);
      }),
    );
  }

  private initializeRpc(
    restored: readonly PersistedSubscription[] = [],
  ): Effect.Effect<void, unknown> {
    if (this.socketTransport) return Effect.void;
    return Effect.gen({ self: this }, function* (this: SocketRuntime<Q, M>) {
      const restoredListenerIds = new Map(
        restored.map((session) => [session.requestId, session.listenerId]),
      );
      const scope = yield* Scope.make();
      const transport = yield* makeCloudflareRpcServerTransport(this.socket);
      const handlers = yield* WebSocketRpc.toHandlers({
        subscribe: (payload, { requestId, headers }) =>
          this.registry
            .subscribeStream(this.socket, {
              requestId,
              listenerId:
                restoredListenerIds.get(requestId) ?? ListenerIdSchema.make(crypto.randomUUID()),
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
            .unsubscribe(this.socket, payload.listenerId)
            .pipe(Effect.mapError(toRpcOperationError)),
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
      this.socketTransport = { scope, transport };

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
    });
  }

  private enqueue(workflow: Effect.Effect<void, unknown>): Promise<void> {
    const current = this.pending.then(() => Effect.runPromise(workflow));
    this.pending = current.catch(() => undefined);
    return current;
  }
}
