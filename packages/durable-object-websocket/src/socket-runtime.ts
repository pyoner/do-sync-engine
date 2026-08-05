import { Effect, Exit, Scope, Schema, Stream } from "effect";
import { RpcSerialization, RpcServer } from "effect/unstable/rpc";
import { UnknownMutationError, UnknownQueryError } from "@do-sync-engine/core";
import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { SessionReadyFrame, RpcOperationError, WebSocketRpc } from "./protocol";
import { makeCloudflareRpcServerTransport } from "./server-transport";
import type { CloudflareRpcServerTransport } from "./server-transport";
import { SocketSubscriptions } from "./subscriptions";

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
  private readonly subscriptions: SocketSubscriptions<Q>;
  private pending: Promise<void> = Promise.resolve();
  private socketTransport: SocketTransport | undefined;

  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<Q, M>,
  ) {
    this.subscriptions = new SocketSubscriptions<Q>(socket, engine);
  }

  start(restored = false): Promise<void> {
    return this.enqueue(
      Effect.gen({ self: this }, function* (this: SocketRuntime<Q, M>) {
        yield* this.subscriptions.restore();
        yield* this.initializeRpc();
        if (this.socket.readyState === WebSocket.OPEN)
          this.socket.send(
            Schema.encodeSync(SessionReadyFrame)({ _tag: "DoSyncEngineSessionReady", restored }),
          );
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
        yield* this.subscriptions.close();
      }),
    );
  }

  private initializeRpc(): Effect.Effect<void, unknown> {
    if (this.socketTransport) return Effect.void;
    return Effect.gen({ self: this }, function* (this: SocketRuntime<Q, M>) {
      const scope = yield* Scope.make();
      const transport = yield* makeCloudflareRpcServerTransport(this.socket);
      const handlers = yield* WebSocketRpc.toHandlers({
        subscribe: (topic) =>
          this.subscriptions
            .subscribe(topic)
            .pipe(
              Stream.mapError((error) =>
                error instanceof UnknownQueryError ? error : toRpcOperationError(error),
              ),
            ),
        unsubscribe: (topic) =>
          this.subscriptions.unsubscribe(topic).pipe(Effect.mapError(toRpcOperationError)),
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
    });
  }

  private enqueue(workflow: Effect.Effect<void, unknown>): Promise<void> {
    const current = this.pending.then(() => Effect.runPromise(workflow));
    this.pending = current.catch(() => undefined);
    return current;
  }
}
