import { Effect, Exit, Option, Queue, Scope, Schema, Stream } from "effect";
import { RpcClient, RpcGroup, RpcMessage, RpcSerialization } from "effect/unstable/rpc";
import type { Rpc } from "effect/unstable/rpc";
import { SessionReadyFrame } from "./protocol";

export interface WebSocketRpcSession<Rpcs extends Rpc.Any> {
  readonly client: RpcClient.RpcClient<Rpcs>;
  readonly restored: Stream.Stream<void>;
}

export const makeWebSocketRpcSessionFor = <Rpcs extends Rpc.Any>(
  socket: WebSocket,
  group: RpcGroup.RpcGroup<Rpcs>,
): Effect.Effect<WebSocketRpcSession<Rpcs>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const restorations = yield* Queue.unbounded<void>();
    const parser = RpcSerialization.json.makeUnsafe();
    let listener: ((event: MessageEvent) => void) | undefined;
    let close: (() => void) | undefined;
    const interrupt = () => {
      try {
        Effect.runFork(Scope.close(scope, Exit.interrupt()));
      } catch {
        // The event handler must not leak lifecycle failures.
      }
    };
    const protocol = yield* RpcClient.Protocol.make((write, clientIds) =>
      Effect.sync(() => {
        listener = (event) => {
          try {
            const sessionReady = Schema.decodeUnknownOption(SessionReadyFrame)(event.data);
            if (Option.isSome(sessionReady)) {
              if (sessionReady.value.restored) Effect.runFork(Queue.offer(restorations, undefined));
              return;
            }
            for (const message of parser.decode(event.data as string | Uint8Array))
              for (const clientId of clientIds)
                Effect.runFork(write(clientId, message as RpcMessage.FromServerEncoded));
          } catch {
            interrupt();
          }
        };
        close = interrupt;
        socket.addEventListener("message", listener);
        socket.addEventListener("close", close);
        socket.addEventListener("error", close);
        return {
          send: (_clientId: number, request: RpcMessage.FromClientEncoded) =>
            Effect.sync(() => {
              if (socket.readyState !== WebSocket.OPEN) return false;
              const encoded = parser.encode(request);
              if (encoded !== undefined) socket.send(encoded);
              return true;
            }).pipe(Effect.flatMap((sent) => (sent ? Effect.void : Effect.interrupt))),
          supportsAck: false,
          supportsTransferables: false,
        };
      }),
    );
    yield* Effect.addFinalizer(() =>
      Effect.gen(function* () {
        if (listener) socket.removeEventListener("message", listener);
        if (close) {
          socket.removeEventListener("close", close);
          socket.removeEventListener("error", close);
        }
        yield* Queue.shutdown(restorations);
        socket.close();
      }),
    );
    const client = yield* RpcClient.make(group).pipe(
      Effect.provideService(RpcClient.Protocol, protocol),
      Effect.provideService(RpcSerialization.RpcSerialization, RpcSerialization.json),
    );
    return { client, restored: Stream.fromQueue(restorations) };
  });
