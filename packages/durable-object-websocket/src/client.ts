import { Effect, Exit, Scope } from "effect";
import { RpcClient, RpcGroup, RpcMessage, RpcSerialization } from "effect/unstable/rpc";
import type { Rpc } from "effect/unstable/rpc";
import { WebSocketRpc } from "./protocol.ts";

export type WebSocketRpcClient = RpcClient.RpcClient<RpcGroup.Rpcs<typeof WebSocketRpc>>;

export const makeWebSocketRpcClientFor = <Rpcs extends Rpc.Any>(
  socket: WebSocket,
  group: RpcGroup.RpcGroup<Rpcs>,
): Effect.Effect<RpcClient.RpcClient<Rpcs>, never, Scope.Scope> =>
  Effect.gen(function* () {
    const scope = yield* Effect.scope;
    const parser = RpcSerialization.json.makeUnsafe();
    let listener: ((event: MessageEvent) => void) | undefined;
    let close: (() => void) | undefined;
    const interrupt = () => {
      try {
        Effect.runFork(Scope.close(scope, Exit.interrupt()));
      } catch {
        // The callback must not leak parser or listener failures.
      }
    };
    const protocol = yield* RpcClient.Protocol.make((write, clientIds) =>
      Effect.sync(() => {
        listener = (event) => {
          try {
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
      Effect.sync(() => {
        if (listener) socket.removeEventListener("message", listener);
        if (close) {
          socket.removeEventListener("close", close);
          socket.removeEventListener("error", close);
        }
        socket.close();
      }),
    );
    return yield* RpcClient.make(group).pipe(
      Effect.provideService(RpcClient.Protocol, protocol),
      Effect.provideService(RpcSerialization.RpcSerialization, RpcSerialization.json),
    );
  });

export const makeWebSocketRpcClient = (socket: WebSocket) =>
  makeWebSocketRpcClientFor(socket, WebSocketRpc);
