import { Effect } from "effect";
import { Queue } from "effect";
import { RpcMessage, RpcSerialization, RpcServer } from "effect/unstable/rpc";

export interface CloudflareRpcServerTransport {
  readonly protocol: RpcServer.Protocol["Service"];
  readonly receive: (frame: string | ArrayBuffer) => Effect.Effect<void>;
  readonly disconnect: Effect.Effect<void>;
}

export const makeCloudflareRpcServerTransport: (
  socket: WebSocket,
) => Effect.Effect<CloudflareRpcServerTransport> = (socket) =>
  Effect.gen(function* () {
    const parser = RpcSerialization.json.makeUnsafe();
    const disconnects = yield* Queue.unbounded<number>();
    let writeRequest!: (
      clientId: number,
      message: RpcMessage.FromClientEncoded,
    ) => Effect.Effect<void>;

    const protocol = yield* RpcServer.Protocol.make((writeRequest_) => {
      writeRequest = writeRequest_;
      return Effect.succeed({
        disconnects,
        send: (_clientId, response) =>
          Effect.sync(() => {
            if (socket.readyState !== WebSocket.OPEN) return;
            const encoded = parser.encode(response);
            if (encoded !== undefined) socket.send(encoded);
          }),
        end: () => Effect.void,
        clientIds: Effect.succeed(new Set([0])),
        initialMessage: Effect.succeedNone,
        supportsAck: false,
        supportsTransferables: false,
        supportsSpanPropagation: true,
      });
    });

    const receive = (frame: string | ArrayBuffer): Effect.Effect<void> =>
      Effect.suspend(() => {
        try {
          const decoded = parser.decode(
            typeof frame === "string" ? frame : new Uint8Array(frame),
          ) as ReadonlyArray<RpcMessage.FromClientEncoded>;
          return Effect.forEach(decoded, (message) => writeRequest(0, message), { discard: true });
        } catch (cause) {
          return Effect.sync(() => {
            if (socket.readyState !== WebSocket.OPEN) return;
            const encoded = parser.encode(RpcMessage.ResponseDefectEncoded(cause));
            if (encoded !== undefined) socket.send(encoded);
          });
        }
      });

    return {
      protocol,
      receive,
      disconnect: Effect.asVoid(Queue.offer(disconnects, 0)),
    };
  });
