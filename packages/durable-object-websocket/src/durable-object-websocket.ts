import { DurableObject } from "cloudflare:workers";
import type { MutationMap, QueryMap, SyncEngineInterface } from "@do-sync-engine/core";
import * as errore from "errore";
import { handleRpc, isJsonRpcRequest } from "typed-rpc/server";
import { SocketService } from "./service";
import { WebsocketStorage } from "./websocket-storage";

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap,
  M extends MutationMap,
> extends DurableObject<Env> {
  readonly #engine: SyncEngineInterface<WebSocket, Q, M>;
  readonly #topicKey: string;
  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<WebSocket, Q, M>,
    topicKey = "topics",
  ) {
    super(ctx, env);
    this.#engine = initialize();
    this.#topicKey = topicKey;
    for (const socket of ctx.getWebSockets()) {
      SocketService.restore(socket, this.#engine, this.#storage(socket), this.#topicKey);
    }
  }

  #storage(socket: WebSocket): WebsocketStorage {
    return new WebsocketStorage(socket);
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }
    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") return;
    const request = errore.try({
      try: () => JSON.parse(message) as unknown,
      catch: (cause) => new Error("Failed to parse RPC message", { cause }),
    });
    if (request instanceof Error) {
      socket.send(
        JSON.stringify({
          jsonrpc: "2.0",
          id: null,
          error: { code: -32700, message: "Parse error" },
        }),
      );
      return;
    }
    const notification = isJsonRpcRequest(request) && !("id" in request);
    const response = await handleRpc(
      request,
      new SocketService(socket, this.#engine, this.#storage(socket), this.#topicKey),
      { onError: (error) => console.error("WebSocket RPC method failed", error) },
    );
    if (!notification) socket.send(JSON.stringify(response));
  }

  webSocketClose(socket: WebSocket, code: number, reason: string): void {
    this.#storage(socket).remove(this.#topicKey);
    if (code !== 1005 && code !== 1006) socket.close(code, reason);
  }

  webSocketError(socket: WebSocket, error: unknown): void {
    console.warn("WebSocket failed", error);
    this.#storage(socket).remove(this.#topicKey);
  }
}
