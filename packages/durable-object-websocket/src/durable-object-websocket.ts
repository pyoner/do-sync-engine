import { DurableObject } from "cloudflare:workers";
import type { MutationRecord, QueryRecord, SyncEngineInterface } from "@do-sync-engine/core";
import { newWebSocketRpcSession } from "capnweb";
import { SocketService } from "./service";

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryRecord,
  M extends MutationRecord,
> extends DurableObject<Env> {
  readonly #engine: SyncEngineInterface<WebSocket, Q, M, Disposable>;

  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<WebSocket, Q, M, Disposable>,
  ) {
    super(ctx, env);
    this.#engine = initialize();
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }
    const pair = new WebSocketPair();
    const server = pair[1];
    server.accept();
    const service = new SocketService(this.#engine, server);
    const session = newWebSocketRpcSession(server, service);
    session.onRpcBroken(() => service[Symbol.dispose]());
    return new Response(null, { status: 101, webSocket: pair[0] });
  }
}
