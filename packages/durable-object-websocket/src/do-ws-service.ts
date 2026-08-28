import { DurableObject } from "cloudflare:workers";
import type {
  QueryMap,
  MutationMap,
  Topic,
  StringKey,
  OperationParams,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { handleRpc, type JsonRpcRequest } from "typed-rpc/server";

function rpcResult<T>(result: T | Error): T {
  if (result instanceof Error) throw result;
  return result;
}

function syncNotification<T>(value: T): JsonRpcRequest {
  return { jsonrpc: "2.0", method: "sync", params: [value] } satisfies JsonRpcRequest;
}

export class Service<Queries extends QueryMap<Queries>, Mutations extends MutationMap<Mutations>> {
  constructor(
    private ws: WebSocket,
    private syncEngine: SyncEngineInterface<Queries, Mutations, WebSocket>,
  ) {}

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): null {
    const { name, params } = topic;
    const validTopic = rpcResult(this.syncEngine.createTopic(name, params));
    this.syncEngine.subscribe(
      validTopic,
      (event) => {
        const notification = syncNotification(event);
        this.ws.send(JSON.stringify(notification));
      },
      this.ws,
    );
    return null;
  }

  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): null {
    const { name, params } = topic;
    const validTopic = rpcResult(this.syncEngine.createTopic(name, params));
    this.syncEngine.unsubscribe(validTopic, this.ws);
    return null;
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): null {
    rpcResult(this.syncEngine.sync(mutation, params));
    return null;
  }
}

interface ConnectionState {
  orderId: string;
  joinedAt: number;
}

export abstract class WebSocketServer<
  Env,
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> extends DurableObject<Env> {
  constructor(
    ctx: DurableObjectState,
    env: Env,
    private syncEngine: SyncEngineInterface<Queries, Mutations, WebSocket>,
  ) {
    super(ctx, env);
  }

  async fetch(_request: Request): Promise<Response> {
    const webSocketPair = new WebSocketPair();
    const [client, server] = Object.values(webSocketPair);

    this.ctx.acceptWebSocket(server);

    // Persist per-connection state that survives hibernation
    // server.serializeAttachment(state);

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer) {
    if (typeof message !== "string") {
      return;
    }
    const m = JSON.parse(message);
    const service = new Service<Queries, Mutations>(ws, this.syncEngine);
    await handleRpc(m, service);
  }

  async webSocketClose(ws: WebSocket, code: number, reason: string, _wasClean: boolean) {
    const state = ws.deserializeAttachment() as ConnectionState;
    console.log(`${state.orderId} disconnected`);
    // With web_socket_auto_reply_to_close (compat date >= 2026-04-07), the runtime
    // auto-replies to Close frames. Calling close() is safe but no longer required.
    ws.close(code, reason);
  }
}
