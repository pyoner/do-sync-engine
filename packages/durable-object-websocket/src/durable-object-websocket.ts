import { DurableObject } from "cloudflare:workers";
import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";
import * as errore from "errore";
import { handleRpc, isJsonRpcRequest, type JsonRpcRequest } from "typed-rpc/server";
import { SYNC_METHOD } from "./service";

type StoredTopic<Q extends QueryMap<Q>> = Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;

function rpcResult<T>(result: T | Error): T {
  if (result instanceof Error) throw result;
  return result;
}
function syncNotification(event: unknown): JsonRpcRequest {
  return { jsonrpc: "2.0", method: SYNC_METHOD, params: [event] } satisfies JsonRpcRequest;
}
function topicKey(topic: unknown): string | undefined {
  return JSON.stringify(topic);
}

class SocketService<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<WebSocket, Q, M>,
  ) {}

  static restore<Q extends QueryMap<Q>, M extends MutationMap<M>>(
    socket: WebSocket,
    engine: SyncEngineInterface<WebSocket, Q, M>,
  ): void {
    const service = new SocketService(socket, engine);
    for (const topic of service.topics()) {
      const valid = engine.createTopic(topic.name, topic.params);
      if (valid instanceof Error) {
        console.warn("Failed to restore WebSocket subscription", valid);
        continue;
      }
      const result = service.listen(valid, false);
      if (result instanceof Error) console.warn("Failed to restore WebSocket subscription", result);
    }
  }

  subscribe<Name extends StringKey<Q>>(topic: Topic<Name, OperationParams<Q[Name]>>): null {
    const valid = rpcResult(this.engine.createTopic(topic.name, topic.params));
    const previous = this.topics();
    const key = topicKey(valid);
    if (previous.some((item) => topicKey(item) === key)) return null;
    this.socket.serializeAttachment([...previous, valid]);
    const result = this.listen(valid, true);
    if (!(result instanceof Error)) return null;
    this.engine.unsubscribe(valid, this.socket);
    this.socket.serializeAttachment(previous);
    throw result;
  }

  unsubscribe<Name extends StringKey<Q>>(topic: Topic<Name, OperationParams<Q[Name]>>): null {
    const valid = rpcResult(this.engine.createTopic(topic.name, topic.params));
    const key = topicKey(valid);
    this.socket.serializeAttachment(this.topics().filter((item) => topicKey(item) !== key));
    this.engine.unsubscribe(valid, this.socket);
    return null;
  }

  sync<Name extends StringKey<M>>(mutation: Name, params: OperationParams<M[Name]>): null {
    rpcResult(this.engine.sync(mutation, params));
    return null;
  }

  private listen<Name extends StringKey<Q>>(
    topic: Topic<Name, OperationParams<Q[Name]>>,
    notifyInitial: boolean,
  ): WebSocket | Error {
    let active = notifyInitial;
    const result = this.engine.subscribe(
      topic,
      (event) => {
        if (active) this.socket.send(JSON.stringify(syncNotification(event)));
      },
      this.socket,
    );
    active = true;
    return result;
  }

  private topics(): Array<StoredTopic<Q>> {
    const attachment = errore.try({
      try: () => this.socket.deserializeAttachment() as unknown,
      catch: (cause) => new Error("Failed to deserialize WebSocket subscriptions", { cause }),
    });
    if (attachment instanceof Error) {
      console.warn(attachment.message, attachment);
      return [];
    }
    if (attachment === null) return [];
    if (Array.isArray(attachment)) return attachment as Array<StoredTopic<Q>>;
    console.warn("Invalid WebSocket subscription attachment", attachment);
    return [];
  }
}

export abstract class DurableObjectWebSocket<
  Env,
  Q extends QueryMap<Q>,
  M extends MutationMap<M>,
> extends DurableObject<Env> {
  readonly #engine: SyncEngineInterface<WebSocket, Q, M>;
  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<WebSocket, Q, M>,
  ) {
    super(ctx, env);
    this.#engine = initialize();
    for (const socket of ctx.getWebSockets()) SocketService.restore(socket, this.#engine);
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
    const response = await handleRpc(request, new SocketService(socket, this.#engine), {
      onError: (error) => console.error("WebSocket RPC method failed", error),
    });
    if (!notification) socket.send(JSON.stringify(response));
  }
  webSocketClose(socket: WebSocket, code: number, reason: string): void {
    this.#engine.unsubscribe(socket);
    if (code !== 1005 && code !== 1006) socket.close(code, reason);
  }
  webSocketError(socket: WebSocket, error: unknown): void {
    console.warn("WebSocket failed", error);
    this.#engine.unsubscribe(socket);
  }
}
