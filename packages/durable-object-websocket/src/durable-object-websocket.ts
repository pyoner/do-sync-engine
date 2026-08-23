import { DurableObject } from "cloudflare:workers";
import type { MutationMap, QueryMap, SyncEngineInterface } from "@do-sync-engine/core";
import * as errore from "errore";
import type { QueryTopic } from "./service";
import {
  createServiceSessions,
  type ServiceSessionAdapter,
  type ServiceSessions,
} from "./service-session";

export abstract class DurableObjectWebSocket<
  Env,
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> extends DurableObject<Env> {
  readonly #sessions: ServiceSessions<Queries>;

  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<Queries, Mutations>,
  ) {
    super(ctx, env);
    this.#sessions = createServiceSessions(initialize());
    for (const socket of ctx.getWebSockets()) {
      const connected = this.#connect(socket, this.#topics(socket));
      if (connected instanceof Error) {
        this.#disconnect(socket, connected, "Failed to restore WebSocket");
      }
    }
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }

    const pair = new WebSocketPair();
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    const connected = this.#connect(server, []);
    if (connected instanceof Error) {
      this.#disconnect(server, connected, "Failed to initialize WebSocket");
      return new Response("Failed to initialize WebSocket.", { status: 500 });
    }
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async webSocketMessage(socket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    await this.#sessions.handle(socket, message);
  }

  webSocketClose(socket: WebSocket): void {
    this.#sessions.close(socket);
  }

  webSocketError(socket: WebSocket, error: unknown): void {
    console.warn("WebSocket failed", error);
    this.#sessions.close(socket);
  }

  #connect(socket: WebSocket, topics: readonly unknown[]): void | Error {
    const adapter: ServiceSessionAdapter<Queries> = {
      topics,
      persist: (values) => this.#persist(socket, values),
      send: (message) => this.#send(socket, message),
      fail: (error) => this.#disconnect(socket, error, "WebSocket transport failed"),
    };
    return this.#sessions.connect(socket, adapter);
  }

  #topics(socket: WebSocket): readonly unknown[] {
    const attachment = errore.try({
      try: () => socket.deserializeAttachment() as unknown,
      catch: (cause) => new Error("Failed to deserialize attachment", { cause }),
    });
    if (attachment instanceof Error) {
      console.warn(attachment.message, attachment);
      return [];
    }
    if (attachment === null) return [];
    if (Array.isArray(attachment)) return attachment;
    console.warn("Invalid WebSocket subscription attachment", attachment);
    return [];
  }

  #persist(socket: WebSocket, topics: ReadonlyArray<QueryTopic<Queries>>): void | Error {
    return errore.try({
      try: () => socket.serializeAttachment(topics),
      catch: (cause) => new Error("Failed to serialize attachment", { cause }),
    });
  }

  #send(socket: WebSocket, message: string): void | Error {
    return errore.try({
      try: () => socket.send(message),
      catch: (cause) => new Error("WebSocket transport failed", { cause }),
    });
  }

  #disconnect(socket: WebSocket, error: Error, reason: string): void {
    console.warn(error.message, error);
    this.#sessions.close(socket);
    const closed = errore.try({
      try: () => socket.close(1011, reason),
      catch: (cause) => new Error("Failed to close WebSocket", { cause }),
    });
    if (closed instanceof Error) console.warn(closed.message, closed);
  }
}
