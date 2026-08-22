import { DurableObject } from "cloudflare:workers";
import type { MutationMap, QueryMap, SyncEngineInterface } from "@do-sync-engine/core";
import * as errore from "errore";
import type { QueryTopic, ServiceFactory, ServiceSession } from "./service";
import { createService } from "./service";

const decoder = new TextDecoder();

export abstract class DurableObjectWebSocket<
  Env,
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> extends DurableObject<Env> {
  readonly #sessions = new Map<WebSocket, ServiceSession>();
  readonly #createSession: ServiceFactory<Queries>;

  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<Queries, Mutations>,
  ) {
    super(ctx, env);
    this.#createSession = createService(initialize());
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
    const session = this.#sessions.get(socket);
    if (session === undefined) return;

    const decoded = this.#decode(message);
    if (decoded instanceof Error) {
      const sent = this.#send(socket, {
        jsonrpc: "2.0",
        id: null,
        error: { code: -32700, message: "Parse error" },
      });
      if (sent instanceof Error) this.#disconnect(socket, sent, "WebSocket transport failed");
      return;
    }

    const response = await session
      .handle(decoded.value)
      .catch((cause) => new Error("Failed to handle WebSocket RPC request", { cause }));
    if (response instanceof Error) {
      this.#disconnect(socket, response, "WebSocket transport failed");
      return;
    }
    if (response === null) return;

    const sent = this.#send(socket, response);
    if (sent instanceof Error) this.#disconnect(socket, sent, "WebSocket transport failed");
  }

  webSocketClose(socket: WebSocket): void {
    this.#close(socket);
  }

  webSocketError(socket: WebSocket, error: unknown): void {
    console.warn("WebSocket failed", error);
    this.#close(socket);
  }

  #connect(socket: WebSocket, topics: readonly unknown[]): void | Error {
    const session = this.#createSession({
      topics,
      persist: (values) => this.#persist(socket, values),
      notify: (request) => this.#send(socket, request),
      fail: (error) => this.#disconnect(socket, error, "WebSocket transport failed"),
    });
    if (session instanceof Error) return session;
    this.#sessions.set(socket, session);
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

  #decode(message: string | ArrayBuffer): Error | { readonly value: unknown } {
    const text = typeof message === "string" ? message : decoder.decode(message);
    return errore.try({
      try: () => ({ value: JSON.parse(text) as unknown }),
      catch: (cause) => new Error("Failed to parse RPC message", { cause }),
    });
  }

  #persist(socket: WebSocket, topics: ReadonlyArray<QueryTopic<Queries>>): void | Error {
    return errore.try({
      try: () => socket.serializeAttachment(topics),
      catch: (cause) => new Error("Failed to serialize attachment", { cause }),
    });
  }

  #send(socket: WebSocket, message: unknown): void | Error {
    const text = errore.try({
      try: () => JSON.stringify(message),
      catch: (cause) => new Error("Failed to serialize RPC message", { cause }),
    });
    if (text instanceof Error) return text;
    if (text === undefined) return new Error("Failed to serialize RPC message");
    return errore.try({
      try: () => socket.send(text),
      catch: (cause) => new Error("WebSocket transport failed", { cause }),
    });
  }

  #close(socket: WebSocket): void {
    const session = this.#sessions.get(socket);
    if (session === undefined) return;
    this.#sessions.delete(socket);
    session.close();
  }

  #disconnect(socket: WebSocket, error: Error, reason: string): void {
    console.warn(error.message, error);
    this.#close(socket);
    const closed = errore.try({
      try: () => socket.close(1011, reason),
      catch: (cause) => new Error("Failed to close WebSocket", { cause }),
    });
    if (closed instanceof Error) console.warn(closed.message, closed);
  }
}
