import * as errore from "errore";
import type { RpcService } from "typed-rpc/server";
import {
  encodeRpcMessage,
  handleRpcFrame,
  type RestoredAttachment,
  type RpcBinding,
  type RpcSession,
  type RpcWireMessage,
} from "./rpc";

export abstract class DurableObjectWebSocket<Env, Service extends RpcService<Service, unknown>> {
  declare readonly [Rpc.__DURABLE_OBJECT_BRAND]: never;
  protected readonly ctx: DurableObjectState;
  protected readonly env: Env;
  readonly #binding: RpcBinding<Service>;
  readonly #sessions = new Map<WebSocket, RpcSession<Service>>();

  protected constructor(ctx: DurableObjectState, env: Env, binding: RpcBinding<Service>) {
    this.ctx = ctx;
    this.env = env;
    this.#binding = binding;
    for (const socket of ctx.getWebSockets()) this.#connect(socket);
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }

    const pair = new WebSocketPair();
    this.ctx.acceptWebSocket(pair[1]);
    const connected = this.#connect(pair[1]);
    if (connected instanceof Error) {
      pair[1].close(1011, "Failed to initialize WebSocket");
      return new Response("Failed to initialize WebSocket.", { status: 500 });
    }
    return new Response(null, { status: 101, webSocket: pair[0] });
  }

  async webSocketMessage(socket: WebSocket, raw: RpcWireMessage): Promise<void> {
    const session = this.#sessions.get(socket);
    if (session === undefined) return;

    const response = await handleRpcFrame({
      raw,
      service: session.service,
    });
    if (response instanceof Error) {
      this.#fail(socket, response);
      return;
    }
    if (response === null) return;

    const sent = this.#send(socket, response);
    if (sent instanceof Error) this.#fail(socket, sent);
  }

  webSocketClose(socket: WebSocket): void {
    this.#close(socket);
  }

  webSocketError(socket: WebSocket, error: unknown): void {
    console.warn("WebSocket failed", error);
    this.#close(socket);
  }

  #connect(socket: WebSocket): void | Error {
    const session = this.#binding.createSession({
      attachment: this.#attachment(socket),
      notify: (request) => {
        const message = encodeRpcMessage(request);
        if (message instanceof Error) return message;
        return this.#send(socket, message);
      },
      persist: (attachment) => this.#persist(socket, attachment),
      fail: (error) => this.#fail(socket, error),
    });
    this.#sessions.set(socket, session);

    const started = session.start();
    if (!(started instanceof Error)) return;
    this.#fail(socket, started);
    return started;
  }

  #attachment(socket: WebSocket): RestoredAttachment {
    return errore.try({
      try: () => ({ value: socket.deserializeAttachment() as unknown }),
      catch: (cause) => new Error("Failed to deserialize attachment", { cause }),
    });
  }

  #persist(socket: WebSocket, attachment: unknown): void | Error {
    return errore.try({
      try: () => socket.serializeAttachment(attachment),
      catch: (cause) => new Error("Failed to serialize attachment", { cause }),
    });
  }

  #send(socket: WebSocket, message: RpcWireMessage): void | Error {
    return errore.try({
      try: () => socket.send(message),
      catch: (cause) => new Error("WebSocket transport failed", { cause }),
    });
  }

  #fail(socket: WebSocket, error: Error): void {
    console.warn(error.message, error);
    this.#close(socket);
  }

  #close(socket: WebSocket): void {
    const session = this.#sessions.get(socket);
    if (session === undefined) return;
    session.close();
    this.#sessions.delete(socket);
  }
}
