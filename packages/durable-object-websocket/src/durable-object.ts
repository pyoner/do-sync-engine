import type {
  Listener,
  ListenerEvent,
  MutationMap,
  OperationParams,
  OperationResult,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  Topic,
} from "@do-sync-engine/core";
import { DurableObject } from "cloudflare:workers";
import { dequal } from "dequal";
import * as errore from "errore";
import { handleRpc } from "typed-rpc/server";
import {
  decodeMessage,
  InvalidRpcParamsError,
  isRecord,
  isTopic,
  WebSocketTransportError,
} from "./protocol";
import type { QueryTopic, ServerAPI } from "./protocol";
type EngineTopic<Queries extends QueryMap<Queries>> = Topic<
  StringKey<Queries>,
  OperationParams<Queries[StringKey<Queries>]>
>;
type EngineEvent<Queries extends QueryMap<Queries>> = ListenerEvent<
  StringKey<Queries>,
  OperationParams<Queries[StringKey<Queries>]>,
  OperationResult<Queries[StringKey<Queries>]>
>;
type EngineListener<Queries extends QueryMap<Queries>> = Listener<EngineEvent<Queries>>;
type Registration<Queries extends QueryMap<Queries>> = {
  readonly topic: EngineTopic<Queries>;
  readonly listener: EngineListener<Queries>;
  latest: EngineEvent<Queries> | null;
};
type Connection<Queries extends QueryMap<Queries>, Mutations extends MutationMap<Mutations>> = {
  readonly api: ServerAPI<Queries, Mutations>;
  readonly registrations: Array<Registration<Queries>>;
  syncing: boolean;
};

class InvalidAttachmentError extends errore.createTaggedError({
  name: "InvalidAttachmentError",
  message: "Invalid WebSocket subscription attachment",
}) {}

export abstract class DurableObjectWebSocket<
  Env,
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> extends DurableObject<Env> {
  readonly #connections = new Map<WebSocket, Connection<Queries, Mutations>>();
  readonly #engine: SyncEngineInterface<Queries, Mutations>;

  protected constructor(
    ctx: DurableObjectState,
    env: Env,
    initialize: () => SyncEngineInterface<Queries, Mutations>,
  ) {
    super(ctx, env);
    this.#engine = initialize();
    for (const webSocket of ctx.getWebSockets()) this.#restore(webSocket);
  }

  fetch(request: Request): Response {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("This endpoint only accepts WebSocket requests.", { status: 400 });
    }

    const pair = new WebSocketPair();
    const client = pair[0];
    const server = pair[1];
    this.ctx.acceptWebSocket(server);
    this.#connection(server);
    const persisted = this.#persist(server);
    if (!(persisted instanceof Error)) {
      return new Response(null, { status: 101, webSocket: client });
    }

    console.error(persisted.message, persisted);
    this.#clear(server);
    server.close(1011, "Failed to initialize WebSocket");
    return new Response("Failed to initialize WebSocket.", { status: 500 });
  }

  async webSocketMessage(webSocket: WebSocket, message: string | ArrayBuffer): Promise<void> {
    const request = decodeMessage(message);
    if (request instanceof Error) {
      const sent = this.#send(webSocket, {
        jsonrpc: "2.0",
        id: null,
        error: { code: -32700, message: "Parse error" },
      });
      if (sent instanceof Error) console.warn(sent.message, sent);
      return;
    }

    const api = this.#connection(webSocket).api;
    const service = {
      subscribe: (topic: QueryTopic<Queries>) => this.#rpcResult(api.subscribe(topic)),
      unsubscribe: (topic: QueryTopic<Queries>) => this.#rpcResult(api.unsubscribe(topic)),
      sync: (
        name: StringKey<Mutations>,
        params: OperationParams<Mutations[StringKey<Mutations>]>,
      ) => this.#rpcResult(api.sync(name, params)),
    };
    const response = await handleRpc<typeof service, unknown>(request, service, {
      onError: (error) => console.error("WebSocket RPC method failed", error),
    });
    const sent = this.#send(webSocket, response);
    if (sent instanceof Error) {
      console.warn(sent.message, sent);
      this.#clear(webSocket);
    }
  }

  webSocketClose(webSocket: WebSocket, _code: number, _reason: string, _wasClean: boolean): void {
    this.#clear(webSocket);
  }
  #rpcResult(result: void | Error): null {
    if (result instanceof Error) throw result;
    return null;
  }

  webSocketError(webSocket: WebSocket, error: unknown): void {
    console.warn("WebSocket failed", error);
    this.#clear(webSocket);
  }

  #connection(webSocket: WebSocket): Connection<Queries, Mutations> {
    const existing = this.#connections.get(webSocket);
    if (existing !== undefined) return existing;

    const connection: Connection<Queries, Mutations> = {
      api: {
        subscribe: (topic) => this.#subscribe(webSocket, topic),
        unsubscribe: (topic) => this.#unsubscribe(webSocket, topic),
        sync: (name, params) => this.#sync(name, params),
      },
      registrations: [],
      syncing: false,
    };
    this.#connections.set(webSocket, connection);
    return connection;
  }

  #createTopic(value: unknown): EngineTopic<Queries> | Error {
    if (!isTopic(value)) return new InvalidRpcParamsError({ method: "topic" });
    return this.#engine.createTopic(
      value.name as StringKey<Queries>,
      value.params as OperationParams<Queries[StringKey<Queries>]>,
    );
  }

  #subscribe(webSocket: WebSocket, value: unknown): void | Error {
    const topic = this.#createTopic(value);
    if (topic instanceof Error) return topic;

    const connection = this.#connection(webSocket);
    const existing = connection.registrations.find((item) => dequal(item.topic, topic));
    if (existing !== undefined) return;

    let active = false;
    const registration: Registration<Queries> = {
      topic,
      listener: (event) => {
        registration.latest = event;
        if (active && !connection.syncing) this.#deliver(webSocket, event);
      },
      latest: null,
    };
    const subscribed = this.#engine.subscribe<StringKey<Queries>>(topic, registration.listener);
    if (subscribed instanceof Error) return subscribed;
    connection.registrations.push(registration);
    const persisted = this.#persist(webSocket);
    if (persisted instanceof Error) {
      this.#remove(connection, registration);
      return persisted;
    }

    active = true;
    if (registration.latest === null) return;
    const delivered = this.#send(webSocket, registration.latest);
    if (!(delivered instanceof Error)) return;

    this.#remove(connection, registration);
    const rolledBack = this.#persist(webSocket);
    if (rolledBack instanceof Error) console.warn(rolledBack.message, rolledBack);
    return delivered;
  }

  #unsubscribe(webSocket: WebSocket, value: unknown): void | Error {
    const topic = this.#createTopic(value);
    if (topic instanceof Error) return topic;
    const connection = this.#connection(webSocket);
    const registration = connection.registrations.find((item) => dequal(item.topic, topic));
    if (registration === undefined) return;
    const persisted = this.#persistTopics(
      webSocket,
      connection.registrations.filter((item) => item !== registration).map((item) => item.topic),
    );
    if (persisted instanceof Error) return persisted;
    this.#remove(connection, registration);
  }

  #sync(name: unknown, params: unknown): void | Error {
    if (typeof name !== "string" || !Array.isArray(params)) {
      return new InvalidRpcParamsError({ method: "sync" });
    }
    for (const connection of this.#connections.values()) connection.syncing = true;
    const result = this.#engine.sync(
      name as StringKey<Mutations>,
      params as OperationParams<Mutations[StringKey<Mutations>]>,
    );
    for (const [webSocket, connection] of this.#connections) {
      connection.syncing = false;
      if (result instanceof Error) continue;
      const topics = connection.registrations
        .map((registration) => registration.latest)
        .filter((event): event is EngineEvent<Queries> => event !== null);
      const sent = this.#send(webSocket, {
        jsonrpc: "2.0",
        method: "synced",
        params: [topics],
      });
      if (sent instanceof Error) console.warn(sent.message, sent);
    }
    return result;
  }

  #restore(webSocket: WebSocket): void {
    const connection = this.#connection(webSocket);
    const topics = this.#readTopics(webSocket);
    if (topics instanceof Error) {
      console.warn(topics.message, topics);
      const persisted = this.#persist(webSocket);
      if (persisted instanceof Error) console.warn(persisted.message, persisted);
      return;
    }

    for (const value of topics) {
      const topic = this.#createTopic(value);
      if (topic instanceof Error) {
        console.warn(topic.message, topic);
        continue;
      }
      if (connection.registrations.some((item) => dequal(item.topic, topic))) continue;

      const state = { restoring: true };
      const registration: Registration<Queries> = {
        topic,
        listener: (event) => {
          registration.latest = event;
          if (!state.restoring && !connection.syncing) this.#deliver(webSocket, event);
        },
        latest: null,
      };
      const subscribed = this.#engine.subscribe<StringKey<Queries>>(topic, registration.listener);
      state.restoring = false;
      if (subscribed instanceof Error) {
        console.warn(subscribed.message, subscribed);
        continue;
      }
      connection.registrations.push(registration);
    }

    const persisted = this.#persist(webSocket);
    if (persisted instanceof Error) console.warn(persisted.message, persisted);
  }

  #readTopics(webSocket: WebSocket): unknown[] | Error {
    const attachment = errore.try({
      try: () => webSocket.deserializeAttachment() as unknown,
      catch: (cause) =>
        new WebSocketTransportError({ operation: "deserialize subscriptions", cause }),
    });
    if (attachment instanceof Error) return attachment;
    if (attachment === null) return [];
    if (!isRecord(attachment) || !Array.isArray(attachment.topics)) {
      return new InvalidAttachmentError();
    }
    return attachment.topics;
  }

  #persist(webSocket: WebSocket): void | Error {
    const registrations = this.#connections.get(webSocket)?.registrations ?? [];
    return this.#persistTopics(
      webSocket,
      registrations.map((registration) => registration.topic),
    );
  }

  #persistTopics(webSocket: WebSocket, topics: ReadonlyArray<EngineTopic<Queries>>): void | Error {
    return errore.try({
      try: () => webSocket.serializeAttachment({ topics }),
      catch: (cause) =>
        new WebSocketTransportError({ operation: "serialize subscriptions", cause }),
    });
  }

  #deliver(webSocket: WebSocket, event: EngineEvent<Queries>): void {
    const sent = this.#send(webSocket, event);
    if (!(sent instanceof Error)) return;
    console.warn(sent.message, sent);
    this.#clear(webSocket);
  }

  #send(webSocket: WebSocket, message: unknown): void | Error {
    if (webSocket.readyState !== WebSocket.OPEN) {
      return new WebSocketTransportError({ operation: "send on a closed socket" });
    }
    const serialized = errore.try({
      try: () => JSON.stringify(message),
      catch: (cause) => new WebSocketTransportError({ operation: "serialize message", cause }),
    });
    if (serialized instanceof Error) return serialized;
    if (serialized === undefined) {
      return new WebSocketTransportError({ operation: "serialize message" });
    }
    return errore.try({
      try: () => webSocket.send(serialized),
      catch: (cause) => new WebSocketTransportError({ operation: "send message", cause }),
    });
  }

  #remove(connection: Connection<Queries, Mutations>, registration: Registration<Queries>): void {
    this.#engine.unsubscribe<StringKey<Queries>>(registration.topic, registration.listener);
    const index = connection.registrations.indexOf(registration);
    if (index !== -1) connection.registrations.splice(index, 1);
  }

  #clear(webSocket: WebSocket): void {
    const connection = this.#connections.get(webSocket);
    if (connection === undefined) return;
    while (connection.registrations.length > 0) {
      const registration = connection.registrations.pop()!;
      this.#engine.unsubscribe<StringKey<Queries>>(registration.topic, registration.listener);
    }
    this.#connections.delete(webSocket);
  }
}
