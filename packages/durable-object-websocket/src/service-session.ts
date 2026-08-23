import type {
  Listener,
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
} from "@do-sync-engine/core";
import { dequal } from "dequal";
import * as errore from "errore";
import { handleRpc, isJsonRpcRequest } from "typed-rpc/server";
import { SYNC_METHOD, type QueryTopic, type Service, type SubscriptionEvent } from "./service";

const decoder = new TextDecoder();

export type ServiceSessionAdapter<Queries extends QueryMap<Queries>> = {
  readonly topics: readonly unknown[];
  persist(topics: ReadonlyArray<QueryTopic<Queries>>): void | Error;
  send(message: string): void | Error;
  fail(error: Error): void;
};

type Registration<Queries extends QueryMap<Queries>> = {
  readonly topic: QueryTopic<Queries>;
  readonly listener: Listener<SubscriptionEvent<Queries>>;
  active: boolean;
  initial: SubscriptionEvent<Queries> | null;
};

type RpcService<Queries extends QueryMap<Queries>, Mutations extends MutationMap<Mutations>> = {
  subscribe(topic: QueryTopic<Queries>): null;
  unsubscribe(topic: QueryTopic<Queries>): null;
  sync(name: StringKey<Mutations>, params: OperationParams<Mutations[StringKey<Mutations>]>): null;
};
type Session<Queries extends QueryMap<Queries>, Mutations extends MutationMap<Mutations>> = {
  readonly connection: object;
  readonly adapter: ServiceSessionAdapter<Queries>;
  readonly registrations: Array<Registration<Queries>>;
  readonly service: Service<Queries, Mutations>;
  readonly rpc: RpcService<Queries, Mutations>;
  handle(message: string | ArrayBuffer): Promise<void>;
  pending: Array<SubscriptionEvent<Queries>>;
  closed: boolean;
};
export type ServiceSessions<Queries extends QueryMap<Queries>> = {
  has(connection: object): boolean;
  handle(connection: object, message: string | ArrayBuffer): Promise<void>;
  connect(connection: object, adapter: ServiceSessionAdapter<Queries>): void | Error;
  close(connection: object): void;
};

type TopicValue = {
  readonly name: string;
  readonly params: readonly unknown[];
};

function isTopic(value: unknown): value is TopicValue {
  return (
    typeof value === "object" &&
    value !== null &&
    !Array.isArray(value) &&
    "name" in value &&
    typeof value.name === "string" &&
    "params" in value &&
    Array.isArray(value.params)
  );
}

function rpcResult(result: void | Error): null {
  if (result instanceof Error) throw result;
  return null;
}

export function createServiceSessions<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
>(engine: SyncEngineInterface<Queries, Mutations>): ServiceSessions<Queries> {
  const sessions = new Map<object, Session<Queries, Mutations>>();

  const createTopic = (value: unknown): QueryTopic<Queries> | Error => {
    if (!isTopic(value)) return new Error("Invalid topic");
    return engine.createTopic(
      value.name as StringKey<Queries>,
      value.params as OperationParams<Queries[StringKey<Queries>]>,
    ) as QueryTopic<Queries> | Error;
  };

  const close = (session: Session<Queries, Mutations>): void => {
    if (session.closed) return;
    session.closed = true;
    while (session.registrations.length > 0) {
      const registration = session.registrations.pop();
      if (registration === undefined) continue;
      engine.unsubscribe(registration.topic, registration.listener);
    }
    session.pending = [];
    sessions.delete(session.connection);
  };

  const send = (session: Session<Queries, Mutations>, message: unknown): void | Error => {
    const text = errore.try({
      try: () => JSON.stringify(message),
      catch: (cause) => new Error("Failed to serialize RPC message", { cause }),
    });
    if (text instanceof Error) {
      close(session);
      session.adapter.fail(text);
      return text;
    }
    if (text === undefined) {
      const error = new Error("Failed to serialize RPC message");
      close(session);
      session.adapter.fail(error);
      return error;
    }
    const sent = session.adapter.send(text);
    if (!(sent instanceof Error)) return;
    close(session);
    session.adapter.fail(sent);
    return sent;
  };

  const registration = (
    session: Session<Queries, Mutations>,
    topic: QueryTopic<Queries>,
  ): Registration<Queries> => {
    const item: Registration<Queries> = {
      topic,
      active: false,
      initial: null,
      listener: (event) => {
        if (!item.active) {
          item.initial = event;
          return;
        }
        session.pending.push(event);
      },
    };
    return item;
  };

  const detach = (session: Session<Queries, Mutations>, item: Registration<Queries>): void => {
    engine.unsubscribe(item.topic, item.listener);
    const index = session.registrations.indexOf(item);
    if (index !== -1) session.registrations.splice(index, 1);
  };

  const subscribe = (session: Session<Queries, Mutations>, value: unknown): void | Error => {
    const topic = createTopic(value);
    if (topic instanceof Error) return topic;
    if (session.registrations.some((item) => dequal(item.topic, topic))) return;

    const item = registration(session, topic);
    const subscribed = engine.subscribe(topic, item.listener);
    if (subscribed instanceof Error) return subscribed;

    const persisted = session.adapter.persist([
      ...session.registrations.map((registered) => registered.topic),
      topic,
    ]);
    if (persisted instanceof Error) {
      engine.unsubscribe(item.topic, item.listener);
      item.initial = null;
      return persisted;
    }

    session.registrations.push(item);
    item.active = true;
    const initial = item.initial;
    item.initial = null;
    if (initial === null) return;
    return send(session, { jsonrpc: "2.0", method: SYNC_METHOD, params: [initial] });
  };

  const unsubscribe = (session: Session<Queries, Mutations>, value: unknown): void | Error => {
    const topic = createTopic(value);
    if (topic instanceof Error) return topic;
    const item = session.registrations.find((registered) => dequal(registered.topic, topic));
    if (item === undefined) return;

    const persisted = session.adapter.persist(
      session.registrations
        .filter((registered) => registered !== item)
        .map((registered) => registered.topic),
    );
    if (persisted instanceof Error) return persisted;
    detach(session, item);
  };

  const sync = (name: unknown, params: unknown): void | Error => {
    if (typeof name !== "string" || !Array.isArray(params)) {
      return new Error("Invalid sync parameters");
    }

    for (const session of sessions.values()) session.pending = [];
    const result = engine.sync(
      name as StringKey<Mutations>,
      params as OperationParams<Mutations[StringKey<Mutations>]>,
    );
    for (const session of sessions.values()) {
      const events = session.pending;
      session.pending = [];
      if (events.length === 0) continue;
      const sent = send(session, { jsonrpc: "2.0", method: SYNC_METHOD, params: events });
      if (sent instanceof Error) console.warn("WebSocket notification failed", sent);
    }
    return result;
  };

  const restore = (session: Session<Queries, Mutations>): void | Error => {
    for (const value of session.adapter.topics) {
      const topic = createTopic(value);
      if (topic instanceof Error) {
        console.warn(topic.message, topic);
        continue;
      }
      if (session.registrations.some((item) => dequal(item.topic, topic))) continue;

      const item = registration(session, topic);
      const subscribed = engine.subscribe(topic, item.listener);
      if (subscribed instanceof Error) {
        console.warn(subscribed.message, subscribed);
        continue;
      }
      item.active = true;
      item.initial = null;
      session.registrations.push(item);
    }

    const persisted = session.adapter.persist(session.registrations.map((item) => item.topic));
    if (!(persisted instanceof Error)) return;
    close(session);
    return persisted;
  };

  const createSession = (
    connection: object,
    adapter: ServiceSessionAdapter<Queries>,
  ): Session<Queries, Mutations> => {
    const session: Session<Queries, Mutations> = {
      connection,
      adapter,
      registrations: [],
      pending: [],
      closed: false,
      service: {
        subscribe: (topic) => subscribe(session, topic),
        unsubscribe: (topic) => unsubscribe(session, topic),
        sync: (name, params) => sync(name, params),
      },
      rpc: {
        subscribe: (topic) => rpcResult(session.service.subscribe(topic)),
        unsubscribe: (topic) => rpcResult(session.service.unsubscribe(topic)),
        sync: (name, params) => rpcResult(session.service.sync(name, params)),
      },
      handle: async (message) => {
        const text = typeof message === "string" ? message : decoder.decode(message);
        const decoded = errore.try({
          try: () => JSON.parse(text) as unknown,
          catch: (cause) => new Error("Failed to parse RPC message", { cause }),
        });
        if (decoded instanceof Error) {
          send(session, {
            jsonrpc: "2.0",
            id: null,
            error: { code: -32700, message: "Parse error" },
          });
          return;
        }

        const notification = isJsonRpcRequest(decoded) && !("id" in decoded);
        const response = await handleRpc<RpcService<Queries, Mutations>, null>(
          decoded,
          session.rpc,
          { onError: (error) => console.error("WebSocket RPC method failed", error) },
        );
        if (notification || response === null || session.closed) return;
        send(session, response);
      },
    };
    return session;
  };

  return {
    has: (connection) => sessions.has(connection),
    connect: (connection, adapter) => {
      if (sessions.has(connection)) return new Error("Service session already connected");
      const session = createSession(connection, adapter);
      const restored = restore(session);
      if (restored instanceof Error) return restored;
      sessions.set(connection, session);
    },
    handle: async (connection, message) => {
      const session = sessions.get(connection);
      if (session === undefined) return;
      await session.handle(message);
    },
    close: (connection) => {
      const session = sessions.get(connection);
      if (session === undefined) return;
      close(session);
    },
  };
}
