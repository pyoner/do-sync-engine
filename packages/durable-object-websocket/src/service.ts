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
import { dequal } from "dequal";
import type { JsonRpcRequest, JsonRpcResponse } from "typed-rpc";
import { handleRpc, isJsonRpcRequest } from "typed-rpc/server";

export type QueryTopic<Queries extends QueryMap<Queries>> = {
  [Name in StringKey<Queries>]: Topic<Name, OperationParams<Queries[Name]>>;
}[StringKey<Queries>];

export type SubscriptionEvent<Queries extends QueryMap<Queries>> = {
  [Name in StringKey<Queries>]: ListenerEvent<
    Name,
    OperationParams<Queries[Name]>,
    OperationResult<Queries[Name]>
  >;
}[StringKey<Queries>];

export interface Service<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> {
  subscribe(topic: QueryTopic<Queries>): void | Error;
  unsubscribe(topic: QueryTopic<Queries>): void | Error;
  sync<Name extends StringKey<Mutations>>(
    name: Name,
    params: OperationParams<Mutations[Name]>,
  ): void | Error;
}

export const SYNC_METHOD = "sync";

export type ServiceContext<Queries extends QueryMap<Queries>> = {
  readonly topics: readonly unknown[];
  persist(topics: ReadonlyArray<QueryTopic<Queries>>): void | Error;
  notify(request: JsonRpcRequest): void | Error;
  fail(error: Error): void;
};

type Registration<Queries extends QueryMap<Queries>> = {
  readonly topic: QueryTopic<Queries>;
  readonly listener: Listener<SubscriptionEvent<Queries>>;
  active: boolean;
  initial: SubscriptionEvent<Queries> | null;
};

type Session<Queries extends QueryMap<Queries>> = {
  readonly context: ServiceContext<Queries>;
  readonly registrations: Array<Registration<Queries>>;
  pending: Array<SubscriptionEvent<Queries>>;
  closed: boolean;
};

export type ServiceSession = {
  handle(request: unknown): Promise<JsonRpcResponse | null>;
  close(): void;
};

export type ServiceFactory<Queries extends QueryMap<Queries>> = (
  context: ServiceContext<Queries>,
) => ServiceSession | Error;

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

export function createService<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
>(engine: SyncEngineInterface<Queries, Mutations>): ServiceFactory<Queries> {
  const sessions = new Set<Session<Queries>>();

  const createTopic = (value: unknown): QueryTopic<Queries> | Error => {
    if (!isTopic(value)) return new Error("Invalid topic");
    return engine.createTopic(
      value.name as StringKey<Queries>,
      value.params as OperationParams<Queries[StringKey<Queries>]>,
    ) as QueryTopic<Queries> | Error;
  };

  const close = (session: Session<Queries>): void => {
    if (session.closed) return;
    session.closed = true;
    while (session.registrations.length > 0) {
      const registration = session.registrations.pop();
      if (registration === undefined) continue;
      engine.unsubscribe(registration.topic, registration.listener);
    }
    session.pending = [];
    sessions.delete(session);
  };

  const notify = (
    session: Session<Queries>,
    events: Array<SubscriptionEvent<Queries>>,
  ): void | Error => {
    if (events.length === 0) return;
    const sent = session.context.notify({
      jsonrpc: "2.0",
      method: SYNC_METHOD,
      params: events,
    });
    if (!(sent instanceof Error)) return;
    session.context.fail(sent);
    close(session);
    return sent;
  };

  const registration = (
    session: Session<Queries>,
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

  const detach = (session: Session<Queries>, item: Registration<Queries>): void => {
    engine.unsubscribe(item.topic, item.listener);
    const index = session.registrations.indexOf(item);
    if (index !== -1) session.registrations.splice(index, 1);
  };

  const subscribe = (session: Session<Queries>, value: unknown): void | Error => {
    const topic = createTopic(value);
    if (topic instanceof Error) return topic;
    if (session.registrations.some((item) => dequal(item.topic, topic))) return;

    const item = registration(session, topic);
    const subscribed = engine.subscribe(topic, item.listener);
    if (subscribed instanceof Error) return subscribed;

    const persisted = session.context.persist([
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
    return notify(session, [initial]);
  };

  const unsubscribe = (session: Session<Queries>, value: unknown): void | Error => {
    const topic = createTopic(value);
    if (topic instanceof Error) return topic;
    const item = session.registrations.find((registered) => dequal(registered.topic, topic));
    if (item === undefined) return;

    const persisted = session.context.persist(
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

    for (const session of sessions) session.pending = [];
    const result = engine.sync(
      name as StringKey<Mutations>,
      params as OperationParams<Mutations[StringKey<Mutations>]>,
    );
    for (const session of sessions) {
      const events = session.pending;
      session.pending = [];
      const sent = notify(session, events);
      if (sent instanceof Error) console.warn("WebSocket notification failed", sent);
    }
    return result;
  };

  const restore = (session: Session<Queries>): void | Error => {
    for (const value of session.context.topics) {
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

    const persisted = session.context.persist(session.registrations.map((item) => item.topic));
    if (!(persisted instanceof Error)) return;
    close(session);
    return persisted;
  };

  return (context: ServiceContext<Queries>): ServiceSession | Error => {
    const session: Session<Queries> = {
      context,
      registrations: [],
      pending: [],
      closed: false,
    };
    const restored = restore(session);
    if (restored instanceof Error) return restored;
    sessions.add(session);

    const service: Service<Queries, Mutations> = {
      subscribe: (topic) => subscribe(session, topic),
      unsubscribe: (topic) => unsubscribe(session, topic),
      sync: (name, params) => sync(name, params),
    };
    const rpc = {
      subscribe: (topic: QueryTopic<Queries>) => rpcResult(service.subscribe(topic)),
      unsubscribe: (topic: QueryTopic<Queries>) => rpcResult(service.unsubscribe(topic)),
      sync: (
        name: StringKey<Mutations>,
        params: OperationParams<Mutations[StringKey<Mutations>]>,
      ) => rpcResult(service.sync(name, params)),
    };

    return {
      handle: async (request) => {
        const notification = isJsonRpcRequest(request) && !("id" in request);
        const response = await handleRpc<typeof rpc, null>(request, rpc, {
          onError: (error) => console.error("WebSocket RPC method failed", error),
        });
        if (notification) return null;
        return response;
      },
      close: () => close(session),
    };
  };
}
