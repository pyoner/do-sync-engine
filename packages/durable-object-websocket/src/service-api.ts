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
import type { JsonRpcRequest } from "typed-rpc";
import type { RpcSessionFactory } from "./rpc";

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

export interface ServiceAPI<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> {
  subscribe(topic: QueryTopic<Queries>): null;
  unsubscribe(topic: QueryTopic<Queries>): null;
  sync<Name extends StringKey<Mutations>>(
    name: Name,
    params: OperationParams<Mutations[Name]>,
  ): null;
}

export const LISTENER_EVENT_METHOD = "listener";

export function listenerNotification<Queries extends QueryMap<Queries>>(
  event: SubscriptionEvent<Queries>,
): JsonRpcRequest {
  return { jsonrpc: "2.0", method: LISTENER_EVENT_METHOD, params: [event] };
}

type Registration<Queries extends QueryMap<Queries>> = {
  readonly topic: QueryTopic<Queries>;
  readonly listener: Listener<SubscriptionEvent<Queries>>;
  latest: SubscriptionEvent<Queries> | null;
};

function rpcResult(result: void | Error): null {
  if (result instanceof Error) throw result;
  return null;
}

export function createServiceApiSession<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
>(
  initialize: () => SyncEngineInterface<Queries, Mutations>,
): RpcSessionFactory<ServiceAPI<Queries, Mutations>> {
  const engine = initialize();

  return (context) => {
    const registrations: Array<Registration<Queries>> = [];

    const close = (): void => {
      while (registrations.length > 0) {
        const registration = registrations.pop()!;
        engine.unsubscribe(registration.topic, registration.listener);
      }
    };

    const fail = (error: Error): void => {
      close();
      context.fail(error);
    };

    const persistTopics = (topics: ReadonlyArray<QueryTopic<Queries>>): void | Error =>
      context.persist({ topics });

    const persist = (): void | Error =>
      persistTopics(registrations.map((registration) => registration.topic));

    const createTopic = (value: unknown): QueryTopic<Queries> | Error => {
      if (
        value === null ||
        typeof value !== "object" ||
        Array.isArray(value) ||
        !("name" in value) ||
        typeof value.name !== "string" ||
        !("params" in value) ||
        !Array.isArray(value.params)
      ) {
        return new Error("Invalid topic");
      }
      return engine.createTopic(
        value.name as StringKey<Queries>,
        value.params as OperationParams<Queries[StringKey<Queries>]>,
      );
    };

    const remove = (registration: Registration<Queries>): void => {
      engine.unsubscribe(registration.topic, registration.listener);
      const index = registrations.indexOf(registration);
      if (index !== -1) registrations.splice(index, 1);
    };

    const deliver = (event: SubscriptionEvent<Queries>): void | Error => {
      const sent = context.notify(listenerNotification(event));
      if (!(sent instanceof Error)) return;
      fail(sent);
      return sent;
    };

    const subscribe = (value: unknown): void | Error => {
      const topic = createTopic(value);
      if (topic instanceof Error) return topic;
      if (registrations.some((registration) => dequal(registration.topic, topic))) return;

      let active = false;
      const registration: Registration<Queries> = {
        topic,
        latest: null,
        listener: (event) => {
          registration.latest = event;
          if (active) deliver(event);
        },
      };
      const subscribed = engine.subscribe(topic, registration.listener);
      if (subscribed instanceof Error) return subscribed;
      registrations.push(registration);

      const persisted = persist();
      if (persisted instanceof Error) {
        remove(registration);
        fail(persisted);
        return persisted;
      }

      active = true;
      if (registration.latest === null) return;
      return deliver(registration.latest);
    };

    const unsubscribe = (value: unknown): void | Error => {
      const topic = createTopic(value);
      if (topic instanceof Error) return topic;
      const registration = registrations.find((item) => dequal(item.topic, topic));
      if (registration === undefined) return;

      const persisted = persistTopics(
        registrations.filter((item) => item !== registration).map((item) => item.topic),
      );
      if (persisted instanceof Error) {
        fail(persisted);
        return persisted;
      }
      remove(registration);
    };

    const sync = (name: unknown, params: unknown): void | Error => {
      if (typeof name !== "string" || !Array.isArray(params)) {
        return new Error("Invalid sync parameters");
      }
      return engine.sync(
        name as StringKey<Mutations>,
        params as OperationParams<Mutations[StringKey<Mutations>]>,
      );
    };

    const service: ServiceAPI<Queries, Mutations> = {
      subscribe: (topic) => rpcResult(subscribe(topic)),
      unsubscribe: (topic) => rpcResult(unsubscribe(topic)),
      sync: (name, params) => rpcResult(sync(name, params)),
    };

    const restore = (): void | Error => {
      const attachment = context.attachment;
      if (attachment instanceof Error) {
        console.warn(attachment.message, attachment);
        return persistTopics([]);
      }
      if (attachment.value === null || attachment.value === undefined) return;
      if (
        typeof attachment.value !== "object" ||
        Array.isArray(attachment.value) ||
        !("topics" in attachment.value) ||
        !Array.isArray(attachment.value.topics)
      ) {
        console.warn("Invalid WebSocket subscription attachment");
        return persistTopics([]);
      }

      for (const value of attachment.value.topics) {
        const topic = createTopic(value);
        if (topic instanceof Error) {
          console.warn(topic.message, topic);
          continue;
        }
        if (registrations.some((registration) => dequal(registration.topic, topic))) continue;

        let restoring = true;
        const registration: Registration<Queries> = {
          topic,
          latest: null,
          listener: (event) => {
            registration.latest = event;
            if (restoring) return;
            deliver(event);
          },
        };
        const subscribed = engine.subscribe(topic, registration.listener);
        restoring = false;
        if (subscribed instanceof Error) {
          console.warn(subscribed.message, subscribed);
          continue;
        }
        registrations.push(registration);
      }
      return persist();
    };

    return { service, start: restore, close };
  };
}
