import * as errore from "errore";
import { HashMap } from "@tykowale/ts-hash-map";
import { hash } from "ohash";
import {
  MissingSubscriptionIdError,
  MutationExecutionError,
  QueryExecutionError,
  UnknownMutationError,
  UnknownQueryError,
} from "./errors";
import { assertKnownQuery, createTopic } from "./helpers";
import type {
  Delivery,
  Listener,
  ListenerEvent,
  MutationRecord,
  OpParams,
  OpResult,
  QueryRecord,
  Registry,
  StringKey,
  SyncEngineInterface,
  SyncEngineOptions,
  Table,
  Topic,
  Topics,
  Subscription,
  Subscriptions,
  ListenerEvents,
} from "./types";

export class SyncEngine<
  Id,
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
  ListenerProperties extends object = object,
> implements SyncEngineInterface<Id, Queries, Mutations, ListenerProperties> {
  private readonly queries: Queries;
  private readonly mutations: Mutations;
  private readonly createId?: SyncEngineOptions<
    Id,
    Queries,
    Mutations,
    ListenerProperties
  >["createId"];
  private readonly registry: Registry<
    Queries,
    Id,
    Listener<ListenerEvents<Queries>, ListenerProperties>
  > = new HashMap();
  private readonly delivery: Delivery<Listener<ListenerEvents<Queries>, ListenerProperties>> =
    new WeakMap();

  constructor(options: SyncEngineOptions<Id, Queries, Mutations, ListenerProperties>) {
    this.createId = options.createId;
    this.queries = options.queries;
    this.mutations = options.mutations;
  }

  createTopic<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    name: Name,
    params: Params,
  ): Topic<Name, Params> | UnknownQueryError {
    const knownQuery = assertKnownQuery(name, this.queries);
    if (knownQuery instanceof Error) return knownQuery;
    return createTopic(name, params);
  }

  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): Id | UnknownQueryError | QueryExecutionError | MissingSubscriptionIdError;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
    id: Id,
  ): Id | UnknownQueryError | QueryExecutionError;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
    id?: Id,
  ): Id | UnknownQueryError | QueryExecutionError | MissingSubscriptionIdError {
    const listeners = this.registry.get(topic);
    const listenerId =
      id ??
      (() => {
        for (const [registeredId, registeredListener] of listeners ?? []) {
          if (registeredListener === listener) return registeredId;
        }
        return this.createId?.(topic, listener);
      })();

    if (listenerId === undefined) {
      return new MissingSubscriptionIdError();
    }

    const value = this.query(topic);
    if (value instanceof Error) return value;

    const registeredListeners =
      listeners ??
      (() => {
        const created = new Map<Id, Listener<ListenerEvents<Queries>, ListenerProperties>>();
        this.registry.set(topic, created);
        return created;
      })();
    const registered = listener as Listener<ListenerEvents<Queries>, ListenerProperties>;
    registeredListeners.set(listenerId, registered);
    const event = { topic, value };
    this.delivery.set(registered, hash(event));
    listener(event);
    return listenerId;
  }

  unsubscribe(id: Id): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    id: Id,
  ): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topicOrId: Topic<Name, Params> | Id,
    idOrListener?:
      | Id
      | Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>, ListenerProperties>,
  ): void {
    if (arguments.length === 1) {
      for (const [topic, listeners] of this.registry.entries()) {
        listeners.delete(topicOrId as Id);
        if (listeners.size === 0) this.registry.delete(topic);
      }
      return;
    }
    const listeners = this.registry.get(topicOrId as Topic<Name, Params>);
    if (listeners === undefined) return;
    if (typeof idOrListener === "function") {
      for (const [id, registeredListener] of listeners) {
        if (registeredListener === idOrListener) listeners.delete(id);
      }
    } else if (idOrListener !== undefined) {
      listeners.delete(idOrListener);
    }
    if (listeners.size === 0) this.registry.delete(topicOrId as Topic<Name, Params>);
  }

  protected mutate<Name extends StringKey<Mutations>, Params extends OpParams<Mutations[Name]>>(
    mutation: Name,
    params: Params,
  ): Set<Table> | UnknownMutationError | MutationExecutionError {
    const mutationDefinition = this.mutations[mutation];
    if (mutationDefinition === undefined) return new UnknownMutationError({ mutation });
    const result = errore.try({
      try: () => Reflect.apply(mutationDefinition.run, mutationDefinition, params),
      catch: (cause) => new MutationExecutionError({ cause }),
    });
    if (result instanceof MutationExecutionError) return result;
    return mutationDefinition.tables;
  }
  protected query<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): OpResult<Queries[Name]> | UnknownQueryError | QueryExecutionError {
    const knownQuery = assertKnownQuery(topic.name, this.queries);
    if (knownQuery instanceof Error) return knownQuery;
    const queryDefinition = this.queries[topic.name];
    if (queryDefinition === undefined) return new QueryExecutionError();
    return errore.try({
      try: () =>
        Reflect.apply(queryDefinition.run, queryDefinition, topic.params) as OpResult<
          Queries[Name]
        >,
      catch: (cause) => new QueryExecutionError({ cause }),
    });
  }

  protected publish<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    event: ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
  ): void {
    const listeners = this.registry.get(event.topic);
    if (listeners === undefined) return;
    const eventHash = hash(event);
    for (const listener of listeners.values()) {
      if (this.delivery.get(listener) === eventHash) continue;
      this.delivery.set(listener, eventHash);
      void listener(event);
    }
  }

  subscriptions(): IterableIterator<Readonly<Subscriptions<Id, Queries, ListenerProperties>>>;
  subscriptions<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): IterableIterator<
    Readonly<
      Subscription<
        Id,
        Topic<Name, Params>,
        Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>, ListenerProperties>
      >
    >
  >;
  *subscriptions(topic?: Topics<Queries>) {
    if (topic !== undefined) {
      const listeners = this.registry.get(topic);
      if (listeners === undefined) return;
      for (const [id, listener] of listeners) {
        yield { id, topic, listener } as unknown as Readonly<
          Subscriptions<Id, Queries, ListenerProperties>
        >;
      }
      return;
    }
    for (const [registeredTopic, value] of this.registry) {
      for (const [id, listener] of value) {
        yield { id, topic: registeredTopic, listener } as unknown as Readonly<
          Subscriptions<Id, Queries, ListenerProperties>
        >;
      }
    }
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OpParams<Mutations[Name]>,
  ):
    | void
    | UnknownQueryError
    | UnknownMutationError
    | MutationExecutionError
    | QueryExecutionError {
    const changedTables = this.mutate(mutation, params);
    if (changedTables instanceof Error) return changedTables;
    for (const topic of this.registry.keys()) {
      const queryDefinition = this.queries[topic.name];
      if (queryDefinition === undefined) continue;
      if (![...queryDefinition.tables].some((table) => changedTables.has(table))) continue;
      const value = this.query(topic);
      if (value instanceof Error) return value;
      this.publish({ topic, value });
    }
  }
}
