import * as errore from "errore";
import HashMap from "hashmap";
import {
  MissingSubscriptionIdError,
  MutationExecutionError,
  QueryExecutionError,
  UnknownMutationError,
  UnknownQueryError,
} from "./errors";
import { assertKnownQuery, createTopic } from "./helpers";
import type {
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
  Subscriptions,
  ListenerEvents,
} from "./types";

export class SyncEngine<
  Id,
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
> implements SyncEngineInterface<Id, Queries, Mutations> {
  private readonly queries: Queries;
  private readonly mutations: Mutations;
  private readonly createId?: () => Id;
  private readonly registry: Registry<Queries, Id> = new HashMap();

  constructor(options: SyncEngineOptions<Id, Queries, Mutations>) {
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
    listener: Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
  ): Id | UnknownQueryError | QueryExecutionError | MissingSubscriptionIdError;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
    id: Id,
  ): Id | UnknownQueryError | QueryExecutionError;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
    id?: Id,
  ): Id | UnknownQueryError | QueryExecutionError | MissingSubscriptionIdError {
    const listeners =
      this.registry.get(topic) ??
      (() => {
        const registeredListeners = new Map<Id, Listener<ListenerEvents<Queries>>>();
        this.registry.set(topic, registeredListeners);
        return registeredListeners;
      })();

    const listenerId =
      id ??
      (() => {
        for (const [registeredId, registeredListener] of listeners) {
          if (registeredListener === listener) return registeredId;
        }
        return this.createId?.();
      })();

    if (listenerId === undefined) {
      return new MissingSubscriptionIdError();
    }

    listeners.set(listenerId, listener as Listener);

    const value = this.query(topic);
    if (value instanceof Error) return value;

    listener({ topic, value });
    return listenerId;
  }

  unsubscribe(id: Id): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    id: Id,
  ): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
  ): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topicOrId: Topic<Name, Params> | Id,
    idOrListener?: Id | Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>>,
  ): void {
    if (arguments.length === 1) {
      for (const listeners of this.registry.values()) listeners.delete(topicOrId as Id);
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
    event: ListenerEvent,
  ): void {
    const registeredEvent = event as ListenerEvent<Topic<Name, Params>>;
    const listeners = this.registry.get(registeredEvent.topic);
    if (listeners === undefined) return;
    for (const listener of listeners.values()) void (listener as Listener)(registeredEvent);
  }

  *subscriptions(): IterableIterator<Readonly<Subscriptions<Id, Queries>>> {
    for (const { key: topic, value } of this.registry) {
      for (const [id, listener] of value) {
        yield { id, topic, listener } as unknown as Readonly<Subscriptions<Id, Queries>>;
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
