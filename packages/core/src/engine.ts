import * as errore from "errore";
import HashMap from "hashmap";
import {
  MutationExecutionError,
  QueryExecutionError,
  UnknownMutationError,
  UnknownQueryError,
} from "./errors";
import { assertKnownQuery, createTopic } from "./helpers";
import type {
  BaseParams,
  Listener,
  ListenerEvent,
  Mutation,
  MutationMap,
  OperationParams,
  OperationResult,
  Query,
  QueryMap,
  Registry,
  StringKey,
  SyncEngineInterface,
  SyncEngineOptions,
  Table,
  Topic,
} from "./types";
type Listeners<Id, Queries extends QueryMap> = Map<
  Id,
  Listener<ListenerEvent<StringKey<Queries>, OperationParams<Queries[string]>>>
>;

export class SyncEngine<
  Queries extends QueryMap = QueryMap,
  Mutations extends MutationMap = MutationMap,
  Id = string,
> implements SyncEngineInterface<Queries, Mutations, Id> {
  private readonly queries: ReadonlyMap<string, Query<BaseParams, unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<BaseParams, unknown>>;
  private readonly createId: () => Id;
  private readonly registry: Registry<Queries, Id> = new HashMap();

  constructor(options: SyncEngineOptions<Queries, Mutations, Id>) {
    this.createId = options.createId ?? (() => crypto.randomUUID() as unknown as Id);
    this.queries = new Map(
      Object.entries(options.queries) as Array<[string, Query<BaseParams, unknown>]>,
    );
    this.mutations = new Map(
      Object.entries(options.mutations) as Array<[string, Mutation<BaseParams, unknown>]>,
    );
  }

  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Topic<Name, OperationParams<Queries[Name]>> | UnknownQueryError {
    const knownQuery = assertKnownQuery(name, this.queries);
    if (knownQuery instanceof Error) return knownQuery;
    return createTopic(name, params);
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): Id | UnknownQueryError | QueryExecutionError;
  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
    id: Id,
  ): Id | UnknownQueryError | QueryExecutionError;
  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
    id?: Id,
  ): Id | UnknownQueryError | QueryExecutionError {
    const value = this.query(topic);
    if (value instanceof Error) return value;
    const listeners =
      this.registry.get(topic) ??
      (() => {
        const registeredListeners: Listeners<Id, Queries> = new Map();
        this.registry.set(topic, registeredListeners);
        return registeredListeners;
      })();
    const listenerId =
      id ??
      (() => {
        for (const [registeredId, registeredListener] of listeners) {
          if (registeredListener === listener) return registeredId;
        }
        return this.createId();
      })();
    listeners.set(listenerId, listener as Listener);
    listener({ topic, value });
    return listenerId;
  }

  unsubscribe(id: Id): void;
  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    id: Id,
  ): void;
  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): void;
  unsubscribe<Name extends StringKey<Queries>>(
    topicOrId: Topic<Name, OperationParams<Queries[Name]>> | Id,
    idOrListener?:
      | Id
      | Listener<
          ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
        >,
  ): void {
    if (arguments.length === 1) {
      for (const listeners of this.registry.values()) listeners.delete(topicOrId as Id);
      return;
    }
    const listeners = this.registry.get(topicOrId as Topic<Name, OperationParams<Queries[Name]>>);
    if (listeners === undefined) return;
    if (typeof idOrListener === "function") {
      for (const [id, registeredListener] of listeners) {
        if (registeredListener === idOrListener) listeners.delete(id);
      }
    } else if (idOrListener !== undefined) {
      listeners.delete(idOrListener);
    }
    if (listeners.size === 0)
      this.registry.delete(topicOrId as Topic<Name, OperationParams<Queries[Name]>>);
  }

  protected mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Set<Table> | UnknownMutationError | MutationExecutionError {
    const mutationDefinition = this.mutations.get(mutation);
    if (mutationDefinition === undefined) return new UnknownMutationError({ mutation });
    const result = errore.try({
      try: () => mutationDefinition.run(...(params as BaseParams)),
      catch: (cause) => new MutationExecutionError({ cause }),
    });
    if (result instanceof MutationExecutionError) return result;
    return mutationDefinition.tables;
  }
  protected query(
    topic: Topic<StringKey<Queries>, BaseParams>,
  ): OperationResult<Queries[StringKey<Queries>]> | UnknownQueryError | QueryExecutionError {
    const knownQuery = assertKnownQuery(topic.name, this.queries);
    if (knownQuery instanceof Error) return knownQuery;
    const queryDefinition = this.queries.get(topic.name);
    if (queryDefinition === undefined) return new QueryExecutionError();
    return errore.try({
      try: () =>
        queryDefinition.run(...topic.params) as OperationResult<Queries[StringKey<Queries>]>,
      catch: (cause) => new QueryExecutionError({ cause }),
    });
  }

  protected publish(event: ListenerEvent): void {
    const registeredEvent = event as ListenerEvent<
      StringKey<Queries>,
      OperationParams<Queries[string]>
    >;
    const listeners = this.registry.get(registeredEvent.topic);
    if (listeners === undefined) return;
    for (const listener of listeners.values()) void (listener as Listener)(registeredEvent);
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ):
    | void
    | UnknownQueryError
    | UnknownMutationError
    | MutationExecutionError
    | QueryExecutionError {
    const changedTables = this.mutate(mutation, params);
    if (changedTables instanceof Error) return changedTables;
    for (const topic of this.registry.keys()) {
      const queryDefinition = this.queries.get(topic.name);
      if (queryDefinition === undefined) continue;
      if (![...queryDefinition.tables].some((table) => changedTables.has(table))) continue;
      const value = this.query(topic);
      if (value instanceof Error) return value;
      this.publish({ topic, value });
    }
  }
}
