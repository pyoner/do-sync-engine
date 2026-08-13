import { dequal } from "dequal";
import * as errore from "errore";
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
  StringKey,
  SyncEngineInterface,
  SyncEngineOptions,
  Table,
  Topic,
} from "./types";
type Listeners = Set<unknown>;

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> implements SyncEngineInterface<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<BaseParams, unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<BaseParams, unknown>>;
  private readonly registry = new Map<Topic<StringKey<Queries>, BaseParams>, Listeners>();

  constructor(options: SyncEngineOptions<Queries, Mutations>) {
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
  ): void | UnknownQueryError | QueryExecutionError {
    const value = this.query(topic);
    if (value instanceof Error) return value;
    let listeners: Listeners | undefined;
    for (const [registeredTopic, registeredListeners] of this.registry) {
      if (!dequal(registeredTopic, topic)) continue;
      listeners = registeredListeners;
      break;
    }
    if (listeners === undefined) {
      listeners = new Set();
      this.registry.set(topic, listeners);
    }
    listeners.add(listener);
    listener({ topic, value });
  }

  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): void {
    for (const [registeredTopic, listeners] of this.registry) {
      if (!dequal(registeredTopic, topic)) continue;
      listeners.delete(listener);
      if (listeners.size === 0) this.registry.delete(registeredTopic);
      return;
    }
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
    for (const [topic, listeners] of this.registry) {
      if (!dequal(topic, event.topic)) continue;
      for (const listener of listeners) if (typeof listener === "function") void listener(event);
      return;
    }
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
