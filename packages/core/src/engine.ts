import { dequal } from "dequal";
import * as errore from "errore";
import {
  InvalidListenerError,
  MutationExecutionError,
  QueryExecutionError,
  UnknownMutationError,
} from "./errors";
import { assertKnownQuery, createTopic, validateTopic } from "./helpers";
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
type Listeners = Set<Listener>;

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> implements SyncEngineInterface<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<BaseParams, unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<BaseParams, unknown>>;
  private readonly registry = new Map<Topic<StringKey<Queries>, BaseParams>, Listeners>();

  constructor(options: SyncEngineOptions<Queries, Mutations>) {
    this.queries = new Map(
      Object.entries(options.queries) as [string, Query<BaseParams, unknown>][],
    );
    this.mutations = new Map(
      Object.entries(options.mutations) as [string, Mutation<BaseParams, unknown>][],
    );
  }

  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Topic<Name, OperationParams<Queries[Name]>> | Error {
    const knownQuery = assertKnownQuery(name, this.queries);
    if (knownQuery instanceof Error) return knownQuery;
    return createTopic(name, params);
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): void | Error {
    if (typeof listener !== "function") return new InvalidListenerError();

    let listeners: Listeners | undefined;
    for (const [registeredTopic, registeredListeners] of this.registry) {
      if (dequal(registeredTopic, topic)) {
        listeners = registeredListeners;
        break;
      }
    }
    if (listeners === undefined) {
      listeners = new Set();
      this.registry.set(topic as Topic<StringKey<Queries>, BaseParams>, listeners);
    }
    listeners.add(listener as Listener);
  }

  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): void | Error {
    if (typeof listener !== "function") return new InvalidListenerError();
    for (const [registeredTopic, listeners] of this.registry) {
      if (!dequal(registeredTopic, topic)) continue;
      listeners.delete(listener as Listener);
      if (listeners.size === 0) this.registry.delete(registeredTopic);
      return;
    }
  }

  protected mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Set<Table> | Error {
    const mutationDefinition = this.mutations.get(mutation);
    if (mutationDefinition === undefined) return new UnknownMutationError({ mutation });
    const result = errore.try({
      try: () => mutationDefinition.run(...(params as BaseParams)),
      catch: (cause) => new MutationExecutionError({ cause }),
    });
    if (result instanceof Error) return result;
    return mutationDefinition.tables;
  }

  query<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): OperationResult<Queries[Name]> | Error {
    const validated = validateTopic(topic);
    if (validated instanceof Error) return validated;
    const knownQuery = assertKnownQuery(validated.name, this.queries);
    if (knownQuery instanceof Error) return knownQuery;
    const queryDefinition = this.queries.get(validated.name);
    if (queryDefinition === undefined) return new QueryExecutionError();
    return errore.try({
      try: () => queryDefinition.run(...validated.params) as OperationResult<Queries[Name]>,
      catch: (cause) => new QueryExecutionError({ cause }),
    });
  }

  protected publish(event: ListenerEvent): void {
    for (const [topic, listeners] of this.registry) {
      if (!dequal(topic, event.topic)) continue;
      for (const listener of listeners) void listener(event);
      return;
    }
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void | Error {
    const changedTables = this.mutate(mutation, params);
    if (changedTables instanceof Error) return changedTables;
    for (const topic of this.registry.keys()) {
      const queryDefinition = this.queries.get(topic.name);
      if (queryDefinition === undefined) continue;
      if (![...queryDefinition.tables].some((table) => changedTables.has(table))) continue;
      const value = this.query(topic as never);
      if (value instanceof Error) return value;
      this.publish({ topic, value });
    }
  }
}
