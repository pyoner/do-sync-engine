import * as errore from "errore";
import {
  InvalidListenerError,
  MutationExecutionError,
  QueryExecutionError,
  TopicCollisionError,
  UnknownMutationError,
} from "./errors";
import { assertKnownQuery, buildTopic, cloneOrThrow, validateTopic } from "./helpers";
import type {
  Listener,
  ListenerEvent,
  ListenerId,
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
  TopicHash,
} from "./types";

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> implements SyncEngineInterface<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<unknown[], unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<unknown[], unknown>>;
  private readonly registry = new Map<
    TopicHash,
    { topic: Topic<StringKey<Queries>, readonly unknown[]>; listeners: Map<ListenerId, Listener> }
  >();

  constructor(options: SyncEngineOptions<Queries, Mutations>) {
    this.queries = new Map(
      Object.entries(options.queries) as [string, Query<unknown[], unknown>][],
    );
    this.mutations = new Map(
      Object.entries(options.mutations) as [string, Mutation<unknown[], unknown>][],
    );
  }

  async createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Promise<Topic<Name, OperationParams<Queries[Name]>> | Error> {
    const knownQuery = assertKnownQuery(name, this.queries);
    if (knownQuery instanceof Error) return knownQuery;
    const topicParams = cloneOrThrow(params, "Topic params");
    if (topicParams instanceof Error) return topicParams;
    return buildTopic(name, topicParams);
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): ListenerId | Error {
    const clonedTopic = cloneOrThrow(topic, "Topic");
    if (clonedTopic instanceof Error) return clonedTopic;
    const validatedTopic = validateTopic(clonedTopic, this.queries);
    if (validatedTopic instanceof Error) return validatedTopic;
    if (typeof listener !== "function") return new InvalidListenerError();

    let entry = this.registry.get(validatedTopic.hash);
    if (entry === undefined) {
      entry = {
        topic: validatedTopic as Topic<StringKey<Queries>, readonly unknown[]>,
        listeners: new Map(),
      };
      this.registry.set(validatedTopic.hash, entry);
    } else {
      const existingParams = JSON.stringify(entry.topic.params);
      const nextParams = JSON.stringify(validatedTopic.params);
      if (entry.topic.name !== validatedTopic.name || existingParams !== nextParams)
        return new TopicCollisionError({ hash: validatedTopic.hash });
    }

    for (const [listenerId, existingListener] of entry.listeners) {
      if (existingListener === listener) return listenerId;
    }
    const listenerId = globalThis.crypto.randomUUID() as ListenerId;
    entry.listeners.set(listenerId, listener as Listener);
    return listenerId;
  }

  unsubscribe(listenerId: ListenerId): boolean {
    for (const [topicHash, entry] of this.registry) {
      if (!entry.listeners.delete(listenerId)) continue;
      if (entry.listeners.size === 0) this.registry.delete(topicHash);
      return true;
    }
    return false;
  }

  protected mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Set<Table> | Error {
    const mutationDefinition = this.mutations.get(mutation);
    if (mutationDefinition === undefined) return new UnknownMutationError({ mutation });
    const result = errore.try({
      try: () => mutationDefinition.run(...params),
      catch: (cause) => new MutationExecutionError({ cause }),
    });
    if (result instanceof Error) return result;
    return mutationDefinition.tables;
  }

  query<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): OperationResult<Queries[Name]> | Error {
    const validated = validateTopic(topic, this.queries);
    if (validated instanceof Error) return validated;
    const queryDefinition = this.queries.get(validated.name);
    if (queryDefinition === undefined) return new QueryExecutionError();
    return errore.try({
      try: () => queryDefinition.run(...validated.params) as OperationResult<Queries[Name]>,
      catch: (cause) => new QueryExecutionError({ cause }),
    });
  }

  protected publish(event: ListenerEvent): void {
    const entry = this.registry.get(event.topic.hash);
    if (!entry) return;
    for (const listener of entry.listeners.values()) void listener(event);
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void | Error {
    const changedTables = this.mutate(mutation, params);
    if (changedTables instanceof Error) return changedTables;
    for (const { topic } of this.registry.values()) {
      const queryDefinition = this.queries.get(topic.name);
      if (queryDefinition === undefined) continue;
      if (![...queryDefinition.tables].some((table) => changedTables.has(table))) continue;
      const value = this.query(topic as never);
      if (value instanceof Error) return value;
      this.publish({ topic, value });
    }
  }
}
