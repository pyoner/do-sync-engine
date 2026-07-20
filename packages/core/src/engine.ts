import { SyncEngineBase } from "./types";
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
  SyncEngineOptions,
  Table,
  Topic,
  TopicHash,
} from "./types";

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> extends SyncEngineBase<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<unknown[], unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<unknown[], unknown>>;
  private readonly registry = new Map<
    TopicHash,
    { topic: Topic<StringKey<Queries>, readonly unknown[]>; listeners: Map<ListenerId, Listener> }
  >();

  constructor(options: SyncEngineOptions<Queries, Mutations>) {
    super();
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
  ): Promise<Topic<Name, OperationParams<Queries[Name]>>> {
    assertKnownQuery(name, this.queries);
    const topicParams = cloneOrThrow(params, "Topic params");
    return buildTopic(name, topicParams);
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): ListenerId {
    const clonedTopic = cloneOrThrow(topic, "Topic");
    const validatedTopic = validateTopic(clonedTopic, this.queries);
    if (typeof listener !== "function") {
      throw new TypeError("Listener must be a function");
    }

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
      if (entry.topic.name !== validatedTopic.name || existingParams !== nextParams) {
        throw new RangeError(`Topic hash collision: ${validatedTopic.hash}`);
      }
    }

    for (const [listenerId, existingListener] of entry.listeners) {
      if (existingListener === listener) {
        return listenerId;
      }
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
  ): Set<Table> {
    const mutationDefinition = this.mutations.get(mutation);
    if (mutationDefinition === undefined) {
      throw new ReferenceError(`Unknown mutation: ${mutation}`);
    }

    mutationDefinition.run(...params);
    return mutationDefinition.tables;
  }

  protected query<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): OperationResult<Queries[Name]> {
    const queryDefinition = this.queries.get(name);
    if (queryDefinition === undefined) {
      throw new ReferenceError(`Unknown query: ${name}`);
    }

    return queryDefinition.run(...params) as OperationResult<Queries[Name]>;
  }

  protected publish(event: ListenerEvent): void {
    const entry = this.registry.get(event.topic.hash);
    if (!entry) return;

    for (const listener of entry.listeners.values()) {
      listener(event);
    }
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void {
    const changedTables = this.mutate(mutation, params);

    for (const { topic } of this.registry.values()) {
      const queryDefinition = this.queries.get(topic.name);
      if (queryDefinition === undefined) continue;
      let touchesChangedTable = false;
      for (const table of queryDefinition.tables) {
        if (changedTables.has(table)) {
          touchesChangedTable = true;
          break;
        }
      }
      if (!touchesChangedTable) continue;

      const value = this.query(
        topic.name,
        topic.params as OperationParams<Queries[StringKey<Queries>]>,
      );
      this.publish({ topic, value });
    }
  }
}
