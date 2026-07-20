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
  private readonly listeners = new Map<TopicHash, Map<ListenerId, Listener>>();
  private topics: Topic<StringKey<Queries>, readonly unknown[]>[] = [];

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

    const existingTopic = this.topics.find((topic) => topic.hash === validatedTopic.hash);
    if (existingTopic === undefined) {
      this.topics.push(validatedTopic as Topic<StringKey<Queries>, readonly unknown[]>);
    } else {
      const existingParams = JSON.stringify(existingTopic.params);
      const nextParams = JSON.stringify(validatedTopic.params);
      if (existingTopic.name !== validatedTopic.name || existingParams !== nextParams) {
        throw new RangeError(`Topic hash collision: ${validatedTopic.hash}`);
      }
    }

    let listenersForTopic = this.listeners.get(validatedTopic.hash);
    if (listenersForTopic === undefined) {
      listenersForTopic = new Map();
      this.listeners.set(validatedTopic.hash, listenersForTopic);
    }
    for (const [listenerId, existingListener] of listenersForTopic) {
      if (existingListener === listener) {
        return listenerId;
      }
    }

    const listenerId = globalThis.crypto.randomUUID() as ListenerId;
    listenersForTopic.set(listenerId, listener as Listener);
    return listenerId;
  }

  unsubscribe(listenerId: ListenerId): boolean {
    for (const [topicHash, listenersForTopic] of this.listeners) {
      if (!listenersForTopic.delete(listenerId)) continue;
      if (listenersForTopic.size === 0) {
        this.listeners.delete(topicHash);
        const topicIndex = this.topics.findIndex((topic) => topic.hash === topicHash);
        if (topicIndex !== -1) this.topics.splice(topicIndex, 1);
      }
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
    const listenersForTopic = this.listeners.get(event.topic.hash);
    if (!listenersForTopic) return;

    const listenerIds = listenersForTopic.keys();
    for (const listenerId of listenerIds) {
      const listener = listenersForTopic.get(listenerId);
      listener?.(event);
    }
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void {
    const changedTables = this.mutate(mutation, params);

    for (const topic of this.topics) {
      if (!this.listeners.has(topic.hash)) continue;

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
