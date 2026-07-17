import { SyncEngineBase } from "./types";
import type {
  Mutation,
  MutationMap,
  OperationParams,
  OperationResult,
  Listener,
  Query,
  QueryMap,
  StringKey,
  ListenerId,
  Subscription,
  SyncEngineOptions,
  Topic,
  TopicHash,
} from "./types";

function cloneOrThrow<T>(value: T, label: string): T {
  try {
    return structuredClone(value);
  } catch (cause) {
    throw new TypeError(`${label} must support structuredClone`, { cause });
  }
}

function assertKnownQuery(query: string, knownQueries: { has(query: string): boolean }): void {
  if (!knownQueries.has(query)) {
    throw new ReferenceError(`Unknown query: ${query}`);
  }
}

async function buildTopic<Name extends string, Params extends readonly unknown[]>(
  name: Name,
  params: Params,
): Promise<Topic<Name, Params>> {
  const input = { name, params };
  const serialized = JSON.stringify(input);
  const bytes = new TextEncoder().encode(serialized);
  const digest = await globalThis.crypto.subtle.digest("SHA-256", bytes);
  const hash = Array.from(new Uint8Array(digest), (byte) =>
    byte.toString(16).padStart(2, "0"),
  ).join("") as TopicHash;
  return { name, params, hash };
}

function validateTopic(
  topic: unknown,
  knownQueryNames: { has(query: string): boolean },
): Topic<string, readonly unknown[]> {
  if (typeof topic !== "object" || topic === null) {
    throw new TypeError("Topic must be an object");
  }

  const candidate = topic as { name?: unknown; params?: unknown; hash?: unknown };
  if (typeof candidate.name !== "string") {
    throw new TypeError("Topic name must be a string");
  }
  assertKnownQuery(candidate.name, knownQueryNames);
  if (!Array.isArray(candidate.params)) {
    throw new TypeError("Topic params must be an array");
  }
  if (typeof candidate.hash !== "string" || !/^[0-9a-f]{64}$/.test(candidate.hash)) {
    throw new TypeError("Topic hash must be 64 lowercase hexadecimal characters");
  }

  return candidate as Topic<string, readonly unknown[]>;
}

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
    const topicParams = cloneOrThrow(params, "Subscription params");
    return buildTopic(name, topicParams);
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener,
  ): Subscription {
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
        return { topicHash: validatedTopic.hash, listenerId };
      }
    }

    const listenerId = globalThis.crypto.randomUUID() as ListenerId;
    listenersForTopic.set(listenerId, listener);
    return { topicHash: validatedTopic.hash, listenerId };
  }

  unsubscribe(subscription: Subscription): boolean {
    const listenersForTopic = this.listeners.get(subscription.topicHash);
    if (listenersForTopic === undefined || !listenersForTopic.delete(subscription.listenerId))
      return false;

    if (listenersForTopic.size === 0) this.listeners.delete(subscription.topicHash);
    return true;
  }

  protected async mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Promise<readonly string[]> {
    const mutationDefinition = this.mutations.get(mutation);
    if (mutationDefinition === undefined) {
      throw new ReferenceError(`Unknown mutation: ${mutation}`);
    }

    await mutationDefinition.run(...params);
    return [...mutationDefinition.tables];
  }

  protected async query<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Promise<OperationResult<Queries[Name]>> {
    const queryDefinition = this.queries.get(name);
    if (queryDefinition === undefined) {
      throw new ReferenceError(`Unknown query: ${name}`);
    }

    return (await queryDefinition.run(...params)) as OperationResult<Queries[Name]>;
  }

  protected async publish(topic: Topic, value: unknown): Promise<void> {
    const listenersForTopic = this.listeners.get(topic.hash);
    if (listenersForTopic === undefined) return;

    const listenerIds = Array.from(listenersForTopic.keys());
    for (const listenerId of listenerIds) {
      const listener = listenersForTopic.get(listenerId);
      if (listener !== undefined) await listener(topic, value);
    }
  }

  async update<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Promise<void> {
    const affectedTables = await this.mutate(mutation, params);
    const changedTables = new Set(affectedTables);
    const topicsToSync = Array.from(this.topics);

    for (const topic of topicsToSync) {
      if (!this.listeners.has(topic.hash)) continue;

      const queryDefinition = this.queries.get(topic.name);
      if (queryDefinition === undefined) continue;
      const touchesChangedTable = queryDefinition.tables.some((table) => changedTables.has(table));
      if (!touchesChangedTable) continue;

      const value = await this.query(
        topic.name,
        topic.params as OperationParams<Queries[StringKey<Queries>]>,
      );
      await this.publish(topic, value);
    }
  }
}
