import { Effect, Equal, Hash } from "effect";
import { ListenerIdSchema } from "./types";
import {
  assertKnownQueryEffect,
  assertSupportedParams,
  buildTopic,
  cloneOrThrow,
  TopicBuildError,
  UnknownQueryError,
  validateTopic,
} from "./helpers";
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
} from "./types";

function queryTopic(
  topic: Topic,
  queries: ReadonlyMap<string, Query<unknown[], unknown>>,
): unknown {
  const validated = validateTopic(topic, queries);
  const queryDefinition = queries.get(validated.name);
  if (queryDefinition === undefined) throw new ReferenceError(`Unknown query: ${validated.name}`);
  return queryDefinition.run(...validated.params);
}

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> implements SyncEngineInterface<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<unknown[], unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<unknown[], unknown>>;
  private readonly registry = new Map<
    string,
    Array<{
      topic: Topic<StringKey<Queries>, readonly unknown[]>;
      listeners: Map<ListenerId, Listener>;
    }>
  >();

  constructor(options: SyncEngineOptions<Queries, Mutations>) {
    this.queries = new Map(
      Object.entries(options.queries) as [string, Query<unknown[], unknown>][],
    );
    this.mutations = new Map(
      Object.entries(options.mutations) as [string, Mutation<unknown[], unknown>][],
    );
  }

  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Effect.Effect<
    Topic<Name, OperationParams<Queries[Name]>>,
    TopicBuildError | UnknownQueryError
  > {
    const queries = this.queries;
    return Effect.gen(function* () {
      yield* assertKnownQueryEffect(name, queries);
      const topicParams = yield* Effect.try({
        try: () => {
          assertSupportedParams(params);
          return structuredClone(params);
        },
        catch: (cause) => TopicBuildError.make({ cause, operation: "clone" }),
      });
      return yield* buildTopic(name, topicParams);
    });
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): ListenerId {
    assertSupportedParams(topic.params);
    validateTopic(topic, this.queries);
    const clonedTopic = cloneOrThrow(topic, "Topic");
    const validatedTopic = validateTopic(clonedTopic, this.queries);
    if (typeof listener !== "function") {
      throw new TypeError("Listener must be a function");
    }

    const bucketKey = String(Hash.hash(validatedTopic));
    const bucket = this.registry.get(bucketKey) ?? [];
    this.registry.set(bucketKey, bucket);
    let entry = bucket.find(({ topic: existingTopic }) =>
      Equal.equals(existingTopic, validatedTopic),
    );
    if (entry === undefined) {
      entry = {
        topic: validatedTopic as Topic<StringKey<Queries>, readonly unknown[]>,
        listeners: new Map(),
      };
      bucket.push(entry);
    }

    for (const [listenerId, existingListener] of entry.listeners) {
      if (existingListener === listener) {
        return listenerId;
      }
    }

    const listenerId = ListenerIdSchema.make(globalThis.crypto.randomUUID());
    entry.listeners.set(listenerId, listener as Listener);
    return listenerId;
  }

  unsubscribe(listenerId: ListenerId): boolean {
    for (const [bucketKey, bucket] of this.registry) {
      for (let index = 0; index < bucket.length; index++) {
        const entry = bucket[index];
        if (!entry.listeners.delete(listenerId)) continue;
        if (entry.listeners.size === 0) {
          bucket.splice(index, 1);
          if (bucket.length === 0) this.registry.delete(bucketKey);
        }
        return true;
      }
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

  query<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): OperationResult<Queries[Name]> {
    const validated = validateTopic(topic, this.queries);
    const queryDefinition = this.queries.get(validated.name);
    if (queryDefinition === undefined) {
      throw new ReferenceError(`Unknown query: ${validated.name}`);
    }

    return queryDefinition.run(...validated.params) as OperationResult<Queries[Name]>;
  }

  protected publish(event: ListenerEvent): void {
    const bucket = this.registry.get(String(Hash.hash(event.topic)));
    const entry = bucket?.find(({ topic }) => Equal.equals(topic, event.topic));
    if (!entry) return;

    for (const listener of entry.listeners.values()) {
      void listener(event);
    }
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void {
    const changedTables = this.mutate(mutation, params);

    for (const bucket of this.registry.values()) {
      for (const { topic } of bucket) {
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

        const value = queryTopic(topic, this.queries);
        this.publish({ topic, value });
      }
    }
  }
}
