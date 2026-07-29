import { Effect, Equal, Hash } from "effect";
import { ListenerIdSchema } from "./types";
import {
  assertKnownQueryEffect,
  assertSupportedParams,
  buildTopic,
  clone,
  TopicBuildError,
  TopicValidationError,
  UnknownMutationError,
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
  OperationError,
  Query,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  SyncEngineOptions,
  Table,
  Topic,
} from "./types";

const queryEffect = (
  topic: Topic,
  queries: ReadonlyMap<string, Query<unknown[], unknown>>,
): Effect.Effect<unknown, TopicValidationError | UnknownQueryError | OperationError<Query>> =>
  Effect.gen(function* () {
    const validated = yield* validateTopic(topic, queries);
    const definition = queries.get(validated.name);
    if (!definition) return yield* Effect.fail(UnknownQueryError.make({ query: validated.name }));
    return yield* definition.run(...validated.params);
  });

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
    return Effect.gen(
      function* (this: SyncEngine<Queries, Mutations>) {
        yield* assertKnownQueryEffect(name, this.queries);
        yield* Effect.try({
          try: () => assertSupportedParams(params),
          catch: (cause) => TopicBuildError.make({ cause, operation: "clone" }),
        });
        const cloned = yield* clone(params);
        return yield* buildTopic(name, cloned);
      }.bind(this),
    );
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): Effect.Effect<ListenerId, TopicValidationError | UnknownQueryError> {
    return Effect.gen(
      function* (this: SyncEngine<Queries, Mutations>) {
        const validated = yield* validateTopic(topic, this.queries);
        const cloned = yield* clone(validated).pipe(
          Effect.mapError((error) => new TopicValidationError({ cause: error })),
        );
        const validatedClone = yield* validateTopic(cloned, this.queries);
        yield* Effect.try({
          try: () => {
            if (typeof listener !== "function") throw new TypeError("Listener must be a function");
          },
          catch: (cause) => new TopicValidationError({ cause }),
        });
        return yield* Effect.sync(() => {
          const key = String(Hash.hash(validatedClone));
          const bucket = this.registry.get(key) ?? [];
          this.registry.set(key, bucket);
          let entry = bucket.find(({ topic: existing }) => Equal.equals(existing, validatedClone));
          if (!entry) {
            entry = {
              topic: validatedClone as Topic<StringKey<Queries>, readonly unknown[]>,
              listeners: new Map(),
            };
            bucket.push(entry);
          }
          for (const [id, existing] of entry.listeners) if (existing === listener) return id;
          const id = ListenerIdSchema.make(globalThis.crypto.randomUUID());
          entry.listeners.set(id, listener as Listener);
          return id;
        });
      }.bind(this),
    );
  }

  unsubscribe(listenerId: ListenerId): Effect.Effect<boolean> {
    return Effect.sync(() => {
      for (const [key, bucket] of this.registry)
        for (let i = 0; i < bucket.length; i++) {
          const entry = bucket[i];
          if (!entry.listeners.delete(listenerId)) continue;
          if (entry.listeners.size === 0) {
            bucket.splice(i, 1);
            if (bucket.length === 0) this.registry.delete(key);
          }
          return true;
        }
      return false;
    });
  }

  private mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Effect.Effect<Set<Table>, UnknownMutationError | OperationError<Mutations[Name]>> {
    const definition = this.mutations.get(mutation);
    if (!definition) return Effect.fail(UnknownMutationError.make({ mutation }));
    return definition.run(...params).pipe(Effect.map(() => definition.tables));
  }

  query<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): Effect.Effect<
    OperationResult<Queries[Name]>,
    TopicValidationError | UnknownQueryError | OperationError<Queries[Name]>
  > {
    return queryEffect(topic, this.queries) as Effect.Effect<
      OperationResult<Queries[Name]>,
      TopicValidationError | UnknownQueryError | OperationError<Queries[Name]>
    >;
  }

  protected publish(event: ListenerEvent): void {
    const bucket = this.registry.get(String(Hash.hash(event.topic)));
    const entry = bucket?.find(({ topic }) => Equal.equals(topic, event.topic));
    if (!entry) return;
    for (const listener of entry.listeners.values()) void listener(event);
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Effect.Effect<
    void,
    | UnknownMutationError
    | OperationError<Mutations[Name]>
    | TopicValidationError
    | UnknownQueryError
    | OperationError<Queries[StringKey<Queries>]>
  > {
    return Effect.gen(
      function* (this: SyncEngine<Queries, Mutations>) {
        const changed = yield* this.mutate(mutation, params);
        for (const bucket of this.registry.values())
          for (const { topic } of bucket) {
            const definition = this.queries.get(topic.name);
            if (!definition || !Array.from(definition.tables).some((table) => changed.has(table)))
              continue;
            const value = yield* queryEffect(topic, this.queries);
            yield* Effect.sync(() => this.publish({ topic, value }));
          }
      }.bind(this),
    );
  }
}
