import { Effect, MutableHashMap, Option } from "effect";
import { ListenerIdSchema, Topic } from "./types";
import { assertKnownQueryEffect, UnknownMutationError, UnknownQueryError } from "./helpers";
import type {
  Listener,
  ListenerEvent,
  ListenerId,
  Mutation,
  MutationMap,
  OperationError,
  OperationParams,
  OperationResult,
  Query,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  SyncEngineOptions,
  Table,
} from "./types";

export class SyncEngine<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> implements SyncEngineInterface<Queries, Mutations> {
  private readonly queries: ReadonlyMap<string, Query<unknown[], unknown>>;
  private readonly mutations: ReadonlyMap<string, Mutation<unknown[], unknown>>;
  private readonly registry = MutableHashMap.empty<
    Topic<StringKey<Queries>, readonly unknown[]>,
    Map<ListenerId, Listener>
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
  ): Effect.Effect<Topic<Name, OperationParams<Queries[Name]>>, UnknownQueryError> {
    return assertKnownQueryEffect(name, this.queries).pipe(
      Effect.map(() => new Topic({ name, params })),
    );
  }

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): Effect.Effect<ListenerId, UnknownQueryError | OperationError<Queries[Name]>> {
    return Effect.gen(
      function* (this: SyncEngine<Queries, Mutations>) {
        yield* assertKnownQueryEffect(topic.name, this.queries);
        const subscription = yield* Effect.sync(() => {
          const listeners = Option.getOrElse(MutableHashMap.get(this.registry, topic), () => {
            const listenerMap = new Map<ListenerId, Listener>();
            MutableHashMap.set(this.registry, topic, listenerMap);
            return listenerMap;
          });
          for (const [id, existing] of listeners)
            if (existing === listener) return { id, added: false };
          const id = ListenerIdSchema.make(globalThis.crypto.randomUUID());
          listeners.set(id, listener as Listener);
          return { id, added: true };
        });
        if (!subscription.added) return subscription.id;
        return yield* this.query(topic.name, topic.params).pipe(
          Effect.flatMap((value) => Effect.sync(() => listener({ topic, value }))),
          Effect.catch((error) =>
            Effect.sync(() => {
              const listeners = Option.getOrUndefined(MutableHashMap.get(this.registry, topic));
              if (!listeners) return;
              listeners.delete(subscription.id);
              if (listeners.size === 0) MutableHashMap.remove(this.registry, topic);
            }).pipe(Effect.flatMap(() => Effect.fail(error))),
          ),
          Effect.as(subscription.id),
        );
      }.bind(this),
    );
  }

  unsubscribe(listenerId: ListenerId): Effect.Effect<boolean> {
    return Effect.sync(() => {
      for (const [topic, listeners] of this.registry) {
        if (!listeners.delete(listenerId)) continue;
        if (listeners.size === 0) MutableHashMap.remove(this.registry, topic);
        return true;
      }
      return false;
    });
  }

  protected mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Effect.Effect<Set<Table>, UnknownMutationError | OperationError<Mutations[Name]>> {
    const definition = this.mutations.get(mutation);
    if (!definition) return Effect.fail(UnknownMutationError.make({ mutation }));
    return definition.run(...params).pipe(Effect.map(() => definition.tables));
  }

  protected query<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Effect.Effect<
    OperationResult<Queries[Name]>,
    UnknownQueryError | OperationError<Queries[Name]>
  > {
    const queries = this.queries;
    return Effect.gen(function* () {
      const definition = queries.get(name);
      if (!definition) return yield* Effect.fail(UnknownQueryError.make({ query: name }));
      return yield* definition.run(...params);
    }) as Effect.Effect<
      OperationResult<Queries[Name]>,
      UnknownQueryError | OperationError<Queries[Name]>
    >;
  }

  protected publish(event: ListenerEvent): void {
    const listeners = Option.getOrUndefined(MutableHashMap.get(this.registry, event.topic));
    if (!listeners) return;
    for (const listener of listeners.values()) void listener(event);
  }

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Effect.Effect<
    void,
    | UnknownMutationError
    | OperationError<Mutations[Name]>
    | UnknownQueryError
    | OperationError<Queries[StringKey<Queries>]>
  > {
    const self = this;
    return Effect.gen(function* () {
      const changed = yield* self.mutate(mutation, params);
      for (const topic of MutableHashMap.keys(self.registry)) {
        const definition = self.queries.get(topic.name);
        if (!definition) continue;
        let overlaps = false;
        for (const table of definition.tables)
          if (changed.has(table)) {
            overlaps = true;
            break;
          }
        if (!overlaps) continue;
        const value = yield* self.query(
          topic.name,
          topic.params as OperationParams<Queries[StringKey<Queries>]>,
        );
        yield* Effect.sync(() => self.publish({ topic, value }));
      }
    });
  }
}
