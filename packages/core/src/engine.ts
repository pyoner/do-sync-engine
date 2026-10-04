import { Effect, Layer, MutableHashMap, Option } from "effect";
import type { Context } from "effect";
import { hash } from "ohash";
import { MissingSubscriptionIdError, UnknownMutationError, UnknownQueryError } from "./errors";
import type {
  Delivery,
  Listener,
  ListenerEvent,
  ListenerEvents,
  MutationRecord,
  OpError,
  OpParams,
  OpResult,
  QueryRecord,
  Registry,
  StringKey,
  Subscription,
  Subscriptions,
  SyncEngine,
  SyncEngineOptions,
  SyncEngineServices,
  Topic,
  Topics,
} from "./types";

type AnyListener<Q extends QueryRecord, LP extends object> = Listener<ListenerEvents<Q>, LP>;

class SyncEngineImpl<
  Id,
  Queries extends QueryRecord,
  Mutations extends MutationRecord,
  ListenerProperties extends object,
> implements SyncEngine<Id, Queries, Mutations, ListenerProperties> {
  private readonly registry: Registry<Queries, Id, AnyListener<Queries, ListenerProperties>> =
    MutableHashMap.empty();
  private readonly delivery: Delivery<AnyListener<Queries, ListenerProperties>> = new WeakMap();
  private readonly options: SyncEngineOptions<Id, Queries, Mutations, ListenerProperties>;
  private readonly context: Context.Context<SyncEngineServices<Queries, Mutations>>;

  constructor(
    options: SyncEngineOptions<Id, Queries, Mutations, ListenerProperties>,
    context: Context.Context<SyncEngineServices<Queries, Mutations>>,
  ) {
    this.options = options;
    this.context = context;
  }

  createTopic<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    name: Name,
    params: Params,
  ): Effect.Effect<Topic<Name, Params>, UnknownQueryError> {
    if (!Object.hasOwn(this.options.queries, name)) {
      return Effect.fail(new UnknownQueryError({ query: name }));
    }
    return Effect.succeed({ name, params });
  }

  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): Effect.Effect<Id, UnknownQueryError | MissingSubscriptionIdError | OpError<Queries[Name]>>;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
    id: Id,
  ): Effect.Effect<Id, UnknownQueryError | OpError<Queries[Name]>>;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
    id?: Id,
  ): Effect.Effect<Id, UnknownQueryError | MissingSubscriptionIdError | OpError<Queries[Name]>> {
    return Effect.gen({ self: this }, function* () {
      const listeners = Option.getOrUndefined(
        MutableHashMap.get(this.registry, topic as unknown as Topics<Queries>),
      );
      let listenerId = id;
      if (listenerId === undefined) {
        for (const [registeredId, registered] of listeners ?? []) {
          if (registered === (listener as unknown)) {
            listenerId = registeredId;
            break;
          }
        }
      }
      listenerId ??= this.options.createId?.(topic, listener);
      if (listenerId === undefined) return yield* new MissingSubscriptionIdError();

      const value = yield* this.query(topic);
      const registered = listener as unknown as AnyListener<Queries, ListenerProperties>;
      if (listeners === undefined) {
        MutableHashMap.set(
          this.registry,
          topic as unknown as Topics<Queries>,
          new Map([[listenerId, registered]]),
        );
      } else {
        listeners.set(listenerId, registered);
      }
      const event = { topic, value };
      this.delivery.set(registered, hash(event));
      listener(event);
      return listenerId;
    });
  }

  unsubscribe(id: Id): Effect.Effect<void>;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    id: Id,
  ): Effect.Effect<void>;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): Effect.Effect<void>;
  unsubscribe(
    ...args:
      | [id: Id]
      | [topic: Topics<Queries>, idOrListener: Id | AnyListener<Queries, ListenerProperties>]
  ): Effect.Effect<void> {
    return Effect.sync(() => {
      if (args.length === 1) {
        for (const [topic, listeners] of Array.from(this.registry)) {
          listeners.delete(args[0]);
          if (listeners.size === 0) MutableHashMap.remove(this.registry, topic);
        }
        return;
      }
      const [topic, idOrListener] = args;
      const listeners = Option.getOrUndefined(MutableHashMap.get(this.registry, topic));
      if (listeners === undefined) return;
      if (typeof idOrListener === "function") {
        for (const [id, registered] of Array.from(listeners)) {
          if (registered === idOrListener) listeners.delete(id);
        }
      } else {
        listeners.delete(idOrListener);
      }
      if (listeners.size === 0) MutableHashMap.remove(this.registry, topic);
    });
  }

  sync<Name extends StringKey<Mutations>, Params extends OpParams<Mutations[Name]>>(
    mutation: Name,
    params: Params,
  ): Effect.Effect<
    void,
    | UnknownMutationError
    | UnknownQueryError
    | OpError<Mutations[Name]>
    | OpError<Queries[StringKey<Queries>]>
  > {
    return Effect.gen({ self: this }, function* () {
      const definition = this.options.mutations[mutation];
      if (definition === undefined || !Object.hasOwn(this.options.mutations, mutation)) {
        return yield* new UnknownMutationError({ mutation });
      }
      const run = Reflect.apply(definition.run, definition, params) as unknown as Effect.Effect<
        unknown,
        OpError<Mutations[Name]>,
        SyncEngineServices<Queries, Mutations>
      >;
      yield* Effect.provideContext(run, this.context);
      for (const topic of Array.from(MutableHashMap.keys(this.registry))) {
        const query = this.options.queries[topic.name];
        if (query === undefined) continue;
        if (![...query.tables].some((table) => definition.tables.has(table))) continue;
        const value = yield* this.query(topic);
        this.publish({ topic, value });
      }
    });
  }

  subscriptions(): Effect.Effect<
    ReadonlyArray<Readonly<Subscriptions<Id, Queries, ListenerProperties>>>
  >;
  subscriptions<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): Effect.Effect<
    ReadonlyArray<
      Readonly<
        Subscription<
          Id,
          Topic<Name, Params>,
          Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>, ListenerProperties>
        >
      >
    >
  >;
  subscriptions(topic?: Topics<Queries>): Effect.Effect<readonly never[]> {
    return Effect.sync(() => {
      const entries =
        topic === undefined
          ? Array.from(this.registry)
          : Option.match(MutableHashMap.get(this.registry, topic), {
              onNone: () => [],
              onSome: (listeners) => [[topic, listeners] as const],
            });
      return entries.flatMap(([registeredTopic, listeners]) =>
        Array.from(listeners, ([id, listener]) => ({ id, topic: registeredTopic, listener })),
      ) as unknown as readonly never[];
    });
  }

  private query<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): Effect.Effect<OpResult<Queries[Name]>, UnknownQueryError | OpError<Queries[Name]>> {
    const definition = this.options.queries[topic.name];
    if (definition === undefined || !Object.hasOwn(this.options.queries, topic.name)) {
      return Effect.fail(new UnknownQueryError({ query: topic.name }));
    }
    return Effect.provideContext(
      Reflect.apply(definition.run, definition, topic.params) as unknown as Effect.Effect<
        OpResult<Queries[Name]>,
        OpError<Queries[Name]>,
        SyncEngineServices<Queries, Mutations>
      >,
      this.context,
    );
  }

  private publish(event: ListenerEvent<Topics<Queries>>): void {
    const listeners = Option.getOrUndefined(MutableHashMap.get(this.registry, event.topic));
    if (listeners === undefined) return;
    const eventHash = hash(event);
    for (const listener of Array.from(listeners.values())) {
      if (this.delivery.get(listener) === eventHash) continue;
      this.delivery.set(listener, eventHash);
      listener(event);
    }
  }
}

export const makeSyncEngine = <
  Id,
  Queries extends QueryRecord,
  Mutations extends MutationRecord,
  ListenerProperties extends object = object,
>(
  options: SyncEngineOptions<Id, Queries, Mutations, ListenerProperties>,
): Effect.Effect<
  SyncEngine<Id, Queries, Mutations, ListenerProperties>,
  never,
  SyncEngineServices<Queries, Mutations>
> =>
  Effect.map(
    Effect.context<SyncEngineServices<Queries, Mutations>>(),
    (context): SyncEngine<Id, Queries, Mutations, ListenerProperties> =>
      new SyncEngineImpl(options, context),
  );

export const syncEngineLayer = <
  Identifier,
  Id,
  Queries extends QueryRecord,
  Mutations extends MutationRecord,
  ListenerProperties extends object = object,
>(
  service: Context.Key<Identifier, SyncEngine<Id, Queries, Mutations, ListenerProperties>>,
  options: SyncEngineOptions<Id, Queries, Mutations, ListenerProperties>,
): Layer.Layer<Identifier, never, SyncEngineServices<Queries, Mutations>> =>
  Layer.effect(service, makeSyncEngine(options));
