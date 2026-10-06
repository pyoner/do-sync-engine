import {
  Array as Arr,
  Effect,
  Iterable,
  Layer,
  MutableHashMap,
  Option,
  Predicate,
  Record,
} from "effect";
import type { Context } from "effect";
import { hash } from "ohash";
import { MissingSubscriptionIdError, UnknownMutationError, UnknownQueryError } from "./errors";
import type {
  BaseParams,
  Listener,
  ListenerEvent,
  MutationRecord,
  QueryRecord,
  SyncEngine,
  SyncEngineOptions,
  SyncEngineServices,
  Topic,
} from "./types";

type AnyListener = Listener<ListenerEvent, object>;

const make = <
  Id,
  Queries extends QueryRecord,
  Mutations extends MutationRecord,
  ListenerProperties extends object,
>(
  options: SyncEngineOptions<Id, Queries, Mutations, ListenerProperties>,
  context: Context.Context<SyncEngineServices<Queries, Mutations>>,
): SyncEngine<Id, Queries, Mutations, ListenerProperties> => {
  // Erased view of the options; the typed overloads come from `SyncEngine`.
  const queries: QueryRecord = options.queries;
  const mutations: MutationRecord = options.mutations;
  const createId = options.createId as ((topic: Topic, listener: AnyListener) => Id) | undefined;
  const registry = MutableHashMap.empty<Topic, Map<Id, AnyListener>>();
  /** Last event delivered to each listener; one slot per listener object. */
  const delivery = new WeakMap<AnyListener, string>();

  const remember = (listener: AnyListener, digest: string): void => {
    delivery.set(listener, digest);
  };

  const prune = (topic: Topic, listeners: Map<Id, AnyListener>): void => {
    if (listeners.size === 0) MutableHashMap.remove(registry, topic);
  };

  const query = Effect.fnUntraced(function* (topic: Topic) {
    const definition = Record.get(queries, topic.name);
    if (Option.isNone(definition)) return yield* new UnknownQueryError({ query: topic.name });
    return yield* Effect.provideContext(definition.value.run(...topic.params), context);
  });

  const publish = (event: ListenerEvent): void => {
    const listeners = MutableHashMap.get(registry, event.topic);
    if (Option.isNone(listeners)) return;
    // Hash now: a query may return a live object that a later mutation changes in place.
    const digest = hash(event);
    for (const listener of Array.from(listeners.value.values())) {
      if (delivery.get(listener) === digest) continue;
      remember(listener, digest);
      listener(event);
    }
  };

  const createTopic = (name: string, params: BaseParams) =>
    Record.has(queries, name)
      ? Effect.succeed({ name, params })
      : Effect.fail(new UnknownQueryError({ query: name }));

  const subscribe = Effect.fnUntraced(function* (topic: Topic, listener: AnyListener, id?: Id) {
    const listeners = MutableHashMap.get(registry, topic);
    const listenerId = Option.fromUndefinedOr(id).pipe(
      Option.orElse(() =>
        Option.flatMap(listeners, (registered) =>
          Arr.findFirst(registered, ([, candidate]) => candidate === listener),
        ).pipe(Option.map(([registeredId]) => registeredId)),
      ),
      Option.orElse(() => Option.fromUndefinedOr(createId?.(topic, listener))),
    );
    if (Option.isNone(listenerId)) return yield* new MissingSubscriptionIdError();

    const value = yield* query(topic);
    if (Option.isSome(listeners)) listeners.value.set(listenerId.value, listener);
    else MutableHashMap.set(registry, topic, new Map([[listenerId.value, listener]]));
    const event = { topic, value };
    remember(listener, hash(event));
    listener(event);
    return listenerId.value;
  });

  const unsubscribe = (...args: [id: Id] | [topic: Topic, idOrListener: Id | AnyListener]) =>
    Effect.sync(() => {
      if (args.length === 1) {
        for (const [topic, listeners] of Array.from(registry)) {
          listeners.delete(args[0]);
          prune(topic, listeners);
        }
        return;
      }
      const [topic, idOrListener] = args;
      const listeners = MutableHashMap.get(registry, topic);
      if (Option.isNone(listeners)) return;
      if (Predicate.isFunction(idOrListener)) {
        for (const [id, registered] of Array.from(listeners.value)) {
          if (registered === idOrListener) listeners.value.delete(id);
        }
      } else {
        listeners.value.delete(idOrListener);
      }
      prune(topic, listeners.value);
    });

  const sync = Effect.fnUntraced(function* (mutation: string, params: BaseParams) {
    const definition = Record.get(mutations, mutation);
    if (Option.isNone(definition)) return yield* new UnknownMutationError({ mutation });
    yield* Effect.provideContext(definition.value.run(...params), context);
    for (const topic of Array.from(MutableHashMap.keys(registry))) {
      const affected = Option.exists(Record.get(queries, topic.name), ({ tables }) =>
        Iterable.some(tables, (table) => definition.value.tables.has(table)),
      );
      if (!affected) continue;
      publish({ topic, value: yield* query(topic) });
    }
  });

  const subscriptions = (topic?: Topic) =>
    Effect.sync(() => {
      const entries: ReadonlyArray<readonly [Topic, Map<Id, AnyListener>]> =
        topic === undefined
          ? Array.from(registry)
          : Option.match(MutableHashMap.get(registry, topic), {
              onNone: () => [],
              onSome: (listeners) => [[topic, listeners] as const],
            });
      return entries.flatMap(([registeredTopic, listeners]) =>
        Array.from(listeners, ([id, listener]) => ({ id, topic: registeredTopic, listener })),
      );
    });

  return { createTopic, subscribe, unsubscribe, sync, subscriptions } as unknown as SyncEngine<
    Id,
    Queries,
    Mutations,
    ListenerProperties
  >;
};

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
  Effect.map(Effect.context<SyncEngineServices<Queries, Mutations>>(), (context) =>
    make(options, context),
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
