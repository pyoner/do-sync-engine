import {
  Effect,
  Exit,
  MutableHashMap,
  Option,
  PubSub,
  Random,
  Schema,
  Semaphore,
  Stream,
} from "effect";
import {
  type Listener,
  type ListenerEvent,
  type MutationMap,
  type OperationError,
  type OperationParams,
  type QueryMap,
  type StringKey,
  type SyncEngineInterface,
  Topic,
  TopicSchema,
  type UnknownQueryError,
} from "@do-sync-engine/core";

const webSocketOperationError = (cause: unknown) =>
  new Error("WebSocket operation failed", { cause });

const Attachment = Schema.Struct({
  id: Schema.Number,
  topics: Schema.Array(TopicSchema),
});
export type Attachment = typeof Attachment.Type;

type SubscriptionError<Q extends QueryMap<Q>> =
  | Error
  | UnknownQueryError
  | OperationError<Q[StringKey<Q>]>;
export type QueryEvent = { readonly topic: Topic; readonly value: unknown };

type QueryTopic<Q extends QueryMap<Q>> = Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;
type QueryListener<Q extends QueryMap<Q>> = Listener<
  ListenerEvent<StringKey<Q>, OperationParams<Q[StringKey<Q>]>, unknown>
>;
type Entry<Q extends QueryMap<Q>> = {
  readonly topic: QueryTopic<Q>;
  readonly listener: QueryListener<Q>;
  readonly events: PubSub.PubSub<QueryEvent>;
};

export class SubscriptionRegistry<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  private readonly entries = MutableHashMap.empty<QueryTopic<Q>, Entry<Q>>();
  private readonly lock = Effect.runSync(Semaphore.make(1));
  private id = Effect.runSync(Random.nextInt);

  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<Q, M>,
  ) {}

  subscribeStream(topic: QueryTopic<Q>): Stream.Stream<QueryEvent, SubscriptionError<Q>> {
    return Stream.unwrap(
      this.lock
        .withPermits(1)(this.attach(topic, true))
        .pipe(Effect.map((entry) => Stream.fromPubSub(entry.events))),
    );
  }

  unsubscribe(topic: Topic): Effect.Effect<void, SubscriptionError<Q>> {
    return this.lock.withPermits(1)(this.unsubscribeUnsafe(topic));
  }

  private unsubscribeUnsafe(topic: Topic): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const entry = Option.getOrUndefined(MutableHashMap.get(this.entries, topic as QueryTopic<Q>));
      if (!entry) return;
      yield* this.persist(
        Array.from(this.entries, ([candidate]) => candidate).filter(
          (candidate) => candidate !== entry.topic,
        ),
      );
      yield* Effect.sync(() => MutableHashMap.remove(this.entries, entry.topic));
      yield* this.engine.unsubscribe(entry.topic, entry.listener);
      yield* PubSub.shutdown(entry.events);
    });
  }

  restore(): Effect.Effect<void, SubscriptionError<Q>> {
    return this.lock.withPermits(1)(this.restoreUnsafe());
  }

  private restoreUnsafe(): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const raw = yield* Effect.try({
        try: () => this.socket.deserializeAttachment(),
        catch: webSocketOperationError,
      });
      const decoded = Schema.decodeUnknownOption(Attachment)(raw);
      if (Option.isNone(decoded)) {
        this.id = yield* Random.nextInt;
        yield* this.persist([]);
        return;
      }

      this.id = decoded.value.id;
      for (const topic of decoded.value.topics) {
        const admission = yield* Effect.exit(this.attach(topic as QueryTopic<Q>, false));
        if (admission._tag === "Success") continue;
        yield* this.clearUnsafe().pipe(Effect.ignore);
        yield* this.persist([]).pipe(Effect.ignore);
        return yield* Effect.failCause(admission.cause);
      }
      yield* this.persist();
    });
  }

  clear(): Effect.Effect<void, SubscriptionError<Q>> {
    return this.lock.withPermits(1)(this.clearUnsafe());
  }

  private clearUnsafe(): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const entries = Array.from(this.entries, ([, entry]) => entry);
      yield* Effect.sync(() => {
        for (const entry of entries) MutableHashMap.remove(this.entries, entry.topic);
      });
      for (const entry of entries) {
        yield* this.engine.unsubscribe(entry.topic, entry.listener);
        yield* PubSub.shutdown(entry.events);
      }
    });
  }

  private attach(
    topic: QueryTopic<Q>,
    persist: boolean,
  ): Effect.Effect<Entry<Q>, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const existing = Option.getOrUndefined(MutableHashMap.get(this.entries, topic));
      if (existing) return existing;

      const events = yield* PubSub.unbounded<QueryEvent>({ replay: 1 });
      const listener: QueryListener<Q> = (event) => {
        PubSub.publishUnsafe(events, { topic: event.topic, value: event.value });
      };
      const entry = { topic, listener, events };
      yield* Effect.sync(() => MutableHashMap.set(this.entries, topic, entry));
      return yield* this.engine.subscribe(topic, listener).pipe(
        Effect.onExit((exit) =>
          Exit.isFailure(exit)
            ? Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
                yield* Effect.sync(() => MutableHashMap.remove(this.entries, topic));
                yield* this.engine.unsubscribe(topic, listener).pipe(Effect.ignore);
                yield* PubSub.shutdown(events).pipe(Effect.ignore);
              })
            : Effect.void,
        ),
        Effect.flatMap(() => (persist ? this.persist() : Effect.succeed(undefined))),
        Effect.as(entry),
      );
    });
  }

  private persist(
    topics = Array.from(this.entries, ([topic]) => topic),
  ): Effect.Effect<void, Error> {
    return Effect.try({
      try: () => this.socket.serializeAttachment({ id: this.id, topics }),
      catch: webSocketOperationError,
    });
  }
}
