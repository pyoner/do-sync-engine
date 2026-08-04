import { Cache, Effect, Option, PubSub, Random, Schema, Stream } from "effect";
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

const serializeAttachment = (socket: WebSocket, value: Attachment) =>
  Effect.try({
    try: () => socket.serializeAttachment(value),
    catch: webSocketOperationError,
  });
export class SubscriptionRegistry<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  private readonly cache: Cache.Cache<QueryTopic<Q>, Entry<Q>, SubscriptionError<Q>>;
  private id: number;

  constructor(
    private readonly socket: WebSocket,
    private readonly engine: SyncEngineInterface<Q, M>,
  ) {
    this.id = Effect.runSync(Random.nextInt);
    let cache: Cache.Cache<QueryTopic<Q>, Entry<Q>, SubscriptionError<Q>> | undefined;
    this.cache = Effect.runSync(
      Cache.make<QueryTopic<Q>, Entry<Q>, SubscriptionError<Q>>({
        capacity: Number.POSITIVE_INFINITY,
        lookup: (topic) => {
          let events: PubSub.PubSub<QueryEvent> | undefined;
          return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
            events = yield* PubSub.unbounded<QueryEvent>({ replay: 1 });
            const listener: QueryListener<Q> = (event) => {
              PubSub.publishUnsafe(events!, { topic: event.topic, value: event.value });
            };
            yield* this.engine.subscribe(topic, listener);
            return { topic, listener, events };
          }).pipe(
            Effect.catchCause((cause) =>
              Effect.gen(function* () {
                if (events) yield* PubSub.shutdown(events);
                if (cache) yield* Cache.invalidate(cache, topic);
                return yield* Effect.failCause(cause);
              }),
            ),
          );
        },
      }),
    );
    cache = this.cache;
  }

  subscribeStream(
    query: StringKey<Q>,
    params: OperationParams<Q[StringKey<Q>]>,
  ): Stream.Stream<QueryEvent, SubscriptionError<Q>> {
    return Stream.unwrap(
      Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
        const topic = yield* this.engine.createTopic(query, params);
        const entry = yield* this.get(topic);
        yield* this.serializeCurrent();
        return Stream.fromPubSub(entry.events);
      }),
    );
  }

  unsubscribe(topic: Topic): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const entry = yield* Cache.getOption(this.cache, topic as QueryTopic<Q>);
      if (Option.isNone(entry)) return;
      const remaining = yield* this.entries();
      yield* serializeAttachment(this.socket, {
        id: this.id,
        topics: remaining
          .filter((candidate) => candidate !== entry.value)
          .map((candidate) => candidate.topic),
      });
      yield* this.engine.unsubscribe(entry.value.topic, entry.value.listener);
      yield* PubSub.shutdown(entry.value.events);
      yield* Cache.invalidate(this.cache, entry.value.topic);
    });
  }

  restore(): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const raw = yield* Effect.try({
        try: () => this.socket.deserializeAttachment(),
        catch: webSocketOperationError,
      });
      const decoded = Schema.decodeUnknownOption(Attachment)(raw);
      if (Option.isNone(decoded)) {
        this.id = yield* Random.nextInt;
        yield* serializeAttachment(this.socket, { id: this.id, topics: [] });
        return;
      }

      this.id = decoded.value.id;
      const restored: Entry<Q>[] = [];
      for (const topic of decoded.value.topics) {
        const admission = yield* Effect.exit(this.get(topic as QueryTopic<Q>));
        if (admission._tag === "Success") {
          if (!restored.some((entry) => entry === admission.value)) restored.push(admission.value);
          continue;
        }
        if (admission._tag === "Failure") {
          yield* this.clear().pipe(Effect.ignore);
          yield* serializeAttachment(this.socket, { id: this.id, topics: [] }).pipe(Effect.ignore);
          return yield* Effect.failCause(admission.cause);
        }
      }
      yield* this.serializeCurrent();
    });
  }

  clear(): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const entries = yield* this.entries();
      for (const entry of entries) {
        yield* this.engine.unsubscribe(entry.topic, entry.listener);
        yield* PubSub.shutdown(entry.events);
      }
      yield* Cache.invalidateAll(this.cache);
    });
  }

  private get(topic: QueryTopic<Q>): Effect.Effect<Entry<Q>, SubscriptionError<Q>> {
    return Cache.get(this.cache, topic).pipe(
      Effect.catchCause((cause) =>
        Effect.flatMap(Cache.invalidate(this.cache, topic), () => Effect.failCause(cause)),
      ),
    );
  }

  private entries(): Effect.Effect<readonly Entry<Q>[]> {
    return Effect.map(Cache.entries(this.cache), (entries) =>
      Array.from(entries, ([, entry]) => entry),
    );
  }

  private serializeCurrent(): Effect.Effect<void, Error> {
    return Effect.flatMap(this.entries(), (entries) =>
      serializeAttachment(this.socket, {
        id: this.id,
        topics: entries.map((entry) => entry.topic),
      }),
    );
  }
}
