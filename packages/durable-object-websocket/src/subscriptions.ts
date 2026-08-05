import { Effect, Equal, MutableHashMap, Option, PubSub, Schema, Semaphore, Stream } from "effect";
import {
  type Listener,
  type ListenerEvent,
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

type SubscriptionError<Q extends QueryMap<Q>> =
  | Error
  | UnknownQueryError
  | OperationError<Q[StringKey<Q>]>;
type QueryTopic<Q extends QueryMap<Q>> = Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;
type SubscriptionEngine<Q extends QueryMap<Q>> = Pick<
  SyncEngineInterface<Q, never>,
  "subscribe" | "unsubscribe"
>;
type Entry = {
  readonly events: PubSub.PubSub<ListenerEvent>;
  readonly release: Effect.Effect<void>;
};

const openSubscription = <Q extends QueryMap<Q>>(
  engine: SubscriptionEngine<Q>,
  topic: QueryTopic<Q>,
): Effect.Effect<Entry, UnknownQueryError | OperationError<Q[StringKey<Q>]>> =>
  Effect.gen(function* () {
    const events = yield* PubSub.unbounded<ListenerEvent>({ replay: 1 });
    const listener: Listener = (event) => PubSub.publishUnsafe(events, event);
    const release = engine
      .unsubscribe(topic, listener)
      .pipe(Effect.andThen(PubSub.shutdown(events)));
    yield* engine.subscribe(topic, listener).pipe(
      Effect.onError(() =>
        Effect.all(
          [engine.unsubscribe(topic, listener).pipe(Effect.ignore), PubSub.shutdown(events)],
          {
            discard: true,
          },
        ),
      ),
    );
    return { events, release };
  });

export class SocketSubscriptions<Q extends QueryMap<Q>> {
  private readonly entries = MutableHashMap.empty<Topic, Entry>();
  private readonly lock = Semaphore.makeUnsafe(1);

  constructor(
    private readonly socket: Pick<WebSocket, "deserializeAttachment" | "serializeAttachment">,
    private readonly engine: SubscriptionEngine<Q>,
  ) {}

  subscribe(topic: Topic): Stream.Stream<ListenerEvent, SubscriptionError<Q>> {
    return Stream.unwrap(
      this.lock
        .withPermit(this.attach(topic, true))
        .pipe(Effect.map((entry) => Stream.fromPubSub(entry.events))),
    );
  }

  unsubscribe(topic: Topic): Effect.Effect<void, Error> {
    return this.lock.withPermit(
      Effect.gen({ self: this }, function* (this: SocketSubscriptions<Q>) {
        const entry = this.get(topic);
        if (!entry) return;
        yield* this.persist(
          Array.from(MutableHashMap.keys(this.entries)).filter(
            (candidate) => !Equal.equals(candidate, topic),
          ),
        );
        yield* this.remove(topic, entry);
      }),
    );
  }

  restore(): Effect.Effect<void, SubscriptionError<Q>> {
    return this.lock.withPermit(
      Effect.gen({ self: this }, function* (this: SocketSubscriptions<Q>) {
        const raw = yield* Effect.try({
          try: () => this.socket.deserializeAttachment(),
          catch: webSocketOperationError,
        });
        const decoded = Schema.decodeUnknownOption(Attachment)(raw);
        if (Option.isNone(decoded)) {
          yield* this.persist([]);
          return;
        }
        const result = yield* Effect.exit(
          Effect.gen({ self: this }, function* (this: SocketSubscriptions<Q>) {
            for (const topic of decoded.value.topics) yield* this.attach(topic, false);
            yield* this.persist();
          }),
        );
        if (result._tag === "Failure") {
          yield* this.removeAll().pipe(Effect.ignore);
          yield* this.persist([]).pipe(Effect.ignore);
          return yield* Effect.failCause(result.cause);
        }
      }),
    );
  }

  close(): Effect.Effect<void> {
    return this.lock.withPermit(this.removeAll());
  }

  private get(topic: Topic): Entry | undefined {
    return Option.getOrUndefined(MutableHashMap.get(this.entries, topic));
  }

  private remove(topic: Topic, entry: Entry): Effect.Effect<void> {
    return Effect.sync(() => MutableHashMap.remove(this.entries, topic)).pipe(
      Effect.andThen(entry.release),
    );
  }

  private removeAll(): Effect.Effect<void> {
    return Effect.forEach(Array.from(this.entries), ([topic, entry]) => this.remove(topic, entry), {
      discard: true,
    });
  }

  private attach(
    topic: Topic,
    writeAttachment: boolean,
  ): Effect.Effect<Entry, SubscriptionError<Q>> {
    const existing = this.get(topic);
    if (existing) return Effect.succeed(existing);
    const queryTopic = topic as QueryTopic<Q>;
    return openSubscription(this.engine, queryTopic).pipe(
      Effect.tap((entry) => Effect.sync(() => MutableHashMap.set(this.entries, topic, entry))),
      Effect.tapError(() => Effect.void),
      Effect.flatMap((entry) =>
        writeAttachment
          ? this.persist().pipe(
              Effect.onError(() => this.remove(topic, entry)),
              Effect.as(entry),
            )
          : Effect.succeed(entry),
      ),
    );
  }

  private persist(
    topics = Array.from(MutableHashMap.keys(this.entries)),
  ): Effect.Effect<void, Error> {
    return Effect.try({
      try: () => this.socket.serializeAttachment({ topics }),
      catch: webSocketOperationError,
    });
  }
}
