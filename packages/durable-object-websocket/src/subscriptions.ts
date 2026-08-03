import { Effect, Equal, Exit, Queue, Schema, Stream } from "effect";
import {
  type ListenerId,
  type MutationMap,
  type OperationError,
  type OperationParams,
  type QueryMap,
  type StringKey,
  type SyncEngineInterface,
  type Topic,
  type UnknownQueryError,
} from "@do-sync-engine/core";

const webSocketOperationError = (cause: unknown) =>
  new Error("WebSocket operation failed", { cause });
const Persisted = Schema.Struct({
  requestId: Schema.Union([Schema.String, Schema.Number]),
  query: Schema.String,
  params: Schema.Array(Schema.Unknown),
  headers: Schema.Array(Schema.Tuple([Schema.String, Schema.String])),
});
export type PersistedSubscription = typeof Persisted.Type;
const empty = { version: 1 as const, subscriptions: [] as readonly PersistedSubscription[] };
const attachment = (ws: WebSocket, value: unknown) =>
  Effect.try({
    try: () => ws.serializeAttachment(value),
    catch: webSocketOperationError,
  });

type SubscriptionError<Q extends QueryMap<Q>> =
  | Error
  | UnknownQueryError
  | OperationError<Q[StringKey<Q>]>;
type QueryEvent = { readonly topic: Topic; readonly value: unknown };
type Session = PersistedSubscription & {
  readonly topic: Topic;
  queue?: Queue.Queue<QueryEvent, never>;
};
type Entry = {
  readonly topic: Topic;
  readonly listenerId: ListenerId;
  readonly sessions: Map<string | number, Session>;
};
type SocketState = Map<string, Entry[]>;

export class SubscriptionRegistry<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  private readonly states = new Map<WebSocket, SocketState>();
  constructor(private readonly engine: SyncEngineInterface<Q, M>) {}
  subscribeStream(
    ws: WebSocket,
    session: PersistedSubscription,
  ): Stream.Stream<QueryEvent, SubscriptionError<Q>> {
    return Stream.unwrap(
      Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
        const topic = yield* this.engine.createTopic<StringKey<Q>>(
          session.query as StringKey<Q>,
          session.params as OperationParams<Q[StringKey<Q>]>,
        );
        const state = this.states.get(ws) ?? new Map<string, Entry[]>();
        this.states.set(ws, state);
        const bucket = state.get(topic.name) ?? [];
        let entry = bucket.find((x) => Equal.equals(x.topic, topic));
        const queue = yield* Queue.unbounded<QueryEvent>();
        const accepted: Session = { ...session, topic, queue };
        if (entry) {
          const restored = entry.sessions.get(session.requestId);
          if (restored && !restored.queue) entry.sessions.set(session.requestId, accepted);
          else {
            entry.sessions.set(session.requestId, accepted);
            let snapshot: QueryEvent | undefined;
            const id = yield* this.engine.subscribe<StringKey<Q>>(topic, (e) => {
              snapshot = e;
            });
            yield* this.engine.unsubscribe(id);
            if (snapshot) yield* Queue.offer(queue, snapshot);
          }
          yield* attachment(ws, { version: 1, subscriptions: this.sessions(state) });
        } else {
          let initial: QueryEvent | undefined;
          const sessions = new Map([[session.requestId, accepted]]);
          const listenerId = yield* this.engine.subscribe<StringKey<Q>>(topic, (e) => {
            if (!initial) initial = e;
            else for (const s of sessions.values()) if (s.queue) Queue.offerUnsafe(s.queue, e);
          });
          entry = { topic, listenerId, sessions };
          bucket.push(entry);
          state.set(topic.name, bucket);
          if (initial) yield* Queue.offer(queue, initial);
          yield* attachment(ws, { version: 1, subscriptions: this.sessions(state) });
        }
        return Stream.fromQueue(queue).pipe(
          Stream.ensuring(
            Effect.sync(() => {
              const current = entry?.sessions.get(session.requestId);
              if (current?.queue === queue)
                entry?.sessions.set(session.requestId, { ...current, queue: undefined });
            }),
          ),
        );
      }),
    );
  }
  unsubscribe(ws: WebSocket, topic: Topic): Effect.Effect<boolean, Error> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const state = this.states.get(ws);
      const entries = state?.get(topic.name)?.filter((e) => Equal.equals(e.topic, topic)) ?? [];
      if (!state || entries.length === 0) return false;
      yield* attachment(ws, {
        version: 1,
        subscriptions: this.sessions(state, topic),
      });
      let removed = false;
      for (const entry of entries) {
        removed = (yield* this.engine.unsubscribe(entry.listenerId)) || removed;
        for (const s of entry.sessions.values()) if (s.queue) yield* Queue.shutdown(s.queue);
        const bucket = state.get(topic.name) ?? [];
        const i = bucket.indexOf(entry);
        if (i >= 0) bucket.splice(i, 1);
      }
      if ((state.get(topic.name)?.length ?? 0) === 0) state.delete(topic.name);
      return removed;
    });
  }
  restore(ws: WebSocket): Effect.Effect<readonly PersistedSubscription[], SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const raw: unknown = yield* Effect.try({
        try: () => ws.deserializeAttachment(),
        catch: webSocketOperationError,
      });
      const records = yield* Effect.sync(() => {
        const value = raw;
        if (
          typeof value !== "object" ||
          value === null ||
          !("version" in value) ||
          value.version !== 1
        )
          return [] as PersistedSubscription[];
        const subscriptions = "subscriptions" in value ? value.subscriptions : undefined;
        if (!Array.isArray(subscriptions)) return [];
        return subscriptions.flatMap((record): PersistedSubscription[] => {
          const decoded = Schema.decodeUnknownOption(Persisted)(record);
          return decoded._tag === "Some"
            ? [
                {
                  ...decoded.value,
                  headers: decoded.value.headers.map(([key, value]) => [key, value]),
                },
              ]
            : [];
        });
      });
      const state: SocketState = new Map();
      this.states.set(ws, state);
      const accepted: PersistedSubscription[] = [];
      const seen = new Set<string | number>();
      for (const record of records) {
        if (seen.has(record.requestId)) continue;
        seen.add(record.requestId);
        const topicExit = yield* Effect.exit(
          this.engine.createTopic<StringKey<Q>>(
            record.query as StringKey<Q>,
            record.params as OperationParams<Q[StringKey<Q>]>,
          ),
        );
        if (Exit.isFailure(topicExit)) {
          yield* this.rollback(ws, state);
          return yield* Effect.failCause(topicExit.cause);
        }
        const topic = topicExit.value;
        let entry = state.get(topic.name)?.find((e) => Equal.equals(e.topic, topic));
        if (!entry) {
          const sessions = new Map<string | number, Session>();
          const sub = yield* Effect.exit(
            this.engine.subscribe<StringKey<Q>>(topic, (e) => {
              for (const s of sessions.values()) if (s.queue) Queue.offerUnsafe(s.queue, e);
            }),
          );
          if (Exit.isFailure(sub)) {
            yield* this.rollback(ws, state);
            return yield* Effect.failCause(sub.cause);
          }
          entry = { topic, listenerId: sub.value, sessions };
          const bucket = state.get(topic.name) ?? [];
          bucket.push(entry);
          state.set(topic.name, bucket);
        }
        const acceptedRecord: PersistedSubscription = {
          ...record,
          headers: record.headers.map(([key, value]) => [key, value]),
        };
        entry.sessions.set(record.requestId, { ...acceptedRecord, topic });
        accepted.push(acceptedRecord);
      }
      yield* attachment(ws, { version: 1, subscriptions: accepted });
      return accepted;
    });
  }
  clear(ws: WebSocket): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const state = this.states.get(ws);
      if (!state) return;
      for (const entries of state.values())
        for (const entry of entries) {
          for (const s of entry.sessions.values()) if (s.queue) yield* Queue.shutdown(s.queue);
          yield* this.engine.unsubscribe(entry.listenerId);
        }
      this.states.delete(ws);
    });
  }
  private sessions(state: SocketState, exclude?: Topic): PersistedSubscription[] {
    return [...state.values()].flatMap((es) =>
      es
        .filter((e) => !exclude || !Equal.equals(e.topic, exclude))
        .flatMap((e) =>
          [...e.sessions.values()].map(({ queue: _, topic: __, ...session }) => session),
        ),
    );
  }
  private rollback(ws: WebSocket, state: SocketState): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      for (const es of state.values())
        for (const e of es) yield* this.engine.unsubscribe(e.listenerId).pipe(Effect.ignore);
      state.clear();
      this.states.delete(ws);
      yield* attachment(ws, empty).pipe(Effect.ignore);
    });
  }
}
