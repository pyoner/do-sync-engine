import { Effect, Equal, Exit, Queue, Schema, Stream } from "effect";
import {
  ListenerIdSchema,
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
  listenerId: ListenerIdSchema,
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
type QueryEvent = {
  readonly listenerId: ListenerId;
  readonly topic: Topic;
  readonly value: unknown;
};
type Session = PersistedSubscription & {
  readonly topic: Topic;
  queue?: Queue.Queue<QueryEvent, never>;
};
type Entry = {
  readonly topic: Topic;
  readonly engineListenerId: ListenerId;
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
        const state = this.states.get(ws) ?? new Map<string, Entry[]>();
        this.states.set(ws, state);
        const queue = yield* Queue.unbounded<QueryEvent>();
        const entry = yield* this.admitSession(state, session, queue);
        yield* attachment(ws, { version: 1, subscriptions: this.sessions(state) });
        return Stream.fromQueue(queue).pipe(
          Stream.ensuring(
            Effect.sync(() => {
              const current = entry.sessions.get(session.requestId);
              if (current?.queue === queue)
                entry.sessions.set(session.requestId, { ...current, queue: undefined });
            }),
          ),
        );
      }),
    );
  }

  private admitSession(
    state: SocketState,
    session: PersistedSubscription,
    queue?: Queue.Queue<QueryEvent, never>,
  ): Effect.Effect<Entry, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const topic = yield* this.engine.createTopic<StringKey<Q>>(
        session.query as StringKey<Q>,
        session.params as OperationParams<Q[StringKey<Q>]>,
      );
      const bucket = state.get(topic.name) ?? [];
      let entry = bucket.find((candidate) => Equal.equals(candidate.topic, topic));
      const accepted: Session = { ...session, topic, queue };
      if (entry) {
        const restored = entry.sessions.get(session.requestId);
        entry.sessions.set(session.requestId, accepted);
        if (queue && (!restored || restored.queue)) {
          let snapshot: unknown;
          const id = yield* this.engine.subscribe<StringKey<Q>>(topic, (event) => {
            snapshot = event.value;
          });
          yield* this.engine.unsubscribe(id);
          if (snapshot !== undefined)
            yield* Queue.offer(queue, { listenerId: session.listenerId, topic, value: snapshot });
        }
        return entry;
      }

      let initial: unknown;
      const sessions = new Map([[session.requestId, accepted]]);
      const engineListenerId = yield* this.engine.subscribe<StringKey<Q>>(topic, (event) => {
        if (initial === undefined) initial = event.value;
        else
          for (const active of sessions.values())
            if (active.queue)
              Queue.offerUnsafe(active.queue, {
                listenerId: active.listenerId,
                topic,
                value: event.value,
              });
      });
      entry = { topic, engineListenerId, sessions };
      bucket.push(entry);
      state.set(topic.name, bucket);
      if (queue && initial !== undefined)
        yield* Queue.offer(queue, { listenerId: session.listenerId, topic, value: initial });
      return entry;
    });
  }
  unsubscribe(ws: WebSocket, listenerId: ListenerId): Effect.Effect<boolean, Error> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const state = this.states.get(ws);
      if (!state) return false;
      let found: { entry: Entry; session: Session } | undefined;
      for (const entries of state.values())
        for (const entry of entries) {
          const session = [...entry.sessions.values()].find(
            (candidate) => candidate.listenerId === listenerId,
          );
          if (session) found = { entry, session };
        }
      if (!found) return false;
      yield* attachment(ws, { version: 1, subscriptions: this.sessions(state, listenerId) });
      found.entry.sessions.delete(found.session.requestId);
      if (found.entry.sessions.size === 0) {
        yield* this.engine.unsubscribe(found.entry.engineListenerId);
        const bucket = state.get(found.entry.topic.name) ?? [];
        const index = bucket.indexOf(found.entry);
        if (index >= 0) bucket.splice(index, 1);
        if (bucket.length === 0) state.delete(found.entry.topic.name);
      }
      if (found.session.queue) yield* Queue.shutdown(found.session.queue);
      return true;
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
        const admission = yield* Effect.exit(this.admitSession(state, record));
        if (Exit.isFailure(admission)) {
          yield* this.rollback(ws, state);
          return yield* Effect.failCause(admission.cause);
        }
        accepted.push(record);
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
          for (const session of entry.sessions.values())
            if (session.queue) yield* Queue.shutdown(session.queue);
          yield* this.engine.unsubscribe(entry.engineListenerId);
        }
      this.states.delete(ws);
    });
  }
  private sessions(state: SocketState, exclude?: ListenerId): PersistedSubscription[] {
    return [...state.values()].flatMap((entries) =>
      entries.flatMap((entry) =>
        [...entry.sessions.values()]
          .filter((session) => session.listenerId !== exclude)
          .map(({ queue: _, topic: __, ...session }) => session),
      ),
    );
  }
  private rollback(ws: WebSocket, state: SocketState): Effect.Effect<void> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      for (const entries of state.values())
        for (const entry of entries)
          yield* this.engine.unsubscribe(entry.engineListenerId).pipe(Effect.ignore);
      state.clear();
      this.states.delete(ws);
      yield* attachment(ws, empty).pipe(Effect.ignore);
    });
  }
}
