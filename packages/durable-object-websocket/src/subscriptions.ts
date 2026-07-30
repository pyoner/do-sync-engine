import { Data, Effect, Equal, Exit } from "effect";

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

export class WebSocketOperationError extends Data.TaggedError("WebSocketOperationError")<{
  readonly cause: unknown;
}> {}

type DurableObjectWebSocketAttachment = { topics?: unknown[] };
type SubscriptionError<Q extends QueryMap<Q>> =
  | WebSocketOperationError
  | UnknownQueryError
  | OperationError<Q[StringKey<Q>]>;

const serializeAttachment = (ws: WebSocket, value: unknown) =>
  Effect.try({
    try: () => ws.serializeAttachment(value),
    catch: (cause) => new WebSocketOperationError({ cause }),
  });

export class SubscriptionRegistry<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  private readonly subscriptions = new Map<
    WebSocket,
    Map<
      string,
      Array<{
        topic: Topic;
        listenerId: ListenerId;
      }>
    >
  >();
  constructor(
    private readonly engine: SyncEngineInterface<Q, M>,
    private readonly send: (ws: WebSocket, message: unknown) => void,
  ) {}
  subscribe(
    ws: WebSocket,
    name: StringKey<Q>,
    params: readonly unknown[],
    requestId: string,
  ): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
      const topic = yield* this.engine.createTopic<StringKey<Q>>(
        name,
        params as OperationParams<Q[StringKey<Q>]>,
      );
      const value = yield* this.engine.query<StringKey<Q>>(topic);
      const initial = { type: "queryResult", requestId, topic, value };
      yield* Effect.try({
        try: () => JSON.stringify(initial),
        catch: (cause) => new WebSocketOperationError({ cause }),
      });
      const map =
        this.subscriptions.get(ws) ??
        new Map<string, Array<{ topic: Topic; listenerId: ListenerId }>>();
      const bucket = map.get(topic.name) ?? [];
      if (bucket.some(({ topic: existingTopic }) => Equal.equals(existingTopic, topic))) {
        yield* this.sendEffect(ws, initial);
        return;
      }
      const previous = yield* Effect.try({
        try: () => ws.deserializeAttachment(),
        catch: (cause) => new WebSocketOperationError({ cause }),
      });
      const listener = (event: { topic: Topic; value: unknown }) => {
        try {
          this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
        } catch {}
      };
      const listenerId = yield* this.engine.subscribe<StringKey<Q>>(topic, listener);
      yield* Effect.sync(() => {
        this.subscriptions.set(ws, map);
        map.set(topic.name, bucket);
        bucket.push({ topic, listenerId });
      });
      const topics = [...map.values()].flatMap((entries) => entries.map((entry) => entry.topic));
      yield* serializeAttachment(ws, { topics }).pipe(
        Effect.matchEffect({
          onFailure: (error) =>
            Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
              yield* this.engine.unsubscribe(listenerId);
              yield* Effect.sync(() => this.removeEntry(ws, map, topic.name, listenerId));
              return yield* Effect.fail(error);
            }),
          onSuccess: () => Effect.void,
        }),
      );
      yield* this.sendEffect(ws, initial).pipe(
        Effect.matchEffect({
          onFailure: (error) => this.rollbackSubscribe(ws, previous, map, topic, listenerId, error),
          onSuccess: () => Effect.void,
        }),
      );
    });
  }
  unsubscribe(ws: WebSocket, topic: Topic): Effect.Effect<boolean, WebSocketOperationError> {
    return Effect.gen(
      function* (this: SubscriptionRegistry<Q, M>) {
        const map = this.subscriptions.get(ws);
        const bucket = map?.get(topic.name);
        const index =
          bucket?.findIndex(({ topic: existingTopic }) => Equal.equals(existingTopic, topic)) ?? -1;
        if (!map || !bucket || index < 0) return false;
        const topics = [...map.values()].flatMap((entries) =>
          entries
            .filter((_, entryIndex) => entries !== bucket || entryIndex !== index)
            .map((entry) => entry.topic),
        );
        yield* serializeAttachment(ws, { topics });
        const entry = bucket[index];
        const removed = yield* this.engine.unsubscribe(entry.listenerId);
        yield* Effect.sync(() => {
          bucket.splice(index, 1);
          if (bucket.length === 0) map.delete(topic.name);
          if (map.size === 0) this.subscriptions.delete(ws);
        });
        return removed;
      }.bind(this),
    );
  }
  restore(ws: WebSocket, sendSnapshots = true): Effect.Effect<void, SubscriptionError<Q>> {
    return Effect.gen(
      function* (this: SubscriptionRegistry<Q, M>) {
        yield* this.clear(ws);
        const candidates = yield* Effect.sync(() => this.read(ws));
        const map = new Map<string, Array<{ topic: Topic; listenerId: ListenerId }>>();
        yield* Effect.sync(() => this.subscriptions.set(ws, map));
        const topics: Topic[] = [];
        const snapshots: Array<{ topic: Topic; value: unknown }> = [];
        for (const candidate of candidates) {
          if (typeof candidate !== "object" || candidate === null || Array.isArray(candidate))
            continue;
          if ("hash" in candidate) continue;
          const record = candidate as { name?: unknown; params?: unknown };
          if (typeof record.name !== "string" || !Array.isArray(record.params)) continue;
          const result = yield* Effect.exit(
            this.engine.createTopic<StringKey<Q>>(
              record.name as StringKey<Q>,
              record.params as OperationParams<Q[StringKey<Q>]>,
            ),
          );
          if (Exit.isFailure(result)) continue;
          const topic = result.value;
          const bucket = map.get(topic.name) ?? [];
          yield* Effect.sync(() => map.set(topic.name, bucket));
          if (bucket.some(({ topic: existingTopic }) => Equal.equals(existingTopic, topic)))
            continue;
          const listener = (event: { topic: Topic; value: unknown }) => {
            try {
              this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
            } catch {}
          };
          const typedTopic = topic as Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;
          const value = yield* this.engine.query<StringKey<Q>>(typedTopic);
          const listenerId = yield* this.engine.subscribe<StringKey<Q>>(typedTopic, listener);
          yield* Effect.sync(() => {
            topics.push(topic);
            bucket.push({ topic, listenerId });
          });
          if (sendSnapshots) snapshots.push({ topic, value });
        }
        yield* serializeAttachment(ws, { topics });
        if (sendSnapshots)
          yield* Effect.sync(() => {
            for (const snapshot of snapshots) this.send(ws, { type: "queryResult", ...snapshot });
          });
      }.bind(this),
    );
  }
  clear(ws: WebSocket): Effect.Effect<void> {
    return Effect.gen(
      function* (this: SubscriptionRegistry<Q, M>) {
        const map = this.subscriptions.get(ws);
        if (!map) return;
        for (const bucket of map.values())
          for (const entry of bucket) yield* this.engine.unsubscribe(entry.listenerId);
        yield* Effect.sync(() => this.subscriptions.delete(ws));
      }.bind(this),
    );
  }

  private sendEffect(
    ws: WebSocket,
    message: unknown,
  ): Effect.Effect<void, WebSocketOperationError> {
    return Effect.try({
      try: () => this.send(ws, message),
      catch: (cause) => new WebSocketOperationError({ cause }),
    });
  }

  private removeEntry(
    ws: WebSocket,
    map: Map<string, Array<{ topic: Topic; listenerId: ListenerId }>>,
    name: string,
    listenerId: ListenerId,
  ): void {
    const bucket = map.get(name);
    if (!bucket) return;
    const index = bucket.findIndex((entry) => entry.listenerId === listenerId);
    if (index < 0) return;
    bucket.splice(index, 1);
    if (bucket.length === 0) map.delete(name);
    if (map.size === 0) this.subscriptions.delete(ws);
  }

  private rollbackSubscribe(
    ws: WebSocket,
    previous: unknown,
    map: Map<string, Array<{ topic: Topic; listenerId: ListenerId }>>,
    topic: Topic,
    listenerId: ListenerId,
    error: SubscriptionError<Q>,
  ): Effect.Effect<void, SubscriptionError<Q>> {
    return serializeAttachment(ws, previous).pipe(
      Effect.matchEffect({
        onFailure: () =>
          Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
            yield* Effect.sync(() => ws.close(1011, "Subscription rollback failed"));
            yield* this.clear(ws);
          }),
        onSuccess: () =>
          Effect.gen({ self: this }, function* (this: SubscriptionRegistry<Q, M>) {
            yield* this.engine.unsubscribe(listenerId);
            yield* Effect.sync(() => this.removeEntry(ws, map, topic.name, listenerId));
            return yield* Effect.fail(error);
          }),
      }),
    );
  }
  private read(ws: WebSocket): unknown[] {
    const value = ws.deserializeAttachment() as DurableObjectWebSocketAttachment | undefined;
    return Array.isArray(value?.topics) ? value.topics : [];
  }
}
