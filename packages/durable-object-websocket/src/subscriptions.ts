import { Effect, Equal, Exit } from "effect";

import {
  type ListenerId,
  type MutationMap,
  type OperationParams,
  type QueryMap,
  type StringKey,
  type SyncEngineInterface,
  type Topic,
  type TopicBuildError,
  type UnknownQueryError,
} from "@do-sync-engine/core";

type DurableObjectWebSocketAttachment = { topics?: unknown[] };
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
  ): Effect.Effect<void, TopicBuildError | UnknownQueryError> {
    return Effect.gen(
      function* (this: SubscriptionRegistry<Q, M>) {
        const topic = yield* this.engine.createTopic<StringKey<Q>>(
          name,
          params as OperationParams<Q[StringKey<Q>]>,
        );
        const value = this.engine.query<StringKey<Q>>(topic);
        const initial = { type: "queryResult", requestId, topic, value };
        JSON.stringify(initial);
        const map =
          this.subscriptions.get(ws) ??
          new Map<string, Array<{ topic: Topic; listenerId: ListenerId }>>();
        this.subscriptions.set(ws, map);
        const bucket = map.get(topic.name) ?? [];
        map.set(topic.name, bucket);
        if (bucket.some(({ topic: existingTopic }) => Equal.equals(existingTopic, topic))) {
          this.send(ws, initial);
          return;
        }
        const previous = ws.deserializeAttachment();
        const listener = (event: { topic: Topic; value: unknown }) =>
          this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
        const listenerId = this.engine.subscribe<StringKey<Q>>(topic, listener);
        bucket.push({ topic, listenerId });
        try {
          ws.serializeAttachment({
            topics: [...map.values()].flatMap((entries) => entries.map((entry) => entry.topic)),
          });
        } catch (error) {
          this.engine.unsubscribe(listenerId);
          bucket.pop();
          throw error;
        }
        try {
          this.send(ws, initial);
        } catch (error) {
          try {
            ws.serializeAttachment(previous);
          } catch {
            ws.close(1011, "Subscription rollback failed");
            this.clear(ws);
            return;
          }
          this.engine.unsubscribe(listenerId);
          bucket.pop();
          throw error;
        }
      }.bind(this),
    );
  }
  unsubscribe(ws: WebSocket, topic: Topic): boolean {
    const map = this.subscriptions.get(ws);
    const bucket = map?.get(topic.name);
    const index =
      bucket?.findIndex(({ topic: existingTopic }) => Equal.equals(existingTopic, topic)) ?? -1;
    if (!map || !bucket || index < 0) return false;
    ws.serializeAttachment({
      topics: [...map.values()].flatMap((entries) =>
        entries
          .filter((_, entryIndex) => entries !== bucket || entryIndex !== index)
          .map((entry) => entry.topic),
      ),
    });
    const entry = bucket[index];
    const removed = this.engine.unsubscribe(entry.listenerId);
    bucket.splice(index, 1);
    if (bucket.length === 0) map.delete(topic.name);
    if (map.size === 0) this.subscriptions.delete(ws);
    return removed;
  }
  restore(ws: WebSocket, sendSnapshots = true): Effect.Effect<void> {
    return Effect.gen(
      function* (this: SubscriptionRegistry<Q, M>) {
        const existing = this.subscriptions.get(ws);
        if (existing) {
          for (const bucket of existing.values()) {
            for (const entry of bucket) this.engine.unsubscribe(entry.listenerId);
          }
        }
        const candidates = this.read(ws);
        const map = new Map<string, Array<{ topic: Topic; listenerId: ListenerId }>>();
        this.subscriptions.set(ws, map);
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
          map.set(topic.name, bucket);
          if (bucket.some(({ topic: existingTopic }) => Equal.equals(existingTopic, topic)))
            continue;
          const listener = (event: { topic: Topic; value: unknown }) =>
            this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
          const typedTopic = topic as Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;
          const value = this.engine.query<StringKey<Q>>(typedTopic);
          const listenerId = this.engine.subscribe<StringKey<Q>>(typedTopic, listener);
          topics.push(topic);
          bucket.push({ topic, listenerId });
          if (sendSnapshots) snapshots.push({ topic, value });
        }
        ws.serializeAttachment({ topics });
        for (const snapshot of snapshots) this.send(ws, { type: "queryResult", ...snapshot });
      }.bind(this),
    );
  }
  clear(ws: WebSocket): void {
    const map = this.subscriptions.get(ws);
    if (!map) return;
    for (const bucket of map.values()) {
      for (const entry of bucket) this.engine.unsubscribe(entry.listenerId);
    }
    this.subscriptions.delete(ws);
  }
  private read(ws: WebSocket): unknown[] {
    const value = ws.deserializeAttachment() as DurableObjectWebSocketAttachment | undefined;
    return Array.isArray(value?.topics) ? value.topics : [];
  }
}
