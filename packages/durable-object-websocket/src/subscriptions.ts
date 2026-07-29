import { Effect, Exit } from "effect";

import {
  type ListenerId,
  type MutationMap,
  type OperationParams,
  type QueryMap,
  type StringKey,
  type SyncEngineInterface,
  type Topic,
  type TopicBuildError,
  type TopicHash,
  type TopicHasher,
  type UnknownQueryError,
} from "@do-sync-engine/core";

type DurableObjectWebSocketAttachment = { topics?: Topic[] };
type Entry = {
  topic: Topic;
  listenerId: ListenerId;
};
export class SubscriptionRegistry<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  private readonly subscriptions = new Map<WebSocket, Map<TopicHash, Entry>>();
  constructor(
    private readonly engine: SyncEngineInterface<Q, M>,
    private readonly send: (ws: WebSocket, message: unknown) => void,
  ) {}
  subscribe(
    ws: WebSocket,
    name: StringKey<Q>,
    params: readonly unknown[],
    requestId: string,
  ): Effect.Effect<void, TopicBuildError | UnknownQueryError, TopicHasher> {
    return Effect.gen(
      function* (this: SubscriptionRegistry<Q, M>) {
        const topic = yield* this.engine.createTopic<StringKey<Q>>(
          name,
          params as OperationParams<Q[StringKey<Q>]>,
        );
        const value = this.engine.query<StringKey<Q>>(topic);
        const initial = { type: "queryResult", requestId, topic, value };
        JSON.stringify(initial);
        const map = this.subscriptions.get(ws) ?? new Map<TopicHash, Entry>();
        this.subscriptions.set(ws, map);
        if (map.has(topic.hash)) {
          this.send(ws, initial);
          return;
        }
        const previous = ws.deserializeAttachment();
        const listener = (event: { topic: Topic; value: unknown }) =>
          this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
        const listenerId = this.engine.subscribe<StringKey<Q>>(topic, listener);
        map.set(topic.hash, { topic, listenerId });
        try {
          ws.serializeAttachment({ topics: [...map.values()].map((e) => e.topic) });
        } catch (error) {
          this.engine.unsubscribe(listenerId);
          map.delete(topic.hash);
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
          map.delete(topic.hash);
          throw error;
        }
      }.bind(this),
    );
  }
  unsubscribe(ws: WebSocket, hash: TopicHash): boolean {
    const map = this.subscriptions.get(ws);
    const entry = map?.get(hash);
    if (!map || !entry) return false;
    ws.serializeAttachment({
      topics: [...map.values()].filter((e) => e.topic.hash !== hash).map((e) => e.topic),
    });
    const removed = this.engine.unsubscribe(entry.listenerId);
    map.delete(hash);
    return removed;
  }
  restore(ws: WebSocket, sendSnapshots = true): Effect.Effect<void, never, TopicHasher> {
    return Effect.gen(
      function* (this: SubscriptionRegistry<Q, M>) {
        const existing = this.subscriptions.get(ws);
        if (existing) {
          for (const entry of existing.values()) this.engine.unsubscribe(entry.listenerId);
        }
        const candidates = this.read(ws);
        const map = new Map<TopicHash, Entry>();
        this.subscriptions.set(ws, map);
        const topics: Topic[] = [];
        const snapshots: Array<{ topic: Topic; value: unknown }> = [];
        for (const candidate of candidates) {
          if (
            typeof candidate?.name !== "string" ||
            !Array.isArray(candidate.params) ||
            typeof candidate.hash !== "string" ||
            !/^[0-9a-f]{64}$/.test(candidate.hash)
          )
            continue;
          const result = yield* Effect.exit(
            this.engine.createTopic<StringKey<Q>>(
              candidate.name as StringKey<Q>,
              candidate.params as OperationParams<Q[StringKey<Q>]>,
            ),
          );
          if (Exit.isFailure(result)) continue;
          const topic = result.value;
          if (topic.hash !== candidate.hash || map.has(topic.hash)) continue;
          const listener = (event: { topic: Topic; value: unknown }) =>
            this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
          const typedTopic = topic as Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;
          const value = this.engine.query<StringKey<Q>>(typedTopic);
          const listenerId = this.engine.subscribe<StringKey<Q>>(typedTopic, listener);
          topics.push(topic);
          map.set(topic.hash, { topic, listenerId });
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
    for (const entry of map.values()) this.engine.unsubscribe(entry.listenerId);
    this.subscriptions.delete(ws);
  }
  private read(ws: WebSocket): Topic[] {
    const value = ws.deserializeAttachment() as DurableObjectWebSocketAttachment | undefined;
    return Array.isArray(value?.topics) ? value.topics : [];
  }
}
