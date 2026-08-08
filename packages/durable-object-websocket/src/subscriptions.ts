import * as errore from "errore";
import type {
  ListenerId,
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  SyncEngineInterface,
  Topic,
  TopicHash,
} from "@do-sync-engine/core";
class SubscriptionPersistenceError extends errore.createTaggedError({
  name: "SubscriptionPersistenceError",
  message: "Failed to persist subscription",
}) {}
type DurableObjectWebSocketAttachment = { topics?: Topic[] };
type Entry = { topic: Topic; listenerId: ListenerId };
export class SubscriptionRegistry<Q extends QueryMap<Q>, M extends MutationMap<M>> {
  private readonly subscriptions = new Map<WebSocket, Map<TopicHash, Entry>>();
  constructor(
    private readonly engine: SyncEngineInterface<Q, M>,
    private readonly send: (ws: WebSocket, message: unknown) => void,
  ) {}
  async subscribe(
    ws: WebSocket,
    name: StringKey<Q>,
    params: unknown[],
    requestId: string,
  ): Promise<void | Error> {
    const topic = await this.engine.createTopic<StringKey<Q>>(
      name,
      params as OperationParams<Q[StringKey<Q>]>,
    );
    if (topic instanceof Error) return topic;
    const value = this.engine.query<StringKey<Q>>(topic);
    if (value instanceof Error) return value;
    const initial = { type: "queryResult", requestId, topic, value };
    const map = this.subscriptions.get(ws) ?? new Map<TopicHash, Entry>();
    this.subscriptions.set(ws, map);
    if (map.has(topic.hash)) {
      this.send(ws, initial);
      return;
    }
    const listener = (event: { topic: Topic; value: unknown }) =>
      this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
    const listenerId = this.engine.subscribe<StringKey<Q>>(topic, listener);
    if (listenerId instanceof Error) return listenerId;
    map.set(topic.hash, { topic, listenerId });
    const persisted = errore.try({
      try: () => ws.serializeAttachment({ topics: [...map.values()].map((entry) => entry.topic) }),
      catch: (cause) => new SubscriptionPersistenceError({ cause }),
    });
    if (persisted instanceof Error) {
      this.engine.unsubscribe(listenerId);
      map.delete(topic.hash);
      return persisted;
    }
    this.send(ws, initial);
  }
  unsubscribe(ws: WebSocket, hash: TopicHash): boolean {
    const map = this.subscriptions.get(ws);
    const entry = map?.get(hash);
    if (!map || !entry) return false;
    ws.serializeAttachment({
      topics: [...map.values()]
        .filter((item) => item.topic.hash !== hash)
        .map((item) => item.topic),
    });
    const removed = this.engine.unsubscribe(entry.listenerId);
    map.delete(hash);
    return removed;
  }
  async restore(ws: WebSocket, sendSnapshots = true): Promise<void> {
    const existing = this.subscriptions.get(ws);
    if (existing) for (const entry of existing.values()) this.engine.unsubscribe(entry.listenerId);
    const map = new Map<TopicHash, Entry>();
    this.subscriptions.set(ws, map);
    const topics: Topic[] = [];
    const snapshots: Array<{ topic: Topic; value: unknown }> = [];
    for (const candidate of this.read(ws)) {
      if (
        typeof candidate?.name !== "string" ||
        !Array.isArray(candidate.params) ||
        typeof candidate.hash !== "string" ||
        !/^[0-9a-f]{64}$/.test(candidate.hash)
      )
        continue;
      const topic = await this.engine.createTopic<StringKey<Q>>(
        candidate.name as StringKey<Q>,
        candidate.params as OperationParams<Q[StringKey<Q>]>,
      );
      if (topic instanceof Error || topic.hash !== candidate.hash || map.has(topic.hash)) continue;
      const typedTopic = topic as Topic<StringKey<Q>, OperationParams<Q[StringKey<Q>]>>;
      const value = this.engine.query<StringKey<Q>>(typedTopic);
      if (value instanceof Error) continue;
      const listener = (event: { topic: Topic; value: unknown }) =>
        this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
      const listenerId = this.engine.subscribe<StringKey<Q>>(typedTopic, listener);
      if (listenerId instanceof Error) continue;
      topics.push(topic);
      map.set(topic.hash, { topic, listenerId });
      if (sendSnapshots) snapshots.push({ topic, value });
    }
    ws.serializeAttachment({ topics });
    for (const snapshot of snapshots) this.send(ws, { type: "queryResult", ...snapshot });
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
