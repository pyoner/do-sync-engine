import type {
  ListenerId,
  QueryMap,
  MutationMap,
  SyncEngineInterface,
  Topic,
  TopicHash,
  StringKey,
} from "@do-sync-engine/core";
import type { QueryResultMessage } from "./protocol.ts";

type DurableObjectWebSocketAttachment = { topics?: Topic[] };
export type QueryDefinitions<Q extends object> = {
  [K in keyof Q]: Q[K] & { run(...params: never[]): unknown };
};
type Entry = {
  topic: Topic;
  listenerId: ListenerId;
};
export class SubscriptionRegistry<Q extends object, M extends object> {
  private readonly subscriptions = new Map<WebSocket, Map<TopicHash, Entry>>();
  constructor(
    private readonly engine: SyncEngineInterface<QueryMap<Q>, MutationMap<M>>,
    private readonly queries: QueryDefinitions<Q>,
    private readonly send: (ws: WebSocket, message: unknown) => void,
  ) {}
  async subscribe(
    ws: WebSocket,
    name: StringKey<Q>,
    params: unknown[],
    requestId: string,
  ): Promise<void> {
    const topic = await this.engine.createTopic(name as never, params as never);
    const value = this.queries[name].run(...(params as never[]));
    const initial: QueryResultMessage<Q> = {
      type: "queryResult",
      requestId,
      topic: topic as never,
      value: value as never,
    };
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
    const listenerId = this.engine.subscribe(topic as never, listener as never);
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
  async restore(ws: WebSocket): Promise<void> {
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
      if (!Object.hasOwn(this.queries, candidate.name)) continue;
      const topic = await this.engine.createTopic(
        candidate.name as never,
        candidate.params as never,
      );
      if (topic.hash !== candidate.hash || map.has(topic.hash)) continue;
      const value = this.queries[candidate.name as StringKey<Q>].run(
        ...(candidate.params as never[]),
      );
      const listener = (event: { topic: Topic; value: unknown }) =>
        this.send(ws, { type: "queryResult", topic: event.topic, value: event.value });
      const listenerId = this.engine.subscribe(topic as never, listener as never);
      map.set(topic.hash, { topic, listenerId });
      topics.push(topic);
      snapshots.push({ topic, value });
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
