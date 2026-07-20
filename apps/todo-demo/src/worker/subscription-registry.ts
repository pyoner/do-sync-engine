import type { ListenerId, SyncEngineInterface } from "@do-sync-engine/core";
import { isTodoQueryName } from "../todo-protocol";
import type { TodoMutations, TodoQueries, TodoQueryName, TodoQueryResults } from "../todo-protocol";

export interface WebSocketSubscriptionAttachment {
  queries?: TodoQueryName[];
}

export type QueryResultSender = <Name extends TodoQueryName>(
  ws: WebSocket,
  name: Name,
  result: TodoQueryResults[Name],
) => void;

export class SubscriptionRegistry {
  private readonly subscriptions = new Map<WebSocket, Map<TodoQueryName, ListenerId>>();
  private readonly engine: SyncEngineInterface<TodoQueries, TodoMutations>;
  private readonly queries: TodoQueries;
  private readonly sendQueryResult: QueryResultSender;

  constructor(
    engine: SyncEngineInterface<TodoQueries, TodoMutations>,
    queries: TodoQueries,
    sendQueryResult: QueryResultSender,
  ) {
    this.engine = engine;
    this.queries = queries;
    this.sendQueryResult = sendQueryResult;
  }

  async restore(ws: WebSocket): Promise<void> {
    this.subscriptions.set(ws, new Map());
    await this.subscribe(ws, this.readAttachedQueries(ws));
  }

  async subscribe(ws: WebSocket, names: readonly TodoQueryName[]): Promise<void> {
    let socketSubscriptions = this.subscriptions.get(ws);
    if (!socketSubscriptions) {
      socketSubscriptions = new Map();
      this.subscriptions.set(ws, socketSubscriptions);
    }

    for (const name of names) {
      if (!socketSubscriptions.has(name)) {
        const topic = await this.engine.createTopic(name, []);
        socketSubscriptions.set(
          name,
          this.engine.subscribe(topic, ({ value }) => {
            this.sendQueryResult(ws, name, value as never);
          }),
        );
      }

      this.sendQueryResult(ws, name, this.queries[name].run());
    }

    this.persist(ws);
  }

  unsubscribe(ws: WebSocket, names: readonly TodoQueryName[]): void {
    const socketSubscriptions = this.subscriptions.get(ws);
    if (!socketSubscriptions) {
      return;
    }

    for (const name of names) {
      const listenerId = socketSubscriptions.get(name);
      if (listenerId !== undefined) {
        this.engine.unsubscribe(listenerId);
      }
      socketSubscriptions.delete(name);
    }

    this.persist(ws);
  }

  clear(ws: WebSocket): void {
    const socketSubscriptions = this.subscriptions.get(ws);
    if (socketSubscriptions) {
      for (const listenerId of socketSubscriptions.values()) {
        this.engine.unsubscribe(listenerId);
      }
      this.subscriptions.delete(ws);
    }

    if (ws.readyState !== WebSocket.CLOSED) {
      this.persist(ws);
    }
  }

  private persist(ws: WebSocket): void {
    const queries = [...(this.subscriptions.get(ws)?.keys() ?? [])];
    ws.serializeAttachment({ queries } satisfies WebSocketSubscriptionAttachment);
  }

  private readAttachedQueries(ws: WebSocket): TodoQueryName[] {
    const attachment = ws.deserializeAttachment();
    if (typeof attachment !== "object" || attachment === null) {
      return [];
    }

    const { queries } = attachment as WebSocketSubscriptionAttachment;
    if (!Array.isArray(queries)) {
      return [];
    }

    return queries.filter(isTodoQueryName);
  }
}
