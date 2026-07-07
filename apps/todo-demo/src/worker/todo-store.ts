import { DurableObject } from "cloudflare:workers";
import { SyncEngine } from "@do-sync-engine/core";
import type { Mutation, Query, QueryResult, SubscriptionId } from "@do-sync-engine/core";
import type {
  ClientMessage,
  MutationMetadata,
  MutationResponse,
  ServerMessage,
  TodoQueryName,
  TodoQueryResults,
} from "../todo-protocol";
import { DurableObjectSqlStorage } from "./storage";

type TodoQueries = {
  [Name in TodoQueryName]: Query<[], TodoQueryResults[Name]>;
};

interface TodoMutations {
  addTodo: Mutation<[string], MutationMetadata>;
  toggleTodo: Mutation<[number], MutationMetadata>;
  deleteTodo: Mutation<[number], MutationMetadata>;
  clearCompleted: Mutation<[], MutationMetadata>;
}

interface WebSocketSubscriptionAttachment {
  selectors?: TodoQueryName[];
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )
`;

const TODO_QUERY_NAMES = [
  "allTodos",
  "incompleteTodos",
  "completedTodos",
  "todoCount",
] as const satisfies readonly TodoQueryName[];

function readTablesFromSql(sql: string) {
  const lower = sql.toLowerCase();
  return /\b(from|join)\s+todos\b/.test(lower) ? ["todos"] : [];
}

function writeTablesFromSql(sql: string) {
  const lower = sql.toLowerCase();
  return /^\s*(insert\s+into|update|delete\s+from)\s+todos\b/.test(lower) ? ["todos"] : [];
}

function isTodoQueryName(value: unknown): value is TodoQueryName {
  return typeof value === "string" && TODO_QUERY_NAMES.includes(value as TodoQueryName);
}

function parseQueryNames(value: unknown): TodoQueryName[] | string {
  if (!Array.isArray(value) || value.length === 0) {
    return "queries required";
  }

  const queries: TodoQueryName[] = [];
  for (const query of value) {
    if (!isTodoQueryName(query)) {
      return "Unknown query";
    }
    if (!queries.includes(query)) {
      queries.push(query);
    }
  }

  return queries;
}

function createQueries(storage: DurableObjectSqlStorage): TodoQueries {
  const allTodosSql = "SELECT id, title, completed, created_at FROM todos ORDER BY id";
  const incompleteTodosSql = "SELECT id, title FROM todos WHERE completed = 0 ORDER BY id";
  const completedTodosSql = "SELECT id, title FROM todos WHERE completed = 1 ORDER BY id";
  const todoCountSql = "SELECT COUNT(*) AS total_count FROM todos";

  return {
    allTodos: {
      tables: readTablesFromSql(allTodosSql),
      run: () =>
        storage.query(allTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
          completed: Number(row.completed),
          created_at: Number(row.created_at),
        })),
    },
    incompleteTodos: {
      tables: readTablesFromSql(incompleteTodosSql),
      run: () =>
        storage.query(incompleteTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
        })),
    },
    completedTodos: {
      tables: readTablesFromSql(completedTodosSql),
      run: () =>
        storage.query(completedTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
        })),
    },
    todoCount: {
      tables: readTablesFromSql(todoCountSql),
      run: () =>
        storage.query(todoCountSql).map((row) => ({
          total_count: Number(row.total_count),
        })),
    },
  };
}

function createMutations(storage: DurableObjectSqlStorage): TodoMutations {
  const addTodoSql = "INSERT INTO todos (title) VALUES (?)";
  const toggleTodoSql = "UPDATE todos SET completed = NOT completed WHERE id = ?";
  const deleteTodoSql = "DELETE FROM todos WHERE id = ?";
  const clearCompletedSql = "DELETE FROM todos WHERE completed = 1";

  return {
    addTodo: {
      tables: writeTablesFromSql(addTodoSql),
      run: (title) => storage.execute(addTodoSql, title),
    },
    toggleTodo: {
      tables: writeTablesFromSql(toggleTodoSql),
      run: (id) => storage.execute(toggleTodoSql, id),
    },
    deleteTodo: {
      tables: writeTablesFromSql(deleteTodoSql),
      run: (id) => storage.execute(deleteTodoSql, id),
    },
    clearCompleted: {
      tables: writeTablesFromSql(clearCompletedSql),
      run: () => storage.execute(clearCompletedSql),
    },
  } satisfies TodoMutations;
}

export class TodoStore extends DurableObject<Env> {
  private engine!: SyncEngine<TodoQueries, TodoMutations>;
  private queries!: TodoQueries;
  private subscriptions = new Map<WebSocket, Map<TodoQueryName, SubscriptionId>>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(SCHEMA);
      const storage = new DurableObjectSqlStorage(this.ctx.storage.sql);
      this.queries = createQueries(storage);
      const mutations = createMutations(storage);
      this.engine = new SyncEngine({
        queries: { ...this.queries },
        mutations: { ...mutations },
      });
      for (const ws of this.ctx.getWebSockets()) {
        this.subscriptions.set(ws, new Map());
        await this.subscribeQueries(ws, this.readAttachedQueries(ws));
      }
    });
  }

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    this.subscriptions.set(server, new Map());
    server.serializeAttachment({ selectors: [] } satisfies WebSocketSubscriptionAttachment);

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") {
      this.send(ws, { type: "error", message: "Expected text WebSocket message" });
      return;
    }

    let value: unknown;
    try {
      value = JSON.parse(message);
    } catch {
      this.send(ws, { type: "error", message: "Invalid JSON message" });
      return;
    }

    const parsed = this.parseClientMessage(value);
    if ("error" in parsed) {
      this.send(ws, {
        type: "error",
        requestId: parsed.requestId,
        message: parsed.error,
      });
      return;
    }

    try {
      switch (parsed.type) {
        case "subscribe":
          await this.subscribeQueries(ws, parsed.queries);
          return;
        case "unsubscribe":
          this.unsubscribeQueries(ws, parsed.queries);
          return;
        case "addTodo":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.publishMutation("addTodo", parsed.title),
          });
          return;
        case "toggleTodo":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.publishMutation("toggleTodo", parsed.todoId),
          });
          return;
        case "deleteTodo":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.publishMutation("deleteTodo", parsed.todoId),
          });
          return;
        case "clearCompleted":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.publishMutation("clearCompleted"),
          });
          return;
      }
    } catch (error) {
      this.send(ws, {
        type: "error",
        requestId: parsed.requestId,
        message: String(error),
      });
    }
  }

  webSocketClose(ws: WebSocket): void {
    this.clearSubscriptions(ws);
  }

  webSocketError(ws: WebSocket): void {
    this.clearSubscriptions(ws);
  }

  private send(ws: WebSocket, message: ServerMessage): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  private parseClientMessage(
    value: unknown,
  ): ClientMessage | { error: string; requestId?: string } {
    if (typeof value !== "object" || value === null) {
      return { error: "requestId required" };
    }

    const candidate = value as Record<string, unknown>;
    const requestId = typeof candidate.requestId === "string" ? candidate.requestId : undefined;
    if (!requestId?.trim()) {
      return { error: "requestId required" };
    }

    switch (candidate.type) {
      case "subscribe":
      case "unsubscribe": {
        const queries = parseQueryNames(candidate.queries);
        if (typeof queries === "string") {
          return { error: queries, requestId };
        }

        return { type: candidate.type, requestId, queries };
      }
      case "addTodo": {
        if (typeof candidate.title !== "string" || !candidate.title.trim()) {
          return { error: "title required", requestId };
        }

        return { type: "addTodo", requestId, title: candidate.title.trim() };
      }
      case "toggleTodo": {
        if (!this.isPositiveInteger(candidate.todoId)) {
          return { error: "todoId required", requestId };
        }

        return { type: "toggleTodo", requestId, todoId: candidate.todoId };
      }
      case "deleteTodo": {
        if (!this.isPositiveInteger(candidate.todoId)) {
          return { error: "todoId required", requestId };
        }

        return { type: "deleteTodo", requestId, todoId: candidate.todoId };
      }
      case "clearCompleted":
        return { type: "clearCompleted", requestId };
      default:
        return requestId
          ? { error: "Unknown message type", requestId }
          : { error: "Unknown message type" };
    }
  }

  private isPositiveInteger(value: unknown): value is number {
    return typeof value === "number" && Number.isInteger(value) && value > 0;
  }

  private readAttachedQueries(ws: WebSocket): TodoQueryName[] {
    const attachment = ws.deserializeAttachment();
    if (typeof attachment !== "object" || attachment === null) {
      return [];
    }

    const { selectors } = attachment as WebSocketSubscriptionAttachment;
    if (!Array.isArray(selectors)) {
      return [];
    }

    return selectors.filter(isTodoQueryName);
  }

  private sendQueryResult<Name extends TodoQueryName>(
    ws: WebSocket,
    name: Name,
    result: TodoQueryResults[Name],
  ): void {
    const message = {
      type: "queryResult",
      query: name,
      result,
    } as ServerMessage;
    this.send(ws, message);
  }

  private sendPublishedQueryResult(selection: QueryResult<TodoQueryName>): void {
    // subscription ids are unique
    for (const [ws, socketSubscriptions] of this.subscriptions) {
      const subId = socketSubscriptions.get(selection.query);
      if (subId === selection.subscriptionId) {
        this.sendQueryResult(ws, selection.query, selection.result as never);
        return; // at most one socket per query name
      }
    }
  }

  private async subscribeQuery(
    ws: WebSocket,
    socketSubscriptions: Map<TodoQueryName, SubscriptionId>,
    name: TodoQueryName,
  ): Promise<void> {
    if (!socketSubscriptions.has(name)) {
      socketSubscriptions.set(name, this.engine.subscribe(name));
    }

    const result = await this.queries[name].run();
    this.sendQueryResult(ws, name, result);
  }

  private async subscribeQueries(ws: WebSocket, queries: readonly TodoQueryName[]): Promise<void> {
    let socketSubscriptions = this.subscriptions.get(ws);
    if (!socketSubscriptions) {
      socketSubscriptions = new Map();
      this.subscriptions.set(ws, socketSubscriptions);
    }

    for (const name of queries) {
      await this.subscribeQuery(ws, socketSubscriptions, name);
    }

    ws.serializeAttachment({ selectors: [...socketSubscriptions.keys()] });
  }

  private unsubscribeQueries(ws: WebSocket, queries: readonly TodoQueryName[]): void {
    const socketSubscriptions = this.subscriptions.get(ws);
    if (!socketSubscriptions) {
      return;
    }

    for (const name of queries) {
      const subscriptionId = socketSubscriptions.get(name);
      if (subscriptionId !== undefined) {
        this.engine.unsubscribe(subscriptionId);
      }
      socketSubscriptions.delete(name);
    }

    ws.serializeAttachment({ selectors: [...socketSubscriptions.keys()] });
  }

  private clearSubscriptions(ws: WebSocket): void {
    const socketSubscriptions = this.subscriptions.get(ws);
    if (socketSubscriptions) {
      for (const subscriptionId of socketSubscriptions.values()) {
        this.engine.unsubscribe(subscriptionId);
      }
      this.subscriptions.delete(ws);
    }

    if (ws.readyState !== WebSocket.CLOSED) {
      ws.serializeAttachment({ selectors: [] });
    }
  }

  private async publishMutation<Name extends keyof TodoMutations>(
    mutation: Name,
    ...params: Parameters<TodoMutations[Name]["run"]>
  ): Promise<MutationResponse> {
    const result = await this.engine.mutate(mutation, ...params);
    for (const queryResult of result.results) {
      this.sendPublishedQueryResult(queryResult);
    }
    return { metadata: result.metadata };
  }
}
