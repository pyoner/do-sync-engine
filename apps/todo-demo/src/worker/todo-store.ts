import { DurableObject } from "cloudflare:workers";
import { SyncEngine } from "@do-sync-engine/core";
import type { OperationParams, StringKey } from "@do-sync-engine/core";
import { parseClientMessage } from "../todo-protocol";
import type {
  MutationCommand,
  MutationResponse,
  ServerMessage,
  TodoMutations,
  TodoQueries,
  TodoQueryName,
  TodoQueryResults,
} from "../todo-protocol";
import { DurableObjectSqlStorage } from "./storage";
import { SubscriptionRegistry } from "./subscription-registry";
import { readTablesFromSql, writeTablesFromSql } from "@do-sync-engine/utils";
import type { SqlDatabase } from "@do-sync-engine/utils";

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )
`;

function createQueries(storage: SqlDatabase): TodoQueries {
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

function createMutations(storage: SqlDatabase): TodoMutations {
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
  private mutations!: TodoMutations;
  private registry!: SubscriptionRegistry;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(SCHEMA);
      const storage = new DurableObjectSqlStorage(this.ctx.storage.sql);
      const queries = createQueries(storage);
      this.mutations = createMutations(storage);
      this.engine = new SyncEngine({
        queries: { ...queries },
        mutations: { ...this.mutations },
      });
      this.registry = new SubscriptionRegistry(this.engine, queries, (ws, name, result) =>
        this.sendQueryResult(ws, name, result),
      );
      for (const ws of this.ctx.getWebSockets()) {
        await this.registry.restore(ws);
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
    await this.registry.restore(server);

    return new Response(null, { status: 101, webSocket: client });
  }

  async webSocketMessage(ws: WebSocket, message: string | ArrayBuffer): Promise<void> {
    if (typeof message !== "string") {
      this.send(ws, { type: "error", message: "Expected text WebSocket message" });
      return;
    }

    const parsed = parseClientMessage(message);
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
          await this.registry.subscribe(ws, parsed.queries);
          return;
        case "unsubscribe":
          this.registry.unsubscribe(ws, parsed.queries);
          return;
        default:
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: this.runMutation(parsed),
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
    this.registry.clear(ws);
  }

  webSocketError(ws: WebSocket): void {
    this.registry.clear(ws);
  }

  private runMutation(command: MutationCommand): MutationResponse {
    switch (command.type) {
      case "addTodo":
        return this.publishMutation("addTodo", [command.title]);
      case "toggleTodo":
        return this.publishMutation("toggleTodo", [command.todoId]);
      case "deleteTodo":
        return this.publishMutation("deleteTodo", [command.todoId]);
      case "clearCompleted":
        return this.publishMutation("clearCompleted", []);
    }
  }

  private publishMutation<Name extends StringKey<TodoMutations>>(
    mutation: Name,
    params: OperationParams<TodoMutations[Name]>,
  ): MutationResponse {
    this.engine.sync(mutation, params);
    return { affectedTables: [...this.mutations[mutation].tables] };
  }

  private send(ws: WebSocket, message: ServerMessage): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
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
}
