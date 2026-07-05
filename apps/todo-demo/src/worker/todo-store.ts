import { DurableObject } from "cloudflare:workers";
import { SyncEngine } from "@do-sync-engine/core";
import type { Broker, Mutator, Selector } from "@do-sync-engine/core";
import type {
  ClientMessage,
  MutationResponse as WireMutationResponse,
  ServerMessage,
  Todo,
} from "../todo-protocol";
import { DoSyncStorage } from "./do-storage";
import type { MutationMetadata, SqlRow } from "./do-storage";

export interface Env {
  TODO_STORE: DurableObjectNamespace<TodoStore>;
}

export type MutationResponse = WireMutationResponse;

interface TodoSelectors {
  allTodos: Selector<[], Todo[]>;
  incompleteTodos: Selector<[], SqlRow[]>;
  completedTodos: Selector<[], SqlRow[]>;
  todoCount: Selector<[], SqlRow[]>;
}

interface TodoMutators {
  addTodo: Mutator<[string], MutationMetadata>;
  toggleTodo: Mutator<[number], MutationMetadata>;
  deleteTodo: Mutator<[number], MutationMetadata>;
  clearCompleted: Mutator<[], MutationMetadata>;
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )
`;

function todoTablesFromSql(sql: string) {
  return /\btodos\b/i.test(sql) ? ["todos"] : [];
}
function createSelectors(storage: DoSyncStorage): TodoSelectors {
  const allTodosSql = "SELECT id, title, completed, created_at FROM todos ORDER BY id";
  const incompleteTodosSql = "SELECT id, title FROM todos WHERE completed = 0 ORDER BY id";
  const completedTodosSql = "SELECT id, title FROM todos WHERE completed = 1 ORDER BY id";
  const todoCountSql = "SELECT COUNT(*) AS total_count FROM todos";

  return {
    allTodos: {
      tables: todoTablesFromSql(allTodosSql),
      run: () =>
        storage.query(allTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
          completed: Number(row.completed),
          created_at: Number(row.created_at),
        })),
      callback: () => {},
    },
    incompleteTodos: {
      tables: todoTablesFromSql(incompleteTodosSql),
      run: () => storage.query(incompleteTodosSql),
      callback: () => {},
    },
    completedTodos: {
      tables: todoTablesFromSql(completedTodosSql),
      run: () => storage.query(completedTodosSql),
      callback: () => {},
    },
    todoCount: {
      tables: todoTablesFromSql(todoCountSql),
      run: () => storage.query(todoCountSql),
      callback: () => {},
    },
  };
}

function createMutators(storage: DoSyncStorage): TodoMutators {
  const addTodoSql = "INSERT INTO todos (title) VALUES (?)";
  const toggleTodoSql = "UPDATE todos SET completed = NOT completed WHERE id = ?";
  const deleteTodoSql = "DELETE FROM todos WHERE id = ?";
  const clearCompletedSql = "DELETE FROM todos WHERE completed = 1";

  return {
    addTodo: {
      tables: todoTablesFromSql(addTodoSql),
      run: (title) => storage.execute(addTodoSql, title),
    },
    toggleTodo: {
      tables: todoTablesFromSql(toggleTodoSql),
      run: (id) => storage.execute(toggleTodoSql, id),
    },
    deleteTodo: {
      tables: todoTablesFromSql(deleteTodoSql),
      run: (id) => storage.execute(deleteTodoSql, id),
    },
    clearCompleted: {
      tables: todoTablesFromSql(clearCompletedSql),
      run: () => storage.execute(clearCompletedSql),
    },
  } satisfies TodoMutators;
}

export class TodoStore extends DurableObject<Env> {
  private engine!: Broker;
  private selectors!: TodoSelectors;
  private mutators!: TodoMutators;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(SCHEMA);
      const storage = new DoSyncStorage(this.ctx.storage.sql);
      this.engine = new SyncEngine();
      this.selectors = createSelectors(storage);
      this.mutators = createMutators(storage);
      this.engine.subscribe({
        ...this.selectors.allTodos,
        callback: (todos) => {
          this.broadcast({ type: "todos", todos });
        },
      });
    });
  }

  async fetch(request: Request): Promise<Response> {
    if (request.headers.get("Upgrade")?.toLowerCase() !== "websocket") {
      return new Response("Expected WebSocket", { status: 426 });
    }

    const pair = new WebSocketPair();
    const [client, server] = Object.values(pair);
    this.ctx.acceptWebSocket(server);
    this.send(server, { type: "todos", todos: await this.getAllTodos() });

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
      let mutation: MutationResponse;
      switch (parsed.type) {
        case "addTodo":
          mutation = await this.addTodo(parsed.title);
          break;
        case "toggleTodo":
          mutation = await this.toggleTodo(parsed.todoId);
          break;
        case "deleteTodo":
          mutation = await this.deleteTodo(parsed.todoId);
          break;
        case "clearCompleted":
          mutation = await this.clearCompleted();
          break;
      }

      this.send(ws, { type: "mutation", requestId: parsed.requestId, mutation });
    } catch (error) {
      this.send(ws, {
        type: "error",
        requestId: parsed.requestId,
        message: String(error),
      });
    }
  }

  private send(ws: WebSocket, message: ServerMessage): void {
    if (ws.readyState === WebSocket.OPEN) {
      ws.send(JSON.stringify(message));
    }
  }

  private broadcast(message: ServerMessage): void {
    for (const ws of this.ctx.getWebSockets()) {
      this.send(ws, message);
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

  private async getAllTodos(): Promise<Todo[]> {
    return this.selectors.allTodos.run();
  }

  private async addTodo(title: string): Promise<MutationResponse> {
    return this.publishMutation(this.mutators.addTodo, title);
  }

  private async toggleTodo(id: number): Promise<MutationResponse> {
    return this.publishMutation(this.mutators.toggleTodo, id);
  }

  private async deleteTodo(id: number): Promise<MutationResponse> {
    return this.publishMutation(this.mutators.deleteTodo, id);
  }

  private async clearCompleted(): Promise<MutationResponse> {
    return this.publishMutation(this.mutators.clearCompleted);
  }

  private async publishMutation<Params extends unknown[]>(
    mutator: Mutator<Params, MutationMetadata>,
    ...params: Params
  ): Promise<MutationResponse> {
    let metadata: MutationMetadata | undefined;

    const capturingMutator: Mutator<Params, MutationMetadata> = {
      tables: mutator.tables,
      run: async (...runParams) => {
        metadata = await mutator.run(...runParams);
        return metadata;
      },
    };

    await this.engine.publish(capturingMutator, ...params);

    if (metadata === undefined) {
      throw new Error("Mutation did not produce metadata");
    }

    const recomputedSelectors = ["todoCount", "allTodos"];
    const recomputeResults: Record<string, unknown[]> = {
      todoCount: await this.selectors.todoCount.run(),
      allTodos: await this.getAllTodos(),
    };

    return { metadata, recomputedSelectors, recomputeResults };
  }
}
