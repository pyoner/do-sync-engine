import { DurableObject } from "cloudflare:workers";
import { SyncEngine } from "@do-sync-engine/core";
import type { Broker, Mutator, Selector, Unsubscribe } from "@do-sync-engine/core";
import type {
  ClientMessage,
  MutationResponse as WireMutationResponse,
  ServerMessage,
  TodoSelectorName,
  TodoSelectorResults,
} from "../todo-protocol";
import { DoSyncStorage } from "./do-storage";
import type { MutationMetadata } from "./do-storage";

export interface Env {
  TODO_STORE: DurableObjectNamespace<TodoStore>;
}

export type MutationResponse = WireMutationResponse;

type SelectorDefinition<Result> = Selector<[], Result>;
type TodoSelectors = {
  [Name in TodoSelectorName]: SelectorDefinition<TodoSelectorResults[Name]>;
};

interface TodoMutators {
  addTodo: Mutator<[string], MutationMetadata>;
  toggleTodo: Mutator<[number], MutationMetadata>;
  deleteTodo: Mutator<[number], MutationMetadata>;
  clearCompleted: Mutator<[], MutationMetadata>;
}

interface WebSocketSubscriptionAttachment {
  selectors?: TodoSelectorName[];
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )
`;

const TODO_SELECTOR_NAMES = [
  "allTodos",
  "incompleteTodos",
  "completedTodos",
  "todoCount",
] as const satisfies readonly TodoSelectorName[];

function todoTablesFromSql(sql: string) {
  return /\btodos\b/i.test(sql) ? ["todos"] : [];
}

function isTodoSelectorName(value: unknown): value is TodoSelectorName {
  return typeof value === "string" && TODO_SELECTOR_NAMES.includes(value as TodoSelectorName);
}

function parseSelectorNames(value: unknown): TodoSelectorName[] | string {
  if (!Array.isArray(value) || value.length === 0) {
    return "selectors required";
  }

  const selectors: TodoSelectorName[] = [];
  for (const selector of value) {
    if (!isTodoSelectorName(selector)) {
      return "Unknown selector";
    }
    if (!selectors.includes(selector)) {
      selectors.push(selector);
    }
  }

  return selectors;
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
    },
    incompleteTodos: {
      tables: todoTablesFromSql(incompleteTodosSql),
      run: () =>
        storage.query(incompleteTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
        })),
    },
    completedTodos: {
      tables: todoTablesFromSql(completedTodosSql),
      run: () =>
        storage.query(completedTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
        })),
    },
    todoCount: {
      tables: todoTablesFromSql(todoCountSql),
      run: () =>
        storage.query(todoCountSql).map((row) => ({
          total_count: Number(row.total_count),
        })),
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
  private subscriptions = new Map<WebSocket, Map<TodoSelectorName, Unsubscribe>>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(SCHEMA);
      const storage = new DoSyncStorage(this.ctx.storage.sql);
      this.engine = new SyncEngine();
      this.selectors = createSelectors(storage);
      this.mutators = createMutators(storage);

      for (const ws of this.ctx.getWebSockets()) {
        this.subscriptions.set(ws, new Map());
        await this.subscribeSelectors(ws, this.readAttachedSelectors(ws));
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
          await this.subscribeSelectors(ws, parsed.selectors);
          return;
        case "unsubscribe":
          this.unsubscribeSelectors(ws, parsed.selectors);
          return;
        case "addTodo":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.addTodo(parsed.title),
          });
          return;
        case "toggleTodo":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.toggleTodo(parsed.todoId),
          });
          return;
        case "deleteTodo":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.deleteTodo(parsed.todoId),
          });
          return;
        case "clearCompleted":
          this.send(ws, {
            type: "mutation",
            requestId: parsed.requestId,
            mutation: await this.clearCompleted(),
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
        const selectors = parseSelectorNames(candidate.selectors);
        if (typeof selectors === "string") {
          return { error: selectors, requestId };
        }

        return { type: candidate.type, requestId, selectors };
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

  private readAttachedSelectors(ws: WebSocket): TodoSelectorName[] {
    const attachment = ws.deserializeAttachment();
    if (typeof attachment !== "object" || attachment === null) {
      return [];
    }

    const { selectors } = attachment as WebSocketSubscriptionAttachment;
    if (!Array.isArray(selectors)) {
      return [];
    }

    return selectors.filter(isTodoSelectorName);
  }

  private sendSelectorResult<Name extends TodoSelectorName>(
    ws: WebSocket,
    name: Name,
    result: TodoSelectorResults[Name],
  ): void {
    const message = {
      type: "selectorResult",
      selector: name,
      result,
    } as ServerMessage;
    this.send(ws, message);
  }

  private async subscribeSelector<Name extends TodoSelectorName>(
    ws: WebSocket,
    socketSubscriptions: Map<TodoSelectorName, Unsubscribe>,
    name: Name,
  ): Promise<void> {
    const selector = this.selectors[name];
    if (!socketSubscriptions.has(name)) {
      socketSubscriptions.set(
        name,
        this.engine.subscribe(selector, [], (result) => {
          this.sendSelectorResult(ws, name, result);
        }),
      );
    }

    const result = await selector.run();
    this.sendSelectorResult(ws, name, result);
  }

  private async subscribeSelectors(
    ws: WebSocket,
    selectors: readonly TodoSelectorName[],
  ): Promise<void> {
    let socketSubscriptions = this.subscriptions.get(ws);
    if (!socketSubscriptions) {
      socketSubscriptions = new Map();
      this.subscriptions.set(ws, socketSubscriptions);
    }

    for (const name of selectors) {
      await this.subscribeSelector(ws, socketSubscriptions, name);
    }

    ws.serializeAttachment({ selectors: [...socketSubscriptions.keys()] });
  }

  private unsubscribeSelectors(ws: WebSocket, selectors: readonly TodoSelectorName[]): void {
    const socketSubscriptions = this.subscriptions.get(ws);
    if (!socketSubscriptions) {
      return;
    }

    for (const name of selectors) {
      socketSubscriptions.get(name)?.();
      socketSubscriptions.delete(name);
    }

    ws.serializeAttachment({ selectors: [...socketSubscriptions.keys()] });
  }

  private clearSubscriptions(ws: WebSocket): void {
    const socketSubscriptions = this.subscriptions.get(ws);
    if (socketSubscriptions) {
      for (const unsubscribe of socketSubscriptions.values()) {
        unsubscribe();
      }
      this.subscriptions.delete(ws);
    }

    if (ws.readyState !== WebSocket.CLOSED) {
      ws.serializeAttachment({ selectors: [] });
    }
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

    return { metadata };
  }
}
