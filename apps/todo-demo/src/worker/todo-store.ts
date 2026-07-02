import { DurableObject } from "cloudflare:workers";
import { SyncEngine } from "@do-sync-engine/core";
import type { MutationResult, Mutator, Selector } from "@do-sync-engine/core";
import { DoSyncStorage } from "./do-storage";
import type { MutationMetadata, SqlRow } from "./do-storage";

export interface Env {
  TODO_STORE: DurableObjectNamespace<TodoStore>;
}

export interface MutationResponse {
  metadata: MutationMetadata;
  recomputedSelectors: string[];
  recomputeResults: Record<string, SqlRow[]>;
}

interface TodoSelectors {
  allTodos: Selector<[], SqlRow[]>;
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
      run: () => storage.query(allTodosSql),
    },
    incompleteTodos: {
      tables: todoTablesFromSql(incompleteTodosSql),
      run: () => storage.query(incompleteTodosSql),
    },
    completedTodos: {
      tables: todoTablesFromSql(completedTodosSql),
      run: () => storage.query(completedTodosSql),
    },
    todoCount: {
      tables: todoTablesFromSql(todoCountSql),
      run: () => storage.query(todoCountSql),
    },
  } satisfies Record<string, Selector<[], SqlRow[]>>;
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
  private engine!: SyncEngine;
  private selectors!: TodoSelectors;
  private mutators!: TodoMutators;
  private selectorNames = new WeakMap<object, string>();

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(SCHEMA);
      const storage = new DoSyncStorage(this.ctx.storage.sql);
      this.engine = new SyncEngine();
      this.selectors = createSelectors(storage);
      this.mutators = createMutators(storage);

      for (const [name, selector] of Object.entries(this.selectors)) {
        this.selectorNames.set(selector, name);
      }
    });
  }

  async getAllTodos(): Promise<SqlRow[]> {
    await this.engine.query(this.selectors.todoCount);
    return this.engine.query(this.selectors.allTodos);
  }

  async addTodo(title: string): Promise<MutationResponse> {
    const result = await this.engine.mutate(this.mutators.addTodo, title);
    return this.toMutationResponse(result);
  }

  async toggleTodo(id: number): Promise<MutationResponse> {
    const result = await this.engine.mutate(this.mutators.toggleTodo, id);
    return this.toMutationResponse(result);
  }

  async deleteTodo(id: number): Promise<MutationResponse> {
    const result = await this.engine.mutate(this.mutators.deleteTodo, id);
    return this.toMutationResponse(result);
  }

  async clearCompleted(): Promise<MutationResponse> {
    const result = await this.engine.mutate(this.mutators.clearCompleted);
    return this.toMutationResponse(result);
  }

  private toMutationResponse(result: MutationResult<MutationMetadata>): MutationResponse {
    const recomputedSelectors: string[] = [];
    const recomputeResults: Record<string, SqlRow[]> = {};

    for (const entry of result.recomputedSelectors) {
      const selectorName = this.selectorNames.get(entry.selector);
      if (!selectorName) {
        throw new Error("Unknown recomputed selector");
      }

      recomputedSelectors.push(selectorName);
      recomputeResults[selectorName] = entry.result as SqlRow[];
    }

    return {
      metadata: result.metadata,
      recomputedSelectors,
      recomputeResults,
    };
  }
}
