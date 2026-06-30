import { DurableObject } from "cloudflare:workers";
import { SyncEngine } from "@do-sync-engine/core";
import type { SqlRow, MutationResult } from "@do-sync-engine/core";
import { DoSyncStorage } from "./do-storage";

export interface Env {
  TODO_STORE: DurableObjectNamespace<TodoStore>;
}

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )
`;

const SELECTORS = {
  allTodos: "SELECT id, title, completed, created_at FROM todos ORDER BY id",
  incompleteTodos: "SELECT id, title FROM todos WHERE completed = 0 ORDER BY id",
  completedTodos: "SELECT id, title FROM todos WHERE completed = 1 ORDER BY id",
  todoCount: "SELECT COUNT(*) AS total_count FROM todos",
} as const;

const MUTATORS = {
  addTodo: "INSERT INTO todos (title) VALUES (?)",
  toggleTodo: "UPDATE todos SET completed = NOT completed WHERE id = ?",
  deleteTodo: "DELETE FROM todos WHERE id = ?",
  clearCompleted: "DELETE FROM todos WHERE completed = 1",
} as const;

export class TodoStore extends DurableObject<Env> {
  private engine!: SyncEngine;

  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env);
    void this.ctx.blockConcurrencyWhile(async () => {
      this.ctx.storage.sql.exec(SCHEMA);
      const storage = new DoSyncStorage(this.ctx.storage.sql);
      this.engine = new SyncEngine(storage);
      for (const [name, sql] of Object.entries(SELECTORS)) {
        this.engine.registerSelector(name, sql);
      }
      for (const [name, sql] of Object.entries(MUTATORS)) {
        this.engine.registerMutator(name, sql);
      }
    });
  }

  async getAllTodos(): Promise<SqlRow[]> {
    this.engine.query("todoCount");
    return this.engine.query("allTodos");
  }

  async addTodo(title: string): Promise<MutationResult> {
    return this.engine.mutate("addTodo", title);
  }

  async toggleTodo(id: number): Promise<MutationResult> {
    return this.engine.mutate("toggleTodo", id);
  }

  async deleteTodo(id: number): Promise<MutationResult> {
    return this.engine.mutate("deleteTodo", id);
  }

  async clearCompleted(): Promise<MutationResult> {
    return this.engine.mutate("clearCompleted");
  }
}
