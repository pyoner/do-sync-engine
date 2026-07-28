import { SyncEngine } from "@do-sync-engine/core";
import { DurableObjectWebSocket } from "@do-sync-engine/durable-object-websocket";
import type { TodoMutations, TodoQueries } from "../todo-protocol";
import { DurableObjectSqlStorage } from "./storage";
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
        storage
          .query(incompleteTodosSql)
          .map((row) => ({ id: Number(row.id), title: String(row.title) })),
    },
    completedTodos: {
      tables: readTablesFromSql(completedTodosSql),
      run: () =>
        storage
          .query(completedTodosSql)
          .map((row) => ({ id: Number(row.id), title: String(row.title) })),
    },
    todoCount: {
      tables: readTablesFromSql(todoCountSql),
      run: () =>
        storage.query(todoCountSql).map((row) => ({ total_count: Number(row.total_count) })),
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
      run: (title) => storage.execute(addTodoSql, title as string),
    },
    toggleTodo: {
      tables: writeTablesFromSql(toggleTodoSql),
      run: (id) => storage.execute(toggleTodoSql, id as number),
    },
    deleteTodo: {
      tables: writeTablesFromSql(deleteTodoSql),
      run: (id) => storage.execute(deleteTodoSql, id as number),
    },
    clearCompleted: {
      tables: writeTablesFromSql(clearCompletedSql),
      run: () => storage.execute(clearCompletedSql),
    },
  } satisfies TodoMutations;
}
export class TodoStore extends DurableObjectWebSocket<Env, TodoQueries, TodoMutations> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, async () => {
      ctx.storage.sql.exec(SCHEMA);
      const storage = new DurableObjectSqlStorage(ctx.storage.sql);
      const queries = createQueries(storage);
      const mutations = createMutations(storage);
      const engine = new SyncEngine<TodoQueries, TodoMutations>({
        queries,
        mutations,
      });
      return { engine };
    });
  }
}
