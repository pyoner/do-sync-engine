import { SyncEngine } from "@do-sync-engine/core";
import { DurableObjectWebSocket } from "@do-sync-engine/durable-object-websocket";
import type { TodoMutations, TodoQueries } from "../todo-protocol";
import { DurableObjectSqlStorage } from "./storage";

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )
`;

function createQueries(storage: DurableObjectSqlStorage): TodoQueries {
  const allTodosSql = "SELECT id, title, completed, created_at FROM todos ORDER BY id";
  const incompleteTodosSql =
    "SELECT id, title, completed FROM todos WHERE completed = 0 ORDER BY id";
  const completedTodosSql =
    "SELECT id, title, completed FROM todos WHERE completed = 1 ORDER BY id";

  return {
    allTodos: {
      tables: storage.tables(allTodosSql),
      run: () =>
        storage.query(allTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
          completed: Number(row.completed),
          created_at: Number(row.created_at),
        })),
    },
    incompleteTodos: {
      tables: storage.tables(incompleteTodosSql),
      run: () =>
        storage.query(incompleteTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
          completed: Number(row.completed),
        })),
    },
    completedTodos: {
      tables: storage.tables(completedTodosSql),
      run: () =>
        storage.query(completedTodosSql).map((row) => ({
          id: Number(row.id),
          title: String(row.title),
          completed: Number(row.completed),
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
      tables: storage.tables(addTodoSql),
      run: (title) => storage.execute(addTodoSql, title),
    },
    toggleTodo: {
      tables: storage.tables(toggleTodoSql),
      run: (id) => storage.execute(toggleTodoSql, id),
    },
    deleteTodo: {
      tables: storage.tables(deleteTodoSql),
      run: (id) => storage.execute(deleteTodoSql, id),
    },
    clearCompleted: {
      tables: storage.tables(clearCompletedSql),
      run: () => storage.execute(clearCompletedSql),
    },
  };
}
export class TodoStore extends DurableObjectWebSocket<Env, TodoQueries, TodoMutations> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(ctx, env, () => {
      ctx.storage.sql.exec(SCHEMA);
      const storage = new DurableObjectSqlStorage(ctx.storage.sql);
      return new SyncEngine<WebSocket, TodoQueries, TodoMutations, Disposable>({
        queries: createQueries(storage),
        mutations: createMutations(storage),
      });
    });
  }
}
