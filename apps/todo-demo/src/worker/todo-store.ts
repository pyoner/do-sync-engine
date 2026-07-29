import { Effect } from "effect";
import { SyncEngine } from "@do-sync-engine/core";
import { DurableObjectWebSocket } from "@do-sync-engine/durable-object-websocket";
import type { Todo, TodoCount, TodoMutations, TodoQueries, TodoSummary } from "../todo-protocol";
import { DurableObjectSqlStorage } from "./storage";
import { createAdapter, SqlAdapterError, type SqlRow } from "@do-sync-engine/sql-regex-adapter";
type SqlDatabase = InstanceType<typeof DurableObjectSqlStorage>;

const SCHEMA = `
  CREATE TABLE IF NOT EXISTS todos (
    id INTEGER PRIMARY KEY AUTOINCREMENT,
    title TEXT NOT NULL,
    completed INTEGER NOT NULL DEFAULT 0,
    created_at INTEGER NOT NULL DEFAULT (unixepoch())
  )
`;

function createQueries(storage: SqlDatabase): Effect.Effect<TodoQueries, SqlAdapterError> {
  const allTodosSql = "SELECT id, title, completed, created_at FROM todos ORDER BY id";
  const incompleteTodosSql = "SELECT id, title FROM todos WHERE completed = 0 ORDER BY id";
  const completedTodosSql = "SELECT id, title FROM todos WHERE completed = 1 ORDER BY id";
  const todoCountSql = "SELECT COUNT(*) AS total_count FROM todos";

  return Effect.gen(function* (): Effect.gen.Return<TodoQueries, SqlAdapterError, never> {
    const allTodosTables = yield* storage.tables(allTodosSql);
    const incompleteTodosTables = yield* storage.tables(incompleteTodosSql);
    const completedTodosTables = yield* storage.tables(completedTodosSql);
    const todoCountTables = yield* storage.tables(todoCountSql);

    return {
      allTodos: {
        tables: allTodosTables,
        run: () =>
          storage.query(allTodosSql).pipe(
            Effect.map<SqlRow[], Todo[]>((rows) =>
              rows.map((row) => ({
                id: Number(row.id),
                title: String(row.title),
                completed: Number(row.completed),
                created_at: Number(row.created_at),
              })),
            ),
          ),
      },
      incompleteTodos: {
        tables: incompleteTodosTables,
        run: () =>
          storage
            .query(incompleteTodosSql)
            .pipe(
              Effect.map<SqlRow[], TodoSummary[]>((rows) =>
                rows.map((row) => ({ id: Number(row.id), title: String(row.title) })),
              ),
            ),
      },
      completedTodos: {
        tables: completedTodosTables,
        run: () =>
          storage
            .query(completedTodosSql)
            .pipe(
              Effect.map<SqlRow[], TodoSummary[]>((rows) =>
                rows.map((row) => ({ id: Number(row.id), title: String(row.title) })),
              ),
            ),
      },
      todoCount: {
        tables: todoCountTables,
        run: () =>
          storage
            .query(todoCountSql)
            .pipe(
              Effect.map<SqlRow[], TodoCount[]>((rows) =>
                rows.map((row) => ({ total_count: Number(row.total_count) })),
              ),
            ),
      },
    } satisfies TodoQueries;
  });
}

function createMutations(storage: SqlDatabase): Effect.Effect<TodoMutations, SqlAdapterError> {
  const addTodoSql = "INSERT INTO todos (title) VALUES (?)";
  const toggleTodoSql = "UPDATE todos SET completed = NOT completed WHERE id = ?";
  const deleteTodoSql = "DELETE FROM todos WHERE id = ?";
  const clearCompletedSql = "DELETE FROM todos WHERE completed = 1";

  return Effect.gen(function* (): Effect.gen.Return<TodoMutations, SqlAdapterError, never> {
    const addTodoTables = yield* storage.tables(addTodoSql);
    const toggleTodoTables = yield* storage.tables(toggleTodoSql);
    const deleteTodoTables = yield* storage.tables(deleteTodoSql);
    const clearCompletedTables = yield* storage.tables(clearCompletedSql);

    return {
      addTodo: {
        tables: addTodoTables,
        run: (title: string) => storage.execute(addTodoSql, title),
      },
      toggleTodo: {
        tables: toggleTodoTables,
        run: (id: number) => storage.execute(toggleTodoSql, id),
      },
      deleteTodo: {
        tables: deleteTodoTables,
        run: (id: number) => storage.execute(deleteTodoSql, id),
      },
      clearCompleted: {
        tables: clearCompletedTables,
        run: () => storage.execute(clearCompletedSql),
      },
    } satisfies TodoMutations;
  });
}
export class TodoStore extends DurableObjectWebSocket<Cloudflare.Env, TodoQueries, TodoMutations> {
  constructor(ctx: DurableObjectState, env: Cloudflare.Env) {
    super(ctx, env, () =>
      Effect.runPromise(
        Effect.gen(function* (): Effect.gen.Return<
          { readonly engine: SyncEngine<TodoQueries, TodoMutations> },
          SqlAdapterError,
          never
        > {
          yield* Effect.try({
            try: () => ctx.storage.sql.exec(SCHEMA),
            catch: (cause) => SqlAdapterError.make({ cause, operation: "schema" }),
          });
          const adapter = yield* createAdapter(ctx.storage.sql);
          const storage = new DurableObjectSqlStorage(ctx.storage.sql, adapter);
          const queries = yield* createQueries(storage);
          const mutations = yield* createMutations(storage);
          return {
            engine: new SyncEngine<TodoQueries, TodoMutations>({ queries, mutations }),
          };
        }),
      ),
    );
  }
}
