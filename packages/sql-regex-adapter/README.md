# @do-sync-engine/sql-regex-adapter

Turn plain SQL strings into `@do-sync-engine/core` queries and mutations. It reads `SELECT`, `INSERT`, `UPDATE` and `DELETE` statements with regular expressions to find the affected tables, so no SQL parser or extra dependency is needed. Works with Cloudflare Durable Object SQL storage and Node SQLite.

## Usage

```ts
import { SyncEngine } from "@do-sync-engine/core";
import { createAdapter } from "@do-sync-engine/sql-regex-adapter";

// `db` is a Node SQLite database or Cloudflare Durable Object `ctx.storage.sql`.
const adapt = createAdapter(db);
if (adapt instanceof Error) throw adapt;

const allTodos = adapt("SELECT * FROM todos ORDER BY id");
const addTodo = adapt("INSERT INTO todos (title) VALUES (?)");
if (allTodos instanceof Error) throw allTodos;
if (addTodo instanceof Error) throw addTodo;

// allTodos.tables is Set { "todos" }; pass both to SyncEngine.
const engine = new SyncEngine({ queries: { allTodos }, mutations: { addTodo } });
```

Unsupported SQL returns a `SqlAdapterError` instead of throwing.

## Development

```bash
vp test    # tests
vp check   # format, lint, types
vp pack    # build package
```
