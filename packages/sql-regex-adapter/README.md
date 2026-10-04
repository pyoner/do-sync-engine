# @do-sync-engine/sql-regex-adapter

Turn plain SQL strings into `@do-sync-engine/core` queries and mutations. It reads `SELECT`, `INSERT`, `UPDATE` and `DELETE` statements with regular expressions to find the affected tables, so no SQL parser or extra dependency is needed. Works with Cloudflare Durable Object SQL storage and Node SQLite.

## Usage

```ts
import { makeSyncEngine } from "@do-sync-engine/core";
import { Effect } from "effect";
import { createAdapter } from "@do-sync-engine/sql-regex-adapter";

// `db` is a Node SQLite database or Cloudflare Durable Object `ctx.storage.sql`.
const adapt = createAdapter(db);
if (adapt instanceof Error) throw adapt;

const allTodos = adapt("SELECT * FROM todos ORDER BY id");
const addTodo = adapt("INSERT INTO todos (title) VALUES (?)");
if (allTodos instanceof Error) throw allTodos;
if (addTodo instanceof Error) throw addTodo;

// allTodos.tables is Set { "todos" }; pass both to makeSyncEngine.
const engine = Effect.runSync(makeSyncEngine({ queries: { allTodos }, mutations: { addTodo } }));
```

`run` returns an `Effect` that fails with `SqlAdapterError` when execution throws. Unsupported SQL returns a `SqlAdapterError` instead of throwing.

## Development

```bash
vp test    # tests
vp check   # format, lint, types
vp pack    # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
