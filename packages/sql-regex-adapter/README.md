# @do-sync-engine/sql-regex-adapter

Turn plain SQL strings into `@do-sync-engine/core` queries and mutations. It reads `SELECT`, `INSERT`, `UPDATE` and `DELETE` statements with regular expressions to find the affected tables, so no SQL parser or extra dependency is needed. Works with Cloudflare Durable Object SQL storage and Node SQLite.

## Usage

```ts
import { makeSyncEngine } from "@do-sync-engine/core";
import { createAdapter } from "@do-sync-engine/sql-regex-adapter";
import { Effect } from "effect";

// `db` is a Node SQLite database or Cloudflare Durable Object `ctx.storage.sql`.
const program = Effect.gen(function* () {
  const adapt = yield* createAdapter(db);
  const allTodos = yield* adapt("SELECT * FROM todos ORDER BY id");
  const addTodo = yield* adapt("INSERT INTO todos (title) VALUES (?)");

  // allTodos.tables is Set { "todos" }; pass both to makeSyncEngine.
  return yield* makeSyncEngine({ queries: { allTodos }, mutations: { addTodo } });
});
```

`createAdapter()` and the function it returns give an `Effect` that fails with `SqlAdapterError` (a `Schema.TaggedError`) for an unsupported database, unsupported SQL, or unreadable table metadata. `run` returns an `Effect` that fails with the same error when execution throws.

## Development

```bash
vp test    # tests
vp check   # format, lint, types
vp pack    # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
