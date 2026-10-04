# @do-sync-engine/drizzle-adapter

Use [Drizzle ORM](https://orm.drizzle.team) SQLite queries and mutations with `@do-sync-engine/core`. Wrap a Drizzle builder with `adapter()` and get back a query or mutation that already knows which tables it touches, so subscribers refresh when those tables change.

## Usage

```ts
import { adapter } from "@do-sync-engine/drizzle-adapter";
import { eq } from "drizzle-orm";

// Works with synchronous Drizzle SQLite builders (e.g. Durable Object SQLite, node:sqlite).
const allTodos = adapter(db.select().from(todos));
const addTodo = adapter(db.insert(todos).values({ title: "Buy milk" }));
if (allTodos instanceof Error) throw allTodos;
if (addTodo instanceof Error) throw addTodo;

// Pass them to SyncEngine like any other query or mutation.
const engine = new SyncEngine({
  queries: { allTodos },
  mutations: { addTodo },
});
```

Both calls return a `DrizzleAdapterError` instead of throwing when the builder is asynchronous or its tables cannot be read.

## Development

```bash
vp test    # unit tests
vp check   # format, lint, types
vp pack    # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
