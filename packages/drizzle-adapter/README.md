# @do-sync-engine/drizzle-adapter

Use [Drizzle ORM](https://orm.drizzle.team) SQLite queries and mutations with `@do-sync-engine/core`. Wrap a Drizzle builder with `adapter()` and get back a query or mutation that already knows which tables it touches, so subscribers refresh when those tables change.

## Usage

```ts
import { adapter } from "@do-sync-engine/drizzle-adapter";
import { makeSyncEngine } from "@do-sync-engine/core";
import { Effect } from "effect";
import { eq } from "drizzle-orm";

// Works with synchronous Drizzle SQLite builders (e.g. Durable Object SQLite, node:sqlite).
const program = Effect.gen(function* () {
  const allTodos = yield* adapter(db.select().from(todos));
  const addTodo = yield* adapter(db.insert(todos).values({ title: "Buy milk" }));

  // Pass them to makeSyncEngine like any other query or mutation.
  return yield* makeSyncEngine({ queries: { allTodos }, mutations: { addTodo } });
});
```

`adapter()` returns an `Effect` that fails with `DrizzleAdapterError` (a `Schema.TaggedError`) when the builder is asynchronous or its tables cannot be read. The query or mutation's `run` returns an `Effect` that fails with the same error when execution throws.

## Development

```bash
vp test    # unit tests
vp check   # format, lint, types
vp pack    # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
