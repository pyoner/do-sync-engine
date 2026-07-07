# @do-sync-engine/core

Minimal sync engine for Cloudflare Durable Objects: subscribe to queries, apply mutations, and get typed results.

## Usage

```ts
import { SyncEngine } from "@do-sync-engine/core";
import type { Mutation, Query, QueryResult } from "@do-sync-engine/core";

// Define query and mutation handlers
const queries = {
  allTodos: {
    tables: ["todos"],
    run: () => db.query("SELECT * FROM todos ORDER BY id"),
  } satisfies Query<[], Todo[]>,
};

const mutations = {
  addTodo: {
    tables: ["todos"],
    run: (title: string) => db.execute("INSERT INTO todos (title) VALUES (?)", title),
  } satisfies Mutation<[string], MutationMetadata>,
};

const engine = new SyncEngine({ queries, mutations });

// Subscribe to a query
const subId = engine.subscribe("allTodos");

// Run a mutation — subscribed queries whose tables overlap are re-run
const result = await engine.mutate("addTodo", "Buy milk");
// result.results[0] is a QueryResult with the re-run query data
// result.metadata is the mutation return value

// Unsubscribe
engine.unsubscribe(subId);

// Snapshot & restore
const snap = engine.snapshot();
const restored = new SyncEngine({ queries, mutations, snapshot: snap });
```

## Development

Run from `packages/core`:

```bash
vp test      # unit tests
vp check     # format, lint, types
vp pack      # build package
```
