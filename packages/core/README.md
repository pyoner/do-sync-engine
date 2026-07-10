# @do-sync-engine/core

Minimal sync engine for Cloudflare Durable Objects: subscribe to queries, apply mutations, and get typed results.

## Usage

```ts
import { SyncEngine } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";

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

// Run a mutation — returns affected tables only
const affectedTables = await engine.mutate("addTodo", "Buy milk");
// affectedTables is ["todos"]

// Sync runs the mutation and re-runs overlapping subscribed queries
const synced = await engine.sync("addTodo", "Buy eggs");
// synced.affectedTables is ["todos"]
// synced.results contains QueryResult entries for overlapping subscriptions

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
