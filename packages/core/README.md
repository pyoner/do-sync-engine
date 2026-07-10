# @do-sync-engine/core

Minimal sync engine for Cloudflare Durable Objects: subscribe to queries, apply mutations, and receive typed pushed updates.

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

// Subscribe to a query and receive updates after each matching sync
const subId = engine.subscribe("allTodos", [], ({ result }) => {
  console.log(result);
});

// Run a mutation — returns affected tables only
const affectedTables = await engine.mutate("addTodo", ["Buy milk"]);
// affectedTables is ["todos"]

// Sync runs the mutation and pushes overlapping query results to callbacks
await engine.sync("addTodo", ["Buy eggs"]);

// Unsubscribe
engine.unsubscribe(subId);

// Snapshot & restore
const snap = engine.snapshot();
const restored = new SyncEngine({ queries, mutations, snapshot: snap });
// Snapshot data does not include callbacks; subscribe again after restore to receive updates.
```

## Development

Run from `packages/core`:

```bash
vp test      # unit tests
vp check     # format, lint, types
vp pack      # build package
```
