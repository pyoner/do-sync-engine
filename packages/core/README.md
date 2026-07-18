# @do-sync-engine/core

Minimal engine for synchronizing query subscribers after mutations.

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
  } satisfies Mutation<[string]>,
};

const engine = new SyncEngine({ queries, mutations });

// Create a canonical topic, then subscribe one or more listeners to it.
const topic = await engine.createTopic("allTodos", []);
const subscription = engine.subscribe(topic, (publishedTopic, result) => {
  console.log(publishedTopic.hash, result);
});

// Sync runs the mutation and publishes results for subscribed topics whose tables overlap.
engine.sync("addTodo", ["Buy milk"]);

// Unsubscribe one listener without removing the topic binding.
engine.unsubscribe(subscription);
```

A `Topic` contains the query `name`, query `params`, and a `hash`. The hash is the lowercase hexadecimal SHA-256 digest of `JSON.stringify({ name, params })`. Topic inputs are cloned when the topic is created, so later caller mutation cannot change the query inputs represented by its hash. A single topic hash can have many listeners; a `Subscription` contains the topic hash and listener ID required by `unsubscribe`.

## Development

Run from `packages/core`:

```bash
vp test      # unit tests
vp check     # format, lint, types
vp pack      # build package
```
