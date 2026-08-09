# @do-sync-engine/core

Minimal engine for synchronizing query subscribers after mutations.

## Usage

```ts
import { SyncEngine, toTables } from "@do-sync-engine/core";
import type { Mutation, Query, Table } from "@do-sync-engine/core";

// Define query and mutation handlers
const queries = {
  allTodos: {
    tables: toTables(["todos"]),
    run: () => db.query("SELECT * FROM todos ORDER BY id"),
  } satisfies Query<[], Todo[]>,
};

const mutations = {
  addTodo: {
    tables: toTables(["todos"]),
    run: (title: string) => db.execute("INSERT INTO todos (title) VALUES (?)", title),
  } satisfies Mutation<[string]>,
};

const engine = new SyncEngine({ queries, mutations });

// Create a canonical topic, then subscribe one or more listeners to it.
const topic = engine.createTopic("allTodos", []);
const listener = ({ topic, value }) => {
  console.log(topic.name, value);
};
engine.subscribe(topic, listener);

// Sync runs the mutation and publishes results for subscribed topics whose tables overlap.
engine.sync("addTodo", ["Buy milk"]);

// Unsubscribe one listener without removing the topic binding.
engine.unsubscribe(topic, listener);
```

A `Topic` contains the query `name` and query `params`. Topic inputs are cloned when the topic is created, so later caller mutation cannot change the query inputs. Structurally equal topics share listeners; `subscribe` and `unsubscribe` use the same Topic/listener pair.

## Development

Run from `packages/core`:

```bash
vp test      # unit tests
vp check     # format, lint, types
vp pack      # build package
```
