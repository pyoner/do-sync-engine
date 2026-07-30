# @do-sync-engine/core

Minimal engine for synchronizing query subscribers after mutations.

## Usage

```ts
import { Effect } from "effect";
import { SyncEngine, toTables } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";

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

const program = Effect.gen(function* () {
  const topic = yield* engine.createTopic("allTodos", []);
  const listenerId = yield* engine.subscribe(topic, ({ topic, value }) => {
    console.log(topic.name, topic.params, value);
  });
  yield* engine.sync("addTodo", ["Buy milk"]);
  yield* engine.unsubscribe(listenerId);
});

Effect.runSync(program);
```

A `Topic` contains the query `name` and query `params`. Topics participate in Effect equality and hashing for local in-memory lookup. Parameters are retained by reference and must be treated as immutable after their first equality or hash use.

## Development

Run from `packages/core`:

```bash
vp test      # unit tests
vp check     # format, lint, types
vp pack      # build package
```
