# @do-sync-engine/core

Minimal engine for synchronizing query subscribers after mutations.

## Usage

```ts
import { Context, Effect } from "effect";
import { syncEngineLayer, toTables } from "@do-sync-engine/core";
import type { Mutation, Query, SyncEngine } from "@do-sync-engine/core";

// Queries and mutations return Effects. Declare the error (E) and service (R) types
// as the last two parameters; both default to `never`.
const queries = {
  allTodos: {
    tables: toTables(["todos"]),
    run: () => Db.use((db) => db.query("SELECT * FROM todos ORDER BY id")),
  } satisfies Query<[], Todo[], DbError, Db>,
};

const mutations = {
  addTodo: {
    tables: toTables(["todos"]),
    run: (title: string) =>
      Db.use((db) => db.execute("INSERT INTO todos (title) VALUES (?)", title)),
  } satisfies Mutation<[string], unknown, DbError, Db>,
};

// The engine is an Effect service. Declare your own tag and build it with a Layer.
class Engine extends Context.Service<
  Engine,
  SyncEngine<string, typeof queries, typeof mutations>
>()("app/Engine") {}
const EngineLive = syncEngineLayer(Engine, {
  queries,
  mutations,
  createId: () => crypto.randomUUID(),
}); // Layer<Engine, never, Db>

const program = Effect.gen(function* () {
  const engine = yield* Engine;

  // Create a canonical topic, then subscribe one or more listeners to it.
  const topic = yield* engine.createTopic("allTodos", []);
  const listener = ({ topic, value }) => {
    console.log(topic.name, value);
  };
  const id = yield* engine.subscribe(topic, listener);

  // Sync runs the mutation and publishes results for subscribed topics whose tables overlap.
  // Listeners only receive events that differ from the last one delivered to them.
  yield* engine.sync("addTodo", ["Buy milk"]);

  // Unsubscribe one listener without removing the topic binding.
  yield* engine.unsubscribe(topic, listener);
});
```

`makeSyncEngine(options)` builds the engine as an `Effect` when you do not need a layer. Services that queries and mutations require become requirements of `makeSyncEngine` or of the layer; engine methods themselves have no requirements.

Every engine method returns an `Effect`. Failures are typed: `UnknownQueryError`, `UnknownMutationError`, `MissingSubscriptionIdError` (all `Schema.TaggedError`), plus the `E` of the query or mutation that failed. A throw inside `run` is a defect, not a typed failure. `sync` stops at the first failing query. `subscriptions` returns an array.

A `Topic` contains the query `name` and `params`. Do not mutate topic params after creating a topic. Structurally equivalent topic objects share listeners, including across serialization boundaries; subscribing with a different topic object with equivalent parameters addresses the same subscription.

Delivery is deduplicated per listener object: the engine stores the last event sent to each listener in a `WeakMap`, and `sync` skips listeners whose last event is structurally equal (`Equal.equals` from `effect`) to the new one. The engine keeps a `structuredClone` of each delivered event, so queries may return live objects that mutations change in place; the clone costs time and memory proportional to the result size, taken once per published event. Values that cannot be cloned (for example functions) are always re-delivered. Class instances are cloned to plain objects and still compare equal when their fields match. Listeners receive the original event, so do not mutate event values. `subscribe` always delivers the current value and records it, so the next unchanged `sync` is not re-sent. Entries disappear when the listener is garbage-collected.

For full deduplication, use one listener object per topic: a listener subscribed to several topics keeps a single last event, so alternating topics can cause re-sends (never missed updates). The same listener subscribed twice to one topic under different IDs receives each event once.

## Development

Run from `packages/core`:

```bash
vp test      # unit tests
vp check     # format, lint, types
vp pack      # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
