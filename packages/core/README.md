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

`tables` on a query or mutation is a `ReadonlySet<Table>` and the `makeSyncEngine` options (`queries`, `mutations`, `createId`) are `readonly` properties. The engine reads them once and keeps the object references, so do not mutate the `tables` set or add entries to the `queries`/`mutations` records after creating the engine; the type check only blocks reassignment and `tables` mutation.

Every engine method returns an `Effect`. Failures are typed: `UnknownQueryError`, `UnknownMutationError`, `MissingSubscriptionIdError` (all `Schema.TaggedError`), plus the `E` of the query or mutation that failed. A throw inside `run` is a defect, not a typed failure. `sync` stops at the first failing query. `subscriptions` returns an array.

A `Topic` contains the query `name` and `params`. Do not mutate topic params after creating a topic. Structurally equivalent topic objects share listeners, including across serialization boundaries; subscribing with a different topic object with equivalent parameters addresses the same subscription.

Delivery is deduplicated per listener object: the engine stores an `ohash` digest of the last event sent to each listener in a `WeakMap`, and `sync` skips listeners whose last digest equals the new event's digest. The digest is taken when the event is published, so queries may return live objects that mutations change in place; hashing costs time proportional to the result size, once per published event. Limit: `ohash` hashes functions by source text, not captured state, so two function values with the same source but different closures share a digest and the update is skipped. Do not put closures that carry changing state in query results. Listeners receive the original event, so do not mutate event values. `subscribe` always delivers the current value and records it, so the next unchanged `sync` is not re-sent. Entries disappear when the listener is garbage-collected.

`ListenerEvent` is `ReadonlyDeep` (from `type-fest`): listeners receive `value` as a deeply readonly type, so mutating it fails to compile; `topic` keeps its type and can be passed back to `subscribe`, `unsubscribe` and `subscriptions`. The check is type-only; casts and untyped JavaScript bypass it.

For full deduplication, use one listener object per topic: a listener subscribed to several topics keeps a single last event, so alternating topics can cause re-sends (never missed updates). The same listener subscribed twice to one topic under different IDs receives each event once.

## Development

Run from `packages/core`:

```bash
vp test      # unit tests
vp check     # format, lint, types
vp pack      # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
