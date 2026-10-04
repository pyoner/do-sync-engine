# @do-sync-engine/xstate-store

Client-side store for `@do-sync-engine`. `createSyncStore()` connects to a Durable Object over WebSocket and keeps an [XState Store](https://stately.ai/docs/xstate-store) in sync with live query results, so UI frameworks can read server state like any other store.

## Usage

```ts
import { createSyncStore } from "@do-sync-engine/xstate-store";

const sync = createSyncStore<Queries, Mutations>({ url: "wss://example.com/api/todos" });

sync.store.subscribe((snapshot) => console.log(snapshot.context.status, snapshot.context.topics));

sync.connect();
// When status is "ready": results appear in store.context.topics[key].
sync.subscribe({ name: "allTodos", params: [] }, "todos");
sync.sync("addTodo", ["Buy milk"]);

sync.unsubscribe({ name: "allTodos", params: [] }, "todos");
sync.disconnect();
sync[Symbol.dispose](); // close the socket and stop the store
```

`status` is `idle`, `connecting`, `ready` or `disconnected`. `error` holds the last connection failure.

## Development

```bash
vp test    # unit tests
vp check   # format, lint, types
vp pack    # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
