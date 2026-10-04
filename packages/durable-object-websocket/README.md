# @do-sync-engine/durable-object-websocket

WebSocket transport that connects browsers to a Cloudflare Durable Object running `@do-sync-engine/core`. Extend `DurableObjectWebSocket` on the server; clients talk to it over [Cap'n Web](https://github.com/cloudflare/capnweb) RPC and receive live query updates.

## Usage

Extend `DurableObjectWebSocket` and pass it an `Effect` that builds a `SyncEngine` (see `makeSyncEngine`). The effect runs once, synchronously, in the constructor, so any services it needs must already be provided. Arguments are evaluated before `super()` returns, but an `Effect` is lazy: wrap setup that touches storage (such as `CREATE TABLE`) in `Effect.suspend` or `Effect.gen` so it runs when the effect runs. Clients that open a WebSocket to the object can subscribe to queries and run mutations over RPC.

```ts
import { makeSyncEngine } from "@do-sync-engine/core";
import { DurableObjectWebSocket } from "@do-sync-engine/durable-object-websocket";

export class TodoStore extends DurableObjectWebSocket<Env, Queries, Mutations> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(
      ctx,
      env,
      makeSyncEngine({ queries: createQueries(ctx), mutations: createMutations(ctx) }),
    );
  }
}

// In the Worker: forward WebSocket upgrade requests to the object.
export default {
  fetch: (request, env) => env.TODO_STORE.getByName("default").fetch(request),
} satisfies ExportedHandler<Env>;
```

Engine failures reach clients as `Error` values; defects (throws inside `run`) are returned the same way. Queries and mutations run synchronously: an effect that suspends asynchronously is returned to the client as an `Error`.

Bind the class as a Durable Object in `wrangler.jsonc`. Non-WebSocket requests to the object get a `400` response.

- `@do-sync-engine/durable-object-websocket` — `DurableObjectWebSocket` base class.
- `@do-sync-engine/durable-object-websocket/service` — `SocketService` and RPC types for subscribe, unsubscribe and sync calls.

## Development

```bash
vp test    # tests (Workers pool)
vp check   # format, lint, types
vp pack    # build package
```

`exports` in `package.json` points at `src/` so workspace packages need no build. `vp pack` writes the `dist` paths into `publishConfig.exports`. Publish with `pnpm publish` only; `npm publish` ignores `publishConfig.exports` and would ship `src/` paths.
