# @do-sync-engine/durable-object-websocket

WebSocket transport that connects browsers to a Cloudflare Durable Object running `@do-sync-engine/core`. Extend `DurableObjectWebSocket` on the server; clients talk to it over [Cap'n Web](https://github.com/cloudflare/capnweb) RPC and receive live query updates.

## Usage

Extend `DurableObjectWebSocket` and pass it a `SyncEngine`. Clients that open a WebSocket to the object can subscribe to queries and run mutations over RPC.

```ts
import { SyncEngine } from "@do-sync-engine/core";
import { DurableObjectWebSocket } from "@do-sync-engine/durable-object-websocket";

export class TodoStore extends DurableObjectWebSocket<Env, Queries, Mutations> {
  constructor(ctx: DurableObjectState, env: Env) {
    super(
      ctx,
      env,
      () => new SyncEngine({ queries: createQueries(ctx), mutations: createMutations(ctx) }),
    );
  }
}

// In the Worker: forward WebSocket upgrade requests to the object.
export default {
  fetch: (request, env) => env.TODO_STORE.getByName("default").fetch(request),
} satisfies ExportedHandler<Env>;
```

Bind the class as a Durable Object in `wrangler.jsonc`. Non-WebSocket requests to the object get a `400` response.

- `@do-sync-engine/durable-object-websocket` — `DurableObjectWebSocket` base class.
- `@do-sync-engine/durable-object-websocket/service` — `SocketService` and RPC types for subscribe, unsubscribe and sync calls.

## Development

```bash
vp test    # tests (Workers pool)
vp check   # format, lint, types
vp pack    # build package
```
