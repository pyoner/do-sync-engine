# do-sync-engine

A sync engine for Cloudflare Durable Objects. Clients subscribe to queries; when a mutation changes the tables a query reads, subscribers get fresh results automatically.

## Packages

| Package                                                                           | Description                                                          |
| --------------------------------------------------------------------------------- | -------------------------------------------------------------------- |
| [`@do-sync-engine/core`](./packages/core)                                         | Query/mutation engine that notifies subscribers after table changes. |
| [`@do-sync-engine/drizzle-adapter`](./packages/drizzle-adapter)                   | Use Drizzle SQLite queries and mutations with the engine.            |
| [`@do-sync-engine/sql-regex-adapter`](./packages/sql-regex-adapter)               | Use plain SQL strings with the engine; tables detected by regex.     |
| [`@do-sync-engine/durable-object-websocket`](./packages/durable-object-websocket) | WebSocket + RPC transport for Durable Objects.                       |
| [`@do-sync-engine/xstate-store`](./packages/xstate-store)                         | Client store that mirrors live query results.                        |

## Apps

| App                             | Description                                          |
| ------------------------------- | ---------------------------------------------------- |
| [`todo-demo`](./apps/todo-demo) | Real-time todo app on Cloudflare Workers and Svelte. |
| [`website`](./apps/website)     | Project website (starter page).                      |

## Development

- Check everything is ready:

```bash
vp run ready
```

- Run the tests:

```bash
vp run -r test
```

- Build the monorepo:

```bash
vp run -r build
```

- Run the development server:

```bash
vp run dev
```
