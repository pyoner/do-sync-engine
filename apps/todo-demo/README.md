# Todo Demo

A real-time todo application built with Svelte, `@do-sync-engine/core`, and a Cloudflare Durable Object. The browser keeps a WebSocket connection to the Worker; the `TodoStore` Durable Object owns todo state and broadcasts query updates to connected clients.

## Prerequisites

- Node.js `>=22.18.0`
- The repository's Vite+ toolchain (`vp`)
- pnpm, provided through the repository's configured Vite+ package-manager setup

Install dependencies from the repository root:

```bash
vp install
```

## Development

From the repository root, start the demo with Portless:

```bash
vp run todo-demo#dev
```

Portless starts Vite through a named `.localhost` URL and assigns the underlying port automatically. On first use, Portless may prompt to configure local HTTPS trust. The startup output is the source of truth for the URL.

## Commands

Run these from the repository root:

| Command                    | Purpose                               |
| -------------------------- | ------------------------------------- |
| `vp run todo-demo#dev`     | Start the Portless development server |
| `vp run todo-demo#build`   | Build the Worker and client assets    |
| `vp run todo-demo#preview` | Preview the production build          |
| `vp run todo-demo#test`    | Run the demo's tests                  |
| `vp run todo-demo#deploy`  | Build and deploy with Wrangler        |

The package-local scripts are also available after changing into this directory:

```bash
vp run dev
vp run build
vp run preview
vp run test
vp run deploy
```

## Deployment

The demo is configured as a Cloudflare Worker in [`wrangler.jsonc`](./wrangler.jsonc). It uses:

- The `TodoStore` Durable Object binding named `TODO_STORE`.
- A SQLite-backed Durable Object migration tagged `v1`.
- SPA fallback for client assets.
- Worker-first handling for `/api/*`; the Worker accepts WebSocket connections at `/api/todos`.

Authenticate Wrangler before deploying, then run:

```bash
vp run todo-demo#deploy
```

## Project layout

- [`src/App.svelte`](./src/App.svelte) — Todo UI and subscription status display.
- [`src/main.ts`](./src/main.ts) — Client entry point.
- [`src/worker/index.ts`](./src/worker/index.ts) — Worker fetch handler and Durable Object export.
- [`src/worker/todo-store.ts`](./src/worker/todo-store.ts) — Durable Object todo state and mutations.
- [`src/worker/subscription-registry.ts`](./src/worker/subscription-registry.ts) — WebSocket subscription management.
- [`vite.config.ts`](./vite.config.ts) — Svelte, Cloudflare, and test plugin configuration.
- [`test/`](./test/) — Worker and subscription tests.
