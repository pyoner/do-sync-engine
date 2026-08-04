<script lang="ts">
  import { Effect, Exit, Fiber, Option, Schema, Stream } from "effect";
  import { onMount } from "svelte";
  import {
    makeWebSocketRpcClient,
    RpcOperationError,
    type WebSocketRpcClient,
  } from "@do-sync-engine/durable-object-websocket";
  import { Topic, UnknownMutationError, UnknownQueryError } from "@do-sync-engine/core";
  import {
    TODO_WS_PATH,
    todoCountSchema,
    todoSchema,
    type Todo,
    type TodoCount,
    type TodoMutations,
  } from "./todo-protocol";

  const defaultQueries = ["allTodos", "todoCount"] as const;
  let newTitle = $state("");
  let queryResults = $state<{
    allTodos?: readonly Todo[];
    todoCount?: readonly TodoCount[];
  }>({});
  let todos = $derived(queryResults.allTodos ?? []);
  let adding = $state(false);
  let clearing = $state(false);
  let pendingTodoIds = $state<number[]>([]);
  let socket = $state<WebSocket | null>(null);
  let rpcClient: WebSocketRpcClient | null = null;
  let connected = $state(false);
  let errorMessage = $state<string | null>(null);
  let connectionGeneration = 0;
  let connectionFiber: Fiber.Fiber<unknown, unknown> | null = null;
  let subscriptionsFiber: Fiber.Fiber<unknown, unknown> | null = null;
  let refreshingSubscriptions = false;
  let reconnectTimer: ReturnType<typeof setTimeout> | null = null;
  let removeSocketListeners: (() => void) | null = null;
  let stopped = false;
  let hasConnected = $state(false);

  function websocketUrl(): string {
    return `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}${TODO_WS_PATH}`;
  }

  function messageForError(error: unknown): string {
    if (error instanceof RpcOperationError && error.message !== "") return error.message;
    if (error instanceof UnknownMutationError) return `Unknown mutation: ${error.mutation}`;
    if (error instanceof UnknownQueryError) return `Unknown query: ${error.query}`;
    if (error instanceof Error && error.message !== "") return error.message;
    return "Todo operation failed";
  }

  function scheduleReconnect(generation: number): void {
    if (stopped || generation !== connectionGeneration || reconnectTimer !== null) return;
    reconnectTimer = setTimeout(() => {
      reconnectTimer = null;
      if (!stopped && generation === connectionGeneration) connect();
    }, 1_000);
  }

  function subscribeQueries(client: WebSocketRpcClient, isCurrent: () => boolean): void {
    const allTodos = client.subscribe(new Topic({ name: "allTodos", params: [] })).pipe(
      Stream.runForEach((event) =>
        Schema.decodeUnknownEffect(Schema.Array(todoSchema))(event.value).pipe(
          Effect.tap((value) =>
            Effect.sync(() => {
              if (isCurrent()) queryResults = { ...queryResults, allTodos: value };
            }),
          ),
        ),
      ),
    );
    const todoCount = client.subscribe(new Topic({ name: "todoCount", params: [] })).pipe(
      Stream.runForEach((event) =>
        Schema.decodeUnknownEffect(Schema.Array(todoCountSchema))(event.value).pipe(
          Effect.tap((value) =>
            Effect.sync(() => {
              if (isCurrent()) queryResults = { ...queryResults, todoCount: value };
            }),
          ),
        ),
      ),
    );
    subscriptionsFiber = Effect.runFork(
      Effect.scoped(
        Effect.all([allTodos, todoCount], { concurrency: "unbounded", discard: true }),
      ).pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (isCurrent() && Exit.isFailure(exit) && !Exit.hasInterrupts(exit)) {
              const error = Exit.findErrorOption(exit);
              errorMessage = Option.isSome(error) ? messageForError(error.value) : "Todo connection failed";
            }
          }),
        ),
      ),
    );
  }

  function refreshQueries(client: WebSocketRpcClient, isCurrent: () => boolean): void {
    if (!isCurrent() || rpcClient !== client || refreshingSubscriptions) return;
    refreshingSubscriptions = true;
    Effect.runFork(
      Effect.gen(function* () {
        const previous = subscriptionsFiber;
        if (previous) yield* Fiber.interrupt(previous);
        if (isCurrent() && rpcClient === client) subscribeQueries(client, isCurrent);
      }).pipe(Effect.ensuring(Effect.sync(() => (refreshingSubscriptions = false)))),
    );
  }

  function connect(): void {
    const generation = ++connectionGeneration;
    let ws: WebSocket;
    try {
      ws = new WebSocket(websocketUrl());
    } catch {
      if (!stopped && generation === connectionGeneration) {
        errorMessage = "WebSocket error";
        scheduleReconnect(generation);
      }
      return;
    }

    socket = ws;
    const isCurrent = (): boolean =>
      !stopped && generation === connectionGeneration && socket === ws;
    const removeListeners = (): void => {
      ws.removeEventListener("open", onOpen);
      ws.removeEventListener("close", onClose);
      ws.removeEventListener("error", onError);
    };
    const onOpen = (): void => {
      if (!isCurrent()) {
        removeListeners();
        ws.close();
        return;
      }

      let fiber: Fiber.Fiber<unknown, unknown> | null = null;
      const connection = Effect.scoped(
        Effect.gen(function* () {
          let client: WebSocketRpcClient | undefined;
          const nextClient = yield* makeWebSocketRpcClient(ws, () => {
            if (client) refreshQueries(client, isCurrent);
          });
          client = nextClient;
          if (!isCurrent()) return;
          rpcClient = nextClient;
          connected = true;
          hasConnected = true;
          errorMessage = null;
          subscribeQueries(nextClient, isCurrent);
          yield* Effect.never;
        }),
      ).pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (!isCurrent()) return;
            if (connectionFiber === fiber) connectionFiber = null;
            if (Exit.isFailure(exit) && !Exit.hasInterrupts(exit)) {
              const error = Exit.findErrorOption(exit);
              errorMessage = Option.isSome(error)
                ? messageForError(error.value)
                : "Todo connection failed";
              ws.close();
            }
          }),
        ),
      );
      fiber = Effect.runFork(connection);
      if (isCurrent()) connectionFiber = fiber;
    };
    const onClose = (): void => {
      removeListeners();
      if (!isCurrent()) return;
      if (removeSocketListeners === removeListeners) removeSocketListeners = null;
      const interrupted = connectionFiber;
      const interruptedSubscriptions = subscriptionsFiber;
      connectionFiber = null;
      subscriptionsFiber = null;
      rpcClient = null;
      socket = null;
      connected = false;
      if (interrupted) Effect.runFork(Fiber.interrupt(interrupted));
      if (interruptedSubscriptions) Effect.runFork(Fiber.interrupt(interruptedSubscriptions));
    };
    const onError = (): void => {
      if (!isCurrent()) {
        removeListeners();
        ws.close();
        return;
      }
      errorMessage = "WebSocket error";
      ws.close();
    };
    removeSocketListeners = removeListeners;
    ws.addEventListener("open", onOpen);
    ws.addEventListener("close", onClose);
    ws.addEventListener("error", onError);
  }

  function runMutation(
    mutation: keyof TodoMutations,
    params: readonly unknown[],
    onSuccess: () => void,
    onSettled: () => void,
  ): void {
    const activeClient = rpcClient;
    if (activeClient === null) {
      errorMessage = "WebSocket is not connected";
      try {
        onSettled();
      } catch {
        errorMessage = "Todo operation failed";
      }
      return;
    }
    errorMessage = null;
    let settled = false;
    const settle = (): void => {
      if (settled) return;
      settled = true;
      try {
        onSettled();
      } catch {
        errorMessage = "Todo operation failed";
      }
    };
    const invokeSuccess = (): void => {
      try {
        onSuccess();
      } catch {
        errorMessage = "Todo operation failed";
      }
    };
    Effect.runFork(
      activeClient.sync({ mutation, params }).pipe(
        Effect.onExit((exit) =>
          Effect.sync(() => {
            if (Exit.isSuccess(exit)) {
              invokeSuccess();
            } else {
              if (!Exit.hasInterrupts(exit)) {
                const error = Exit.findErrorOption(exit);
                errorMessage = Option.isSome(error)
                  ? messageForError(error.value)
                  : "Todo operation failed";
              }
            }
            settle();
          }),
        ),
      ),
    );
  }

  function addTodo(): void {
    const title = newTitle.trim();
    if (!connected || rpcClient === null || adding || title === "") return;
    adding = true;
    runMutation(
      "addTodo",
      [title],
      () => (newTitle = ""),
      () => (adding = false),
    );
  }

  function toggleTodo(id: number): void {
    if (!connected || rpcClient === null || pendingTodoIds.includes(id)) return;
    pendingTodoIds = [...pendingTodoIds, id];
    runMutation(
      "toggleTodo",
      [id],
      () => undefined,
      () => (pendingTodoIds = pendingTodoIds.filter((pendingId) => pendingId !== id)),
    );
  }

  function deleteTodo(id: number): void {
    if (!connected || rpcClient === null || pendingTodoIds.includes(id)) return;
    pendingTodoIds = [...pendingTodoIds, id];
    runMutation(
      "deleteTodo",
      [id],
      () => undefined,
      () => (pendingTodoIds = pendingTodoIds.filter((pendingId) => pendingId !== id)),
    );
  }

  function clearCompleted(): void {
    if (
      !connected ||
      rpcClient === null ||
      clearing ||
      todos.some((todo) => todo.completed && pendingTodoIds.includes(todo.id))
    )
      return;
    clearing = true;
    runMutation(
      "clearCompleted",
      [],
      () => undefined,
      () => (clearing = false),
    );
  }

  onMount(() => {
    connect();
    return () => {
      stopped = true;
      connectionGeneration += 1;
      if (reconnectTimer !== null) {
        clearTimeout(reconnectTimer);
        reconnectTimer = null;
      }
      const remove = removeSocketListeners;
      removeSocketListeners = null;
      if (remove) remove();
      const interrupted = connectionFiber;
      const interruptedSubscriptions = subscriptionsFiber;
      connectionFiber = null;
      subscriptionsFiber = null;
      if (interrupted) Effect.runFork(Fiber.interrupt(interrupted));
      if (interruptedSubscriptions) Effect.runFork(Fiber.interrupt(interruptedSubscriptions));
      const currentSocket = socket;
      socket = null;
      rpcClient = null;
      connected = false;
      if (currentSocket) currentSocket.close();
    };
  });
</script>

<main>
  <h1>TODO Demo</h1>
  <p class="subtitle">Powered by <code>@do-sync-engine/core</code> + Cloudflare Durable Objects</p>

  {#if errorMessage}
    <p class="status error" role="alert">{errorMessage}</p>
  {:else if !connected}
    <p class="status" aria-live="polite">{hasConnected ? "Reconnecting..." : "Connecting..."}</p>
  {/if}

  <form onsubmit={(e) => { e.preventDefault(); addTodo(); }}>
    <input
      type="text"
      bind:value={newTitle}
      placeholder="What needs doing?"
      disabled={adding || !connected}
    />
    <button type="submit" disabled={adding || !connected || !newTitle.trim()}>Add</button>
  </form>

  {#if queryResults.allTodos === undefined}
    <p class="empty">Loading todos...</p>
  {:else if todos.length === 0}
    <p class="empty">No todos yet. Add one above!</p>
  {:else}
    <ul class="todo-list">
      {#each todos as todo (todo.id)}
        <li class:completed={todo.completed}>
          <label>
            <input
              type="checkbox"
              checked={!!todo.completed}
              onchange={() => toggleTodo(todo.id)}
              disabled={!connected || (todo.completed && clearing) || pendingTodoIds.includes(todo.id)}
            />
            <span>{todo.title}</span>
          </label>
          <button
            class="delete"
            onclick={() => deleteTodo(todo.id)}
            disabled={!connected || (todo.completed && clearing) || pendingTodoIds.includes(todo.id)}
            aria-label={`Delete ${todo.title}`}
          >×</button>
        </li>
      {/each}
    </ul>

    {#if todos.some(t => t.completed)}
      <button
        class="clear"
        onclick={clearCompleted}
        disabled={
          !connected ||
          clearing ||
          todos.some((todo) => todo.completed && pendingTodoIds.includes(todo.id))
        }
      >Clear completed</button>
    {/if}
  {/if}

  <div class="recompute-panel">
    <h2>Subscribed queries</h2>
    <ul class="query-list">
      {#each defaultQueries as query}
        <li>
          <code>{query}</code>
          {#if query === "allTodos"}
            <span class="row-count">({todos.length} rows)</span>
          {:else}
            <span class="row-count">({queryResults.todoCount?.[0]?.total_count ?? 0} total)</span>
          {/if}
        </li>
      {/each}
    </ul>
    <details>
      <summary>Latest query results (JSON)</summary>
      <pre>{JSON.stringify(queryResults, null, 2)}</pre>
    </details>
  </div>
</main>

<style>
  :root {
    --accent: #4fc3f7;
    --accent-dim: #1a3a4a;
    --border: #333;
    --danger: #ef5350;
    --panel-bg: #1a1a2e;
  }

  main {
    max-width: 640px;
    margin: 2rem auto;
    padding: 0 1rem;
    font-family: system-ui, -apple-system, sans-serif;
    color: var(--fg);
  }

  h1 { margin-bottom: 0.25rem; }

  .subtitle {
    color: #888;
    margin-top: 0;
    margin-bottom: 1.5rem;
  }

  .status {
    color: #888;
    margin: 0 0 1rem;
  }

  .status.error {
    color: var(--danger);
  }

  form {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 1rem;
  }

  input[type="text"] {
    flex: 1;
    padding: 0.5rem 0.75rem;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: #1a1a1a;
    color: var(--fg);
    font-size: 1rem;
  }

  button {
    padding: 0.5rem 1rem;
    border: 1px solid var(--border);
    border-radius: 6px;
    background: var(--accent-dim);
    color: var(--accent);
    font-size: 0.9rem;
    cursor: pointer;
  }

  button:disabled { opacity: 0.5; cursor: not-allowed; }
  button:hover:not(:disabled) { background: #244a5e; }

  .empty { color: #666; text-align: center; padding: 2rem 0; }

  .todo-list {
    list-style: none;
    padding: 0;
    margin: 0 0 1rem;
  }

  .todo-list li {
    display: flex;
    align-items: center;
    justify-content: space-between;
    padding: 0.6rem 0.75rem;
    border: 1px solid var(--border);
    border-radius: 6px;
    margin-bottom: 0.4rem;
    background: #1a1a1a;
  }

  .todo-list li.completed span {
    text-decoration: line-through;
    color: #666;
  }

  .todo-list label {
    display: flex;
    align-items: center;
    gap: 0.6rem;
    cursor: pointer;
    flex: 1;
  }

  .delete {
    background: transparent;
    border: none;
    color: var(--danger);
    font-size: 1.3rem;
    padding: 0 0.3rem;
    line-height: 1;
  }

  .clear {
    background: transparent;
    border-color: var(--danger);
    color: var(--danger);
    margin-bottom: 1.5rem;
  }

  .recompute-panel {
    margin-top: 2rem;
    padding: 1rem;
    border: 1px solid var(--accent-dim);
    border-radius: 8px;
    background: var(--panel-bg);
  }

  .recompute-panel h2 {
    margin: 0 0 0.5rem;
    font-size: 1rem;
    color: var(--accent);
  }

  .meta {
    margin: 0.25rem 0;
    font-size: 0.9rem;
  }

  .query-list {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .query-list li {
    font-size: 0.9rem;
    padding: 0.15rem 0;
  }

  .row-count { color: #888; }

  details { margin-top: 0.75rem; }
  summary { cursor: pointer; color: #aaa; font-size: 0.85rem; }
  pre {
    margin-top: 0.5rem;
    padding: 0.75rem;
    background: #111;
    border-radius: 6px;
    overflow-x: auto;
    font-size: 0.8rem;
    line-height: 1.4;
  }
</style>
