<script lang="ts">
  import { onMount } from "svelte";
  import { newWebSocketRpcSession, type WebSocketRpcClient } from "./websocket-rpc-client";
  import {
    TODO_WS_PATH,
    type Todo,
    type TodoMutations,
    type TodoQueries,
    type TodoQueryResults,
    type TodoSummary,
  } from "./todo-protocol";
  const filters = [
    { label: "All", query: "allTodos" },
    { label: "Active", query: "incompleteTodos" },
    { label: "Completed", query: "completedTodos" },
  ] as const;
  type TodoFilter = (typeof filters)[number];
  type TodoListItem = TodoSummary & Pick<Todo, "completed">;

  let todos = $state<TodoListItem[]>([]);
  let newTitle = $state("");
  let queryResults = $state<Partial<TodoQueryResults>>({});
  let selectedFilter = $state<TodoFilter>(filters[0]);
  let filterLoading = $state(false);
  let loading = $state(false);
  let api: WebSocketRpcClient<TodoQueries, TodoMutations> | null = null;
  let connected = $state(false);
  let errorMessage = $state<string | null>(null);
  let filterSubscriptionVersion = 0;
  let unsubscribeActiveFilter = $state<(() => void) | null>(null);

  function disconnect(): void {
    const root = api;
    filterSubscriptionVersion += 1;
    unsubscribeActiveFilter?.();
    unsubscribeActiveFilter = null;
    api = null;
    connected = false;
    filterLoading = false;
    loading = false;
    root?.[Symbol.dispose]();
  }

  function showSubscriptionError(root: WebSocketRpcClient<TodoQueries, TodoMutations>, version: number, error: unknown): void {
    if (api !== root || filterSubscriptionVersion !== version) return;
    filterLoading = false;
    errorMessage = error instanceof Error ? error.message : String(error);
  }

  function toTodoListItems(filter: TodoFilter, value: unknown): TodoListItem[] {
    const results = value as TodoSummary[];
    if (filter.query === "allTodos") return results as TodoListItem[];
    return results.map((todo) => ({
      ...todo,
      completed: filter.query === "completedTodos" ? 1 : 0,
    }));
  }

  async function subscribeToFilter(
    root: WebSocketRpcClient<TodoQueries, TodoMutations>,
    filter: TodoFilter,
    version: number,
  ): Promise<void> {
    const topic = await root.createTopic(filter.query, []);
    if (topic instanceof Error) {
      showSubscriptionError(root, version, topic);
      return;
    }
    if (api !== root || filterSubscriptionVersion !== version) return;

    const listener = (event: { value: unknown }) => {
      if (api !== root || filterSubscriptionVersion !== version) return;
      queryResults = { ...queryResults, [filter.query]: event.value };
      todos = toTodoListItems(filter, event.value);
      filterLoading = false;
    };
    const subscribed = await root.subscribe(topic, listener);
    if (subscribed instanceof Error) {
      showSubscriptionError(root, version, subscribed);
      return;
    }

    const unsubscribe = () => {
      void root.unsubscribe(topic, listener).catch((error) => {
        globalThis.console.warn("Failed to unsubscribe from todo filter:", error);
      });
    };
    if (api !== root || filterSubscriptionVersion !== version) {
      unsubscribe();
      return;
    }
    unsubscribeActiveFilter = unsubscribe;
  }

  function selectFilter(filter: TodoFilter): void {
    if (selectedFilter.query === filter.query) return;

    selectedFilter = filter;
    todos = [];
    queryResults = {};
    filterLoading = true;
    errorMessage = null;
    filterSubscriptionVersion += 1;
    unsubscribeActiveFilter?.();
    unsubscribeActiveFilter = null;

    const root = api;
    const version = filterSubscriptionVersion;
    if (root !== null) void subscribeToFilter(root, filter, version).catch((error) => {
      showSubscriptionError(root, version, error);
    });
  }

  function connect(): void {
    if (api !== null) return;

    const root = newWebSocketRpcSession<TodoQueries, TodoMutations>(
      `${globalThis.location.protocol === "https:" ? "wss:" : "ws:"}//${globalThis.location.host}${TODO_WS_PATH}`,
    );
    api = root;
    connected = true;
    filterLoading = true;
    errorMessage = null;

    const version = filterSubscriptionVersion;
    void subscribeToFilter(root, selectedFilter, version).catch((error) => {
      showSubscriptionError(root, version, error);
    });
    root.onRpcBroken((error) => {
      if (api !== root) return;
      unsubscribeActiveFilter = null;
      api = null;
      connected = false;
      filterLoading = false;
      loading = false;
      errorMessage = error instanceof Error ? error.message : String(error);
    });
  }

  function mutate(
    operation: (root: WebSocketRpcClient<TodoQueries, TodoMutations>) => Promise<void | Error>,
    afterSuccess?: () => void,
  ) {
    const root = api;
    if (!root) return;

    loading = true;
    errorMessage = null;
    void (async () => {
      try {
        const result = await operation(root);
        if (result instanceof Error) throw result;
        if (api !== root) return;
        afterSuccess?.();
      } catch (error) {
        errorMessage = error instanceof Error ? error.message : String(error);
      } finally {
        if (api === root) loading = false;
      }
    })();
  }
  function addTodo() {
    const title = newTitle.trim();
    if (title) mutate((root) => root.sync("addTodo", [title]), () => (newTitle = ""));
  }
  function toggleTodo(id: number) {
    mutate((root) => root.sync("toggleTodo", [id]));
  }
  function deleteTodo(id: number) {
    mutate((root) => root.sync("deleteTodo", [id]));
  }
  function clearCompleted() {
    mutate((root) => root.sync("clearCompleted", []));
  }
  onMount(() => {
    connect();
    return disconnect;
  });
</script>

<main>
  <h1>
      <a href="/">
      TODO Demo
      </a>
  </h1>
  <p class="subtitle">Powered by <code>@do-sync-engine/core</code> + Cloudflare Durable Objects</p>

  <div class="connection-control">
    <button
      type="button"
      onclick={connected ? disconnect : connect}
      aria-label={connected ? "Disconnect WebSocket" : "Connect WebSocket"}
    >
      {connected ? "Disconnect" : "Connect"}
    </button>
    <p class="status" aria-live="polite">{connected ? "Connected" : "Disconnected"}</p>
  </div>

  {#if errorMessage}
    <p class="status error">{errorMessage}</p>
  {/if}

  <form onsubmit={(e) => { e.preventDefault(); addTodo(); }}>
    <input
      type="text"
      bind:value={newTitle}
      placeholder="What needs doing?"
      disabled={loading || !connected}
    />
    <button type="submit" disabled={loading || !connected || !newTitle.trim()}>Add</button>
  </form>

  <div class="filters" role="group" aria-label="Todo filters">
    {#each filters as filter}
      <button
        type="button"
        class:active={selectedFilter.query === filter.query}
        aria-pressed={selectedFilter.query === filter.query}
        onclick={() => selectFilter(filter)}
        disabled={!connected}
      >
        {filter.label}
      </button>
    {/each}
  </div>

  {#if filterLoading}
    <p class="status" aria-live="polite">Loading {selectedFilter.label.toLowerCase()} todos…</p>
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
              disabled={loading || !connected}
            />
            <span>{todo.title}</span>
          </label>
          <button class="delete" onclick={() => deleteTodo(todo.id)} disabled={loading || !connected}>×</button>
        </li>
      {/each}
    </ul>

    {#if todos.some(t => t.completed)}
      <button class="clear" onclick={clearCompleted} disabled={loading || !connected}>Clear completed</button>
    {/if}
  {/if}

  <div class="recompute-panel">
    <h2>Subscribed query</h2>
    <ul class="query-list">
      <li>
        <code>{selectedFilter.query}</code>
        <span class="row-count">({queryResults[selectedFilter.query]?.length ?? 0} rows)</span>
      </li>
    </ul>
    <details>
      <summary>Latest query result (JSON)</summary>
      <pre>{JSON.stringify(queryResults[selectedFilter.query], null, 2)}</pre>
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

  .connection-control {
    display: flex;
    align-items: center;
    gap: 0.75rem;
    margin-bottom: 1rem;
  }

  .connection-control .status {
    margin: 0;
  }

  .filters {
    display: flex;
    gap: 0.5rem;
    margin-bottom: 1rem;
  }

  .filters button.active {
    background: var(--accent);
    color: #111;
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
