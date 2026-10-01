<svelte:options runes={true} />
<script lang="ts">
  import { onMount } from "svelte";
  import { useSelector } from "@xstate/store-svelte";
  import type { Topics } from "@do-sync-engine/core";
  import { createSyncStore } from "@do-sync-engine/xstate-store";
  import { TODO_WS_PATH, type TodoMutations, type TodoQueries } from "./todo-protocol";

  const RESULTS_KEY = "todos";
  const filters = [
    { label: "All", topic: { name: "allTodos", params: [] } },
    { label: "Active", topic: { name: "incompleteTodos", params: [] } },
    { label: "Completed", topic: { name: "completedTodos", params: [] } },
  ] as const satisfies ReadonlyArray<{ label: string; topic: Topics<TodoQueries> }>;

  const syncStore = createSyncStore<TodoQueries, TodoMutations>({
    url: `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}${TODO_WS_PATH}`,
  });
  const status = useSelector(syncStore.store, (state) => state.context.status);
  const error = useSelector(syncStore.store, (state) => state.context.error);
  const items = useSelector(syncStore.store, (state) => state.context.topics[RESULTS_KEY]);

  let selectedFilter = $state.raw<(typeof filters)[number]>(filters[0]);
  let newTitle = $state("");

  // Subscribe whenever the socket is ready; re-runs on filter change and reconnect.
  $effect(() => {
    if ($status !== "ready") return;
    const topic = selectedFilter.topic;
    syncStore.subscribe(topic, RESULTS_KEY);
    return () => void syncStore.unsubscribe(topic, RESULTS_KEY);
  });

  onMount(() => () => syncStore[Symbol.dispose]());

  function addTodo(): void {
    const title = newTitle.trim();
    if (!title) return;
    syncStore.sync("addTodo", [title]);
    newTitle = "";
  }
</script>
<main>
  <h1>
    <a href="/">TODO Demo</a>
  </h1>
  <p class="subtitle">Powered by <code>@do-sync-engine/core</code> + Cloudflare Durable Objects</p>

  <div class="connection-control">
    <button
      type="button"
      onclick={$status === "ready" ? syncStore.disconnect : syncStore.connect}
      aria-label={$status === "ready" ? "Disconnect WebSocket" : "Connect WebSocket"}
      disabled={$status === "connecting"}
    >
      {$status === "ready" ? "Disconnect" : "Connect"}
    </button>
    <p class="status" aria-live="polite">
      {$status === "connecting" ? "Connecting" : $status === "ready" ? "Connected" : "Disconnected"}
    </p>
  </div>

  {#if $error}
    <p class="status error">{$error.message}</p>
  {/if}

  <form onsubmit={(event) => { event.preventDefault(); addTodo(); }}>
    <input
      type="text"
      bind:value={newTitle}
      placeholder="What needs doing?"
      disabled={$status !== "ready"}
    />
    <button type="submit" disabled={$status !== "ready" || !newTitle.trim()}>Add</button>
  </form>

  <div class="filters" role="group" aria-label="Todo filters">
    {#each filters as filter}
      <button
        type="button"
        class:active={selectedFilter === filter}
        aria-pressed={selectedFilter === filter}
        onclick={() => (selectedFilter = filter)}
        disabled={$status !== "ready"}
      >
        {filter.label}
      </button>
    {/each}
  </div>

  {#if $status === "ready" && $items === undefined && $error === null}
    <p class="status" aria-live="polite">Loading {selectedFilter.label.toLowerCase()} todos…</p>
  {:else if !$items?.length}
    <p class="empty">No todos yet. Add one above!</p>
  {:else}
    <ul class="todo-list">
      {#each $items as todo (todo.id)}
        <li class:completed={todo.completed}>
          <label>
            <input
              type="checkbox"
              checked={!!todo.completed}
              onchange={() => syncStore.sync("toggleTodo", [todo.id])}
              disabled={$status !== "ready"}
            />
            <span>{todo.title}</span>
          </label>
          <button class="delete" onclick={() => syncStore.sync("deleteTodo", [todo.id])} disabled={$status !== "ready"}>×</button>
        </li>
      {/each}
    </ul>

    {#if $items?.some((todo) => todo.completed)}
      <button class="clear" onclick={() => syncStore.sync("clearCompleted", [])} disabled={$status !== "ready"}>Clear completed</button>
    {/if}
  {/if}

  <div class="recompute-panel">
    <h2>Subscribed query</h2>
    <ul class="query-list">
      <li>
        <code>{selectedFilter.topic.name}</code>
        <span class="row-count">({$items?.length ?? 0} rows)</span>
      </li>
    </ul>
    <details>
      <summary>Latest query result (JSON)</summary>
      <pre>{JSON.stringify($items, null, 2)}</pre>
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
