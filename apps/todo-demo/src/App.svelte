<script lang="ts">
  import { onMount } from "svelte";
  interface Todo {
    id: number;
    title: string;
    completed: number;
    created_at: number;
  }

  interface MutationResponse {
    metadata: { rowsAffected: number; lastInsertRowid: number | null };
    recomputedSelectors: string[];
    recomputeResults: Record<string, unknown[]>;
  }

  let todos = $state<Todo[]>([]);
  let newTitle = $state("");
  let lastMutation = $state<MutationResponse | null>(null);
  let loading = $state(false);

  async function fetchTodos() {
    const res = await fetch("/api/todos");
    const data = await res.json();
    todos = data.todos;
  }

  async function addTodo() {
    if (!newTitle.trim()) return;
    loading = true;
    const res = await fetch("/api/todos", {
      method: "POST",
      headers: { "content-type": "application/json" },
      body: JSON.stringify({ title: newTitle.trim() }),
    });
    const mutation: MutationResponse = await res.json();
    lastMutation = mutation;
    newTitle = "";
    await fetchTodos();
    loading = false;
  }

  async function toggleTodo(id: number) {
    loading = true;
    const res = await fetch(`/api/todos/${id}`, { method: "PATCH" });
    const mutation: MutationResponse = await res.json();
    lastMutation = mutation;
    await fetchTodos();
    loading = false;
  }

  async function deleteTodo(id: number) {
    loading = true;
    const res = await fetch(`/api/todos/${id}`, { method: "DELETE" });
    const mutation: MutationResponse = await res.json();
    lastMutation = mutation;
    await fetchTodos();
    loading = false;
  }

  async function clearCompleted() {
    loading = true;
    const res = await fetch("/api/todos", { method: "DELETE" });
    const mutation: MutationResponse = await res.json();
    lastMutation = mutation;
    await fetchTodos();
    loading = false;
  }

  onMount(() => {
    fetchTodos();
  });
</script>

<main>
  <h1>TODO Demo</h1>
  <p class="subtitle">Powered by <code>@do-sync-engine/core</code> + Cloudflare Durable Objects</p>

  <form onsubmit={(e) => { e.preventDefault(); addTodo(); }}>
    <input
      type="text"
      bind:value={newTitle}
      placeholder="What needs doing?"
      disabled={loading}
    />
    <button type="submit" disabled={loading || !newTitle.trim()}>Add</button>
  </form>

  {#if todos.length === 0}
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
              disabled={loading}
            />
            <span>{todo.title}</span>
          </label>
          <button class="delete" onclick={() => deleteTodo(todo.id)} disabled={loading}>×</button>
        </li>
      {/each}
    </ul>

    {#if todos.some(t => t.completed)}
      <button class="clear" onclick={clearCompleted} disabled={loading}>Clear completed</button>
    {/if}
  {/if}

  {#if lastMutation}
    <div class="recompute-panel">
      <h2>Last recompute</h2>
      <p class="meta">Rows affected: {lastMutation.metadata.rowsAffected}</p>
      <h3>Recomputed selectors</h3>
      <ul class="selector-list">
        {#each lastMutation.recomputedSelectors as sel}
          <li>
            <code>{sel}</code>
            <span class="row-count">({lastMutation.recomputeResults[sel]?.length ?? 0} rows)</span>
          </li>
        {/each}
      </ul>
      <details>
        <summary>Full recompute results (JSON)</summary>
        <pre>{JSON.stringify(lastMutation.recomputeResults, null, 2)}</pre>
      </details>
    </div>
  {/if}
</main>

<style>
  :root {
    --bg: #0f0f0f;
    --fg: #e0e0e0;
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

  .recompute-panel h3 {
    margin: 0.75rem 0 0.25rem;
    font-size: 0.85rem;
    color: #aaa;
  }

  .meta {
    margin: 0.25rem 0;
    font-size: 0.9rem;
  }

  .selector-list {
    list-style: none;
    padding: 0;
    margin: 0;
  }

  .selector-list li {
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
