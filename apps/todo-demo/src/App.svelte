<script lang="ts">
  import { onMount } from "svelte";
  import { newWebSocketRpcSession, type RpcStub } from "capnweb";
  import {
    TODO_WS_PATH,
    type Todo,
    type TodoMutations,
    type TodoQueries,
    type TodoQueryName,
    type TodoQueryResults,
  } from "./todo-protocol";
  import type { OperationParams } from "@do-sync-engine/core";
  type Event = { topic: { name: TodoQueryName }; value: unknown };
  type Api = {
    subscribe<N extends TodoQueryName>(
      query: N,
      params: OperationParams<TodoQueries[N]>,
      listener: (event: Event) => void,
    ): Promise<{ unsubscribe(): boolean }>;
    sync<N extends keyof TodoMutations & string>(
      mutation: N,
      params: OperationParams<TodoMutations[N]>,
    ): Promise<void>;
  };
  const defaultQueries: TodoQueryName[] = ["allTodos", "todoCount"];
  let todos = $state<Todo[]>([]);
  let newTitle = $state("");
  let queryResults = $state<Partial<TodoQueryResults>>({});
  let loading = $state(false);
  let api = $state<RpcStub<Api> | null>(null);
  let connected = $state(false);
  let errorMessage = $state<string | null>(null);
  let subscriptions: Array<{ unsubscribe(): boolean }> = [];

  function websocketUrl(): string {
    return `${location.protocol === "https:" ? "wss:" : "ws:"}//${location.host}${TODO_WS_PATH}`;
  }
  function connect(): () => void {
    const root = newWebSocketRpcSession<Api>(websocketUrl());
    api = root;
    connected = true;
    errorMessage = null;
    for (const query of defaultQueries) {
      void root.subscribe(query, [], (event) => {
        queryResults = { ...queryResults, [event.topic.name]: event.value };
        if (event.topic.name === "allTodos") todos = event.value as Todo[];
      }).then((subscription) => subscriptions.push(subscription)).catch((error) => {
        errorMessage = error instanceof Error ? error.message : String(error);
      });
    }
    root.onRpcBroken((error) => {
      connected = false;
      api = null;
      errorMessage = error instanceof Error ? error.message : String(error);
    });
    return () => {
      for (const subscription of subscriptions) subscription.unsubscribe();
      subscriptions = [];
      root[Symbol.dispose]();
      api = null;
      connected = false;
    };
  }
  function mutate<N extends keyof TodoMutations & string>(
    mutation: N,
    params: TodoMutations[N] extends { params: infer P } ? P : never,
    afterSuccess?: () => void,
  ) {
    if (!api) return;
    loading = true;
    errorMessage = null;
    void api.sync(mutation, params).then(afterSuccess).catch((error) => {
      errorMessage = error instanceof Error ? error.message : String(error);
    }).finally(() => loading = false);
  }
  function addTodo() {
    const title = newTitle.trim();
    if (title) mutate("addTodo", [title], () => newTitle = "");
  }
  function toggleTodo(id: number) { mutate("toggleTodo", [id]); }
  function deleteTodo(id: number) { mutate("deleteTodo", [id]); }
  function clearCompleted() { mutate("clearCompleted", []); }
  onMount(connect);
</script>

<main>
  <h1>TODO Demo</h1>
  <p class="subtitle">Powered by <code>@do-sync-engine/core</code> + Cloudflare Durable Objects</p>

  {#if errorMessage}
    <p class="status error">{errorMessage}</p>
  {:else if !connected}
    <p class="status">Connecting...</p>
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
    <h2>Subscribed queries</h2>
    <ul class="query-list">
      {#each defaultQueries as query}
        <li>
          <code>{query}</code>
          <span class="row-count">({queryResults[query]?.length ?? 0} rows)</span>
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
