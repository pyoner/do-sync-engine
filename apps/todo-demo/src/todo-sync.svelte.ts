import { useSelector } from "@xstate/store-svelte";
import type { OpParams, StringKey, Topics } from "@do-sync-engine/core";
import { createSyncStore, ManualConnectionStrategy } from "@do-sync-engine/xstate-store";
import { fromStore } from "svelte/store";
import { TODO_WS_PATH, type TodoMutations, type TodoQueries } from "./todo-protocol";

export const RESULTS_KEY = "todos";
export const filters = [
  { label: "All", topic: { name: "allTodos", params: [] } },
  { label: "Active", topic: { name: "incompleteTodos", params: [] } },
  { label: "Completed", topic: { name: "completedTodos", params: [] } },
] as const satisfies ReadonlyArray<{ label: string; topic: Topics<TodoQueries> }>;
export type TodoFilter = (typeof filters)[number];

export function createTodoSync() {
  const url = `${globalThis.location.protocol === "https:" ? "wss:" : "ws:"}//${globalThis.location.host}${TODO_WS_PATH}`;
  const syncStore = createSyncStore<TodoQueries, TodoMutations>({
    url,
    strategy: new ManualConnectionStrategy(),
  });
  const status = fromStore(useSelector(syncStore.store, (state) => state.context.status));
  const error = fromStore(useSelector(syncStore.store, (state) => state.context.error));
  const items = fromStore(
    useSelector(syncStore.store, (state) => state.context.topics[RESULTS_KEY]),
  );
  let selectedFilter = $state.raw<TodoFilter>(filters[0]!);
  let newTitle = $state("");
  let pending = $state(false);
  let mutationError = $state<string | null>(null);
  let mutationVersion = 0;

  function resetMutation(): void {
    mutationVersion++;
    pending = false;
    mutationError = null;
  }

  function mutate<Name extends StringKey<TodoMutations>>(
    name: Name,
    params: OpParams<TodoMutations[Name]>,
    onSuccess?: () => void,
  ): void {
    if (status.current !== "ready" || pending) return;
    const version = mutationVersion;
    pending = true;
    mutationError = null;
    void syncStore
      .sync(name, params)
      .then((result) => {
        if (version !== mutationVersion) return;
        if (result instanceof Error) mutationError = result.message;
        else onSuccess?.();
      })
      .finally(() => {
        if (version === mutationVersion) pending = false;
      });
  }

  function addTodo(): void {
    const title = newTitle.trim();
    if (title) mutate("addTodo", [title], () => (newTitle = ""));
  }

  function toggleTodo(id: number): void {
    mutate("toggleTodo", [id]);
  }

  function deleteTodo(id: number): void {
    mutate("deleteTodo", [id]);
  }

  function clearCompleted(): void {
    mutate("clearCompleted", []);
  }

  const disconnectReset = syncStore.store.subscribe(({ context }) => {
    if (context.status === "disconnected") resetMutation();
  });

  return {
    status,
    error,
    items,
    get selectedFilter() {
      return selectedFilter;
    },
    get newTitle() {
      return newTitle;
    },
    set newTitle(value: string) {
      newTitle = value;
    },
    get pending() {
      return pending;
    },
    get mutationError() {
      return mutationError;
    },
    connect(): void {
      if (status.current !== "disconnected") return;
      syncStore.connect();
      const result = syncStore.subscribe(selectedFilter.topic, RESULTS_KEY);
      if (result instanceof Error) mutationError = result.message;
    },
    selectFilter(filter: TodoFilter): void {
      if (filter === selectedFilter) return;
      const previous = selectedFilter;
      selectedFilter = filter;
      mutationError = null;
      if (status.current !== "ready") return;
      syncStore.unsubscribe(previous.topic);
      const result = syncStore.subscribe(filter.topic, RESULTS_KEY);
      if (result instanceof Error) mutationError = result.message;
    },
    disconnect: syncStore.disconnect,
    addTodo,
    toggleTodo,
    deleteTodo,
    clearCompleted,
    dispose(): void {
      disconnectReset.unsubscribe();
      syncStore[Symbol.dispose]();
    },
  };
}
