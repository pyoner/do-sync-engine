import { fromStore } from "svelte/store";
import { useSelector } from "@xstate/store-svelte";
import { createSyncStore, ManualConnectionStrategy } from "@do-sync-engine/xstate-store";
import type { OpParams, StringKey, Topics } from "@do-sync-engine/core";
import {
  TODO_WS_PATH,
  type Todo,
  type TodoMutations,
  type TodoQueries,
  type TodoSummary,
} from "./todo-protocol";

const filters = [
  { label: "All", query: "allTodos" },
  { label: "Active", query: "incompleteTodos" },
  { label: "Completed", query: "completedTodos" },
] as const;

type TodoFilter = (typeof filters)[number];
type TodoListItem = TodoSummary & Pick<Todo, "completed">;

export function createTodoAppState() {
  const url = `${globalThis.location.protocol === "https:" ? "wss:" : "ws:"}//${globalThis.location.host}${TODO_WS_PATH}`;
  const syncStore = createSyncStore<TodoQueries, TodoMutations>({
    url,
    strategy: new ManualConnectionStrategy(),
  });
  const { store } = syncStore;
  const context = fromStore(useSelector(store, (snapshot) => snapshot.context));
  const state = $state({
    newTitle: "",
    selectedFilter: filters[0] as TodoFilter,
    mutationPending: false,
    mutationError: null as string | null,
    resultStatus: "idle" as "idle" | "loading" | "ready",
  });
  let activeResult: unknown;
  let connectionVersion = 0;
  let previousStatus = store.getSnapshot().context.status;
  const visible = $derived.by(() => {
    const { status, topics } = context.current;
    const filter = state.selectedFilter;
    const value =
      status === "ready" && state.resultStatus === "ready" ? topics[filter.query] : undefined;
    return {
      queryResult: value,
      todos: value === undefined ? [] : toTodoListItems(filter, value),
    };
  });
  function topicFor(filter: TodoFilter): Topics<TodoQueries> {
    return { name: filter.query, params: [] } as Topics<TodoQueries>;
  }

  function toTodoListItems(filter: TodoFilter, value: unknown): TodoListItem[] {
    const results = value as TodoSummary[];
    if (filter.query === "allTodos") return results as TodoListItem[];
    return results.map((todo) => ({
      ...todo,
      completed: filter.query === "completedTodos" ? 1 : 0,
    }));
  }

  function activateFilter(filter: TodoFilter): void {
    activeResult = context.current.topics[filter.query];
    state.resultStatus = "loading";
    const result = syncStore.subscribe(topicFor(filter), filter.query);
    if (result instanceof Error) {
      state.resultStatus = "idle";
      state.mutationError = result.message;
    }
  }

  $effect(() => {
    const { status, error, topics } = context.current;
    if (status !== previousStatus && (status === "ready" || status === "disconnected")) {
      connectionVersion++;
    }
    if (status === "disconnected") {
      state.resultStatus = "idle";
      state.mutationPending = false;
      state.mutationError = null;
    } else if (state.resultStatus === "loading") {
      if (error !== null) {
        state.resultStatus = "idle";
      } else if (topics[state.selectedFilter.query] !== activeResult) {
        state.resultStatus = "ready";
      }
    }
    previousStatus = status;
  });

  function connect(): void {
    if (context.current.status !== "disconnected") return;
    syncStore.connect();
    activateFilter(state.selectedFilter);
  }

  function disconnect(): void {
    syncStore.disconnect();
  }

  function selectFilter(filter: TodoFilter): void {
    if (state.selectedFilter.query === filter.query) return;
    const previousTopic = topicFor(state.selectedFilter);
    const ready = context.current.status === "ready";
    state.selectedFilter = filter;
    state.mutationError = null;
    if (!ready) return;
    activateFilter(filter);
    syncStore.unsubscribe(previousTopic);
  }

  function mutate<Name extends StringKey<TodoMutations>>(
    mutation: Name,
    params: OpParams<TodoMutations[Name]>,
    afterSuccess?: () => void,
  ): void {
    if (context.current.status !== "ready" || state.mutationPending) return;
    const version = connectionVersion;
    state.mutationPending = true;
    state.mutationError = null;
    void syncStore
      .sync(mutation, params)
      .then((result) => {
        if (version !== connectionVersion) return;
        if (result instanceof Error) {
          state.mutationError = result.message;
        } else {
          afterSuccess?.();
        }
      })
      .finally(() => {
        if (version === connectionVersion) state.mutationPending = false;
      });
  }

  function addTodo(): void {
    const title = state.newTitle.trim();
    if (title) mutate("addTodo", [title], () => (state.newTitle = ""));
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

  return {
    get todos() {
      return visible.todos;
    },
    get newTitle() {
      return state.newTitle;
    },
    set newTitle(value: string) {
      state.newTitle = value;
    },
    get queryResult() {
      return visible.queryResult;
    },
    get selectedFilter() {
      return state.selectedFilter;
    },
    get filterLoading() {
      return (
        context.current.status === "ready" &&
        state.resultStatus === "loading" &&
        context.current.error === null
      );
    },
    get loading() {
      return state.mutationPending;
    },
    get connected() {
      return context.current.status === "ready";
    },
    get connecting() {
      return context.current.status === "connecting";
    },
    get errorMessage() {
      return context.current.error?.message ?? state.mutationError;
    },
    filters,
    connect,
    disconnect,
    selectFilter,
    addTodo,
    toggleTodo,
    deleteTodo,
    clearCompleted,
  };
}
