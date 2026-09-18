import { RpcStub, newWebSocketRpcSession } from "capnweb";
import type { Service } from "@do-sync-engine/durable-object-websocket";
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
type TodoService = RpcStub<Service<TodoQueries, TodoMutations>>;

export function createTodoAppState() {
  const state = $state({
    todos: [] as TodoListItem[],
    newTitle: "",
    queryResults: {} as Partial<TodoQueryResults>,
    selectedFilter: filters[0] as TodoFilter,
    filterLoading: false,
    loading: false,
    api: null as TodoService | null,
    connected: false,
    errorMessage: null as string | null,
    filterSubscriptionVersion: 0,
    unsubscribeActiveFilter: null as (() => void) | null,
  });

  function disconnect(): void {
    const root = state.api;
    state.filterSubscriptionVersion += 1;
    state.unsubscribeActiveFilter?.();
    state.unsubscribeActiveFilter = null;
    state.api = null;
    state.connected = false;
    state.filterLoading = false;
    state.loading = false;
    root?.[Symbol.dispose]();
  }

  function showSubscriptionError(root: TodoService, version: number, error: unknown): void {
    if (state.api !== root || state.filterSubscriptionVersion !== version) return;
    state.filterLoading = false;
    state.errorMessage = error instanceof Error ? error.message : String(error);
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
    root: TodoService,
    filter: TodoFilter,
    version: number,
  ): Promise<void> {
    const topic = await root.createTopic(filter.query, []);
    if (topic instanceof Error) {
      showSubscriptionError(root, version, topic);
      return;
    }
    if (state.api !== root || state.filterSubscriptionVersion !== version) return;

    const listener = (event: { value: unknown }) => {
      if (state.api !== root || state.filterSubscriptionVersion !== version) return;
      state.queryResults = { ...state.queryResults, [filter.query]: event.value };
      state.todos = toTodoListItems(filter, event.value);
      state.filterLoading = false;
    };
    const listenerStub = new RpcStub(listener);
    let subscribeResult: void | Error;
    try {
      subscribeResult = await root.subscribe(topic, listenerStub);
    } finally {
      listenerStub[Symbol.dispose]();
    }
    if (subscribeResult instanceof Error) {
      showSubscriptionError(root, version, subscribeResult);
      return;
    }

    const unsubscribe = () => {
      void root
        .unsubscribe(topic)
        .then((result) => {
          if (result instanceof Error) {
            globalThis.console.warn("Failed to unsubscribe from todo filter:", result);
          }
        })
        .catch((error) => {
          globalThis.console.warn("Failed to unsubscribe from todo filter:", error);
        });
    };
    if (state.api !== root || state.filterSubscriptionVersion !== version) {
      unsubscribe();
      return;
    }
    state.unsubscribeActiveFilter = unsubscribe;
  }

  function selectFilter(filter: TodoFilter): void {
    if (state.selectedFilter.query === filter.query) return;

    state.selectedFilter = filter;
    state.todos = [];
    state.queryResults = {};
    state.filterLoading = true;
    state.errorMessage = null;
    state.filterSubscriptionVersion += 1;
    state.unsubscribeActiveFilter?.();
    state.unsubscribeActiveFilter = null;

    const root = state.api;
    const version = state.filterSubscriptionVersion;
    if (root !== null) {
      void subscribeToFilter(root, filter, version).catch((error) => {
        showSubscriptionError(root, version, error);
      });
    }
  }

  function connect(): void {
    if (state.api !== null) return;
    const root = newWebSocketRpcSession<Service<TodoQueries, TodoMutations>>(
      `${globalThis.location.protocol === "https:" ? "wss:" : "ws:"}//${globalThis.location.host}${TODO_WS_PATH}`,
    );
    state.api = root;
    state.connected = true;
    state.filterLoading = true;
    state.errorMessage = null;

    const version = state.filterSubscriptionVersion;
    void subscribeToFilter(root, state.selectedFilter, version).catch((error) => {
      showSubscriptionError(root, version, error);
    });
    root.onRpcBroken((error) => {
      if (state.api !== root) return;
      state.unsubscribeActiveFilter = null;
      state.api = null;
      state.connected = false;
      state.filterLoading = false;
      state.loading = false;
      state.errorMessage = error instanceof Error ? error.message : String(error);
    });
  }

  function mutate(
    operation: (root: TodoService) => Promise<void | Error>,
    afterSuccess?: () => void,
  ): void {
    const root = state.api;
    if (root === null) return;

    state.loading = true;
    state.errorMessage = null;
    void (async () => {
      try {
        const result = await operation(root);
        if (result instanceof Error) throw result;
        if (state.api !== root) return;
        afterSuccess?.();
      } catch (error) {
        state.errorMessage = error instanceof Error ? error.message : String(error);
      } finally {
        if (state.api === root) state.loading = false;
      }
    })();
  }

  function addTodo(): void {
    const title = state.newTitle.trim();
    if (title)
      mutate(
        (root) => root.sync("addTodo", [title]),
        () => (state.newTitle = ""),
      );
  }

  function toggleTodo(id: number): void {
    mutate((root) => root.sync("toggleTodo", [id]));
  }

  function deleteTodo(id: number): void {
    mutate((root) => root.sync("deleteTodo", [id]));
  }

  function clearCompleted(): void {
    mutate((root) => root.sync("clearCompleted", []));
  }

  return {
    get todos() {
      return state.todos;
    },
    get newTitle() {
      return state.newTitle;
    },
    set newTitle(value: string) {
      state.newTitle = value;
    },
    get queryResults() {
      return state.queryResults;
    },
    get selectedFilter() {
      return state.selectedFilter;
    },
    get filterLoading() {
      return state.filterLoading;
    },
    get loading() {
      return state.loading;
    },
    get connected() {
      return state.connected;
    },
    get errorMessage() {
      return state.errorMessage;
    },
    filters,
    disconnect,
    connect,
    selectFilter,
    addTodo,
    toggleTodo,
    deleteTodo,
    clearCompleted,
  };
}
