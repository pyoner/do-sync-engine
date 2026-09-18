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

type SubscriptionState =
  | { tag: "idle" }
  | { tag: "loading"; version: number; loaded: boolean }
  | { tag: "active"; version: number; unsubscribe: () => void };
type MutationState = { tag: "idle" } | { tag: "pending" };
type FeedbackState = { tag: "idle" } | { tag: "error"; message: string };
type SessionState =
  | { tag: "disconnected" }
  | {
      tag: "connected";
      root: TodoService;
      subscription: SubscriptionState;
      mutation: MutationState;
    };

export function createTodoAppState() {
  const state = $state({
    todos: [] as TodoListItem[],
    newTitle: "",
    queryResults: {} as Partial<TodoQueryResults>,
    selectedFilter: filters[0] as TodoFilter,
    session: { tag: "disconnected" } as SessionState,
    feedback: { tag: "idle" } as FeedbackState,
  });
  let nextSubscriptionVersion = 0;

  function errorMessage(error: unknown): string {
    return error instanceof Error ? error.message : String(error);
  }

  function currentSession(
    root: TodoService,
  ): Extract<SessionState, { tag: "connected" }> | undefined {
    const session = state.session;
    return session.tag === "connected" && session.root === root ? session : undefined;
  }

  function currentSubscription(root: TodoService, version: number): boolean {
    const subscription = currentSession(root)?.subscription;
    return subscription?.tag !== "idle" && subscription?.version === version;
  }

  function cancelSubscription(session: Extract<SessionState, { tag: "connected" }>): void {
    if (session.subscription.tag === "active") session.subscription.unsubscribe();
    session.subscription = { tag: "idle" };
  }

  function closeSession(root?: TodoService, error?: unknown): void {
    const session = state.session;
    if (session.tag !== "connected" || (root !== undefined && session.root !== root)) return;

    cancelSubscription(session);
    state.session = { tag: "disconnected" };
    state.feedback =
      error === undefined ? { tag: "idle" } : { tag: "error", message: errorMessage(error) };
    session.root[Symbol.dispose]();
  }

  function disconnect(): void {
    closeSession();
  }

  function showSubscriptionError(root: TodoService, version: number, error: unknown): void {
    const session = currentSession(root);
    if (session === undefined || !currentSubscription(root, version)) return;

    session.subscription = { tag: "idle" };
    state.feedback = { tag: "error", message: errorMessage(error) };
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
    if (!currentSubscription(root, version)) return;

    const listener = (event: { value: unknown }) => {
      if (!currentSubscription(root, version)) return;
      state.queryResults = { ...state.queryResults, [filter.query]: event.value };
      state.todos = toTodoListItems(filter, event.value);
      const session = currentSession(root);
      if (session !== undefined && session.subscription.tag === "loading") {
        session.subscription.loaded = true;
      }
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
    const session = currentSession(root);
    if (session === undefined || !currentSubscription(root, version)) {
      unsubscribe();
      return;
    }
    session.subscription = { tag: "active", version, unsubscribe };
  }

  function startSubscription(root: TodoService, filter: TodoFilter): void {
    const session = currentSession(root);
    if (session === undefined) return;

    cancelSubscription(session);
    const version = ++nextSubscriptionVersion;
    session.subscription = { tag: "loading", version, loaded: false };
    void subscribeToFilter(root, filter, version).catch((error) => {
      showSubscriptionError(root, version, error);
    });
  }

  function selectFilter(filter: TodoFilter): void {
    if (state.selectedFilter.query === filter.query) return;

    state.selectedFilter = filter;
    state.todos = [];
    state.queryResults = {};
    state.feedback = { tag: "idle" };
    if (state.session.tag === "connected") startSubscription(state.session.root, filter);
  }

  function connect(): void {
    if (state.session.tag === "connected") return;

    const root = newWebSocketRpcSession<Service<TodoQueries, TodoMutations>>(
      `${globalThis.location.protocol === "https:" ? "wss:" : "ws:"}//${globalThis.location.host}${TODO_WS_PATH}`,
    );
    state.session = {
      tag: "connected",
      root,
      subscription: { tag: "idle" },
      mutation: { tag: "idle" },
    };
    state.feedback = { tag: "idle" };
    startSubscription(root, state.selectedFilter);
    root.onRpcBroken((error) => {
      closeSession(root, error);
    });
  }

  function mutate(
    operation: (root: TodoService) => Promise<void | Error>,
    afterSuccess?: () => void,
  ): void {
    const session = state.session;
    if (session.tag !== "connected") return;

    const root = session.root;
    session.mutation = { tag: "pending" };
    state.feedback = { tag: "idle" };
    void (async () => {
      try {
        const result = await operation(root);
        if (result instanceof Error) throw result;
        if (currentSession(root) === undefined) return;
        afterSuccess?.();
      } catch (error) {
        if (currentSession(root) !== undefined) {
          state.feedback = { tag: "error", message: errorMessage(error) };
        }
      } finally {
        const activeSession = currentSession(root);
        if (activeSession !== undefined) activeSession.mutation = { tag: "idle" };
      }
    })();
  }

  function addTodo(): void {
    const title = state.newTitle.trim();
    if (title) {
      mutate(
        (root) => root.sync("addTodo", [title]),
        () => (state.newTitle = ""),
      );
    }
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
      return (
        state.session.tag === "connected" &&
        state.session.subscription.tag === "loading" &&
        !state.session.subscription.loaded
      );
    },
    get loading() {
      return state.session.tag === "connected" && state.session.mutation.tag === "pending";
    },
    get connected() {
      return state.session.tag === "connected";
    },
    get errorMessage() {
      return state.feedback.tag === "error" ? state.feedback.message : null;
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
