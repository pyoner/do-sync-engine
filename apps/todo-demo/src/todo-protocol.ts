export const TODO_WS_PATH = "/api/todos";

export interface Todo {
  id: number;
  title: string;
  completed: number;
  created_at: number;
}

export interface TodoSummary {
  id: number;
  title: string;
}

export interface TodoCount {
  total_count: number;
}

export interface MutationResponse {
  metadata: { rowsAffected: number; lastInsertRowid: number | null };
}

export interface TodoSelectorResults {
  allTodos: Todo[];
  incompleteTodos: TodoSummary[];
  completedTodos: TodoSummary[];
  todoCount: TodoCount[];
}

export type TodoSelectorName = keyof TodoSelectorResults;

export type SelectorResultMessage = {
  [Name in TodoSelectorName]: {
    type: "selectorResult";
    selector: Name;
    result: TodoSelectorResults[Name];
  };
}[TodoSelectorName];

export type MutationCommand =
  | { type: "addTodo"; title: string }
  | { type: "toggleTodo"; todoId: number }
  | { type: "deleteTodo"; todoId: number }
  | { type: "clearCompleted" };

export type SubscriptionCommand =
  | { type: "subscribe"; selectors: TodoSelectorName[] }
  | { type: "unsubscribe"; selectors: TodoSelectorName[] };

export type ClientCommand = MutationCommand | SubscriptionCommand;

export type ClientMessage = ClientCommand & { requestId: string };

export type ServerMessage =
  | SelectorResultMessage
  | { type: "mutation"; requestId: string; mutation: MutationResponse }
  | { type: "error"; requestId?: string; message: string };
