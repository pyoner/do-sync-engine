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

export interface MutationMetadata {
  rowsAffected: number;
  lastInsertRowid: number | null;
}

export interface MutationResponse {
  metadata: MutationMetadata;
}

export interface TodoQueryResults {
  allTodos: Todo[];
  incompleteTodos: TodoSummary[];
  completedTodos: TodoSummary[];
  todoCount: TodoCount[];
}

export type TodoQueryName = keyof TodoQueryResults;

export type QueryResultMessage = {
  [Name in TodoQueryName]: {
    type: "queryResult";
    query: Name;
    result: TodoQueryResults[Name];
  };
}[TodoQueryName];

export type MutationCommand =
  | { type: "addTodo"; title: string }
  | { type: "toggleTodo"; todoId: number }
  | { type: "deleteTodo"; todoId: number }
  | { type: "clearCompleted" };

export type SubscriptionCommand =
  | { type: "subscribe"; queries: TodoQueryName[] }
  | { type: "unsubscribe"; queries: TodoQueryName[] };

export type ClientCommand = MutationCommand | SubscriptionCommand;

export type ClientMessage = ClientCommand & { requestId: string };
export type ServerMessage =
  | QueryResultMessage
  | { type: "mutation"; requestId: string; mutation: MutationResponse }
  | { type: "error"; requestId?: string; message: string };
