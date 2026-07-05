export const TODO_WS_PATH = "/api/todos";

export interface Todo {
  id: number;
  title: string;
  completed: number;
  created_at: number;
}

export interface MutationResponse {
  metadata: { rowsAffected: number; lastInsertRowid: number | null };
  recomputedSelectors: string[];
  recomputeResults: Record<string, unknown[]>;
}

export type ClientCommand =
  | { type: "addTodo"; title: string }
  | { type: "toggleTodo"; todoId: number }
  | { type: "deleteTodo"; todoId: number }
  | { type: "clearCompleted" };

export type ClientMessage = ClientCommand & { requestId: string };

export type ServerMessage =
  | { type: "todos"; todos: Todo[] }
  | { type: "mutation"; requestId: string; mutation: MutationResponse }
  | { type: "error"; requestId?: string; message: string };
