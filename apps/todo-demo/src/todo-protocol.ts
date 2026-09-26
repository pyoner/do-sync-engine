import type { Mutation, Query } from "@do-sync-engine/core";
import type { MutationMetadata } from "@do-sync-engine/sql-regex-adapter";
export const TODO_WS_PATH = "/api/todos";
export type Todo = {
  id: number;
  title: string;
  completed: number;
  created_at: number;
};
export type TodoSummary = Pick<Todo, "id" | "title" | "completed">;
export type TodoQueries = {
  allTodos: Query<[], Todo[]>;
  incompleteTodos: Query<[], TodoSummary[]>;
  completedTodos: Query<[], TodoSummary[]>;
};
export type TodoMutations = {
  addTodo: Mutation<[string], MutationMetadata>;
  toggleTodo: Mutation<[number], MutationMetadata>;
  deleteTodo: Mutation<[number], MutationMetadata>;
  clearCompleted: Mutation<[], MutationMetadata>;
};
