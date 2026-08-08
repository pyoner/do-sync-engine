import { z } from "zod";
import type { Mutation, Query } from "@do-sync-engine/core";
import type { MutationMetadata } from "@do-sync-engine/sql-regex-adapter";
export const TODO_WS_PATH = "/api/todos";
export const todoSchema = z.object({
  id: z.number(),
  title: z.string(),
  completed: z.number(),
  created_at: z.number(),
});
export type Todo = z.infer<typeof todoSchema>;
export const todoSummarySchema = z.object({ id: z.number(), title: z.string() });
export type TodoSummary = z.infer<typeof todoSummarySchema>;
export const todoCountSchema = z.object({ total_count: z.number() });
export type TodoCount = z.infer<typeof todoCountSchema>;
export type TodoQueries = {
  allTodos: Query<[], Todo[]>;
  incompleteTodos: Query<[], TodoSummary[]>;
  completedTodos: Query<[], TodoSummary[]>;
  todoCount: Query<[], TodoCount[]>;
};
export type TodoMutations = {
  addTodo: Mutation<[string], MutationMetadata>;
  toggleTodo: Mutation<[number], MutationMetadata>;
  deleteTodo: Mutation<[number], MutationMetadata>;
  clearCompleted: Mutation<[], MutationMetadata>;
};
export const TODO_QUERY_NAMES = [
  "allTodos",
  "incompleteTodos",
  "completedTodos",
  "todoCount",
] as const;
export type TodoQueryName = (typeof TODO_QUERY_NAMES)[number];
export type TodoQueryResults = {
  allTodos: Todo[];
  incompleteTodos: TodoSummary[];
  completedTodos: TodoSummary[];
  todoCount: TodoCount[];
};
