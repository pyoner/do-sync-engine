import { Schema } from "effect";
import type { Mutation, Query } from "@do-sync-engine/core";
import type { MutationMetadata, SqlAdapterError } from "@do-sync-engine/sql-regex-adapter";

export const TODO_WS_PATH = "/api/todos";

export const todoSchema = Schema.Struct({
  id: Schema.Number,
  title: Schema.String,
  completed: Schema.Number,
  created_at: Schema.Number,
});
export type Todo = typeof todoSchema.Type;

export const todoSummarySchema = Schema.Struct({ id: Schema.Number, title: Schema.String });
export type TodoSummary = typeof todoSummarySchema.Type;

export const todoCountSchema = Schema.Struct({ total_count: Schema.Number });
export type TodoCount = typeof todoCountSchema.Type;

export type TodoQueries = {
  allTodos: Query<[], Todo[], SqlAdapterError>;
  incompleteTodos: Query<[], TodoSummary[], SqlAdapterError>;
  completedTodos: Query<[], TodoSummary[], SqlAdapterError>;
  todoCount: Query<[], TodoCount[], SqlAdapterError>;
};

export type TodoMutations = {
  addTodo: Mutation<[string], MutationMetadata, SqlAdapterError>;
  toggleTodo: Mutation<[number], MutationMetadata, SqlAdapterError>;
  deleteTodo: Mutation<[number], MutationMetadata, SqlAdapterError>;
  clearCompleted: Mutation<[], MutationMetadata, SqlAdapterError>;
};
