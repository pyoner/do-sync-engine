import { Schema } from "effect";
import type { Mutation, Query } from "@do-sync-engine/core";
import type { MutationMetadata } from "@do-sync-engine/sql-regex-adapter";

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

const queryName = Schema.Literals(TODO_QUERY_NAMES);
const mutationName = Schema.Literals([
  "addTodo",
  "toggleTodo",
  "deleteTodo",
  "clearCompleted",
] as const);
const requestId = Schema.String.check(Schema.isPattern(/\S/));
const params = Schema.Array(Schema.Unknown);

export const clientMessageSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("subscribe"),
    requestId,
    query: queryName,
    params,
  }),
  Schema.Struct({
    type: Schema.Literal("unsubscribe"),
    requestId,
    topicHash: Schema.String.check(Schema.isPattern(/^[0-9a-f]{64}$/)),
  }),
  Schema.Struct({
    type: Schema.Literal("sync"),
    requestId,
    mutation: mutationName,
    params,
  }),
]);
export type ClientMessage = typeof clientMessageSchema.Type;
const topic = Schema.Struct({ name: queryName, params, hash: Schema.String });
export const serverMessageSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("queryResult"),
    requestId: Schema.optionalKey(requestId),
    topic,
    value: Schema.Unknown,
  }),
  Schema.Struct({
    type: Schema.Literal("unsubscribed"),
    requestId,
    topicHash: Schema.String,
    removed: Schema.Boolean,
  }),
  Schema.Struct({ type: Schema.Literal("synced"), requestId }),
  Schema.Struct({
    type: Schema.Literal("error"),
    requestId: Schema.optionalKey(requestId),
    message: Schema.String,
  }),
]);
export type ServerMessage = typeof serverMessageSchema.Type;

export function parseServerMessage(message: string): ServerMessage {
  return Schema.decodeUnknownSync(serverMessageSchema)(JSON.parse(message));
}
