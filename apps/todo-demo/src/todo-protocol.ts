import { z } from "zod";
import type { Mutation, Query } from "@do-sync-engine/core";
import type { MutationMetadata } from "@do-sync-engine/utils";
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
const id = z.string().min(1);
const params = z.array(z.unknown());
export const clientMessageSchema = z.discriminatedUnion("type", [
  z.object({
    type: z.literal("subscribe"),
    requestId: id,
    query: z.enum(TODO_QUERY_NAMES),
    params,
  }),
  z.object({
    type: z.literal("unsubscribe"),
    requestId: id,
    topicHash: z.string().regex(/^[0-9a-f]{64}$/),
  }),
  z.object({
    type: z.literal("sync"),
    requestId: id,
    mutation: z.enum(["addTodo", "toggleTodo", "deleteTodo", "clearCompleted"]),
    params,
  }),
]);
export type ClientMessage = z.infer<typeof clientMessageSchema>;
const topic = z.object({ name: z.enum(TODO_QUERY_NAMES), params, hash: z.string() });
export const serverMessageSchema = z.union([
  z.object({ type: z.literal("queryResult"), requestId: id.optional(), topic, value: z.unknown() }),
  z.object({
    type: z.literal("unsubscribed"),
    requestId: id,
    topicHash: z.string(),
    removed: z.boolean(),
  }),
  z.object({ type: z.literal("synced"), requestId: id }),
  z.object({ type: z.literal("error"), requestId: id.optional(), message: z.string() }),
]);
export type ServerMessage = z.infer<typeof serverMessageSchema>;
export function parseServerMessage(message: string): ServerMessage {
  return serverMessageSchema.parse(JSON.parse(message));
}
