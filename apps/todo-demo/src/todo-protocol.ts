import { z } from "zod";

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

export const mutationMetadataSchema = z.object({
  rowsAffected: z.number(),
  lastInsertRowid: z.number().nullable(),
});
export type MutationMetadata = z.infer<typeof mutationMetadataSchema>;

export const mutationResponseSchema = z.object({ metadata: mutationMetadataSchema });
export type MutationResponse = z.infer<typeof mutationResponseSchema>;

export const TODO_QUERY_NAMES = [
  "allTodos",
  "incompleteTodos",
  "completedTodos",
  "todoCount",
] as const;
export const todoQueryNameSchema = z.enum(TODO_QUERY_NAMES);
export type TodoQueryName = z.infer<typeof todoQueryNameSchema>;
export function isTodoQueryName(value: unknown): value is TodoQueryName {
  return todoQueryNameSchema.safeParse(value).success;
}

export const todoQueryResultsSchema = z.object({
  allTodos: z.array(todoSchema),
  incompleteTodos: z.array(todoSummarySchema),
  completedTodos: z.array(todoSummarySchema),
  todoCount: z.array(todoCountSchema),
});
export type TodoQueryResults = z.infer<typeof todoQueryResultsSchema>;

const requestIdSchema = z.string().refine((value) => value.trim().length > 0);
const todoIdSchema = z.number().int().positive();
const subscriptionQueriesSchema = z
  .array(todoQueryNameSchema)
  .min(1)
  .transform((queries) => [...new Set(queries)] as TodoQueryName[]);

const addTodoCommandSchema = z.object({
  type: z.literal("addTodo"),
  title: z.string().trim().min(1),
});
const toggleTodoCommandSchema = z.object({
  type: z.literal("toggleTodo"),
  todoId: todoIdSchema,
});
const deleteTodoCommandSchema = z.object({
  type: z.literal("deleteTodo"),
  todoId: todoIdSchema,
});
const clearCompletedCommandSchema = z.object({
  type: z.literal("clearCompleted"),
});
const subscribeCommandSchema = z.object({
  type: z.literal("subscribe"),
  queries: subscriptionQueriesSchema,
});
const unsubscribeCommandSchema = z.object({
  type: z.literal("unsubscribe"),
  queries: subscriptionQueriesSchema,
});

export const mutationCommandSchema = z.discriminatedUnion("type", [
  addTodoCommandSchema,
  toggleTodoCommandSchema,
  deleteTodoCommandSchema,
  clearCompletedCommandSchema,
]);
export type MutationCommand = z.infer<typeof mutationCommandSchema>;

export const subscriptionCommandSchema = z.discriminatedUnion("type", [
  subscribeCommandSchema,
  unsubscribeCommandSchema,
]);
export type SubscriptionCommand = z.infer<typeof subscriptionCommandSchema>;

export const clientCommandSchema = z.discriminatedUnion("type", [
  addTodoCommandSchema,
  toggleTodoCommandSchema,
  deleteTodoCommandSchema,
  clearCompletedCommandSchema,
  subscribeCommandSchema,
  unsubscribeCommandSchema,
]);
export type ClientCommand = z.infer<typeof clientCommandSchema>;

const addTodoClientMessageSchema = addTodoCommandSchema.extend({ requestId: requestIdSchema });
const toggleTodoClientMessageSchema = toggleTodoCommandSchema.extend({
  requestId: requestIdSchema,
});
const deleteTodoClientMessageSchema = deleteTodoCommandSchema.extend({
  requestId: requestIdSchema,
});
const clearCompletedClientMessageSchema = clearCompletedCommandSchema.extend({
  requestId: requestIdSchema,
});
const subscribeClientMessageSchema = subscribeCommandSchema.extend({ requestId: requestIdSchema });
const unsubscribeClientMessageSchema = unsubscribeCommandSchema.extend({
  requestId: requestIdSchema,
});

export const clientMessageSchema = z.discriminatedUnion("type", [
  addTodoClientMessageSchema,
  toggleTodoClientMessageSchema,
  deleteTodoClientMessageSchema,
  clearCompletedClientMessageSchema,
  subscribeClientMessageSchema,
  unsubscribeClientMessageSchema,
]);
export type ClientMessage = z.infer<typeof clientMessageSchema>;

const allTodosQueryResultMessageSchema = z.object({
  type: z.literal("queryResult"),
  query: z.literal("allTodos"),
  result: z.array(todoSchema),
});
const incompleteTodosQueryResultMessageSchema = z.object({
  type: z.literal("queryResult"),
  query: z.literal("incompleteTodos"),
  result: z.array(todoSummarySchema),
});
const completedTodosQueryResultMessageSchema = z.object({
  type: z.literal("queryResult"),
  query: z.literal("completedTodos"),
  result: z.array(todoSummarySchema),
});
const todoCountQueryResultMessageSchema = z.object({
  type: z.literal("queryResult"),
  query: z.literal("todoCount"),
  result: z.array(todoCountSchema),
});

export const queryResultMessageSchema = z.union([
  allTodosQueryResultMessageSchema,
  incompleteTodosQueryResultMessageSchema,
  completedTodosQueryResultMessageSchema,
  todoCountQueryResultMessageSchema,
]);
export type QueryResultMessage = z.infer<typeof queryResultMessageSchema>;

const mutationServerMessageSchema = z.object({
  type: z.literal("mutation"),
  requestId: z.string(),
  mutation: mutationResponseSchema,
});
const errorServerMessageSchema = z.object({
  type: z.literal("error"),
  requestId: z.string().optional(),
  message: z.string(),
});

export const serverMessageSchema = z.union([
  queryResultMessageSchema,
  mutationServerMessageSchema,
  errorServerMessageSchema,
]);
export type ServerMessage = z.infer<typeof serverMessageSchema>;

export type ClientMessageParseResult = ClientMessage | { error: string; requestId?: string };

export function parseClientMessage(message: string): ClientMessageParseResult {
  let value: unknown;
  try {
    value = JSON.parse(message);
  } catch {
    return { error: "Invalid JSON message" };
  }

  const clientMessageEnvelopeSchema = z
    .object({ requestId: requestIdSchema, type: z.unknown().optional() })
    .loose();
  const envelope = clientMessageEnvelopeSchema.safeParse(value);
  if (!envelope.success) {
    return { error: "requestId required" };
  }

  const { requestId, type } = envelope.data;

  switch (type) {
    case "subscribe":
    case "unsubscribe": {
      if (!Array.isArray(envelope.data.queries) || envelope.data.queries.length === 0) {
        return { error: "queries required", requestId };
      }
      const schema =
        type === "subscribe" ? subscribeClientMessageSchema : unsubscribeClientMessageSchema;
      const result = schema.safeParse(envelope.data);
      if (!result.success) {
        return { error: "Unknown query", requestId };
      }
      return result.data;
    }
    case "addTodo": {
      const result = addTodoClientMessageSchema.safeParse(envelope.data);
      if (!result.success) {
        return { error: "title required", requestId };
      }
      return result.data;
    }
    case "toggleTodo":
    case "deleteTodo": {
      const schema =
        type === "toggleTodo" ? toggleTodoClientMessageSchema : deleteTodoClientMessageSchema;
      const result = schema.safeParse(envelope.data);
      if (!result.success) {
        return { error: "todoId required", requestId };
      }
      return result.data;
    }
    case "clearCompleted": {
      const result = clearCompletedClientMessageSchema.safeParse(envelope.data);
      if (!result.success) {
        return { error: "Unknown message type", requestId };
      }
      return result.data;
    }
    default:
      return { error: "Unknown message type", requestId };
  }
}

export function parseServerMessage(message: string): ServerMessage {
  const value = JSON.parse(message);
  return serverMessageSchema.parse(value);
}
