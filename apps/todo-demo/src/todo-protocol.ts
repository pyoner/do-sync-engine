import { Effect, Schema } from "effect";
import type { Mutation, Query } from "@do-sync-engine/core";
import type { MutationMetadata, SqlAdapterError } from "@do-sync-engine/sql-regex-adapter";
export class ProtocolDecodeError extends Schema.TaggedErrorClass<ProtocolDecodeError>()(
  "ProtocolDecodeError",
  { cause: Schema.Unknown },
) {}

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
    topic: Schema.Struct({ name: queryName, params }),
  }),
  Schema.Struct({
    type: Schema.Literal("sync"),
    requestId,
    mutation: mutationName,
    params,
  }),
]);
export type ClientMessage = typeof clientMessageSchema.Type;
const topic = Schema.Struct({ name: queryName, params });
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
    topic,
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

export function parseServerMessage(
  message: string,
): Effect.Effect<ServerMessage, ProtocolDecodeError> {
  return Effect.gen(function* () {
    const value = yield* Effect.try({
      try: () => JSON.parse(message),
      catch: (cause) => ProtocolDecodeError.make({ cause }),
    });
    return yield* Schema.decodeUnknownEffect(serverMessageSchema)(value).pipe(
      Effect.mapError((cause) => ProtocolDecodeError.make({ cause })),
    );
  });
}
