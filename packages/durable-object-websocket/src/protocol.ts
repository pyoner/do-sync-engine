import { Effect, Schema } from "effect";
import { TopicSchema } from "@do-sync-engine/core";
import type { OperationParams, OperationResult, StringKey, Topic } from "@do-sync-engine/core";

export type SubscribeCommand<Q extends object> = {
  [N in StringKey<Q>]: {
    type: "subscribe";
    requestId: string;
    query: N;
    params: OperationParams<Q[N]>;
  };
}[StringKey<Q>];
export type UnsubscribeCommand = { type: "unsubscribe"; requestId: string; topic: Topic };
export type SyncCommand<M extends object> = {
  [N in StringKey<M>]: {
    type: "sync";
    requestId: string;
    mutation: N;
    params: OperationParams<M[N]>;
  };
}[StringKey<M>];
export type ClientCommand<Q extends object, M extends object> =
  | SubscribeCommand<Q>
  | UnsubscribeCommand
  | SyncCommand<M>;
export type QueryResultMessage<Q extends object> = {
  [N in StringKey<Q>]: {
    type: "queryResult";
    requestId?: string;
    topic: Topic<N, OperationParams<Q[N]>>;
    value: OperationResult<Q[N]>;
  };
}[StringKey<Q>];
export type UnsubscribedMessage = {
  type: "unsubscribed";
  requestId: string;
  topic: Topic;
  removed: boolean;
};
export type SyncedMessage = { type: "synced"; requestId: string };
export type ErrorMessage = { type: "error"; requestId?: string; message: string };
export type ServerMessage<Q extends object> =
  | QueryResultMessage<Q>
  | UnsubscribedMessage
  | SyncedMessage
  | ErrorMessage;
export type DecodedClientCommand =
  | {
      type: "subscribe";
      requestId: string;
      query: string;
      params: readonly unknown[];
    }
  | {
      type: "unsubscribe";
      requestId: string;
      topic: Topic;
    }
  | {
      type: "sync";
      requestId: string;
      mutation: string;
      params: readonly unknown[];
    };

const requestIdSchema = Schema.String.check(Schema.isPattern(/\S/));
const clientCommandSchema = Schema.Union([
  Schema.Struct({
    type: Schema.Literal("subscribe"),
    requestId: requestIdSchema,
    query: Schema.String,
    params: Schema.Array(Schema.Unknown),
  }),
  Schema.Struct({
    type: Schema.Literal("unsubscribe"),
    requestId: requestIdSchema,
    topic: TopicSchema,
  }),
  Schema.Struct({
    type: Schema.Literal("sync"),
    requestId: requestIdSchema,
    mutation: Schema.String,
    params: Schema.Array(Schema.Unknown),
  }),
]);

export const decodeClientCommand = (message: string | ArrayBuffer) =>
  Effect.gen(function* () {
    if (typeof message !== "string") {
      return yield* Effect.fail<ErrorMessage>({
        type: "error",
        message: "Expected text WebSocket message",
      });
    }
    const value = yield* Effect.try({
      try: () => JSON.parse(message) as unknown,
      catch: () =>
        ({
          type: "error",
          message: "Invalid JSON message",
        }) satisfies ErrorMessage,
    });
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return yield* Effect.fail<ErrorMessage>({
        type: "error",
        message: "Invalid JSON message",
      });
    }
    return yield* Schema.decodeUnknownEffect(clientCommandSchema)(value).pipe(
      Effect.mapError(() => {
        const record = value as Record<string, unknown>;
        const requestId = record.requestId;
        if (typeof requestId !== "string" || !requestId.trim()) {
          return { type: "error", message: "requestId required" } satisfies ErrorMessage;
        }
        if (record.type === "subscribe") {
          return {
            type: "error",
            requestId,
            message: "query and params required",
          } satisfies ErrorMessage;
        }
        if (record.type === "unsubscribe") {
          return {
            type: "error",
            requestId,
            message: "topic required",
          } satisfies ErrorMessage;
        }
        if (record.type === "sync") {
          return {
            type: "error",
            requestId,
            message: "mutation and params required",
          } satisfies ErrorMessage;
        }
        return {
          type: "error",
          requestId,
          message: "Unknown message type",
        } satisfies ErrorMessage;
      }),
    );
  });
