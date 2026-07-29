import { Schema } from "effect";
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

export type DecodeResult = { command: DecodedClientCommand } | { error: ErrorMessage };

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

export function decodeClientCommand(message: string | ArrayBuffer): DecodeResult {
  if (typeof message !== "string") {
    return { error: { type: "error", message: "Expected text WebSocket message" } };
  }
  let value: unknown;
  try {
    value = JSON.parse(message);
  } catch {
    return { error: { type: "error", message: "Invalid JSON message" } };
  }
  try {
    return { command: Schema.decodeUnknownSync(clientCommandSchema)(value) };
  } catch {
    if (typeof value !== "object" || value === null || Array.isArray(value)) {
      return { error: { type: "error", message: "Invalid JSON message" } };
    }
    const requestId = (value as Record<string, unknown>).requestId;
    if (typeof requestId !== "string" || !requestId.trim()) {
      return { error: { type: "error", message: "requestId required" } };
    }
    const record = value as Record<string, unknown>;
    if ((record as Record<string, unknown>).type === "subscribe") {
      return { error: { type: "error", requestId, message: "query and params required" } };
    }
    if ((record as Record<string, unknown>).type === "unsubscribe") {
      return { error: { type: "error", requestId, message: "topic required" } };
    }
    if ((record as Record<string, unknown>).type === "sync") {
      return { error: { type: "error", requestId, message: "mutation and params required" } };
    }
    return { error: { type: "error", requestId, message: "Unknown message type" } };
  }
}
