import type {
  OperationParams,
  OperationResult,
  StringKey,
  Topic,
  TopicHash,
} from "@do-sync-engine/core";

export type SubscribeCommand<Q extends object> = {
  [N in StringKey<Q>]: {
    type: "subscribe";
    requestId: string;
    query: N;
    params: OperationParams<Q[N]>;
  };
}[StringKey<Q>];
export type UnsubscribeCommand = { type: "unsubscribe"; requestId: string; topicHash: TopicHash };
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
  topicHash: TopicHash;
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
      params: unknown[];
    }
  | {
      type: "unsubscribe";
      requestId: string;
      topicHash: TopicHash;
    }
  | {
      type: "sync";
      requestId: string;
      mutation: string;
      params: unknown[];
    };

export type DecodeResult = { command: DecodedClientCommand } | { error: ErrorMessage };

export function decodeClientCommand(message: string | ArrayBuffer): DecodeResult {
  if (typeof message !== "string")
    return { error: { type: "error", message: "Expected text WebSocket message" } };

  let value: unknown;
  try {
    value = JSON.parse(message);
  } catch {
    return { error: { type: "error", message: "Invalid JSON message" } };
  }
  if (!value || typeof value !== "object" || Array.isArray(value))
    return { error: { type: "error", message: "Invalid JSON message" } };

  const record = value as Record<string, unknown>;
  const requestId = record.requestId;
  if (typeof requestId !== "string" || !requestId.trim())
    return { error: { type: "error", message: "requestId required" } };

  if (record.type === "subscribe") {
    if (typeof record.query !== "string" || !Array.isArray(record.params))
      return { error: { type: "error", requestId, message: "query and params required" } };
    return {
      command: { type: "subscribe", requestId, query: record.query, params: record.params },
    };
  }
  if (record.type === "unsubscribe") {
    if (typeof record.topicHash !== "string" || !/^[0-9a-f]{64}$/.test(record.topicHash))
      return { error: { type: "error", requestId, message: "topicHash required" } };
    return {
      command: { type: "unsubscribe", requestId, topicHash: record.topicHash as TopicHash },
    };
  }
  if (record.type === "sync") {
    if (typeof record.mutation !== "string" || !Array.isArray(record.params))
      return { error: { type: "error", requestId, message: "mutation and params required" } };
    return {
      command: { type: "sync", requestId, mutation: record.mutation, params: record.params },
    };
  }
  return { error: { type: "error", requestId, message: "Unknown message type" } };
}
