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
