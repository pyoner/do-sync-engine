import type {
  MutationMap,
  OperationParams,
  OperationResult,
  QueryMap,
  StringKey,
  Topic,
  TopicHash,
} from "@do-sync-engine/core";

type Name<T> = StringKey<T>;
export type SubscribeCommand<Q extends QueryMap<Q>> = {
  [N in Name<Q>]: { type: "subscribe"; requestId: string; query: N; params: OperationParams<Q[N]> };
}[Name<Q>];
export type UnsubscribeCommand = { type: "unsubscribe"; requestId: string; topicHash: TopicHash };
export type SyncCommand<M extends MutationMap<M>> = {
  [N in Name<M>]: { type: "sync"; requestId: string; mutation: N; params: OperationParams<M[N]> };
}[Name<M>];
export type ClientCommand<Q extends QueryMap<Q>, M extends MutationMap<M>> =
  | SubscribeCommand<Q>
  | UnsubscribeCommand
  | SyncCommand<M>;
export type QueryResultMessage<Q extends QueryMap<Q>> = {
  [N in Name<Q>]: {
    type: "queryResult";
    requestId?: string;
    topic: Topic<N, OperationParams<Q[N]>>;
    value: OperationResult<Q[N]>;
  };
}[Name<Q>];
export type UnsubscribedMessage = {
  type: "unsubscribed";
  requestId: string;
  topicHash: TopicHash;
  removed: boolean;
};
export type SyncedMessage = { type: "synced"; requestId: string };
export type ErrorMessage = { type: "error"; requestId?: string; message: string };
export type ServerMessage<Q extends QueryMap<Q>> =
  | QueryResultMessage<Q>
  | UnsubscribedMessage
  | SyncedMessage
  | ErrorMessage;
