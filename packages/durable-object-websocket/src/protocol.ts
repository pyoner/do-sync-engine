import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { TopicSchema, UnknownMutationError, UnknownQueryError } from "@do-sync-engine/core";
import type { OperationParams, OperationResult, StringKey, Topic } from "@do-sync-engine/core";

const Params = Schema.Array(Schema.Unknown);
export class RpcOperationError extends Schema.TaggedErrorClass<RpcOperationError>()(
  "RpcOperationError",
  { message: Schema.String },
) {}

const QueryEvent = Schema.Struct({
  topic: TopicSchema,
  value: Schema.Unknown,
});

export const Subscribe = Rpc.make("subscribe", {
  payload: Schema.Struct({
    query: Schema.String,
    params: Params,
  }),
  success: QueryEvent,
  error: Schema.Union([UnknownQueryError, RpcOperationError]),
  stream: true,
});

export const Unsubscribe = Rpc.make("unsubscribe", {
  payload: Schema.Struct({ topic: TopicSchema }),
  success: Schema.Boolean,
  error: RpcOperationError,
});

export const Sync = Rpc.make("sync", {
  payload: Schema.Struct({
    mutation: Schema.String,
    params: Params,
  }),
  success: Schema.Void,
  error: Schema.Union([UnknownMutationError, RpcOperationError]),
});

export const WebSocketRpc = RpcGroup.make(Subscribe, Unsubscribe, Sync);

export type SubscribePayload<Q extends object> = {
  readonly query: StringKey<Q>;
  readonly params: readonly unknown[];
};
export type UnsubscribePayload = { readonly topic: Topic };
export type SyncPayload<M extends object> = {
  readonly mutation: StringKey<M>;
  readonly params: readonly unknown[];
};
export type QueryEventPayload<Q extends object> = {
  readonly topic: Topic<StringKey<Q>>;
  readonly value: OperationResult<Q[StringKey<Q>]>;
};
export type SubscribeResult<Q extends object> = QueryEventPayload<Q>;
export type OperationParamsFor<Q extends object> = OperationParams<Q[StringKey<Q>]>;
export type { Topic };
