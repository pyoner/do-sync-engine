import { Schema } from "effect";
import { Rpc, RpcGroup } from "effect/unstable/rpc";
import { TopicSchema, UnknownMutationError, UnknownQueryError } from "@do-sync-engine/core";

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
