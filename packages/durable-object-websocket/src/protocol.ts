import type {
  ListenerEvent,
  MutationMap,
  OperationParams,
  OperationResult,
  QueryMap,
  StringKey,
  Topic,
} from "@do-sync-engine/core";
import * as errore from "errore";

export type QueryTopic<Queries extends QueryMap<Queries>> = {
  [Name in StringKey<Queries>]: Topic<Name, OperationParams<Queries[Name]>>;
}[StringKey<Queries>];

export type SubscriptionEvent<Queries extends QueryMap<Queries>> = {
  [Name in StringKey<Queries>]: ListenerEvent<
    Name,
    OperationParams<Queries[Name]>,
    OperationResult<Queries[Name]>
  >;
}[StringKey<Queries>];
export interface ServerAPI<
  Queries extends QueryMap<Queries>,
  Mutations extends MutationMap<Mutations>,
> {
  subscribe(topic: QueryTopic<Queries>): void | Error;
  unsubscribe(topic: QueryTopic<Queries>): void | Error;
  sync<Name extends StringKey<Mutations>>(
    name: Name,
    params: OperationParams<Mutations[Name]>,
  ): void | Error;
}

export class InvalidRpcParamsError extends errore.createTaggedError({
  name: "InvalidRpcParamsError",
  message: "Invalid $method parameters",
}) {}

export class WebSocketTransportError extends errore.createTaggedError({
  name: "WebSocketTransportError",
  message: "WebSocket transport failed to $operation",
}) {}

export function isRecord(value: unknown): value is Record<string, unknown> {
  return typeof value === "object" && value !== null && !Array.isArray(value);
}

export function isTopic(value: unknown): value is Topic<string> {
  return isRecord(value) && typeof value.name === "string" && Array.isArray(value.params);
}

export function decodeMessage(message: string | ArrayBuffer): unknown | Error {
  const text = typeof message === "string" ? message : new TextDecoder().decode(message);
  return errore.try({
    try: () => JSON.parse(text) as unknown,
    catch: (cause) => new WebSocketTransportError({ operation: "decode message", cause }),
  });
}

export function decodeSubscriptionEvent<Queries extends QueryMap<Queries>>(
  message: string | ArrayBuffer,
): SubscriptionEvent<Queries> | Error | null {
  const decoded = decodeMessage(message);
  if (decoded instanceof Error) return decoded;
  if (!isRecord(decoded) || !("topic" in decoded)) return null;
  if (!isTopic(decoded.topic) || !("value" in decoded)) {
    return new InvalidRpcParamsError({ method: "subscription event" });
  }
  return decoded as unknown as SubscriptionEvent<Queries>;
}
