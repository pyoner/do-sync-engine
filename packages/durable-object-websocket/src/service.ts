import type {
  MutationMap,
  OperationParams,
  QueryMap,
  StringKey,
  Topic,
  ListenerEvent,
  OperationResult,
} from "@do-sync-engine/core";

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

export interface Service<
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

export const SYNC_METHOD = "sync";
