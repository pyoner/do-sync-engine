type Operation<Params extends unknown[] = [], Result = unknown> = {
  tables: readonly string[];
  run(...params: Params): Result | PromiseLike<Result>;
};

export type Query<Params extends unknown[] = [], Result = unknown> = Operation<Params, Result>;

export type Mutation<Params extends unknown[] = [], Metadata = unknown> = Operation<
  Params,
  Metadata
>;

export type QueryMap<Queries extends object = Record<string, Query<any[], unknown>>> = {
  [Name in keyof Queries]: Query<any[], unknown>;
};

export type MutationMap<Mutations extends object = Record<string, Mutation<any[], unknown>>> = {
  [Name in keyof Mutations]: Mutation<any[], unknown>;
};

export type SubscriptionId = number;

export interface Subscription<QueryName extends string = string> {
  id: SubscriptionId;
  query: QueryName;
  params: readonly unknown[];
}

export interface Snapshot<QueryName extends string = string> {
  nextSubscriptionId: SubscriptionId;
  subscriptions: readonly Subscription<QueryName>[];
}

export interface QueryResult<QueryName extends string = string, Result = unknown> {
  subscriptionId: SubscriptionId;
  query: QueryName;
  params: readonly unknown[];
  result: Result;
}

export interface MutationResult<Metadata = unknown, QueryName extends string = string> {
  metadata: Metadata;
  results: readonly QueryResult<QueryName>[];
}

export interface SyncEngineOptions<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  queries: Queries;
  mutations: Mutations;
  snapshot?: Snapshot<Extract<keyof Queries, string>>;
}
