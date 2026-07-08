type Operation<Params extends unknown[] = [], Result = unknown> = {
  tables: readonly string[];
  run(...params: Params): Result | PromiseLike<Result>;
};

export type Query<Params extends unknown[] = [], Result = unknown> = Operation<Params, Result>;

export type Mutation<Params extends unknown[] = [], Metadata = unknown> = Operation<
  Params,
  Metadata
>;

export type OperationParams<OperationDef> = OperationDef extends {
  run(...params: infer Params): unknown;
}
  ? Params
  : never;

export type OperationResult<OperationDef> = OperationDef extends {
  run(...params: never[]): infer Result;
}
  ? Awaited<Result>
  : never;

export type StringKey<T> = Extract<keyof T, string>;

export type QueryMap<Queries extends object = Record<string, Query<unknown[], unknown>>> = {
  [Name in keyof Queries]: Query<unknown[], unknown>;
};

export type MutationMap<Mutations extends object = Record<string, Mutation<unknown[], unknown>>> = {
  [Name in keyof Mutations]: Mutation<unknown[], unknown>;
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

export interface MutationResult<
  Metadata = unknown,
  QueryName extends string = string,
  Result = unknown,
> {
  metadata: Metadata;
  results: readonly QueryResult<QueryName, Result>[];
}

export type SyncEngineQueryResult<Queries extends QueryMap<Queries>> = QueryResult<
  StringKey<Queries>,
  OperationResult<Queries[StringKey<Queries>]>
>;

export type SyncEngineMutationResult<
  Queries extends QueryMap<Queries>,
  Metadata = unknown,
> = MutationResult<Metadata, StringKey<Queries>, OperationResult<Queries[StringKey<Queries>]>>;

export interface SyncEngineOptions<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  queries: Queries;
  mutations: Mutations;
  snapshot?: Snapshot<StringKey<Queries>>;
}

export interface SyncEngineInterface<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  subscribe<Name extends StringKey<Queries>>(
    query: Name,
    ...params: OperationParams<Queries[Name]>
  ): SubscriptionId;
  unsubscribe(subscriptionId: SubscriptionId): boolean;
  snapshot(): Snapshot<StringKey<Queries>>;
  mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    ...params: OperationParams<Mutations[Name]>
  ): Promise<SyncEngineMutationResult<Queries, OperationResult<Mutations[Name]>>>;
}
