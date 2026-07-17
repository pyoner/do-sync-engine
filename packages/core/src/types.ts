declare const brand: unique symbol;

export type Branded<
  Primitive extends string | number | boolean | bigint | symbol,
  Tag extends string,
> = Primitive & { readonly [brand]: Tag };

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

export type TopicHash = Branded<string, "TopicHash">;

export type Topic<
  Name extends string = string,
  Params extends readonly unknown[] = readonly unknown[],
> = {
  name: Name;
  params: Params;
  hash: TopicHash;
};

export type Listener = (topic: Topic, value: unknown) => void | PromiseLike<void>;

export type StringKey<T> = Extract<keyof T, string>;

export type QueryMap<Queries extends object = Record<string, Query<unknown[], unknown>>> = {
  [Name in keyof Queries]: Query<unknown[], unknown>;
};

export type MutationMap<Mutations extends object = Record<string, Mutation<unknown[], unknown>>> = {
  [Name in keyof Mutations]: Mutation<unknown[], unknown>;
};

export type ListenerId = Branded<string, "ListenerId">;
export type Subscription = { topicHash: TopicHash; listenerId: ListenerId };

export interface SyncEngineOptions<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  queries: Queries;
  mutations: Mutations;
}

export abstract class SyncEngineBase<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  abstract createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Promise<Topic<Name, OperationParams<Queries[Name]>>>;
  abstract subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener,
  ): Subscription;
  abstract unsubscribe(subscription: Subscription): boolean;
  abstract update<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Promise<void>;

  // Helpers (protected methods)
  protected abstract mutate<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Promise<readonly string[]>;

  protected abstract query<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Promise<OperationResult<Queries[Name]>>;

  protected abstract publish(topic: Topic, value: unknown): Promise<void>;
}
