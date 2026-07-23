declare const brand: unique symbol;

export type Branded<
  Primitive extends string | number | boolean | bigint | symbol,
  Tag extends string,
> = Primitive & { readonly [brand]: Tag };

export type Table = Branded<string, "Table">;

type Operation<Params extends unknown[] = [], Result = unknown> = {
  tables: Set<Table>;
  run(...params: Params): Result;
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
  ? Result
  : never;

export type TopicHash = Branded<string, "TopicHash">;

export type Topic<
  Name extends string = string,
  Params extends readonly unknown[] = readonly unknown[],
> = {
  readonly name: Name;
  readonly params: Params;
  readonly hash: TopicHash;
};

export type ListenerEvent<
  Name extends string = string,
  Params extends readonly unknown[] = readonly unknown[],
  Value = unknown,
> = {
  readonly topic: Topic<Name, Params>;
  readonly value: Value;
};

export type Listener<Event extends ListenerEvent = ListenerEvent> = (event: Event) => unknown;

export type StringKey<T> = Extract<keyof T, string>;

export type QueryMap<Queries extends object = Record<string, Query<unknown[], unknown>>> = {
  [Name in keyof Queries]: Query<unknown[], unknown>;
};

export type MutationMap<Mutations extends object = Record<string, Mutation<unknown[], unknown>>> = {
  [Name in keyof Mutations]: Mutation<unknown[], unknown>;
};

export type ListenerId = Branded<string, "ListenerId">;

export interface SyncEngineOptions<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  queries: Queries;
  mutations: Mutations;
}

export interface SyncEngineInterface<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Promise<Topic<Name, OperationParams<Queries[Name]>>>;
  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): ListenerId;
  unsubscribe(listenerId: ListenerId): boolean;
  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void;
}
