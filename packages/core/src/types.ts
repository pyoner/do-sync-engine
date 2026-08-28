import type { HashMap } from "hashmap";

declare const brand: unique symbol;

export type Branded<
  Primitive extends string | number | boolean | bigint | symbol,
  Tag extends string,
> = Primitive & { readonly [brand]: Tag };

export type Table = Branded<string, "Table">;
export type BaseParams = ReadonlyArray<
  string | number | boolean | bigint | null | undefined | object
>;

type Operation<Params extends BaseParams = [], Result = unknown> = {
  tables: Set<Table>;
  run(...params: Params): Result;
};

export type Query<Params extends BaseParams = [], Result = unknown> = Operation<Params, Result>;

export type Mutation<Params extends BaseParams = [], Metadata = unknown> = Operation<
  Params,
  Metadata
>;

type ValidParams<Params> = [Params] extends [never]
  ? BaseParams
  : Params extends BaseParams
    ? Params
    : never;

export type OperationParams<OperationDef> = OperationDef extends {
  run(...params: infer Params): unknown;
}
  ? ValidParams<Params>
  : never;

export type OperationResult<OperationDef> = OperationDef extends {
  run(...params: never[]): infer Result;
}
  ? Result
  : never;

export type Topic<Name extends string = string, Params extends BaseParams = BaseParams> = {
  readonly name: Name;
  readonly params: Params;
};

export type ListenerEvent<
  Name extends string = string,
  Params extends BaseParams = BaseParams,
  Value = unknown,
> = {
  readonly topic: Topic<Name, Params>;
  readonly value: Value;
};

export type Listener<Event extends ListenerEvent = ListenerEvent> = (event: Event) => unknown;

export type StringKey<T> = Extract<keyof T, string>;

export type QueryMap<Queries extends object = Record<string, Query<BaseParams, unknown>>> = {
  [Name in keyof Queries]: Queries[Name] extends {
    run(...params: infer Params): infer Result;
  }
    ? Query<Params extends BaseParams ? Params : never, Result>
    : never;
};
export type MutationMap<Mutations extends object = Record<string, Mutation<BaseParams, unknown>>> =
  {
    [Name in keyof Mutations]: Mutations[Name] extends {
      run(...params: infer Params): infer Metadata;
    }
      ? Mutation<Params extends BaseParams ? Params : never, Metadata>
      : never;
  };

export type Registry<Queries extends QueryMap = QueryMap, Id = string> = HashMap<
  Topic<StringKey<Queries>, OperationParams<Queries[StringKey<Queries>]>>,
  Map<
    Id,
    Listener<
      ListenerEvent<
        StringKey<Queries>,
        OperationParams<Queries[StringKey<Queries>]>,
        OperationResult<Queries[StringKey<Queries>]>
      >
    >
  >
>;

export type Subscription<Id, Queries extends QueryMap> = {
  id: Id;
  topic: Topic<StringKey<Queries>, OperationParams<Queries[StringKey<Queries>]>>;
  listener: Listener<
    ListenerEvent<
      StringKey<Queries>,
      OperationParams<Queries[StringKey<Queries>]>,
      OperationResult<Queries[StringKey<Queries>]>
    >
  >;
};

export type SyncEngineOptions<
  Queries extends QueryMap = QueryMap,
  Mutations extends MutationMap = MutationMap,
  Id = string,
> = {
  queries: Queries;
  mutations: Mutations;
  createId?: () => Id;
};

export interface SyncEngineInterface<
  Queries extends QueryMap = QueryMap,
  Mutations extends MutationMap = MutationMap,
  Id = string,
> {
  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Topic<Name, OperationParams<Queries[Name]>> | Error;

  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): Id | Error;
  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
    id: Id,
  ): Id | Error;

  unsubscribe(id: Id): void;
  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    id: Id,
  ): void;
  unsubscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): void;

  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): void | Error;

  subscriptions(): IterableIterator<Readonly<Subscription<Id, Queries>>>;
}
