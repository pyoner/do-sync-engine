import type { HashMap } from "hashmap";

export type StringKey<T> = Extract<keyof T, string>;

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

export type OpParams<OperationDef> = OperationDef extends {
  run(...params: infer Params): unknown;
}
  ? ValidParams<Params>
  : never;

export type OpResult<OperationDef> = OperationDef extends {
  run(...params: never[]): infer Result;
}
  ? Result
  : never;

export type Topic<Name extends string = string, Params extends BaseParams = BaseParams> = {
  readonly name: Name;
  readonly params: Params;
};

export type Topics<Q extends QueryRecord> = {
  [Name in StringKey<Q>]: Topic<Name, OpParams<Q[Name]>>;
}[StringKey<Q>];

export type ListenerEvent<T extends Topic = Topic, V = unknown> = {
  readonly topic: T;
  readonly value: V;
};

export type Listener<
  E extends ListenerEvent = ListenerEvent,
  Properties extends object = object,
> = ((event: E) => void) & Properties;

export type ListenerEvents<Q extends QueryRecord> = {
  [Name in StringKey<Q>]: ListenerEvent<Topic<Name, OpParams<Q[Name]>>, OpResult<Q[Name]>>;
}[StringKey<Q>];

export type QueryRecord = Record<string, Query<never[]>>;
export type MutationRecord = Record<string, Mutation<never[]>>;

export type Registry<
  Q extends QueryRecord = QueryRecord,
  Id = string,
  L extends Listener<ListenerEvents<Q>> = Listener<ListenerEvents<Q>>,
> = HashMap<Topics<Q>, Map<Id, L>>;

export type Subscription<
  Id,
  T extends Topic,
  V = unknown,
  L extends Listener<ListenerEvent<T, V>> = Listener<ListenerEvent<T, V>>,
> = {
  id: Id;
  topic: T;
  listener: L;
};

export type Subscriptions<Id, Q extends QueryRecord, Properties extends object = object> = {
  [Name in StringKey<Q>]: Subscription<
    Id,
    Topic<Name, OpParams<Q[Name]>>,
    OpResult<Q[Name]>,
    Listener<ListenerEvent<Topic<Name, OpParams<Q[Name]>>, OpResult<Q[Name]>>, Properties>
  >;
}[StringKey<Q>];

export type SyncEngineOptions<
  Id,
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
> = {
  queries: Queries;
  mutations: Mutations;
  createId?: () => Id;
};

export interface SyncEngineInterface<
  Id,
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
  ListenerProperties extends object = object,
> {
  createTopic<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    name: Name,
    params: Params,
  ): Topic<Name, Params> | Error;

  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): Id | Error;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
    id: Id,
  ): Id | Error;

  has<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): boolean;

  unsubscribe(id: Id): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    id: Id,
  ): void;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): void;

  sync<Name extends StringKey<Mutations>, Params extends OpParams<Mutations[Name]>>(
    mutation: Name,
    params: Params,
  ): void | Error;

  subscriptions(): IterableIterator<Readonly<Subscriptions<Id, Queries, ListenerProperties>>>;
  subscriptions<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): IterableIterator<
    Readonly<
      Subscription<
        Id,
        Topic<Name, Params>,
        OpResult<Queries[Name]>,
        Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>, ListenerProperties>
      >
    >
  >;
}
