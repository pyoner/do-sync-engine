import { Brand } from "effect";
import type { Effect } from "effect";
import type { ReadonlyDeep } from "type-fest";
import type { MissingSubscriptionIdError, UnknownMutationError, UnknownQueryError } from "./errors";

type Any = any; // oxlint-disable-line

export type StringKey<T> = Extract<keyof T, string>;

export type Table = Brand.Branded<string, "Table">;
export const Table = Brand.nominal<Table>();
export type BaseParams = ReadonlyArray<
  string | number | boolean | bigint | null | undefined | object
>;

type Operation<Params extends BaseParams, A, E, R> = {
  readonly tables: ReadonlySet<Table>;
  run(...params: Params): Effect.Effect<A, E, R>;
};

export type Query<Params extends BaseParams, Result, E = never, R = never> = Operation<
  Params,
  Result,
  E,
  R
>;

export type Mutation<Params extends BaseParams, Metadata, E = never, R = never> = Operation<
  Params,
  Metadata,
  E,
  R
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

type OpEffect<OperationDef> = OperationDef extends { run(...params: unknown[]): infer Eff }
  ? Eff
  : never;

export type OpResult<OperationDef> = Effect.Success<OpEffect<OperationDef>>;
export type OpError<OperationDef> = Effect.Error<OpEffect<OperationDef>>;
export type OpServices<OperationDef> = Effect.Services<OpEffect<OperationDef>>;

export type Topic<Name extends string = string, Params extends BaseParams = BaseParams> = {
  readonly name: Name;
  readonly params: Params;
};

export type Topics<Q extends QueryRecord> = {
  [Name in StringKey<Q>]: Topic<Name, OpParams<Q[Name]>>;
}[StringKey<Q>];

export type ListenerEvent<T extends Topic = Topic, V = Any> = {
  readonly topic: T;
  readonly value: ReadonlyDeep<V>;
};

export type Listener<
  E extends ListenerEvent = ListenerEvent,
  Properties extends object = object,
> = ((event: E) => void) & Properties;

export type ListenerEvents<Q extends QueryRecord> = {
  [Name in StringKey<Q>]: ListenerEvent<Topic<Name, OpParams<Q[Name]>>, OpResult<Q[Name]>>;
}[StringKey<Q>];

export type QueryRecord = Record<string, Query<BaseParams, Any, Any, Any>>;
export type MutationRecord = Record<string, Mutation<BaseParams, Any, Any, Any>>;

export type SyncEngineServices<Q extends QueryRecord, M extends MutationRecord> =
  | OpServices<Q[StringKey<Q>]>
  | OpServices<M[StringKey<M>]>;

export type Subscription<
  Id,
  T extends Topic<string, BaseParams>,
  L extends Listener<ListenerEvent<T, Any>>,
> = {
  readonly id: Id;
  readonly topic: T;
  readonly listener: L;
};

export type Subscriptions<Id, Q extends QueryRecord, Properties extends object = object> = {
  [Name in StringKey<Q>]: Subscription<
    Id,
    Topic<Name, OpParams<Q[Name]>>,
    Listener<ListenerEvent<Topic<Name, OpParams<Q[Name]>>, OpResult<Q[Name]>>, Properties>
  >;
}[StringKey<Q>];

export type SyncEngineOptions<
  Id,
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
  ListenerProperties extends object = object,
> = {
  readonly queries: Queries;
  readonly mutations: Mutations;
  readonly createId?: <Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ) => Id;
};

export interface SyncEngine<
  Id,
  Queries extends QueryRecord = QueryRecord,
  Mutations extends MutationRecord = MutationRecord,
  ListenerProperties extends object = object,
> {
  createTopic<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    name: Name,
    params: Params,
  ): Effect.Effect<Topic<Name, Params>, UnknownQueryError>;

  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): Effect.Effect<Id, UnknownQueryError | MissingSubscriptionIdError | OpError<Queries[Name]>>;
  subscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
    id: Id,
  ): Effect.Effect<Id, UnknownQueryError | OpError<Queries[Name]>>;

  unsubscribe(id: Id): Effect.Effect<void>;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    id: Id,
  ): Effect.Effect<void>;
  unsubscribe<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
    listener: Listener<
      ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>,
      ListenerProperties
    >,
  ): Effect.Effect<void>;

  sync<Name extends StringKey<Mutations>, Params extends OpParams<Mutations[Name]>>(
    mutation: Name,
    params: Params,
  ): Effect.Effect<
    void,
    | UnknownMutationError
    | UnknownQueryError
    | OpError<Mutations[Name]>
    | OpError<Queries[StringKey<Queries>]>
  >;

  subscriptions(): Effect.Effect<ReadonlyArray<Subscriptions<Id, Queries, ListenerProperties>>>;
  subscriptions<Name extends StringKey<Queries>, Params extends OpParams<Queries[Name]>>(
    topic: Topic<Name, Params>,
  ): Effect.Effect<
    ReadonlyArray<
      Subscription<
        Id,
        Topic<Name, Params>,
        Listener<ListenerEvent<Topic<Name, Params>, OpResult<Queries[Name]>>, ListenerProperties>
      >
    >
  >;
}
