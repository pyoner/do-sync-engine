import { Brand, Equal, Hash, Schema, SchemaTransformation, type Effect } from "effect";
import type { UnknownMutationError, UnknownQueryError } from "./helpers";

export type Branded<
  Primitive extends string | number | boolean | bigint | symbol,
  Tag extends string,
> = Brand.Branded<Primitive, Tag>;

export const TableSchema = Schema.String.pipe(Schema.brand("Table"));
export type Table = typeof TableSchema.Type;

type Operation<Params extends unknown[] = [], Result = unknown, Error = never> = {
  readonly tables: Set<Table>;
  readonly run: (...params: Params) => Effect.Effect<Result, Error>;
};

export type Query<Params extends unknown[] = [], Result = unknown, Error = never> = Operation<
  Params,
  Result,
  Error
>;

export type Mutation<Params extends unknown[] = [], Metadata = unknown, Error = never> = Operation<
  Params,
  Metadata,
  Error
>;

export type OperationParams<OperationDef> = OperationDef extends {
  run(...params: infer Params): unknown;
}
  ? Params
  : never;

export type OperationResult<OperationDef> = OperationDef extends {
  run(...params: never[]): Effect.Effect<infer Result, unknown>;
}
  ? Result
  : never;

export type OperationError<OperationDef> = OperationDef extends {
  readonly run: (...params: never[]) => Effect.Effect<unknown, infer Error>;
}
  ? Error
  : never;

// Topic params are treated as immutable after construction because Effect caches equality results.
export class Topic<
  Name extends string = string,
  Params extends readonly unknown[] = readonly unknown[],
>
  implements Equal.Equal
{
  readonly name: Name;
  readonly params: Params;

  constructor({ name, params }: { readonly name: Name; readonly params: Params }) {
    this.name = name;
    this.params = params;
  }

  [Equal.symbol](that: Equal.Equal): boolean {
    return (
      that instanceof Topic && this.name === that.name && Equal.equals(this.params, that.params)
    );
  }

  [Hash.symbol](): number {
    return Hash.combine(Hash.string(this.name))(Hash.hash(this.params));
  }
}

export const TopicSchema = Schema.Struct({
  name: Schema.String,
  params: Schema.Array(Schema.Unknown),
}).pipe(
  Schema.decodeTo(
    Schema.instanceOf(Topic),
    SchemaTransformation.transform({
      decode: (topic) => new Topic(topic),
      encode: ({ name, params }) => ({ name, params }),
    }),
  ),
);

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
  [Name in keyof Queries]: Queries[Name] extends {
    run(...params: infer Params): Effect.Effect<infer Result, infer Error>;
  }
    ? Query<Extract<Params, unknown[]>, Result, Error>
    : never;
};

export type MutationMap<Mutations extends object = Record<string, Mutation<unknown[], unknown>>> = {
  [Name in keyof Mutations]: Mutations[Name] extends {
    run(...params: infer Params): Effect.Effect<infer Metadata, infer Error>;
  }
    ? Mutation<Extract<Params, unknown[]>, Metadata, Error>
    : never;
};

export interface SyncEngineOptions<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  readonly queries: Queries;
  readonly mutations: Mutations;
}

export const ListenerIdSchema = Schema.String.check(Schema.isUUID()).pipe(
  Schema.brand("ListenerId"),
);
export type ListenerId = typeof ListenerIdSchema.Type;

export interface SyncEngineInterface<
  Queries extends QueryMap<Queries> = QueryMap,
  Mutations extends MutationMap<Mutations> = MutationMap,
> {
  createTopic<Name extends StringKey<Queries>>(
    name: Name,
    params: OperationParams<Queries[Name]>,
  ): Effect.Effect<Topic<Name, OperationParams<Queries[Name]>>, UnknownQueryError>;
  query<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): Effect.Effect<
    OperationResult<Queries[Name]>,
    UnknownQueryError | OperationError<Queries[Name]>
  >;
  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): Effect.Effect<ListenerId, UnknownQueryError>;
  unsubscribe(listenerId: ListenerId): Effect.Effect<boolean>;
  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Effect.Effect<
    void,
    | UnknownMutationError
    | OperationError<Mutations[Name]>
    | UnknownQueryError
    | OperationError<Queries[StringKey<Queries>]>
  >;
}
