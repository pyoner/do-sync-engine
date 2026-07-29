import { Brand, Equal, Hash, Schema, SchemaTransformation, type Effect } from "effect";
import type {
  TopicBuildError,
  TopicValidationError,
  UnknownMutationError,
  UnknownQueryError,
} from "./helpers";

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

const equalParams = (
  left: unknown,
  right: unknown,
  leftSeen = new WeakMap<object, object>(),
  rightSeen = new WeakMap<object, object>(),
): boolean => {
  if (Object.is(left, right)) return true;
  if (typeof left !== "object" || left === null || typeof right !== "object" || right === null)
    return false;
  if (leftSeen.has(left) || rightSeen.has(right))
    return leftSeen.get(left) === right && rightSeen.get(right) === left;
  leftSeen.set(left, right);
  rightSeen.set(right, left);
  if (left.constructor !== right.constructor) return false;
  if (
    left instanceof Map ||
    left instanceof Set ||
    ArrayBuffer.isView(left) ||
    left instanceof ArrayBuffer
  )
    return Equal.equals(left, right);
  if (Array.isArray(left) || Array.isArray(right)) {
    return (
      Array.isArray(left) &&
      Array.isArray(right) &&
      left.length === right.length &&
      left.every((value, index) => equalParams(value, right[index], leftSeen, rightSeen))
    );
  }
  if (left instanceof Date || right instanceof Date)
    return left instanceof Date && right instanceof Date && left.getTime() === right.getTime();
  const leftKeys = Object.keys(left).sort();
  const rightKeys = Object.keys(right).sort();
  return (
    leftKeys.length === rightKeys.length &&
    leftKeys.every(
      (key, index) =>
        key === rightKeys[index] &&
        equalParams(
          (left as Record<string, unknown>)[key],
          (right as Record<string, unknown>)[key],
          leftSeen,
          rightSeen,
        ),
    )
  );
};

const hashParams = (value: unknown, seen = new WeakMap<object, number>()): number => {
  if (value === null) return Hash.string("null");
  if (typeof value !== "object") return Hash.combine(Hash.string(typeof value))(Hash.hash(value));
  const prior = seen.get(value);
  if (prior !== undefined) return prior;
  const seed = Hash.string(
    Array.isArray(value) ? "array" : value instanceof Date ? `date:${value.getTime()}` : "object",
  );
  seen.set(value, seed);
  if (value instanceof Date) return seed;
  const entries = Array.isArray(value)
    ? value.map((item, index) => [String(index), item] as const)
    : Object.entries(value).sort(([a], [b]) => a.localeCompare(b));
  return entries.reduce(
    (hash, [key, item]) =>
      Hash.combine(hash)(Hash.combine(Hash.string(key))(hashParams(item, seen))),
    seed,
  );
};

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
      that instanceof Topic && this.name === that.name && equalParams(this.params, that.params)
    );
  }

  [Hash.symbol](): number {
    return Hash.combine(Hash.string(this.name))(hashParams(this.params));
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
  ): Effect.Effect<
    Topic<Name, OperationParams<Queries[Name]>>,
    TopicBuildError | UnknownQueryError
  >;
  query<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
  ): Effect.Effect<
    OperationResult<Queries[Name]>,
    TopicValidationError | UnknownQueryError | OperationError<Queries[Name]>
  >;
  subscribe<Name extends StringKey<Queries>>(
    topic: Topic<Name, OperationParams<Queries[Name]>>,
    listener: Listener<
      ListenerEvent<Name, OperationParams<Queries[Name]>, OperationResult<Queries[Name]>>
    >,
  ): Effect.Effect<ListenerId, TopicValidationError | UnknownQueryError>;
  unsubscribe(listenerId: ListenerId): Effect.Effect<boolean>;
  sync<Name extends StringKey<Mutations>>(
    mutation: Name,
    params: OperationParams<Mutations[Name]>,
  ): Effect.Effect<
    void,
    | UnknownMutationError
    | OperationError<Mutations[Name]>
    | TopicValidationError
    | UnknownQueryError
    | OperationError<Queries[StringKey<Queries>]>
  >;
}
