import { Effect, Schema } from "effect";
import { toTables } from "@do-sync-engine/core";
import type { BaseParams, Mutation, Query } from "@do-sync-engine/core";

type SQLiteBuilder = { _: { result: unknown }; prepare(): unknown };
type SelectBuilder = SQLiteBuilder & { _: { tableName: unknown } };
type MutationBuilder = SQLiteBuilder & { _: { table: unknown } };
type ExecuteResult<Builder extends SQLiteBuilder> = Builder["_"]["result"];
type PreparedExecuteParams<Builder extends SQLiteBuilder> =
  ReturnType<Builder["prepare"]> extends { execute: (...params: infer Params) => unknown }
    ? Params
    : never;
type PreparedInternals = {
  resultKind: "sync" | "async";
  queryMetadata?: { tables?: unknown };
  execute(...params: BaseParams): { sync(): unknown };
};

export class DrizzleAdapterError extends Schema.TaggedError<DrizzleAdapterError>()(
  "DrizzleAdapterError",
  {
    message: Schema.String,
    cause: Schema.optional(Schema.Defect()),
  },
) {}

type AdapterResult<Operation> = Effect.Effect<Operation, DrizzleAdapterError>;

const Tables = Schema.Array(Schema.String);

const make = Effect.fnUntraced(function* (builder: {
  prepare(): unknown;
}): Effect.fn.Return<Query<BaseParams, unknown, DrizzleAdapterError>, DrizzleAdapterError> {
  const prepared = yield* Effect.try({
    try: () => builder.prepare() as PreparedInternals,
    catch: (cause) =>
      new DrizzleAdapterError({
        message: "adapter() could not prepare Drizzle SQLite builder",
        cause,
      }),
  });
  if (prepared.resultKind !== "sync")
    return yield* new DrizzleAdapterError({
      message: "adapter() requires a synchronous Drizzle SQLite builder",
    });
  const tables = yield* Schema.decodeUnknownEffect(Tables)(prepared.queryMetadata?.tables).pipe(
    Effect.mapError(
      (cause) =>
        new DrizzleAdapterError({
          message: "adapter() could not read Drizzle table metadata",
          cause,
        }),
    ),
  );
  return {
    tables: toTables(tables),
    run: (...params: BaseParams) =>
      Effect.try({
        try: () => prepared.execute(...params).sync(),
        catch: (cause) => new DrizzleAdapterError({ message: "Drizzle execution failed", cause }),
      }),
  };
});

export function adapter<Builder extends SelectBuilder>(
  builder: Builder,
): AdapterResult<
  Query<PreparedExecuteParams<Builder> & BaseParams, ExecuteResult<Builder>, DrizzleAdapterError>
>;
export function adapter<Builder extends MutationBuilder>(
  builder: Builder,
): AdapterResult<
  Mutation<PreparedExecuteParams<Builder> & BaseParams, ExecuteResult<Builder>, DrizzleAdapterError>
>;
export function adapter(builder: { prepare(): unknown }) {
  return make(builder);
}
