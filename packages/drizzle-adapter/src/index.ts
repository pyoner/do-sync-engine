import { Effect, Schema } from "effect";
import { toTables, type Mutation, type Query } from "@do-sync-engine/core";

type SQLiteBuilder = {
  _: { result: unknown };
  prepare(): unknown;
};
type SelectBuilder = SQLiteBuilder & { _: { tableName: unknown } };
type MutationBuilder = SQLiteBuilder & { _: { table: unknown } };
type ExecuteResult<Builder extends SQLiteBuilder> = Builder["_"]["result"];
type PreparedExecuteParams<Builder extends SQLiteBuilder> =
  ReturnType<Builder["prepare"]> extends {
    execute: (...params: infer Params) => unknown;
  }
    ? Params
    : never;

type PreparedInternals = {
  resultKind: "sync" | "async";
  queryMetadata?: { tables?: unknown };
  execute(...params: unknown[]): { sync(): unknown };
};

export class DrizzleAdapterError extends Schema.TaggedErrorClass<DrizzleAdapterError>()(
  "DrizzleAdapterError",
  {
    cause: Schema.Unknown,
    operation: Schema.String,
  },
) {}

export function adapter<Builder extends SelectBuilder>(
  builder: Builder,
): Effect.Effect<
  Query<PreparedExecuteParams<Builder>, ExecuteResult<Builder>, DrizzleAdapterError>,
  DrizzleAdapterError
>;
export function adapter<Builder extends MutationBuilder>(
  builder: Builder,
): Effect.Effect<
  Mutation<PreparedExecuteParams<Builder>, ExecuteResult<Builder>, DrizzleAdapterError>,
  DrizzleAdapterError
>;
export function adapter(builder: {
  prepare(): unknown;
}): Effect.Effect<
  | Query<unknown[], unknown, DrizzleAdapterError>
  | Mutation<unknown[], unknown, DrizzleAdapterError>,
  DrizzleAdapterError
> {
  return Effect.try({
    try: () => {
      const prepared = builder.prepare() as PreparedInternals;
      if (prepared.resultKind !== "sync") {
        throw new TypeError("adapter() requires a synchronous Drizzle SQLite builder");
      }
      const tables = prepared.queryMetadata?.tables;
      if (
        !Array.isArray(tables) ||
        !tables.every((table): table is string => typeof table === "string")
      ) {
        throw new TypeError("adapter() could not read Drizzle table metadata");
      }
      return {
        tables: toTables(tables),
        run(...params: unknown[]) {
          return Effect.try({
            try: () => prepared.execute(...params).sync(),
            catch: (cause) => DrizzleAdapterError.make({ cause, operation: "run" }),
          });
        },
      };
    },
    catch: (cause) => DrizzleAdapterError.make({ cause, operation: "adapter" }),
  });
}
