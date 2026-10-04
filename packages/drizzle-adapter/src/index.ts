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
  execute(...params: unknown[]): { sync(): unknown };
};

export class DrizzleAdapterError extends Schema.TaggedError<DrizzleAdapterError>()(
  "DrizzleAdapterError",
  {
    reason: Schema.String,
    cause: Schema.optional(Schema.Unknown),
  },
) {
  override get message(): string {
    return this.reason;
  }
}

type AdapterResult<Operation> = Effect.Effect<Operation, DrizzleAdapterError>;

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
  return Effect.gen(function* () {
    const prepared = yield* Effect.try({
      try: () => builder.prepare() as PreparedInternals,
      catch: (cause) =>
        new DrizzleAdapterError({
          reason: "adapter() could not prepare Drizzle SQLite builder",
          cause,
        }),
    });
    if (prepared.resultKind !== "sync")
      return yield* new DrizzleAdapterError({
        reason: "adapter() requires a synchronous Drizzle SQLite builder",
      });
    const tables = prepared.queryMetadata?.tables;
    if (
      !Array.isArray(tables) ||
      !tables.every((table): table is string => typeof table === "string")
    )
      return yield* new DrizzleAdapterError({
        reason: "adapter() could not read Drizzle table metadata",
      });
    return {
      tables: toTables(tables),
      run: (...params: unknown[]) =>
        Effect.try({
          try: () => prepared.execute(...params).sync(),
          catch: (cause) => new DrizzleAdapterError({ reason: "Drizzle execution failed", cause }),
        }),
    };
  });
}
