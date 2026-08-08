import * as errore from "errore";
import { toTables } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";

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

export class DrizzleAdapterError extends errore.createTaggedError({
  name: "DrizzleAdapterError",
  message: "$reason",
}) {}

export function adapter<Builder extends SelectBuilder>(
  builder: Builder,
):
  | Query<PreparedExecuteParams<Builder>, ExecuteResult<Builder> | DrizzleAdapterError>
  | DrizzleAdapterError;
export function adapter<Builder extends MutationBuilder>(
  builder: Builder,
):
  | Mutation<PreparedExecuteParams<Builder>, ExecuteResult<Builder> | DrizzleAdapterError>
  | DrizzleAdapterError;
export function adapter(builder: { prepare(): unknown }) {
  const prepared = errore.try({
    try: () => builder.prepare() as PreparedInternals,
    catch: (cause) =>
      new DrizzleAdapterError({
        reason: "adapter() could not prepare Drizzle SQLite builder",
        cause,
      }),
  });
  if (prepared instanceof Error) return prepared;
  if (prepared.resultKind !== "sync")
    return new DrizzleAdapterError({
      reason: "adapter() requires a synchronous Drizzle SQLite builder",
    });
  const tables = prepared.queryMetadata?.tables;
  if (
    !Array.isArray(tables) ||
    !tables.every((table): table is string => typeof table === "string")
  )
    return new DrizzleAdapterError({ reason: "adapter() could not read Drizzle table metadata" });
  return {
    tables: toTables(tables),
    run(...params: unknown[]) {
      return errore.try({
        try: () => prepared.execute(...params).sync(),
        catch: (cause) => new DrizzleAdapterError({ reason: "Drizzle execution failed", cause }),
      });
    },
  };
}
