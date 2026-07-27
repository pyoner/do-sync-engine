import { toTables } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";

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

export function adapter<Builder extends SelectBuilder>(
  builder: Builder,
): Query<PreparedExecuteParams<Builder>, ExecuteResult<Builder>>;
export function adapter<Builder extends MutationBuilder>(
  builder: Builder,
): Mutation<PreparedExecuteParams<Builder>, ExecuteResult<Builder>>;
export function adapter(builder: { prepare(): unknown }) {
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
      return prepared.execute(...params).sync();
    },
  };
}
