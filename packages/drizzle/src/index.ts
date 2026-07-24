import { toTables } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";
import type {
  AnySQLiteDeleteBase,
  AnySQLiteInsert,
  AnySQLiteSelect,
  AnySQLiteUpdate,
  SQLiteDeleteExecute,
  SQLiteInsertExecute,
  SQLiteSelectExecute,
  SQLiteUpdateExecute,
} from "drizzle-orm/sqlite-core";

type SyncBuilder<Builder> = Builder extends { _: { resultType: "sync" } } ? Builder : never;

type PreparedExecuteParams<Builder> = Builder extends { prepare(): infer Prepared }
  ? Prepared extends { execute: (...params: infer Params) => unknown }
    ? Params
    : never
  : never;

type PreparedInternals = {
  mode: "sync" | "async";
  queryMetadata?: { tables?: unknown };
  execute(...params: unknown[]): { sync(): unknown };
};

export function adapter<Builder extends AnySQLiteSelect>(
  builder: SyncBuilder<Builder>,
): Query<PreparedExecuteParams<Builder>, SQLiteSelectExecute<Builder>>;
export function adapter<Builder extends AnySQLiteInsert>(
  builder: SyncBuilder<Builder>,
): Mutation<PreparedExecuteParams<Builder>, SQLiteInsertExecute<Builder>>;
export function adapter<Builder extends AnySQLiteUpdate>(
  builder: SyncBuilder<Builder>,
): Mutation<PreparedExecuteParams<Builder>, SQLiteUpdateExecute<Builder>>;
export function adapter<Builder extends AnySQLiteDeleteBase>(
  builder: SyncBuilder<Builder>,
): Mutation<PreparedExecuteParams<Builder>, SQLiteDeleteExecute<Builder>>;
export function adapter(builder: { prepare(): unknown }) {
  const prepared = builder.prepare() as PreparedInternals;
  if (prepared.mode !== "sync") {
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
