import * as errore from "errore";
import type { Mutation, Query, Table } from "@do-sync-engine/core";
import { deleteTables } from "./delete.ts";
import { insertTables } from "./insert.ts";
import { selectTables } from "./select.ts";
import { updateTables } from "./update.ts";
import { operationOf } from "./rules.ts";
export type { Table } from "@do-sync-engine/core";

export class SqlAdapterError extends errore.createTaggedError({
  name: "SqlAdapterError",
  message: "$reason",
}) {}

export type SqlValue = string | number | boolean | null | bigint | Uint8Array;
export type SqlRow = Record<string, SqlValue>;
export interface MutationMetadata {
  rowsAffected: number;
  lastInsertRowid: number | bigint | null;
}
export interface SqlDatabase {
  query(sql: string, ...params: SqlValue[]): SqlRow[];
  execute(sql: string, ...params: SqlValue[]): MutationMetadata;
}
export type SqlParameter = string | number | null;
export type NodeSqliteDatabase = {
  prepare(sql: string): {
    all(...params: SqlParameter[]): unknown;
    run(...params: SqlParameter[]): unknown;
  };
};
export type CloudflareSqlStorage = { exec(sql: string, ...params: SqlParameter[]): unknown };
export type SqlAdapterDatabase = NodeSqliteDatabase | CloudflareSqlStorage;
export type SqlOperation = Query<SqlParameter[], unknown> | Mutation<SqlParameter[], unknown>;
export type SqlAdapter = (sql: string) => SqlOperation | SqlAdapterError;

export function createAdapter(db: SqlAdapterDatabase): SqlAdapter | SqlAdapterError {
  if (
    (!("prepare" in db) || typeof db.prepare !== "function") &&
    (!("exec" in db) || typeof db.exec !== "function")
  )
    return new SqlAdapterError({
      reason: "createAdapter() requires a Node SQLite database or Cloudflare SqlStorage",
    });
  return (sql) => {
    if (typeof sql !== "string" || sql.trim() === "")
      return new SqlAdapterError({ reason: "SQL adapter requires a SQL string" });
    const operation = operationOf(sql);
    const tables =
      operation === "select"
        ? selectTables(sql)
        : operation === "update"
          ? updateTables(sql)
          : operation === "insert"
            ? insertTables(sql)
            : operation === "delete"
              ? deleteTables(sql)
              : undefined;
    if (!tables || tables.length === 0)
      return new SqlAdapterError({ reason: "SQL adapter could not read SQL table metadata" });
    return {
      tables: new Set(tables as Table[]),
      run(...params: SqlParameter[]) {
        return errore.try({
          try: () => {
            if ("prepare" in db && typeof db.prepare === "function") {
              const statement = db.prepare(sql);
              return operation === "select" ? statement.all(...params) : statement.run(...params);
            }
            if ("exec" in db) return db.exec(sql, ...params);
            return new SqlAdapterError({ reason: "Unsupported SQL database" });
          },
          catch: (cause) => new SqlAdapterError({ reason: "SQL execution failed", cause }),
        });
      },
    };
  };
}
