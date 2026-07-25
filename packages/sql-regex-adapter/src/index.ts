import type { Mutation, Query, Table } from "@do-sync-engine/core";
import { deleteTables } from "./delete.ts";
import { insertTables } from "./insert.ts";
import { selectTables } from "./select.ts";
import { updateTables } from "./update.ts";

export type NodeSqliteDatabase = {
  prepare(sql: string): {
    all(...params: never[]): unknown;
    run(...params: never[]): unknown;
  };
};

export type CloudflareSqlStorage = {
  exec(sql: string, ...params: never[]): unknown;
};

export type SqlDatabase = NodeSqliteDatabase | CloudflareSqlStorage;
export type SqlOperation = Query<unknown[], unknown> | Mutation<unknown[], unknown>;
export type SqlAdapter = (sql: string) => SqlOperation;

export function createAdapter(db: SqlDatabase): SqlAdapter {
  if (
    (!("prepare" in db) || typeof db.prepare !== "function") &&
    (!("exec" in db) || typeof db.exec !== "function")
  ) {
    throw new TypeError("createAdapter() requires a Node SQLite database or Cloudflare SqlStorage");
  }

  return (sql: string) => {
    if (typeof sql !== "string" || sql.trim() === "")
      throw new TypeError("SQL adapter requires a SQL string");
    const operation = sql
      .trimStart()
      .match(/^(?:with\b[\s\S]*?\)\s*)?(select|update|insert|delete)\b/i)?.[1]
      ?.toLowerCase();
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
      throw new TypeError("SQL adapter could not read SQL table metadata");

    return {
      tables: new Set(tables as Table[]),
      run(...params: unknown[]) {
        if ("prepare" in db) {
          const statement = db.prepare(sql);
          return operation === "select"
            ? statement.all(...(params as never[]))
            : statement.run(...(params as never[]));
        }
        return db.exec(sql, ...(params as never[]));
      },
    };
  };
}
