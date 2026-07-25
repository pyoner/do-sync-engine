import type { Mutation, Query, Table } from "@do-sync-engine/core";
import { deleteTables } from "./delete.ts";
import { insertTables } from "./insert.ts";
import { selectTables } from "./select.ts";
import { updateTables } from "./update.ts";
import { operationOf } from "./rules.ts";

export type SqlParameter = string | number | null;

export type NodeSqliteDatabase = {
  prepare(sql: string): {
    all(...params: SqlParameter[]): unknown;
    run(...params: SqlParameter[]): unknown;
  };
};

export type CloudflareSqlStorage = {
  exec(sql: string, ...params: SqlParameter[]): unknown;
};

export type SqlDatabase = NodeSqliteDatabase | CloudflareSqlStorage;
export type SqlOperation = Query<SqlParameter[], unknown> | Mutation<SqlParameter[], unknown>;
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
      throw new TypeError("SQL adapter could not read SQL table metadata");

    return {
      tables: new Set(tables as Table[]),
      run(...params: SqlParameter[]) {
        if ("prepare" in db && typeof db.prepare === "function") {
          const statement = db.prepare(sql);
          return operation === "select" ? statement.all(...params) : statement.run(...params);
        }
        if ("exec" in db) return db.exec(sql, ...params);
        throw new TypeError("Unsupported SQL database");
      },
    };
  };
}
