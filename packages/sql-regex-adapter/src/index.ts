import { Effect, Schema } from "effect";
import type { Mutation, Query, Table } from "@do-sync-engine/core";
import { deleteTables } from "./delete.ts";
import { insertTables } from "./insert.ts";
import { selectTables } from "./select.ts";
import { updateTables } from "./update.ts";
import { operationOf } from "./rules.ts";
export type { Table } from "@do-sync-engine/core";

export class SqlAdapterError extends Schema.TaggedError<SqlAdapterError>()("SqlAdapterError", {
  reason: Schema.String,
  cause: Schema.optional(Schema.Unknown),
}) {
  override get message(): string {
    return this.reason;
  }
}

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
export type SqlOperation =
  | Query<SqlParameter[], unknown, SqlAdapterError>
  | Mutation<SqlParameter[], unknown, SqlAdapterError>;
export type SqlAdapter = (sql: string) => Effect.Effect<SqlOperation, SqlAdapterError>;

export function createAdapter(db: SqlAdapterDatabase): Effect.Effect<SqlAdapter, SqlAdapterError> {
  if (
    (!("prepare" in db) || typeof db.prepare !== "function") &&
    (!("exec" in db) || typeof db.exec !== "function")
  )
    return Effect.fail(
      new SqlAdapterError({
        reason: "createAdapter() requires a Node SQLite database or Cloudflare SqlStorage",
      }),
    );
  const execute = (sql: string, isSelect: boolean, params: SqlParameter[]): unknown => {
    if ("prepare" in db && typeof db.prepare === "function") {
      const statement = db.prepare(sql);
      return isSelect ? statement.all(...params) : statement.run(...params);
    }
    return (db as CloudflareSqlStorage).exec(sql, ...params);
  };
  return Effect.succeed((sql) => {
    if (typeof sql !== "string" || sql.trim() === "")
      return Effect.fail(new SqlAdapterError({ reason: "SQL adapter requires a SQL string" }));
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
      return Effect.fail(
        new SqlAdapterError({ reason: "SQL adapter could not read SQL table metadata" }),
      );
    return Effect.succeed({
      tables: new Set(tables as Table[]),
      run: (...params: SqlParameter[]) =>
        Effect.try({
          try: () => execute(sql, operation === "select", params),
          catch: (cause) => new SqlAdapterError({ reason: "SQL execution failed", cause }),
        }),
    });
  });
}
