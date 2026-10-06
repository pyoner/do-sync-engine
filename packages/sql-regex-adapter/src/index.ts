import { Effect, Option, Predicate, Schema } from "effect";
import { toTables } from "@do-sync-engine/core";
import type { Mutation, Query } from "@do-sync-engine/core";
import { deleteTables } from "./delete.ts";
import { insertTables } from "./insert.ts";
import { selectTables } from "./select.ts";
import { updateTables } from "./update.ts";
import { operationOf } from "./rules.ts";
import type { Operation } from "./rules.ts";

export class SqlAdapterError extends Schema.TaggedError<SqlAdapterError>()("SqlAdapterError", {
  message: Schema.String,
  cause: Schema.optional(Schema.Defect()),
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
export type SqlOperation =
  | Query<SqlParameter[], unknown, SqlAdapterError>
  | Mutation<SqlParameter[], unknown, SqlAdapterError>;
export type SqlAdapter = (sql: string) => Effect.Effect<SqlOperation, SqlAdapterError>;

const tablesOf: Record<Operation, (sql: string) => string[]> = {
  select: selectTables,
  update: updateTables,
  insert: insertTables,
  delete: deleteTables,
};

type Execute = (sql: string, isSelect: boolean, params: SqlParameter[]) => unknown;

function executorOf(db: SqlAdapterDatabase): Option.Option<Execute> {
  if ("prepare" in db && Predicate.isFunction(db.prepare)) {
    const { prepare } = db;
    return Option.some((sql, isSelect, params) => {
      const statement = prepare.call(db, sql);
      return isSelect ? statement.all(...params) : statement.run(...params);
    });
  }
  if ("exec" in db && Predicate.isFunction(db.exec)) {
    const { exec } = db;
    return Option.some((sql, _isSelect, params) => exec.call(db, sql, ...params));
  }
  return Option.none();
}

export const createAdapter = Effect.fnUntraced(function* (
  db: SqlAdapterDatabase,
): Effect.fn.Return<SqlAdapter, SqlAdapterError> {
  const execute = executorOf(db);
  if (Option.isNone(execute))
    return yield* new SqlAdapterError({
      message: "createAdapter() requires a Node SQLite database or Cloudflare SqlStorage",
    });
  return Effect.fnUntraced(function* (
    sql: string,
  ): Effect.fn.Return<SqlOperation, SqlAdapterError> {
    if (!Predicate.isString(sql) || sql.trim() === "")
      return yield* new SqlAdapterError({ message: "SQL adapter requires a SQL string" });
    const operation = operationOf(sql);
    const tables = Option.match(operation, {
      onNone: () => [],
      onSome: (op) => tablesOf[op](sql),
    });
    if (tables.length === 0)
      return yield* new SqlAdapterError({
        message: "SQL adapter could not read SQL table metadata",
      });
    const isSelect = Option.contains(operation, "select");
    return {
      tables: toTables(tables),
      run: (...params: SqlParameter[]) =>
        Effect.try({
          try: () => execute.value(sql, isSelect, params),
          catch: (cause) => new SqlAdapterError({ message: "SQL execution failed", cause }),
        }),
    };
  });
});
