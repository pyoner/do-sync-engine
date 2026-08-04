import { Effect, Schema } from "effect";
import type { Mutation, Query, Table } from "@do-sync-engine/core";
import { deleteTables } from "./delete";
import { insertTables } from "./insert";
import { selectTables } from "./select";
import { updateTables } from "./update";
import { operationOf } from "./rules";
export type { Table } from "@do-sync-engine/core";

export class SqlAdapterError extends Schema.TaggedErrorClass<SqlAdapterError>()("SqlAdapterError", {
  cause: Schema.Unknown,
  operation: Schema.String,
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

export type CloudflareSqlStorage = {
  exec(sql: string, ...params: SqlParameter[]): unknown;
};

export type SqlAdapterDatabase = NodeSqliteDatabase | CloudflareSqlStorage;
export type SqlOperation =
  | Query<SqlParameter[], unknown, SqlAdapterError>
  | Mutation<SqlParameter[], unknown, SqlAdapterError>;
export type SqlAdapter = (sql: string) => Effect.Effect<SqlOperation, SqlAdapterError>;

export function createAdapter(db: SqlAdapterDatabase): Effect.Effect<SqlAdapter, SqlAdapterError> {
  const supported =
    typeof db === "object" &&
    db !== null &&
    (("prepare" in db && typeof db.prepare === "function") ||
      ("exec" in db && typeof db.exec === "function"));
  if (!supported) {
    return Effect.fail(
      SqlAdapterError.make({
        cause: new TypeError(
          "createAdapter() requires a Node SQLite database or Cloudflare SqlStorage",
        ),
        operation: "adapter",
      }),
    );
  }

  return Effect.succeed((sql: string) => {
    if (typeof sql !== "string" || sql.trim() === "") {
      return Effect.fail(
        SqlAdapterError.make({
          cause: new TypeError("SQL adapter requires a SQL string"),
          operation: "adapter",
        }),
      );
    }

    const parsed = Effect.try({
      try: () => {
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
        return { operation, tables };
      },
      catch: (cause) => SqlAdapterError.make({ cause, operation: "adapter" }),
    });

    return parsed.pipe(
      Effect.flatMap(({ operation, tables }) => {
        if (!tables || tables.length === 0) {
          return Effect.fail(
            SqlAdapterError.make({
              cause: new TypeError("SQL adapter could not read SQL table metadata"),
              operation: "adapter",
            }),
          );
        }

        return Effect.succeed({
          tables: new Set(tables as Table[]),
          run(...params: SqlParameter[]) {
            if ("prepare" in db && typeof db.prepare === "function") {
              return Effect.try({
                try: () => {
                  const statement = db.prepare(sql);
                  return operation === "select"
                    ? statement.all(...params)
                    : statement.run(...params);
                },
                catch: (cause) => SqlAdapterError.make({ cause, operation: "run" }),
              });
            }
            if ("exec" in db && typeof db.exec === "function") {
              return Effect.try({
                try: () => db.exec(sql, ...params),
                catch: (cause) => SqlAdapterError.make({ cause, operation: "run" }),
              });
            }
            return Effect.fail(
              SqlAdapterError.make({
                cause: new TypeError("Unsupported SQL database"),
                operation: "run",
              }),
            );
          },
        });
      }),
    );
  });
}
