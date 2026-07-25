import type { Mutation, Query, Table } from "@do-sync-engine/core";
import { deleteTables } from "./delete.ts";
import { insertTables } from "./insert.ts";
import { selectTables } from "./select.ts";
import { updateTables } from "./update.ts";

export type SqlOperation =
  | Query<[...params: unknown[]], unknown>
  | Mutation<[...params: unknown[]], unknown>;

export function adapter(sql: string): SqlOperation {
  if (typeof sql !== "string" || sql.trim() === "")
    throw new TypeError("adapter() requires a SQL string");
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
    throw new TypeError("adapter() could not read SQL table metadata");
  return {
    tables: new Set(tables as Table[]),
    run(..._params: unknown[]) {
      throw new Error("SQL regex adapter only provides query metadata");
    },
  };
}
