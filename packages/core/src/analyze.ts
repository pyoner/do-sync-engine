import { Parser } from "node-sql-parser";
import type { Selector, Mutator } from "./types";

const parser = new Parser();
const OPT = { database: "SQLite" } as const;

export function analyzeSql(name: string, sql: string): Selector | Mutator {
  const { tableList } = parser.parse(sql, OPT);

  const readTables = new Set<string>();
  const writtenTables = new Set<string>();
  let operation: "select" | "insert" | "update" | "delete" = "select";

  for (const entry of tableList) {
    const [action, , table] = entry.split("::");
    if (action === "select") {
      readTables.add(table);
    } else if (action === "insert" || action === "replace") {
      writtenTables.add(table);
      operation = "insert";
    } else if (action === "update") {
      writtenTables.add(table);
      operation = "update";
    } else if (action === "delete") {
      writtenTables.add(table);
      operation = "delete";
    }
  }

  const tables = operation === "select" ? readTables : writtenTables;
  return { name, sql, operation, tables };
}
